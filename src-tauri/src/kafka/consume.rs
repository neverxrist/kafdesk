use crate::domain::{
    initial_finished_partitions, matches_decoded_message, matches_raw_pre_filter,
    plan_consume_window, sort_messages_by_time, BrokerMessageFilter, ConsumeWindowInput,
    PartitionTimestampSeek, PartitionWatermark,
};
use crate::types::*;
use super::decode::headers_from_message;
use super::session::{KafkaSession, BConsumer};
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::HashMap;
use std::time::{Duration, Instant};

impl KafkaSession {
    pub async fn consume(&self, request: ConsumeRequest) -> Result<Vec<KafkaMessageView>, String> {
        self.ensure_admin()?;
        let topic_info = self.get_topic_offsets(&request.topic)?;
        let topic_offsets: Vec<PartitionWatermark> = topic_info
            .partitions
            .iter()
            .map(|p| PartitionWatermark {
                partition: p.partition_id,
                low: p
                    .offset_low
                    .as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                high: p
                    .offset_high
                    .as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
            })
            .collect();

        let start_from = request
            .start_from
            .clone()
            .unwrap_or_else(|| {
                if request.from_beginning == Some(false) {
                    "end".into()
                } else {
                    "beginning".into()
                }
            });

        let has_content = [
            request.search.as_deref(),
            request.search_key.as_deref(),
            request.search_header_key.as_deref(),
            request.search_header_value.as_deref(),
        ]
        .iter()
        .any(|s| s.map(|x| !x.trim().is_empty()).unwrap_or(false));
        let has_time = request.time_from.is_some() || request.time_to.is_some();

        let mut offsets_by_timestamp = None;
        let mut time_from_seek_failed = false;
        if let Some(tf) = request.time_from {
            match self.offsets_for_times(&request.topic, tf, &topic_offsets) {
                Ok(v) => offsets_by_timestamp = Some(v),
                Err(_) => time_from_seek_failed = true,
            }
        }

        let plan = plan_consume_window(ConsumeWindowInput {
            start_from: start_from.clone(),
            start_offset: request.start_offset.clone(),
            limit: request.limit,
            partitions: request.partitions.clone(),
            offsets_by_timestamp,
            time_from_seek_failed,
            has_content_filter: has_content,
            has_time_filter: has_time,
            topic_offsets: topic_offsets.clone(),
        });
        if plan.empty {
            return Ok(vec![]);
        }

        let finished = initial_finished_partitions(
            &plan.target_partitions,
            &topic_offsets,
            &plan.planned_start,
        );
        if finished.len() >= plan.target_partitions.len() {
            return Ok(vec![]);
        }

        let mut cfg = self.base_config.clone();
        cfg.set(
            "group.id",
            format!("kafdesk-ui-{}", uuid::Uuid::new_v4()),
        );
        cfg.set("enable.auto.commit", "false");
        cfg.set("auto.offset.reset", "earliest");
        cfg.set("enable.partition.eof", "true");
        let consumer: BConsumer =
            cfg.create().map_err(|e| e.to_string())?;

        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in &plan.seek_plan {
            tpl.add_partition_offset(&request.topic, *partition, Offset::Offset(*offset))
                .map_err(|e| e.to_string())?;
        }
        for &p in &plan.target_partitions {
            if plan.seek_plan.iter().any(|(pp, _)| *pp == p) {
                continue;
            }
            if let Some(o) = topic_offsets.iter().find(|x| x.partition == p) {
                let _ = tpl.add_partition_offset(&request.topic, p, Offset::Offset(o.high));
            }
        }
        consumer.assign(&tpl).map_err(|e| e.to_string())?;

        let filter = BrokerMessageFilter {
            search: request.search.clone(),
            search_key: request.search_key.clone(),
            header_key: request.search_header_key.clone(),
            header_value: request.search_header_value.clone(),
            time_from: request.time_from,
            time_to: request.time_to,
        };

        let high_by: HashMap<i32, i64> = topic_offsets.iter().map(|o| (o.partition, o.high)).collect();
        let mut finished_partitions = finished;
        let mut messages = Vec::new();
        let mut taken_by_part: HashMap<i32, i32> = HashMap::new();
        let deadline = Instant::now() + Duration::from_millis(plan.wait_ms);
        let mut last_msg = Instant::now();

        while Instant::now() < deadline {
            if finished_partitions.len() >= plan.target_partitions.len() {
                break;
            }
            if last_msg.elapsed() > Duration::from_millis(plan.idle_ms)
                && !messages.is_empty()
            {
                // idle after activity
                if !plan.has_filter || start_from != "end" {
                    break;
                }
            }

            match consumer.poll(Duration::from_millis(200)) {
                None => continue,
                Some(Err(e)) => {
                    // Partition EOF is expected when catching up; other errors stop the window.
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("partition eof")
                        || msg.to_lowercase().contains("end of partition")
                    {
                        continue;
                    }
                    return Err(format!("Consume error: {msg}"));
                }
                Some(Ok(msg)) => {
                    last_msg = Instant::now();
                    let partition = msg.partition();
                    if let Some(ref ps) = request.partitions {
                        if !ps.contains(&partition) {
                            continue;
                        }
                    }
                    let offset = msg.offset();
                    let high = *high_by.get(&partition).unwrap_or(&0);
                    let start = plan.planned_start.get(&partition).copied();

                    if let Some(s) = start {
                        if offset < s {
                            if offset >= high - 1 {
                                finished_partitions.insert(partition);
                            }
                            continue;
                        }
                    }
                    if offset >= high - 1 {
                        finished_partitions.insert(partition);
                    }

                    let key = msg.key();
                    let headers = headers_from_message(&msg);
                    let ts = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => {
                            ms
                        }
                        _ => 0,
                    };

                    if !matches_raw_pre_filter(key, &ts.to_string(), &headers, &filter) {
                        continue;
                    }

                    let view = self
                        .decode_message_view(
                            msg.topic(),
                            partition,
                            offset,
                            ts,
                            key,
                            msg.payload(),
                            headers,
                        )
                        .await;

                    if !matches_decoded_message(&view, &filter) {
                        continue;
                    }

                    messages.push(view);
                    let taken = taken_by_part.entry(partition).or_insert(0);
                    *taken += 1;

                    if !plan.has_filter && start_from == "beginning" && *taken >= plan.limit {
                        finished_partitions.insert(partition);
                    }
                    if plan.has_filter && start_from != "end" && messages.len() as i32 >= plan.limit
                    {
                        break;
                    }
                    if plan.has_filter
                        && start_from == "end"
                        && messages.len() as i32 >= plan.limit
                        && finished_partitions.len() >= plan.target_partitions.len()
                    {
                        break;
                    }
                }
            }
        }

        let mut sorted = sort_messages_by_time(messages, plan.newest_first);
        sorted.truncate(plan.limit as usize);
        Ok(sorted)
    }
    fn offsets_for_times(
        &self,
        topic: &str,
        time_ms: i64,
        topic_offsets: &[PartitionWatermark],
    ) -> Result<Vec<PartitionTimestampSeek>, String> {
        let consumer = self.metadata_consumer()?;
        let mut tpl = TopicPartitionList::new();
        for o in topic_offsets {
            tpl.add_partition_offset(topic, o.partition, Offset::Offset(time_ms))
                .map_err(|e| e.to_string())?;
        }
        // rdkafka offsets_for_times uses timestamp as offset magic
        let mut tpl2 = TopicPartitionList::new();
        for o in topic_offsets {
            tpl2.add_partition(topic, o.partition);
        }
        // Set timestamps via Offset::Offset which offsets_for_times interprets... 
        // Actually API: TopicPartitionList with Offset::Offset(timestamp)
        let mut query = TopicPartitionList::new();
        for o in topic_offsets {
            query
                .add_partition_offset(topic, o.partition, Offset::Offset(time_ms))
                .map_err(|e| e.to_string())?;
        }
        let res = consumer
            .offsets_for_times(query, Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for elem in res.elements_for_topic(topic) {
            if let Offset::Offset(off) = elem.offset() {
                out.push(PartitionTimestampSeek {
                    partition: elem.partition(),
                    offset: off,
                });
            }
        }
        let _ = tpl;
        let _ = tpl2;
        Ok(out)
    }
}
