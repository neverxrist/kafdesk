use crate::domain::{
    initial_finished_partitions, matches_decoded_message, matches_raw_pre_filter,
    plan_consume_window, sort_messages_by_time, BrokerMessageFilter, ConsumeWindowInput,
    PartitionTimestampSeek, PartitionWatermark,
};
use crate::types::*;
use super::consume_ctx::ConsumeCtx;
use super::decode::{cap_message_value, headers_from_message};
use super::session::KafkaSession;
use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

impl KafkaSession {
    /// Stable per-user id: librdkafka's assign() requires `group.id` (else `Unknown group`).
    /// Nothing is committed and no JoinGroup is sent, so no group appears on the cluster.
    pub(super) fn consume_group_id(&self) -> String {
        self.config
            .as_ref()
            .and_then(|c| c.sasl.as_ref())
            .map(|s| s.username.trim())
            .filter(|u| !u.is_empty())
            .map(|u| format!("kafdesk-{u}"))
            .unwrap_or_else(|| "kafdesk-ui".into())
    }

    /// Shared librdkafka knobs for assign()-only browse (no commit / minimal coordinator).
    fn consume_client_config(&self) -> ClientConfig {
        let mut cfg = self.base_config.clone();
        cfg.set("group.id", self.consume_group_id());
        cfg.set("session.timeout.ms", "6000");
        cfg.set("coordinator.query.interval.ms", "600000");
        cfg.set("enable.auto.commit", "false");
        cfg.set("enable.auto.offset.store", "false");
        cfg.set("auto.offset.reset", "earliest");
        cfg.set("enable.partition.eof", "true");
        cfg.set("socket.timeout.ms", "5000");
        cfg.set("socket.connection.setup.timeout.ms", "5000");
        cfg.set("reconnect.backoff.ms", "100");
        cfg.set("reconnect.backoff.max.ms", "1000");
        cfg.set("metadata.request.timeout.ms", "5000");
        cfg
    }

    /// One message with its full (uncapped) value — "load full" for truncated rows.
    pub async fn fetch_message(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<KafkaMessageView, String> {
        self.ensure_config()?;
        let cfg = self.consume_client_config();
        let ctx = ConsumeCtx::default();
        let err_slot = ctx.clone();
        let consumer: BaseConsumer<ConsumeCtx> =
            BaseConsumer::from_config_and_context(&cfg, ctx).map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(200)) {
                None => {
                    if let Some(err) = err_slot.peek_error() {
                        return Err(err);
                    }
                }
                Some(Err(e)) => {
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("partition eof") {
                        return Err(format!("Offset {offset} is past the end of {topic}/{partition}"));
                    }
                    return Err(msg);
                }
                Some(Ok(msg)) => {
                    if msg.offset() < offset {
                        continue;
                    }
                    if msg.offset() != offset {
                        return Err(format!(
                            "Offset {offset} is no longer in {topic}/{partition} (compacted or deleted)"
                        ));
                    }
                    let ts = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => ms,
                        _ => 0,
                    };
                    let headers = headers_from_message(&msg);
                    return Ok(self
                        .decode_message_view(topic, partition, offset, ts, msg.key(), msg.payload(), headers)
                        .await);
                }
            }
        }
        Err(format!("Timed out reading {topic}/{partition}@{offset}"))
    }

    pub async fn consume_cancellable(
        &self,
        request: ConsumeRequest,
        gen: Arc<AtomicU64>,
        my_gen: u64,
    ) -> Result<Vec<KafkaMessageView>, String> {
        self.ensure_config()?;
        if gen.load(Ordering::SeqCst) != my_gen {
            return Ok(vec![]);
        }
        let (topic_offsets, partition_ids_fallback) = {
            if let Some(ref hints) = request.partition_watermarks {
                if !hints.is_empty() {
                    let wm: Vec<_> = hints
                        .iter()
                        .map(|h| PartitionWatermark {
                            partition: h.partition,
                            low: h.low,
                            high: h.high,
                        })
                        .collect();
                    let ids: Vec<i32> = wm.iter().map(|w| w.partition).collect();
                    (wm, ids)
                } else {
                    let info = self.get_topic_offsets(&request.topic)?;
                    let ids: Vec<i32> = info.partitions.iter().map(|p| p.partition_id).collect();
                    (Self::watermarks_from_topic_info(&info), ids)
                }
            } else {
                let info = self.get_topic_offsets(&request.topic)?;
                let ids: Vec<i32> = info.partitions.iter().map(|p| p.partition_id).collect();
                (Self::watermarks_from_topic_info(&info), ids)
            }
        };

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

        // Watermarks failed (NotLeaderForPartition etc.) — still consume via relative offsets.
        if topic_offsets.is_empty() {
            let parts = request
                .partitions
                .clone()
                .filter(|p| !p.is_empty())
                .unwrap_or(partition_ids_fallback);
            if parts.is_empty() {
                return Ok(vec![]);
            }
            return self
                .consume_relative_offsets(&request, &start_from, &parts, gen, my_gen)
                .await;
        }

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

        let cfg = self.consume_client_config();
        let ctx = ConsumeCtx::default();
        let err_slot = ctx.clone();
        let consumer: BaseConsumer<ConsumeCtx> =
            BaseConsumer::from_config_and_context(&cfg, ctx).map_err(|e| e.to_string())?;
        // Bootstrap IP can work while advertised.listeners DNS does not.
        super::dns_check::ensure_advertised_brokers_resolvable(&consumer, &self.brokers)?;

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
            search_scope: None,
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
            if gen.load(Ordering::SeqCst) != my_gen {
                break;
            }
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
            // Only hard-fail early on ACL / DNS.
            if messages.is_empty() && last_msg.elapsed() > Duration::from_secs(2) {
                if let Some(err) = err_slot.peek_error() {
                    if err.contains("authorization") {
                        return Err(format!("ACL denied: {err}"));
                    }
                    if err.contains("Failed to resolve") {
                        return Err(err);
                    }
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
                    let mut view = view;
                    cap_message_value(&mut view, msg.payload_len());

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

        if messages.is_empty() {
            if let Some(err) = err_slot.peek_error() {
                if err.contains("authorization") {
                    return Err(format!("ACL denied: {err}"));
                }
                if err.contains("Failed to resolve") {
                    return Err(err);
                }
                return Err(format!(
                    "No messages received; broker error while consuming: {err}"
                ));
            }
        }

        let mut sorted = sort_messages_by_time(messages, plan.newest_first);
        sorted.truncate(plan.limit as usize);
        Ok(sorted)
    }

    fn watermarks_from_topic_info(topic_info: &TopicInfo) -> Vec<PartitionWatermark> {
        topic_info
            .partitions
            .iter()
            .filter_map(|p| {
                let low = p.offset_low.as_deref()?.parse().ok()?;
                let high = p.offset_high.as_deref()?.parse().ok()?;
                Some(PartitionWatermark {
                    partition: p.partition_id,
                    low,
                    high,
                })
            })
            .collect()
    }

    /// When QueryWatermarkOffsets fails (stale leader), assign Beginning / OffsetTail instead.
    async fn consume_relative_offsets(
        &self,
        request: &ConsumeRequest,
        start_from: &str,
        partitions: &[i32],
        gen: Arc<AtomicU64>,
        my_gen: u64,
    ) -> Result<Vec<KafkaMessageView>, String> {
        if gen.load(Ordering::SeqCst) != my_gen {
            return Ok(vec![]);
        }
        let limit = crate::domain::clamp_consume_limit(request.limit);
        let cfg = self.consume_client_config();
        let ctx = ConsumeCtx::default();
        let err_slot = ctx.clone();
        let consumer: BaseConsumer<ConsumeCtx> =
            BaseConsumer::from_config_and_context(&cfg, ctx).map_err(|e| e.to_string())?;
        super::dns_check::ensure_advertised_brokers_resolvable(&consumer, &self.brokers)?;

        let mut tpl = TopicPartitionList::new();
        for &p in partitions {
            let off = if start_from == "end" {
                Offset::OffsetTail(limit as i64)
            } else if start_from == "offset" {
                let raw = request
                    .start_offset
                    .as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Offset::Offset(raw)
            } else {
                Offset::Beginning
            };
            tpl.add_partition_offset(&request.topic, p, off)
                .map_err(|e| e.to_string())?;
        }
        consumer.assign(&tpl).map_err(|e| e.to_string())?;

        let filter = BrokerMessageFilter {
            search: request.search.clone(),
            search_key: request.search_key.clone(),
            search_scope: None,
            header_key: request.search_header_key.clone(),
            header_value: request.search_header_value.clone(),
            time_from: request.time_from,
            time_to: request.time_to,
        };

        let mut messages = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(10_000);
        let mut last_msg = Instant::now();
        let mut eof = 0usize;

        while Instant::now() < deadline && messages.len() < limit as usize {
            if gen.load(Ordering::SeqCst) != my_gen {
                break;
            }
            if eof >= partitions.len() && !messages.is_empty() {
                break;
            }
            if last_msg.elapsed() > Duration::from_millis(900) && !messages.is_empty() {
                break;
            }
            match consumer.poll(Duration::from_millis(200)) {
                None => continue,
                Some(Err(e)) => {
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("partition eof")
                        || msg.to_lowercase().contains("end of partition")
                    {
                        eof += 1;
                        continue;
                    }
                    if let Some(err) = err_slot.peek_error() {
                        return Err(format!("Consume error: {msg} ({err})"));
                    }
                    return Err(format!("Consume error: {msg}"));
                }
                Some(Ok(msg)) => {
                    last_msg = Instant::now();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let key = msg.key();
                    let headers = headers_from_message(&msg);
                    let ts = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(ms)
                        | rdkafka::Timestamp::LogAppendTime(ms) => ms,
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
                    let mut view = view;
                    cap_message_value(&mut view, msg.payload_len());
                    messages.push(view);
                }
            }
        }

        if messages.is_empty() {
            if let Some(err) = err_slot.peek_error() {
                return Err(format!(
                    "No messages (watermarks unavailable; relative consume): {err}"
                ));
            }
        }

        let newest_first = start_from == "end";
        let mut sorted = sort_messages_by_time(messages, newest_first);
        sorted.truncate(limit as usize);
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
