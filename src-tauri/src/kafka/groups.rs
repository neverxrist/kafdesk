use crate::types::*;
use super::session::{KafkaSession, BConsumer};
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

impl KafkaSession {
    pub fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>, String> {
        let consumer = self.metadata_consumer()?;
        // Keep short — GroupCoordinator SASL failures otherwise pin the UI for ages.
        let groups = consumer
            .fetch_group_list(None, Duration::from_secs(4))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for g in groups.groups() {
            let id = g.name().to_string();
            if id.starts_with("kafdesk-") {
                continue;
            }
            out.push(ConsumerGroupInfo {
                group_id: id,
                protocol_type: g.protocol_type().to_string(),
                state: Some(format!("{:?}", g.state())),
                members: Some(g.members().len()),
                total_lag: None,
                max_partition_lag: None,
                lag_topics: None,
                lag_partitions: None,
            });
        }
        Ok(out)
    }
    pub fn get_group_offsets(
        &self,
        group_id: &str,
        topic: Option<&str>,
    ) -> Result<Vec<GroupOffsetEntry>, String> {
        let mut cfg = self.base_config.clone();
        cfg.set("group.id", group_id);
        cfg.set("enable.auto.commit", "false");
        let group_consumer: BConsumer = cfg
            .create_with_context(super::consume_ctx::KafdeskCtx::default())
            .map_err(|e| e.to_string())?;

        let topic_names: Vec<String> = if let Some(t) = topic {
            vec![t.to_string()]
        } else {
            self.list_topics()?.into_iter().map(|t| t.name).collect()
        };

        let mut out = Vec::new();
        for topic_name in topic_names {
            let info = match self.get_topic_offsets(&topic_name) {
                Ok(i) => i,
                Err(_) => continue, // topic may be gone
            };
            if info.partitions.is_empty() {
                continue;
            }
            let mut tpl = TopicPartitionList::new();
            for p in &info.partitions {
                tpl.add_partition(&topic_name, p.partition_id);
            }
            let committed = match group_consumer.committed_offsets(tpl, Duration::from_secs(10)) {
                Ok(c) => c,
                Err(_) => continue,
            };

            for p in &info.partitions {
                let low = p.offset_low.clone().unwrap_or_else(|| "0".into());
                let high = p.offset_high.clone().unwrap_or_else(|| "0".into());
                let high_n: i64 = high.parse().unwrap_or(0);
                let committed_off = committed
                    .elements_for_topic(&topic_name)
                    .into_iter()
                    .find(|e| e.partition() == p.partition_id)
                    .and_then(|e| match e.offset() {
                        Offset::Offset(off) if off >= 0 => Some(off),
                        _ => None,
                    });
                // Electron: when topic-scoped, skip partitions with no real commit.
                // Without topic filter, also only emit real commits (matches fetchOffsets).
                let Some(off) = committed_off else {
                    continue;
                };
                out.push(GroupOffsetEntry {
                    topic: topic_name.clone(),
                    partition: p.partition_id,
                    offset: off.to_string(),
                    high: Some(high),
                    low: Some(low),
                    lag: Some((high_n - off).max(0)),
                    metadata: None,
                });
            }
        }
        out.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        Ok(out)
    }
    pub fn get_lag_summary(&self, topic: Option<&str>) -> Result<LagSummaryResult, String> {
        let groups = self.list_consumer_groups()?;
        let mut summaries = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(6);
        for g in groups {
            if Instant::now() >= deadline {
                break;
            }
            let offsets = match self.get_group_offsets(&g.group_id, topic) {
                Ok(o) => o,
                Err(_) => Vec::new(),
            };
            if topic.is_some() && offsets.is_empty() {
                continue;
            }

            let mut total_lag = 0i64;
            let mut max_partition_lag = 0i64;
            let mut worst_topic = None;
            let mut worst_partition = None;
            let mut topic_set = std::collections::HashSet::new();
            for o in &offsets {
                topic_set.insert(o.topic.clone());
                let lag = o.lag.unwrap_or(0);
                total_lag += lag;
                if lag > max_partition_lag {
                    max_partition_lag = lag;
                    worst_topic = Some(o.topic.clone());
                    worst_partition = Some(o.partition);
                }
            }
            summaries.push(GroupLagSummary {
                group_id: g.group_id,
                total_lag,
                max_partition_lag,
                topics: topic_set.len(),
                partitions: offsets.len(),
                worst_topic,
                worst_partition,
            });
        }
        summaries.sort_by(|a, b| {
            b.total_lag
                .cmp(&a.total_lag)
                .then(a.group_id.cmp(&b.group_id))
        });
        let total_lag = summaries.iter().map(|s| s.total_lag).sum();
        let max_lag = summaries.iter().map(|s| s.total_lag).max().unwrap_or(0);
        let updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Ok(LagSummaryResult {
            groups: summaries,
            total_lag,
            max_lag,
            updated_at,
        })
    }
    pub fn set_group_offsets(&self, request: SetGroupOffsetsRequest) -> Result<(), String> {
        self.ensure_config()?;
        let mut cfg = self.base_config.clone();
        cfg.set("group.id", &request.group_id);
        cfg.set("enable.auto.commit", "false");
        let consumer: BConsumer = cfg
            .create_with_context(super::consume_ctx::KafdeskCtx::default())
            .map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        for p in &request.partitions {
            let off: i64 = p.offset.parse().map_err(|_| "bad offset")?;
            tpl.add_partition_offset(&request.topic, p.partition, Offset::Offset(off))
                .map_err(|e| e.to_string())?;
        }
        // Assign before commit so librdkafka has a partition set (admin.setOffsets analogue).
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn reset_group_offsets(
        &self,
        group_id: &str,
        topic: &str,
        earliest: bool,
    ) -> Result<(), String> {
        let info = self.get_topic_offsets(topic)?;
        let partitions: Vec<PartitionOffset> = info
            .partitions
            .iter()
            .map(|p| {
                let offset = if earliest {
                    p.offset_low.clone().unwrap_or_else(|| "0".into())
                } else {
                    p.offset_high.clone().unwrap_or_else(|| "0".into())
                };
                PartitionOffset {
                    partition: p.partition_id,
                    offset,
                }
            })
            .collect();
        self.set_group_offsets(SetGroupOffsetsRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partitions,
        })
    }
}
