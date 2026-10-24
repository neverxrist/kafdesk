use crate::types::*;
use super::admin_group_offsets::{classify_coord_err, list_committed_offsets, CommittedOffset};
use super::consume_ctx::KafdeskCtx;
use super::session::{Admin, BConsumer, KafkaSession};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Concurrent `ListConsumerGroupOffsets` in flight on one Admin client.
const OFFSET_WORKERS: usize = 8;

type CommittedResult = (String, Result<Vec<CommittedOffset>, String>);

/// Committed offsets for `ids` with bounded concurrency. Each request has its own
/// librdkafka queue, so sharing the Admin handle across threads is safe.
/// Groups not started before `deadline` are omitted (caller marks the walk partial).
pub(super) fn fetch_committed_parallel<C: ClientContext>(
    admin: &rdkafka::admin::AdminClient<C>,
    ids: &[String],
    deadline: Instant,
    alive: &AtomicBool,
) -> Vec<CommittedResult> {
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, CommittedResult)>> = Mutex::new(Vec::with_capacity(ids.len()));
    std::thread::scope(|s| {
        for _ in 0..OFFSET_WORKERS.min(ids.len()) {
            s.spawn(|| loop {
                let now = Instant::now();
                if now >= deadline || !alive.load(Ordering::Relaxed) {
                    break;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(id) = ids.get(i) else { break };
                let timeout = (deadline - now).min(Duration::from_secs(8));
                let res = list_committed_offsets(admin, id, timeout).map_err(|e| classify_coord_err(&e));
                if let Ok(mut r) = results.lock() {
                    r.push((i, (id.clone(), res)));
                }
            });
        }
    });
    let mut out = results.into_inner().unwrap_or_default();
    out.sort_by_key(|(i, _)| *i);
    out.into_iter().map(|(_, r)| r).collect()
}

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
                topic_names: Vec::new(),
            });
        }
        Ok(out)
    }

    pub fn describe_consumer_group(&self, group_id: &str) -> Result<ConsumerGroupDetail, String> {
        let admin = self.make_admin()?;
        super::admin_describe_groups::describe_consumer_group(
            &admin,
            group_id,
            Duration::from_secs(12),
        )
    }

    pub(super) fn make_admin(&self) -> Result<Arc<Admin>, String> {
        self.shared_admin()
    }

    /// Committed offsets + lag via Admin `ListConsumerGroupOffsets` (AKHQ-style).
    /// Does **not** open a Consumer with the target group.id.
    pub fn get_group_offsets(
        &self,
        group_id: &str,
        topic: Option<&str>,
    ) -> Result<Vec<GroupOffsetEntry>, String> {
        let admin = self.make_admin()?;
        self.group_offsets_with_admin(&admin, group_id, topic)
    }

    fn group_offsets_with_admin<C: ClientContext>(
        &self,
        admin: &rdkafka::admin::AdminClient<C>,
        group_id: &str,
        topic: Option<&str>,
    ) -> Result<Vec<GroupOffsetEntry>, String> {
        let committed = list_committed_offsets(admin, group_id, Duration::from_secs(12))
            .map_err(|e| classify_coord_err(&e))?;
        let committed: Vec<_> = match topic {
            Some(t) => committed.into_iter().filter(|c| c.topic == t).collect(),
            None => committed,
        };
        let mut highs = HashMap::new();
        let (out, _) = self.offsets_with_highs(committed, &mut highs, None);
        Ok(out)
    }

    /// Join committed offsets with (low, high) watermarks, fetching each topic once
    /// into `highs`. Partitions without a known high get `lag: None`.
    /// Returns `(rows, any_high_missing)`.
    fn offsets_with_highs(
        &self,
        committed: Vec<CommittedOffset>,
        highs: &mut HashMap<String, HashMap<i32, (String, String)>>,
        deadline: Option<Instant>,
    ) -> (Vec<GroupOffsetEntry>, bool) {
        let mut missing = false;
        let mut out = Vec::with_capacity(committed.len());
        for c in committed {
            if !highs.contains_key(&c.topic) {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    missing = true;
                } else {
                    let map = match self.get_topic_offsets(&c.topic) {
                        Ok(info) => info
                            .partitions
                            .into_iter()
                            .map(|p| {
                                (
                                    p.partition_id,
                                    (
                                        p.offset_low.unwrap_or_else(|| "0".into()),
                                        p.offset_high.unwrap_or_else(|| "0".into()),
                                    ),
                                )
                            })
                            .collect(),
                        Err(_) => HashMap::new(),
                    };
                    highs.insert(c.topic.clone(), map);
                }
            }
            let wm = highs.get(&c.topic).and_then(|m| m.get(&c.partition)).cloned();
            if wm.is_none() {
                missing = true;
            }
            let lag = wm
                .as_ref()
                .and_then(|(_, h)| h.parse::<i64>().ok())
                .map(|h| (h - c.offset).max(0));
            out.push(GroupOffsetEntry {
                topic: c.topic,
                partition: c.partition,
                offset: c.offset.to_string(),
                low: wm.as_ref().map(|(l, _)| l.clone()),
                high: wm.map(|(_, h)| h),
                lag,
                metadata: None,
            });
        }
        out.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        (out, missing)
    }

    /// Lag for an explicit set of group ids (UI keyword filter selection), or —
    /// with a topic and no ids — every group that committed on that topic.
    pub fn get_lag_summary(
        &self,
        topic: Option<&str>,
        group_ids: Vec<String>,
    ) -> Result<LagSummaryResult, String> {
        const MAX_SELECTED: usize = 100;
        const MAX_TOPIC_WALK: usize = 3000;
        let input_len = group_ids.len();
        let ids: Vec<String> = if !group_ids.is_empty() {
            group_ids.into_iter().take(MAX_SELECTED).collect()
        } else if topic.is_some() {
            self.list_consumer_groups()?
                .into_iter()
                .map(|g| g.group_id)
                .collect()
        } else {
            Vec::new()
        };
        let listed = if input_len > 0 { input_len } else { ids.len() };
        let mut partial = input_len > MAX_SELECTED || ids.len() > MAX_TOPIC_WALK;
        let ids: Vec<String> = ids.into_iter().take(MAX_TOPIC_WALK).collect();
        let mut summaries = Vec::new();
        let mut last_err: Option<String> = None;
        // One Admin client for the whole walk, not N consumers.
        let admin = self.make_admin()?;
        let budget_secs = if topic.is_some() { 25u64 } else { 12 };
        let deadline = Instant::now() + Duration::from_secs(budget_secs);
        let committed = fetch_committed_parallel(&admin, &ids, deadline, &self.alive);
        let scanned = committed.len();
        if scanned < ids.len() {
            partial = true;
        }
        let mut highs = HashMap::new();
        let mut failed = 0usize;
        for (group_id, res) in committed {
            let rows = match res {
                Ok(rows) => rows,
                Err(e) => {
                    failed += 1;
                    last_err = Some(e);
                    Vec::new()
                }
            };
            let rows: Vec<_> = match topic {
                Some(t) => rows.into_iter().filter(|c| c.topic == t).collect(),
                None => rows,
            };
            if topic.is_some() && rows.is_empty() {
                continue;
            }
            let (offsets, missing) = self.offsets_with_highs(rows, &mut highs, Some(deadline));
            if missing {
                partial = true;
            }

            let mut total_lag = 0i64;
            let mut max_partition_lag = 0i64;
            let mut worst_topic = None;
            let mut worst_partition = None;
            let mut topic_set = std::collections::BTreeSet::new();
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
            let topic_names: Vec<String> = topic_set.iter().take(6).cloned().collect();
            summaries.push(GroupLagSummary {
                group_id,
                total_lag,
                max_partition_lag,
                topics: topic_set.len(),
                partitions: offsets.len(),
                worst_topic,
                worst_partition,
                topic_names,
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

        // Every call failed — surface why instead of an empty «no consumers» result.
        if scanned > 0 && failed == scanned {
            if let Some(e) = last_err {
                return Err(e);
            }
        }

        Ok(LagSummaryResult {
            groups: summaries,
            total_lag,
            max_lag,
            updated_at,
            listed,
            scanned,
            partial,
        })
    }

    pub fn set_group_offsets(&self, request: SetGroupOffsetsRequest) -> Result<(), String> {
        self.ensure_config()?;
        let mut cfg = self.base_config.clone();
        cfg.set("group.id", &request.group_id);
        cfg.set("enable.auto.commit", "false");
        let consumer: BConsumer = cfg
            .create_with_context(KafdeskCtx::default())
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
            .commit(&tpl, CommitMode::Sync)
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
