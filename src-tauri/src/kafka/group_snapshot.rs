//! Cluster-wide consumer-group snapshot: committed offsets of every group + one
//! batched `ListOffsets(LATEST)`, indexed group → topics and topic → groups.
//! Kept in memory per session with a short lag history (trend / stalled).

use super::admin_group_offsets::CommittedOffset;
use super::admin_list_offsets::{list_offsets_chunked, OffsetSpec};
use super::groups::fetch_committed_parallel;
use super::session::KafkaSession;
use crate::types::*;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_GROUPS: usize = 3000;
const HISTORY_POINTS: usize = 20;
const WALK_BUDGET: Duration = Duration::from_secs(25);
const LIST_OFFSETS_TIMEOUT: Duration = Duration::from_secs(10);
const TOPIC_NAMES_PER_GROUP: usize = 6;

#[derive(Debug, Clone, Copy)]
struct LagPoint {
    lag: i64,
    committed: i64,
    end: i64,
}

struct SnapGroup {
    group_id: String,
    state: Option<String>,
    members: usize,
    committed: Vec<CommittedOffset>,
    error: Option<String>,
}

#[derive(Default)]
pub(crate) struct GroupSnapshot {
    updated_at: i64,
    duration_ms: u64,
    listed: usize,
    scanned: usize,
    failed: usize,
    partial: bool,
    groups: Vec<SnapGroup>,
    ends: HashMap<(String, i32), i64>,
    group_history: HashMap<String, VecDeque<LagPoint>>,
    topic_history: HashMap<(String, String), VecDeque<LagPoint>>,
}

/// Lag over a set of committed offsets. `None` when no partition has a known end.
fn lag_point<'a>(
    committed: impl Iterator<Item = &'a CommittedOffset>,
    ends: &HashMap<(String, i32), i64>,
) -> (Option<LagPoint>, i64) {
    let mut point = LagPoint { lag: 0, committed: 0, end: 0 };
    let mut known = false;
    let mut max_partition = 0i64;
    for c in committed {
        let Some(&end) = ends.get(&(c.topic.clone(), c.partition)) else {
            continue;
        };
        known = true;
        let lag = (end - c.offset).max(0);
        point.lag += lag;
        point.committed += c.offset;
        point.end += end;
        max_partition = max_partition.max(lag);
    }
    (known.then_some(point), max_partition)
}

fn push_point(history: &mut VecDeque<LagPoint>, p: LagPoint) {
    history.push_back(p);
    while history.len() > HISTORY_POINTS {
        history.pop_front();
    }
}

/// Trend vs previous point; stalled = lag > 0, committed frozen, log end growing
/// over the last 3 snapshots.
fn trend_and_stalled(history: Option<&VecDeque<LagPoint>>) -> (Option<String>, bool) {
    let Some(h) = history else { return (None, false) };
    let n = h.len();
    if n < 2 {
        return (None, false);
    }
    let last = h[n - 1];
    let prev = h[n - 2];
    let trend = match last.lag.cmp(&prev.lag) {
        std::cmp::Ordering::Greater => "up",
        std::cmp::Ordering::Less => "down",
        std::cmp::Ordering::Equal => "flat",
    };
    let stalled = n >= 3 && {
        let first = h[n - 3];
        last.lag > 0
            && first.committed == prev.committed
            && prev.committed == last.committed
            && last.end > first.end
    };
    (Some(trend.to_string()), stalled)
}

fn history_lags(history: Option<&VecDeque<LagPoint>>) -> Vec<i64> {
    history.map(|h| h.iter().map(|p| p.lag).collect()).unwrap_or_default()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl GroupSnapshot {
    fn view(&self) -> GroupSnapshotView {
        let mut topics: HashMap<&str, (usize, i64)> = HashMap::new();
        let mut rows = Vec::with_capacity(self.groups.len());
        for g in &self.groups {
            let (point, max_partition_lag) = lag_point(g.committed.iter(), &self.ends);
            let topic_set: BTreeSet<&str> = g.committed.iter().map(|c| c.topic.as_str()).collect();
            for t in &topic_set {
                let (p, _) = lag_point(g.committed.iter().filter(|c| c.topic == *t), &self.ends);
                let e = topics.entry(t).or_default();
                e.0 += 1;
                e.1 += p.map(|p| p.lag).unwrap_or(0);
            }
            let hist = self.group_history.get(&g.group_id);
            let (trend, stalled) = trend_and_stalled(hist);
            rows.push(SnapshotGroupRow {
                group_id: g.group_id.clone(),
                state: g.state.clone(),
                members: g.members,
                total_lag: if g.error.is_some() {
                    None
                } else {
                    Some(point.map(|p| p.lag).unwrap_or(0))
                },
                max_partition_lag,
                topics: topic_set.len(),
                topic_names: topic_set
                    .iter()
                    .take(TOPIC_NAMES_PER_GROUP)
                    .map(|s| s.to_string())
                    .collect(),
                trend,
                stalled,
                history: history_lags(hist),
                error: g.error.clone(),
            });
        }
        let mut topics: Vec<TopicLagRow> = topics
            .into_iter()
            .map(|(topic, (groups, total_lag))| TopicLagRow {
                topic: topic.to_string(),
                groups,
                total_lag,
            })
            .collect();
        topics.sort_by(|a, b| a.topic.cmp(&b.topic));
        GroupSnapshotView {
            updated_at: self.updated_at,
            duration_ms: self.duration_ms,
            listed: self.listed,
            scanned: self.scanned,
            failed: self.failed,
            partial: self.partial,
            groups: rows,
            topics,
        }
    }

    fn topic_consumers(&self, topic: &str) -> TopicConsumersView {
        let mut rows = Vec::new();
        for g in &self.groups {
            let committed: Vec<&CommittedOffset> =
                g.committed.iter().filter(|c| c.topic == topic).collect();
            if committed.is_empty() {
                continue;
            }
            let (point, max_partition_lag) = lag_point(committed.iter().copied(), &self.ends);
            let hist = self.topic_history.get(&(g.group_id.clone(), topic.to_string()));
            let (trend, stalled) = trend_and_stalled(hist);
            let mut partitions: Vec<GroupOffsetEntry> = committed
                .iter()
                .map(|c| {
                    let end = self.ends.get(&(c.topic.clone(), c.partition)).copied();
                    GroupOffsetEntry {
                        topic: c.topic.clone(),
                        partition: c.partition,
                        offset: c.offset.to_string(),
                        high: end.map(|e| e.to_string()),
                        low: None,
                        lag: end.map(|e| (e - c.offset).max(0)),
                        metadata: None,
                    }
                })
                .collect();
            partitions.sort_by_key(|p| p.partition);
            rows.push(TopicConsumerRow {
                group_id: g.group_id.clone(),
                state: g.state.clone(),
                members: g.members,
                lag: point.map(|p| p.lag),
                max_partition_lag,
                trend,
                stalled,
                history: history_lags(hist),
                partitions,
            });
        }
        rows.sort_by(|a, b| {
            (b.members > 0)
                .cmp(&(a.members > 0))
                .then(b.lag.unwrap_or(-1).cmp(&a.lag.unwrap_or(-1)))
                .then(a.group_id.cmp(&b.group_id))
        });
        TopicConsumersView {
            topic: topic.to_string(),
            updated_at: self.updated_at,
            partial: self.partial,
            groups: rows,
        }
    }
}

impl KafkaSession {
    pub fn refresh_group_snapshot(&self) -> Result<GroupSnapshotView, String> {
        let started = Instant::now();
        let listed_groups = self.list_consumer_groups()?;
        let listed = listed_groups.len();
        let mut partial = listed > MAX_GROUPS;
        let listed_groups: Vec<ConsumerGroupInfo> =
            listed_groups.into_iter().take(MAX_GROUPS).collect();
        let ids: Vec<String> = listed_groups.iter().map(|g| g.group_id.clone()).collect();

        let admin = self.make_admin()?;
        let committed = fetch_committed_parallel(&admin, &ids, started + WALK_BUDGET, &self.alive);
        if !self.alive.load(Ordering::Relaxed) {
            return Err("Disconnected".into());
        }
        let scanned = committed.len();
        if scanned < ids.len() {
            partial = true;
        }
        let mut by_id: HashMap<String, Result<Vec<CommittedOffset>, String>> =
            committed.into_iter().collect();
        let failed = by_id.values().filter(|r| r.is_err()).count();
        if scanned > 0 && failed == scanned {
            if let Some(Err(e)) = by_id.into_values().next() {
                return Err(e);
            }
            return Err("Committed offsets unavailable for every group".into());
        }

        let mut seen = HashSet::new();
        let mut tps: Vec<(String, i32)> = Vec::new();
        for c in by_id.values().filter_map(|r| r.as_ref().ok()).flatten() {
            if seen.insert((c.topic.as_str(), c.partition)) {
                tps.push((c.topic.clone(), c.partition));
            }
        }
        tps.sort();
        let ends = list_offsets_chunked(&admin, &tps, OffsetSpec::Latest, LIST_OFFSETS_TIMEOUT);

        let groups: Vec<SnapGroup> = listed_groups
            .into_iter()
            .map(|g| {
                let (committed, error) = match by_id.remove(&g.group_id) {
                    Some(Ok(c)) => (c, None),
                    Some(Err(e)) => (Vec::new(), Some(e)),
                    None => (Vec::new(), Some("not scanned (time cap)".to_string())),
                };
                SnapGroup {
                    group_id: g.group_id,
                    state: g.state,
                    members: g.members.unwrap_or(0),
                    committed,
                    error,
                }
            })
            .collect();

        let mut slot = self
            .group_snapshot
            .lock()
            .map_err(|_| "group snapshot lock poisoned".to_string())?;
        // Checked under the lock: disconnect/reconnect must not receive an old cluster's walk.
        if !self.alive.load(Ordering::Relaxed) {
            return Err("Disconnected".into());
        }
        let prev = slot.take().unwrap_or_default();
        let mut group_history = prev.group_history;
        let mut topic_history = prev.topic_history;
        for g in groups.iter().filter(|g| g.error.is_none()) {
            if let (Some(p), _) = lag_point(g.committed.iter(), &ends) {
                push_point(group_history.entry(g.group_id.clone()).or_default(), p);
            }
            let topic_set: BTreeSet<&str> = g.committed.iter().map(|c| c.topic.as_str()).collect();
            for t in topic_set {
                let (p, _) = lag_point(g.committed.iter().filter(|c| c.topic == t), &ends);
                if let Some(p) = p {
                    let key = (g.group_id.clone(), t.to_string());
                    push_point(topic_history.entry(key).or_default(), p);
                }
            }
        }
        // Keep history across a transient per-group error; drop deleted groups.
        let listed_ids: HashSet<&str> = groups.iter().map(|g| g.group_id.as_str()).collect();
        group_history.retain(|k, _| listed_ids.contains(k.as_str()));
        topic_history.retain(|(g, _), _| listed_ids.contains(g.as_str()));

        let snap = GroupSnapshot {
            updated_at: now_ms(),
            duration_ms: started.elapsed().as_millis() as u64,
            listed,
            scanned,
            failed,
            partial,
            groups,
            ends,
            group_history,
            topic_history,
        };
        let view = snap.view();
        *slot = Some(snap);
        Ok(view)
    }

    /// From the last snapshot only — no Kafka calls. None before the first refresh.
    pub fn topic_consumers(&self, topic: &str) -> Option<TopicConsumersView> {
        let slot = self.group_snapshot.lock().ok()?;
        slot.as_ref().map(|s| s.topic_consumers(topic))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn co(topic: &str, partition: i32, offset: i64) -> CommittedOffset {
        CommittedOffset { topic: topic.into(), partition, offset }
    }

    fn pt(lag: i64, committed: i64, end: i64) -> LagPoint {
        LagPoint { lag, committed, end }
    }

    #[test]
    fn lag_point_skips_unknown_ends_and_clamps_negative() {
        let ends: HashMap<(String, i32), i64> =
            [(("t".into(), 0), 100), (("t".into(), 1), 5)].into_iter().collect();
        let committed = [co("t", 0, 40), co("t", 1, 9), co("t", 2, 0)];
        let (p, max) = lag_point(committed.iter(), &ends);
        let p = p.unwrap();
        // p1 committed past end (retention race) → 0, p2 has no end → ignored.
        assert_eq!(p.lag, 60);
        assert_eq!(max, 60);
        assert_eq!(p.end, 105);
    }

    #[test]
    fn lag_point_none_without_any_end() {
        let (p, max) = lag_point([co("t", 0, 1)].iter(), &HashMap::new());
        assert!(p.is_none());
        assert_eq!(max, 0);
    }

    #[test]
    fn push_point_keeps_last_n() {
        let mut h = VecDeque::new();
        for i in 0..(HISTORY_POINTS as i64 + 5) {
            push_point(&mut h, pt(i, 0, 0));
        }
        assert_eq!(h.len(), HISTORY_POINTS);
        assert_eq!(h.front().unwrap().lag, 5);
    }

    #[test]
    fn trend_needs_two_points() {
        let h: VecDeque<_> = [pt(5, 0, 5)].into_iter().collect();
        assert_eq!(trend_and_stalled(Some(&h)), (None, false));
        assert_eq!(trend_and_stalled(None), (None, false));
    }

    #[test]
    fn trend_up_down_flat() {
        let up: VecDeque<_> = [pt(1, 0, 1), pt(3, 0, 3)].into_iter().collect();
        let down: VecDeque<_> = [pt(3, 0, 3), pt(1, 2, 3)].into_iter().collect();
        let flat: VecDeque<_> = [pt(2, 1, 3), pt(2, 2, 4)].into_iter().collect();
        assert_eq!(trend_and_stalled(Some(&up)).0.as_deref(), Some("up"));
        assert_eq!(trend_and_stalled(Some(&down)).0.as_deref(), Some("down"));
        assert_eq!(trend_and_stalled(Some(&flat)).0.as_deref(), Some("flat"));
    }

    #[test]
    fn stalled_when_committed_frozen_and_end_grows() {
        let h: VecDeque<_> = [pt(10, 50, 60), pt(15, 50, 65), pt(20, 50, 70)].into_iter().collect();
        assert!(trend_and_stalled(Some(&h)).1);
    }

    #[test]
    fn not_stalled_when_consumer_moves_or_topic_idle() {
        let moving: VecDeque<_> = [pt(10, 50, 60), pt(10, 55, 65), pt(10, 60, 70)].into_iter().collect();
        let idle: VecDeque<_> = [pt(10, 50, 60), pt(10, 50, 60), pt(10, 50, 60)].into_iter().collect();
        let caught_up: VecDeque<_> = [pt(0, 60, 60), pt(0, 60, 60), pt(0, 60, 60)].into_iter().collect();
        assert!(!trend_and_stalled(Some(&moving)).1);
        assert!(!trend_and_stalled(Some(&idle)).1);
        assert!(!trend_and_stalled(Some(&caught_up)).1);
    }
}
