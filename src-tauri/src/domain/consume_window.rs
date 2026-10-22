use super::consume_limits::{clamp_consume_limit, consume_idle_ms, consume_wait_ms};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct PartitionWatermark {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

#[derive(Debug, Clone)]
pub struct PartitionTimestampSeek {
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct ConsumeWindowInput {
    pub start_from: String,
    pub start_offset: Option<String>,
    pub limit: Option<i32>,
    pub partitions: Option<Vec<i32>>,
    pub offsets_by_timestamp: Option<Vec<PartitionTimestampSeek>>,
    pub time_from_seek_failed: bool,
    pub has_content_filter: bool,
    pub has_time_filter: bool,
    pub topic_offsets: Vec<PartitionWatermark>,
}

#[derive(Debug, Clone)]
pub struct ConsumeWindowPlan {
    pub limit: i32,
    pub has_filter: bool,
    pub wait_ms: u64,
    pub idle_ms: u64,
    pub target_partitions: Vec<i32>,
    pub seek_plan: Vec<(i32, i64)>,
    pub planned_start: HashMap<i32, i64>,
    pub empty: bool,
    pub newest_first: bool,
}

fn approx_for(target: &[i32], high_by: &HashMap<i32, &PartitionWatermark>) -> i64 {
    target.iter().fold(0i64, |sum, p| {
        if let Some(o) = high_by.get(p) {
            sum + (o.high - o.low).max(0)
        } else {
            sum
        }
    })
}

pub fn plan_consume_window(input: ConsumeWindowInput) -> ConsumeWindowPlan {
    let limit = clamp_consume_limit(input.limit);
    let start_from = input.start_from.clone();
    let has_filter = input.has_content_filter || input.has_time_filter;
    let high_by: HashMap<i32, &PartitionWatermark> = input
        .topic_offsets
        .iter()
        .map(|o| (o.partition, o))
        .collect();
    let target_partitions = if let Some(ref ps) = input.partitions {
        if !ps.is_empty() {
            ps.clone()
        } else {
            input.topic_offsets.iter().map(|o| o.partition).collect()
        }
    } else {
        input.topic_offsets.iter().map(|o| o.partition).collect()
    };

    let approx_available = approx_for(&target_partitions, &high_by);
    let wait_ms = consume_wait_ms(limit, target_partitions.len(), has_filter, &start_from);
    let idle_ms = consume_idle_ms(has_filter);
    let newest_first = start_from == "end";

    let mut seek_plan = Vec::new();
    let mut planned_start = HashMap::new();

    if approx_available == 0 && start_from != "offset" {
        return ConsumeWindowPlan {
            limit,
            has_filter,
            wait_ms,
            idle_ms,
            target_partitions,
            seek_plan,
            planned_start,
            empty: true,
            newest_first,
        };
    }

    let end_lookback = if has_filter {
        approx_available
            .max(limit as i64)
            .min((limit as i64 * 50).max(2_000))
    } else {
        limit as i64
    };

    if start_from == "offset" {
        if let Some(ref so) = input.start_offset {
            let raw = so.parse::<i64>().unwrap_or(0);
            for &partition in &target_partitions {
                if let Some(o) = high_by.get(&partition) {
                    let start = raw.clamp(o.low, o.high);
                    planned_start.insert(partition, start);
                    if start < o.high {
                        seek_plan.push((partition, start));
                    }
                }
            }
        }
    } else if let Some(ref by_ts) = input.offsets_by_timestamp {
        for row in by_ts {
            if !target_partitions.contains(&row.partition) {
                continue;
            }
            if let Some(o) = high_by.get(&row.partition) {
                let start = row.offset.clamp(o.low, o.high);
                planned_start.insert(row.partition, start);
                if start < o.high {
                    seek_plan.push((row.partition, start));
                }
            }
        }
    } else if input.time_from_seek_failed {
        for &partition in &target_partitions {
            if let Some(o) = high_by.get(&partition) {
                planned_start.insert(partition, o.low);
                seek_plan.push((partition, o.low));
            }
        }
    } else if start_from == "end" {
        for &partition in &target_partitions {
            if let Some(o) = high_by.get(&partition) {
                let start = (o.high - end_lookback).max(o.low);
                planned_start.insert(partition, start);
                if start < o.high {
                    seek_plan.push((partition, start));
                }
            }
        }
    } else {
        for &partition in &target_partitions {
            if let Some(o) = high_by.get(&partition) {
                planned_start.insert(partition, o.low);
                if o.low < o.high {
                    seek_plan.push((partition, o.low));
                }
            }
        }
    }

    let mut finished_empty = 0;
    for &partition in &target_partitions {
        match high_by.get(&partition) {
            None => finished_empty += 1,
            Some(o) => {
                let start = planned_start.get(&partition);
                if o.high <= o.low || start.map(|s| *s >= o.high).unwrap_or(false) {
                    finished_empty += 1;
                }
            }
        }
    }

    let empty = target_partitions.is_empty() || finished_empty >= target_partitions.len();
    ConsumeWindowPlan {
        limit,
        has_filter,
        wait_ms,
        idle_ms,
        target_partitions,
        seek_plan,
        planned_start,
        empty,
        newest_first,
    }
}

pub fn initial_finished_partitions(
    target: &[i32],
    topic_offsets: &[PartitionWatermark],
    planned_start: &HashMap<i32, i64>,
) -> HashSet<i32> {
    let high_by: HashMap<i32, &PartitionWatermark> =
        topic_offsets.iter().map(|o| (o.partition, o)).collect();
    let mut finished = HashSet::new();
    for &partition in target {
        match high_by.get(&partition) {
            None => {
                finished.insert(partition);
            }
            Some(o) => {
                let start = planned_start.get(&partition);
                if o.high <= o.low || start.map(|s| *s >= o.high).unwrap_or(false) {
                    finished.insert(partition);
                }
            }
        }
    }
    finished
}
