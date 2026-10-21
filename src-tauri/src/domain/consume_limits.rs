pub const CONSUME_LIMIT_MAX: i32 = 500;
pub const PRODUCE_BATCH_MAX: usize = 2000;

pub fn clamp_consume_limit(limit: Option<i32>) -> i32 {
    let n = limit.unwrap_or(25);
    n.clamp(1, CONSUME_LIMIT_MAX)
}

pub fn consume_wait_ms(
    limit: i32,
    partition_count: usize,
    has_filter: bool,
    start_from: &str,
) -> u64 {
    let mut ms = 4_000
        + (limit as u64) * 25
        + (partition_count as u64) * 250
        + if has_filter { 4_000 } else { 0 }
        + if start_from == "beginning" { 1_500 } else { 0 };
    ms = ms.min(30_000);
    ms
}

pub fn consume_idle_ms(has_filter: bool) -> u64 {
    if has_filter {
        1_200
    } else {
        800
    }
}
