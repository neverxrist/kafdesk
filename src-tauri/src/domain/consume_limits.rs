pub const CONSUME_LIMIT_MAX: i32 = 1000;
pub const PRODUCE_BATCH_MAX: usize = 2000;

pub fn clamp_consume_limit(limit: Option<i32>) -> i32 {
    let n = limit.unwrap_or(30);
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
    // Keep windows short — unreachable advertised brokers must not pin the IPC for 30s.
    ms = ms.min(12_000);
    ms
}

pub fn consume_idle_ms(has_filter: bool) -> u64 {
    if has_filter {
        1_200
    } else {
        800
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_consume_limit_bounds() {
        assert_eq!(clamp_consume_limit(None), 30);
        assert_eq!(clamp_consume_limit(Some(0)), 1);
        assert_eq!(clamp_consume_limit(Some(-5)), 1);
        assert_eq!(clamp_consume_limit(Some(500)), 500);
        assert_eq!(clamp_consume_limit(Some(1_000_000)), CONSUME_LIMIT_MAX);
    }

    #[test]
    fn consume_wait_is_capped() {
        assert_eq!(consume_wait_ms(1000, 200, true, "beginning"), 12_000);
        assert!(consume_wait_ms(1, 1, false, "end") < 12_000);
    }
}
