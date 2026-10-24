//! Live-cluster smoke test (ignored by default). Run against the local stack:
//! `KAFDESK_SMOKE_BROKERS=localhost:19092 cargo test smoke -- --ignored --nocapture`
//! Optional: KAFDESK_SMOKE_USER / KAFDESK_SMOKE_PASSWORD (default kafdesk / kafdesk-secret).

use super::session::KafkaSession;
use crate::types::*;
use serde_json::json;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

const SMOKE_GROUP: &str = "smoke-lag-group";
const SMOKE_TOPIC: &str = "demo-big";
/// Auto-created on first produce; keeps demo topics clean.
const BIG_TOPIC: &str = "kafdesk-smoke";

fn smoke_config() -> Option<ClusterConfig> {
    let brokers = std::env::var("KAFDESK_SMOKE_BROKERS").ok()?;
    let user = std::env::var("KAFDESK_SMOKE_USER").unwrap_or_else(|_| "kafdesk".into());
    let pass = std::env::var("KAFDESK_SMOKE_PASSWORD").unwrap_or_else(|_| "kafdesk-secret".into());
    serde_json::from_value(json!({
        "id": "smoke",
        "name": "smoke",
        "brokers": [brokers],
        "authMode": "sasl",
        "sasl": { "mechanism": "scram-sha-256", "username": user, "password": pass },
    }))
    .ok()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_live_cluster() {
    let Some(cfg) = smoke_config() else {
        eprintln!("KAFDESK_SMOKE_BROKERS not set — skip");
        return;
    };
    let mut s = KafkaSession::new();
    s.connect(cfg).await.expect("connect");

    let t = Instant::now();
    let topics = s.list_topics().expect("list_topics");
    eprintln!("list_topics: {} topics in {:?}", topics.len(), t.elapsed());
    assert!(topics.iter().any(|t| t.name == SMOKE_TOPIC));

    let t = Instant::now();
    let info = s.get_topic_offsets(SMOKE_TOPIC).expect("get_topic_offsets");
    eprintln!("get_topic_offsets: {:?} in {:?}", info.approx_messages, t.elapsed());
    assert!(!info.partitions.is_empty());

    let names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
    let t = Instant::now();
    let batched = s.topics_with_offsets(&names).expect("topics_with_offsets");
    eprintln!("topics_with_offsets: {} topics in {:?}", batched.len(), t.elapsed());
    assert_eq!(batched.len(), names.len());
    let t = Instant::now();
    for b in &batched {
        let legacy = s.get_topic_offsets_watermarks(&b.name).expect("watermarks");
        assert_eq!(b.approx_messages, legacy.approx_messages, "topic {}", b.name);
        for (bp, lp) in b.partitions.iter().zip(&legacy.partitions) {
            assert_eq!((&bp.offset_low, &bp.offset_high), (&lp.offset_low, &lp.offset_high));
        }
    }
    eprintln!("legacy watermarks loop: {:?}", t.elapsed());

    let worker = s.clone_for_worker().expect("worker");
    assert!(Arc::ptr_eq(worker.admin.as_ref().unwrap(), s.admin.as_ref().unwrap()));
    assert_eq!(worker.list_topics().expect("worker list").len(), topics.len());

    let req: ConsumeRequest =
        serde_json::from_value(json!({ "topic": "demo-json", "startFrom": "beginning", "limit": 10 }))
            .unwrap();
    let t = Instant::now();
    let msgs = s
        .consume_cancellable(req, Arc::new(AtomicU64::new(0)), 0)
        .await
        .expect("consume");
    eprintln!("consume demo-json: {} msgs in {:?}", msgs.len(), t.elapsed());
    assert!(!msgs.is_empty());

    // Oversized value: list view is capped, fetch_message returns it whole.
    let big = "x".repeat(600 * 1024);
    let produced = s
        .produce(
            serde_json::from_value(json!({ "topic": BIG_TOPIC, "value": big, "format": "string" }))
                .unwrap(),
        )
        .await
        .expect("produce big");
    let off: i64 = produced.offset.as_deref().unwrap().parse().unwrap();
    let req: ConsumeRequest = serde_json::from_value(
        json!({ "topic": BIG_TOPIC, "startFrom": "offset", "startOffset": off.to_string(), "limit": 1 }),
    )
    .unwrap();
    let got = s
        .consume_cancellable(req, Arc::new(AtomicU64::new(0)), 0)
        .await
        .expect("consume big");
    let row = got.iter().find(|m| m.offset == off.to_string()).expect("big row");
    assert_eq!(row.truncated_bytes, Some(big.len() as u64));
    assert!(row.value.as_str().unwrap().len() <= super::decode::MAX_VALUE_BYTES + 4);
    let full = s
        .fetch_message(BIG_TOPIC, produced.partition, off)
        .await
        .expect("fetch_message");
    assert_eq!(full.truncated_bytes, None);
    assert_eq!(full.value.as_str().map(str::len), Some(big.len()));
    eprintln!("big message: capped in list, full via fetch_message");

    // Group with committed offsets at 0 → lag == log end on every partition.
    tokio::task::block_in_place(|| {
        s.set_group_offsets(SetGroupOffsetsRequest {
            group_id: SMOKE_GROUP.into(),
            topic: SMOKE_TOPIC.into(),
            partitions: info
                .partitions
                .iter()
                .map(|p| PartitionOffset { partition: p.partition_id, offset: "0".into() })
                .collect(),
        })
    })
    .expect("set_group_offsets");
    let expected_lag: i64 = info
        .partitions
        .iter()
        .filter_map(|p| p.offset_high.as_deref()?.parse::<i64>().ok())
        .sum();

    let t = Instant::now();
    let snap = tokio::task::block_in_place(|| s.refresh_group_snapshot()).expect("snapshot");
    eprintln!(
        "snapshot: listed={} scanned={} failed={} partial={} in {:?}",
        snap.listed,
        snap.scanned,
        snap.failed,
        snap.partial,
        t.elapsed()
    );
    let row = snap
        .groups
        .iter()
        .find(|g| g.group_id == SMOKE_GROUP)
        .expect("smoke group in snapshot");
    assert_eq!(row.total_lag, Some(expected_lag));

    let tc = s.topic_consumers(SMOKE_TOPIC).expect("topic consumers");
    let tr = tc.groups.iter().find(|g| g.group_id == SMOKE_GROUP).expect("topic row");
    assert_eq!(tr.lag, Some(expected_lag));
    assert!(s
        .topic_consumers("demo-string")
        .unwrap()
        .groups
        .iter()
        .all(|g| g.group_id != SMOKE_GROUP));

    let lag = tokio::task::block_in_place(|| s.get_lag_summary(Some(SMOKE_TOPIC), vec![]))
        .expect("lag summary");
    assert!(lag.groups.iter().any(|g| g.group_id == SMOKE_GROUP && g.total_lag == expected_lag));
    let stale = s.clone_for_worker().expect("worker");
    s.disconnect().await;
    assert!(tokio::task::block_in_place(|| stale.refresh_group_snapshot()).is_err());
    assert!(s.topic_consumers(SMOKE_TOPIC).is_none());
    eprintln!("ok: expected lag {expected_lag}");
}
