//! Exact-id trace across a few topics.
//! Record key → one partition (Kafka murmur2). Body scan is opt-in, capped, and timed.

use super::admin_list_offsets::{list_offsets_chunked, OffsetSpec};
use super::consume_ctx::ConsumeCtx;
use super::session::KafkaSession;
use crate::avro_codec::{decode_datum_to_json, decode_with_schema};
use apache_avro::Schema;
use crate::types::*;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_TOPICS: usize = 20;
const MAX_HITS: usize = 40;
const MAX_NEEDLE: usize = 256;
/// Messages read per topic when also scanning bodies.
const VALUE_CAP: usize = 4_000;
/// Messages read on the single key partition when bodies are off.
const KEY_CAP: usize = 20_000;
const DEADLINE: Duration = Duration::from_secs(12);
const MAX_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const MAX_PARTITIONS: usize = 128;
/// Messages to read from the end when the time lookup did not answer for a partition.
const LOOKUP_TAIL: i64 = 256;

pub async fn trace_key(
    session: &KafkaSession,
    request: TraceRequest,
    generation: u64,
    rt: &tokio::runtime::Handle,
    mut on_progress: impl FnMut(&TraceProgress),
) -> Result<TraceResult, String> {
    let started = Instant::now();
    let needle = request.needle.trim();
    if needle.is_empty() {
        return Err("Empty search".into());
    }
    if needle.len() > MAX_NEEDLE {
        return Err(format!("Search is longer than {MAX_NEEDLE} characters"));
    }
    let mut topics = Vec::new();
    for name in &request.topics {
        let name = name.trim();
        if name.is_empty() || topics.iter().any(|t: &String| t == name) {
            continue;
        }
        topics.push(name.to_string());
        if topics.len() == MAX_TOPICS {
            break;
        }
    }
    if topics.is_empty() {
        return Err("No topics".into());
    }

    let now = now_ms();
    let from_ms = request.from_ms.clamp(now - MAX_WINDOW_MS, now);
    let scan_values = request.scan_values && !needle.contains(['"', '\\', '\n', '\r']);
    let deadline = started + DEADLINE;
    session.ensure_config()?;

    let cfg = session.consume_client_config();
    let consumer: BaseConsumer<ConsumeCtx> =
        BaseConsumer::from_config_and_context(&cfg, ConsumeCtx::default()).map_err(|e| e.to_string())?;

    let mut hits = Vec::new();
    let mut stats = Vec::new();
    let mut cancelled = false;
    let total = topics.len();

    for (idx, topic) in topics.iter().enumerate() {
        if session.trace_stopped(generation) {
            cancelled = true;
            break;
        }
        on_progress(&TraceProgress {
            topic: topic.clone(),
            done: idx,
            total,
            hits: hits.len(),
        });
        if Instant::now() >= deadline {
            stats.push(stat_capped(topic, 0, 0, 0, 0));
            break;
        }
        match scan_topic(
            session,
            rt,
            &consumer,
            topic,
            needle,
            from_ms,
            scan_values,
            deadline,
            generation,
            &mut hits,
        )
        {
            Ok((stat, was_cancelled)) => {
                stats.push(stat);
                if was_cancelled {
                    cancelled = true;
                    break;
                }
            }
            Err(e) => stats.push(TraceTopicStat {
                topic: topic.clone(),
                scanned: 0,
                hits: 0,
                capped: false,
                key_partition: 0,
                partitions: 0,
                error: Some(e),
            }),
        }
        if hits.len() >= MAX_HITS {
            break;
        }
    }

    on_progress(&TraceProgress {
        topic: String::new(),
        done: stats.len(),
        total,
        hits: hits.len(),
    });

    let partial = cancelled
        || hits.len() >= MAX_HITS
        || stats.len() < total
        || stats.iter().any(|s| s.capped);
    Ok(TraceResult {
        hits,
        topics: stats,
        partial,
        cancelled,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

impl KafkaSession {
    pub(crate) fn trace_stopped(&self, generation: u64) -> bool {
        !self.alive.load(Ordering::Relaxed) || self.trace_gen.load(Ordering::Relaxed) != generation
    }
}

struct RawHit {
    partition: i32,
    offset: i64,
    timestamp: i64,
    key: Option<String>,
    matched: &'static str,
    preview: String,
    fields: Vec<String>,
    /// Confluent wire payload, kept only when the body was not JSON and we may need Avro fields.
    wire: Option<Vec<u8>>,
}

fn scan_topic(
    session: &KafkaSession,
    rt: &tokio::runtime::Handle,
    consumer: &BaseConsumer<ConsumeCtx>,
    topic: &str,
    needle: &str,
    from_ms: i64,
    scan_values: bool,
    deadline: Instant,
    generation: u64,
    hits: &mut Vec<TraceHit>,
) -> Result<(TraceTopicStat, bool), String> {
    let meta = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    let tmeta = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("Topic not found: {topic}"))?;
    if let Some(err) = tmeta.error() {
        return Err(format!("{topic}: {err:?}"));
    }
    let part_ids: Vec<i32> = tmeta.partitions().iter().map(|p| p.id()).collect();
    let n = part_ids.len() as i32;
    if n == 0 {
        return Err(format!("Topic not found: {topic}"));
    }
    let key_partition = kafka_partition(needle.as_bytes(), n);
    if scan_values && part_ids.len() > MAX_PARTITIONS {
        return Err(format!(
            "{topic} has {} partitions — body search is limited to {MAX_PARTITIONS}. Search the key only, or narrow the set.",
            part_ids.len()
        ));
    }

    let tps: Vec<(String, i32)> = part_ids.iter().map(|p| (topic.to_string(), *p)).collect();
    let mut highs = HashMap::new();
    if let Ok(admin) = session.shared_admin() {
        highs = list_offsets_chunked(&admin, &tps, OffsetSpec::Latest, Duration::from_secs(6));
    }
    for &p in &part_ids {
        if highs.contains_key(&(topic.to_string(), p)) {
            continue;
        }
        if let Ok((_, high)) = consumer.fetch_watermarks(topic, p, Duration::from_secs(2)) {
            highs.insert((topic.to_string(), p), high);
        }
    }

    let want: Vec<i32> = if scan_values {
        let mut ids = vec![key_partition];
        ids.extend(part_ids.into_iter().filter(|p| *p != key_partition));
        ids
    } else {
        vec![key_partition]
    };
    let time_at = time_offsets(consumer, topic, &want, from_ms)?;

    let mut plan: Vec<(i32, i64, i64)> = Vec::new();
    for p in want {
        let high = highs.get(&(topic.to_string(), p)).copied().unwrap_or(-1);
        if high < 0 {
            continue;
        }
        let start = match time_at.get(&p) {
            Some(TimeStart::At(off)) => (*off).max(0),
            // The broker answered "nothing in this window".
            Some(TimeStart::Empty) => high,
            // No answer for this partition. Read a short tail so a body hit is not skipped.
            Some(TimeStart::Failed) | None => (high - LOOKUP_TAIL).max(0),
        };
        if start < high {
            plan.push((p, start, high));
        }
    }

    let cap = if scan_values { VALUE_CAP } else { KEY_CAP };
    let (raw, scanned, capped, cancel) = read_plan(
        session,
        rt,
        consumer,
        topic,
        needle,
        scan_values,
        &plan,
        cap,
        deadline,
        generation,
        MAX_HITS - hits.len(),
    )?;

    let mut topic_hits = 0u32;
    for raw in raw {
        let mut preview = raw.preview;
        let mut fields = raw.fields;
        let mut matched = raw.matched;
        if let Some(buf) = raw.wire.as_deref() {
            match avro_body(session, rt, buf, needle) {
                AvroBody::Confirmed { preview: p, fields: f } => {
                    preview = p;
                    fields = f;
                }
                // A short number can collide inside Avro bytes. Drop a body-only
                // false positive. A key hit stays, without the body label.
                AvroBody::Rejected => {
                    if matched == "value" {
                        continue;
                    }
                    matched = "key";
                }
                AvroBody::Unknown => {}
            }
        }
        let hit = TraceHit {
            topic: topic.to_string(),
            partition: raw.partition,
            offset: raw.offset.to_string(),
            timestamp: raw.timestamp.to_string(),
            key: raw.key,
            matched: matched.into(),
            preview,
            fields,
        };
        topic_hits += 1;
        hits.push(hit);
    }

    Ok((
        TraceTopicStat {
            topic: topic.to_string(),
            scanned,
            hits: topic_hits,
            capped,
            key_partition,
            partitions: n,
            error: None,
        },
        cancel,
    ))
}

enum TimeStart {
    /// First offset at or after the window start.
    At(i64),
    /// Broker said this partition has nothing in the window.
    Empty,
    /// No usable offset. The caller reads a short tail instead of skipping the partition.
    Failed,
}

fn time_offsets(
    consumer: &BaseConsumer<ConsumeCtx>,
    topic: &str,
    parts: &[i32],
    from_ms: i64,
) -> Result<HashMap<i32, TimeStart>, String> {
    let mut out = HashMap::new();
    // One request for ~100 partitions can time out and used to look like an empty topic.
    for chunk in parts.chunks(48) {
        let mut query = TopicPartitionList::new();
        for &p in chunk {
            query
                .add_partition_offset(topic, p, Offset::Offset(from_ms))
                .map_err(|e| e.to_string())?;
        }
        let res = consumer
            .offsets_for_times(query, Duration::from_secs(6))
            .map_err(|e| e.to_string())?;
        for elem in res.elements_for_topic(topic) {
            let partition = elem.partition();
            // A usable offset wins even when the element also carries an error.
            // Dropping those used to skip the partition that holds the body hit.
            if let Offset::Offset(off) = elem.offset() {
                if off >= 0 {
                    out.insert(partition, TimeStart::At(off));
                    continue;
                }
            }
            if elem.error().is_err() {
                out.insert(partition, TimeStart::Failed);
            } else {
                out.insert(partition, TimeStart::Empty);
            }
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn read_plan(
    session: &KafkaSession,
    rt: &tokio::runtime::Handle,
    consumer: &BaseConsumer<ConsumeCtx>,
    topic: &str,
    needle: &str,
    scan_values: bool,
    plan: &[(i32, i64, i64)],
    cap: usize,
    deadline: Instant,
    generation: u64,
    hit_budget: usize,
) -> Result<(Vec<RawHit>, u64, bool, bool), String> {
    if plan.is_empty() || hit_budget == 0 {
        return Ok((Vec::new(), 0, false, false));
    }
    let mut tpl = TopicPartitionList::new();
    let mut high_of = HashMap::new();
    let mut start_of = HashMap::new();
    for &(p, start, high) in plan {
        tpl.add_partition_offset(topic, p, Offset::Offset(start))
            .map_err(|e| e.to_string())?;
        high_of.insert(p, high);
        start_of.insert(p, start);
    }
    consumer.assign(&tpl).map_err(|e| e.to_string())?;

    let mut open: HashSet<i32> = plan.iter().map(|(p, _, _)| *p).collect();
    let mut raw = Vec::new();
    let mut scanned = 0u64;
    let mut idle = 0u32;
    let mut capped = false;
    let mut cancel = false;
    let mut avro = AvroDecoders::default();
    loop {
        if session.trace_stopped(generation) {
            cancel = true;
            break;
        }
        if open.is_empty() {
            break;
        }
        if raw.len() >= hit_budget {
            break;
        }
        if Instant::now() >= deadline || scanned as usize >= cap {
            capped = !open.is_empty();
            break;
        }
        match consumer.poll(Duration::from_millis(200)) {
            None => {
                // A topic with dozens of partitions delivers in bursts. Stopping after
                // a few quiet seconds used to finish the scan on the first handful.
                if open.len() <= 2 {
                    idle += 1;
                    let limit = if scanned == 0 { 40 } else { 15 };
                    if idle >= limit {
                        capped = !open.is_empty();
                        break;
                    }
                }
            }
            Some(Err(KafkaError::PartitionEOF(p))) => {
                idle = 0;
                open.remove(&p);
            }
            Some(Err(e)) => {
                let msg = e.to_string();
                let lower = msg.to_lowercase();
                if lower.contains("partition eof") || lower.contains("end of partition") {
                    continue;
                }
                let _ = consumer.unassign();
                return Err(msg);
            }
            Some(Ok(msg)) => {
                idle = 0;
                let partition = msg.partition();
                let offset = msg.offset();
                let high = high_of.get(&partition).copied().unwrap_or(i64::MAX);
                let start = start_of.get(&partition).copied().unwrap_or(0);
                if offset < start {
                    continue;
                }
                scanned += 1;
                let key_match = key_eq(msg.key(), needle);
                let value_match =
                    scan_values && body_has_needle(session, rt, &mut avro, msg.payload(), needle);
                if let Some(matched) = match_kind(key_match, value_match) {
                    raw.push(raw_hit(&msg, matched, needle));
                }
                if offset + 1 >= high {
                    open.remove(&partition);
                }
            }
        }
    }
    let _ = consumer.unassign();
    Ok((raw, scanned, capped, cancel))
}

fn raw_hit(msg: &rdkafka::message::BorrowedMessage<'_>, matched: &'static str, needle: &str) -> RawHit {
    let ts = match msg.timestamp() {
        rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => ms,
        _ => 0,
    };
    let key = msg.key().map(|k| String::from_utf8_lossy(k).into_owned());
    let payload = msg.payload().unwrap_or_default();
    let (preview, fields) = if payload.len() > 1024 * 1024 {
        (format!("{} KB", payload.len() / 1024), Vec::new())
    } else {
        preview_and_fields(payload, needle)
    };
    let wire = if matched != "key" && payload.first() == Some(&0) && payload.len() <= 256 * 1024 {
        Some(payload.to_vec())
    } else {
        None
    };
    RawHit {
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp: ts,
        key,
        matched,
        preview,
        fields,
        wire,
    }
}

enum AvroBody {
    Confirmed { preview: String, fields: Vec<String> },
    Rejected,
    Unknown,
}

/// Decode a hit only. Schema is fetched at most once per id and then cached.
fn avro_body(session: &KafkaSession, rt: &tokio::runtime::Handle, payload: &[u8], needle: &str) -> AvroBody {
    let Some((id, body)) = super::decode::strip_confluent_wire(payload) else {
        return AvroBody::Unknown;
    };
    let Some(schema) = schema_text(session, rt, id) else {
        return AvroBody::Unknown;
    };
    let Ok(json) = decode_datum_to_json(&schema, body) else {
        return AvroBody::Unknown;
    };
    if !json_contains_needle(&json, needle) {
        // A long id in the raw bytes is the hit. A short number that only
        // collided inside Avro binary is not.
        if needle_is_specific(needle) {
            return AvroBody::Unknown;
        }
        return AvroBody::Rejected;
    }
    let mut fields = Vec::new();
    collect_ids(&json, needle, &mut fields, 0);
    let preview = match &json {
        Value::String(s) => clip(s, 180),
        other => clip(&other.to_string(), 180),
    };
    AvroBody::Confirmed { preview, fields }
}

fn schema_text(session: &KafkaSession, rt: &tokio::runtime::Handle, id: i32) -> Option<String> {
    if session.sr_status != "ok" {
        return None;
    }
    if let Some(cached) = session.schema_cache.lock().ok().and_then(|c| c.get(&id).cloned()) {
        return Some(cached);
    }
    let reg = session.registry.clone()?;
    let fetched = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(4), reg.get_schema_by_id(id)).await
    });
    match fetched {
        Ok(Ok(schema)) => {
            if let Ok(mut cache) = session.schema_cache.lock() {
                cache.insert(id, schema.clone());
            }
            Some(schema)
        }
        _ => None,
    }
}

#[derive(Default)]
struct AvroDecoders {
    parsed: HashMap<i32, Schema>,
    failed: HashSet<i32>,
}

impl AvroDecoders {
    /// Same text Consume searches: the Avro record decoded to JSON.
    fn matches(
        &mut self,
        session: &KafkaSession,
        rt: &tokio::runtime::Handle,
        payload: &[u8],
        needle: &str,
    ) -> bool {
        let Some((id, body)) = super::decode::strip_confluent_wire(payload) else {
            return false;
        };
        if self.failed.contains(&id) {
            return false;
        }
        if !self.parsed.contains_key(&id) {
            let Some(text) = schema_text(session, rt, id) else {
                self.failed.insert(id);
                return false;
            };
            match Schema::parse_str(&text) {
                Ok(schema) => {
                    self.parsed.insert(id, schema);
                }
                Err(_) => {
                    self.failed.insert(id);
                    return false;
                }
            }
        }
        let Some(schema) = self.parsed.get(&id) else {
            return false;
        };
        let Ok(json) = decode_with_schema(schema, body) else {
            return false;
        };
        json_contains_needle(&json, needle)
    }
}

fn body_has_needle(
    session: &KafkaSession,
    rt: &tokio::runtime::Handle,
    avro: &mut AvroDecoders,
    payload: Option<&[u8]>,
    needle: &str,
) -> bool {
    if payload_has_value(payload, needle) {
        return true;
    }
    let Some(buf) = payload else {
        return false;
    };
    if buf.first() != Some(&0) || buf.len() > 256 * 1024 {
        return false;
    }
    avro.matches(session, rt, buf, needle)
}

fn value_has_exact_id(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(s) => s.eq_ignore_ascii_case(needle),
        Value::Number(n) => n.as_i64().is_some_and(|i| i.to_string().eq_ignore_ascii_case(needle)),
        Value::Object(map) => map.values().any(|v| value_has_exact_id(v, needle)),
        Value::Array(items) => items.iter().take(8).any(|v| value_has_exact_id(v, needle)),
        _ => false,
    }
}

fn preview_and_fields(payload: &[u8], needle: &str) -> (String, Vec<String>) {
    let text = String::from_utf8_lossy(payload);
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        let mut fields = Vec::new();
        collect_ids(&value, needle, &mut fields, 0);
        let preview = match &value {
            Value::String(s) => clip(s, 180),
            other => clip(&other.to_string(), 180),
        };
        return (preview, fields);
    }
    (clip(trimmed, 180), Vec::new())
}

fn collect_ids(value: &Value, needle: &str, out: &mut Vec<String>, depth: u8) {
    if out.len() >= 6 || depth > 3 {
        return;
    }
    match value {
        Value::Object(map) => {
            for v in map.values() {
                collect_ids(v, needle, out, depth + 1);
            }
        }
        Value::Array(items) => {
            for v in items.iter().take(8) {
                collect_ids(v, needle, out, depth + 1);
            }
        }
        Value::String(s) => push_id(s, needle, out),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                push_id(&i.to_string(), needle, out);
            }
        }
        _ => {}
    }
}

fn push_id(s: &str, needle: &str, out: &mut Vec<String>) {
    if id_like(s) && !s.eq_ignore_ascii_case(needle) && !out.iter().any(|e| e == s) {
        out.push(s.to_string());
    }
}

fn id_like(s: &str) -> bool {
    let n = s.len();
    (3..=64).contains(&n)
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        && (s.chars().any(|c| c.is_ascii_digit()) || s.contains('-') || s.contains('_'))
}

fn clip(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    let end: String = s.chars().take(max).collect();
    format!("{end}…")
}

fn stat_capped(topic: &str, scanned: u64, hits: u32, key_partition: i32, partitions: i32) -> TraceTopicStat {
    TraceTopicStat {
        topic: topic.to_string(),
        scanned,
        hits,
        capped: true,
        key_partition,
        partitions,
        error: None,
    }
}

fn key_eq(key: Option<&[u8]>, needle: &str) -> bool {
    let Some(key) = key else { return false };
    if key == needle.as_bytes() {
        return true;
    }
    needle.is_ascii()
        && key.len() == needle.len()
        && key
            .iter()
            .zip(needle.bytes())
            .all(|(a, b)| a.to_ascii_lowercase() == b.to_ascii_lowercase())
}

/// Long tokens (UUIDs) also sit in Avro as raw UTF-8. The byte before them is the
/// length varint, which for a 36-char id is `H`, so a plain word-boundary check would miss it.
fn payload_has_bounded_token(payload: Option<&[u8]>, needle: &str) -> bool {
    let Some(payload) = payload else { return false };
    if needle.len() < 16 || !needle.is_ascii() || needle.contains(['"', '\\']) {
        return false;
    }
    let lower: Vec<u8> = needle.bytes().map(|b| b.to_ascii_lowercase()).collect();
    let avro_len = avro_string_len_byte(needle.len());
    payload.windows(lower.len()).enumerate().any(|(i, w)| {
        if w.iter().zip(&lower).any(|(a, b)| a.to_ascii_lowercase() != *b) {
            return false;
        }
        // Avro string: the length varint already says the field is exactly this long.
        if i > 0 && avro_len.is_some_and(|b| payload[i - 1] == b) {
            return true;
        }
        let before_ok = i == 0 || !is_token_byte(payload[i - 1]);
        let after = i + lower.len();
        let after_ok = after >= payload.len() || !is_token_byte(payload[after]);
        before_ok && after_ok
    })
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

fn avro_string_len_byte(len: usize) -> Option<u8> {
    if len >= 64 {
        return None;
    }
    Some((len << 1) as u8)
}

fn payload_has_value(payload: Option<&[u8]>, needle: &str) -> bool {
    if payload_has_json_string(payload, needle) || payload_has_bounded_token(payload, needle) {
        return true;
    }
    // A checkout-style id is specific. It does not have to be the entire JSON string.
    needle_is_specific(needle) && payload_contains_needle(payload, needle)
}

fn needle_is_specific(needle: &str) -> bool {
    needle.len() >= 8 && needle.is_ascii() && needle.bytes().any(|b| !b.is_ascii_digit())
}

fn payload_contains_needle(payload: Option<&[u8]>, needle: &str) -> bool {
    let Some(payload) = payload else { return false };
    if needle.is_empty() || !needle.is_ascii() {
        return false;
    }
    let needle = needle.as_bytes();
    (0..payload.len()).any(|i| match_needle_at(payload, i, needle).is_some())
}

fn json_contains_needle(value: &Value, needle: &str) -> bool {
    if value_has_exact_id(value, needle) {
        return true;
    }
    if !needle_is_specific(needle) {
        return false;
    }
    let text = value.to_string();
    let needle = needle.to_ascii_lowercase();
    text.to_ascii_lowercase().contains(&needle)
}

fn payload_has_json_string(payload: Option<&[u8]>, needle: &str) -> bool {
    let Some(payload) = payload else { return false };
    if needle.is_empty() || needle.contains(['"', '\\']) {
        return false;
    }
    let needle = needle.as_bytes();
    payload.iter().enumerate().any(|(i, b)| {
        if *b != b'"' {
            return false;
        }
        match_needle_at(payload, i + 1, needle).is_some_and(|end| end < payload.len() && payload[end] == b'"')
    })
}

/// ASCII case-insensitive. A `/` in the needle also matches a JSON `\/`.
fn match_needle_at(hay: &[u8], mut at: usize, needle: &[u8]) -> Option<usize> {
    for &n in needle {
        if at >= hay.len() {
            return None;
        }
        if n == b'/' && hay[at] == b'\\' {
            at += 1;
            if at >= hay.len() {
                return None;
            }
        }
        if hay[at].to_ascii_lowercase() != n.to_ascii_lowercase() {
            return None;
        }
        at += 1;
    }
    Some(at)
}

/// Kafka `Utils.murmur2` + `toPositive` % partition count. Same as the Java default partitioner.
pub(crate) fn kafka_partition(key: &[u8], partitions: i32) -> i32 {
    if partitions <= 1 {
        return 0;
    }
    let positive = (murmur2(key) as u32) & 0x7fff_ffff;
    (positive % partitions as u32) as i32
}

fn murmur2(data: &[u8]) -> i32 {
    let m: i32 = 0x5bd1_e995u32 as i32;
    let mut h: i32 = (0x9747_b28cu32 as i32) ^ (data.len() as i32);
    let mut i = 0;
    while i + 4 <= data.len() {
        let mut k = (data[i] as i32)
            | ((data[i + 1] as i32) << 8)
            | ((data[i + 2] as i32) << 16)
            | ((data[i + 3] as i32) << 24);
        k = k.wrapping_mul(m);
        k ^= ((k as u32) >> 24) as i32;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
        i += 4;
    }
    let rest = data.len() & !3;
    match data.len() % 4 {
        3 => {
            h ^= (data[rest + 2] as i32) << 16;
            h ^= (data[rest + 1] as i32) << 8;
            h ^= data[rest] as i32;
            h = h.wrapping_mul(m);
        }
        2 => {
            h ^= (data[rest + 1] as i32) << 8;
            h ^= data[rest] as i32;
            h = h.wrapping_mul(m);
        }
        1 => {
            h ^= data[rest] as i32;
            h = h.wrapping_mul(m);
        }
        _ => {}
    }
    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(m);
    h ^= ((h as u32) >> 15) as i32;
    h
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// A record can carry the same id in the key and in the body.
fn match_kind(key_match: bool, value_match: bool) -> Option<&'static str> {
    match (key_match, value_match) {
        (false, false) => None,
        (true, true) => Some("both"),
        (true, false) => Some("key"),
        (false, true) => Some("value"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_kind_reports_key_and_value_together() {
        assert_eq!(match_kind(true, true), Some("both"));
        assert_eq!(match_kind(true, false), Some("key"));
        assert_eq!(match_kind(false, true), Some("value"));
        assert_eq!(match_kind(false, false), None);
    }

    #[test]
    fn kafka_partitioner_matches_java_murmur2() {
        assert_eq!(kafka_partition(b"123", 12), 5);
        assert_eq!(kafka_partition(b"order-1", 12), 10);
        assert_eq!(
            kafka_partition(b"550e8400-e29b-41d4-a716-446655440000", 3),
            2
        );
        assert_eq!(kafka_partition(b"123", 1), 0);
    }

    #[test]
    fn json_string_match_is_exact_and_case_insensitive() {
        let body = br#"{"preorderNumber":"AbC-1","note":"see AbC-10"}"#;
        assert!(payload_has_json_string(Some(body), "abc-1"));
        assert!(!payload_has_json_string(Some(body), "abc-10"));
        assert!(!payload_has_json_string(Some(body), "abc"));
        assert!(!payload_has_json_string(Some(body), "AbC-1 extra"));
        let slash = br#"{"id":"135CHECKOUT-OMS\/135614"}"#;
        assert!(payload_has_json_string(Some(slash), "135CHECKOUT-OMS/135614"));
        let plain = br#"{"id":"135CHECKOUT-OMS/135614","next":"135CHECKOUT-OMS/1356149"}"#;
        assert!(payload_has_json_string(Some(plain), "135CHECKOUT-OMS/135614"));
        assert!(payload_has_json_string(Some(plain), "135CHECKOUT-OMS/1356149"));
        assert!(!payload_has_json_string(Some(plain), "135CHECKOUT-OMS/13561"));
        let embedded = br#"{"note":"ref 135CHECKOUT-OMS/135614 tail"}"#;
        assert!(payload_has_value(Some(embedded), "135CHECKOUT-OMS/135614"));
        assert!(!payload_has_value(Some(br#"{"n":10001999}"#), "10001"));
    }

    #[test]
    fn avro_string_field_is_visible_in_the_body() {
        let schema = r#"{"type":"record","name":"M","fields":[
            {"name":"tracking","type":"string"},
            {"name":"maybe","type":["null","string"],"default":null}
        ]}"#;
        let json = serde_json::json!({
            "tracking": "783CHECKOUT-OMS/L15972",
            "maybe": "ref 783CHECKOUT-OMS/L15972 tail"
        });
        let body = crate::avro_codec::encode_json_with_schema(schema, &json).expect("encode");
        let wire = super::super::decode::wrap_confluent_avro(7, &body);
        assert!(
            payload_has_value(Some(&wire), "783CHECKOUT-OMS/L15972"),
            "avro bytes did not contain the id"
        );
        assert!(
            payload_has_value(Some(&body), "783CHECKOUT-OMS/L15972"),
            "bare avro bytes did not contain the id"
        );
    }

    #[test]
    fn key_match_ignores_ascii_case_only() {
        assert!(key_eq(Some(b"ABC-1"), "abc-1"));
        assert!(!key_eq(Some(b"ABC-10"), "abc-1"));
        assert!(!key_eq(None, "abc-1"));
    }

    #[test]
    fn id_fields_skip_the_needle_and_prose() {
        let v: Value = serde_json::json!({"preorderNumber": "uuid-1", "orderNumber": "10001", "note": "hello"});
        let mut out = Vec::new();
        collect_ids(&v, "uuid-1", &mut out, 0);
        assert_eq!(out, vec!["10001".to_string()]);
    }

    #[test]
    fn numeric_order_id_is_a_follow_chip() {
        let v: Value = serde_json::json!({"orderNumber": 10001, "note": "hello"});
        let mut out = Vec::new();
        collect_ids(&v, "550e8400-e29b-41d4-a716-446655440000", &mut out, 0);
        assert_eq!(out, vec!["10001".to_string()]);
    }

    #[test]
    fn avro_uuid_bytes_match_without_quotes() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let mut body = vec![avro_string_len_byte(id.len()).unwrap()];
        body.extend_from_slice(id.as_bytes());
        body.push(0);
        assert!(payload_has_bounded_token(Some(&body), id));
        let mut glued = body.clone();
        glued.pop();
        glued.push(b'a');
        assert!(payload_has_bounded_token(Some(&glued), id));
        let mut bare = id.as_bytes().to_vec();
        bare.push(b'a');
        assert!(!payload_has_bounded_token(Some(&bare), id));
        assert!(!payload_has_bounded_token(Some(br#""10001""#), "10001"));
    }
}
