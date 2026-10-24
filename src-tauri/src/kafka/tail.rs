use crate::avro_codec::decode_datum_to_json;
use crate::sr::SchemaRegistryClient;
use crate::types::*;
use super::decode::{
    cap_message_value, decode_owned_message, decode_with_avro_json, headers_from_message,
    strip_confluent_wire,
};
use super::session::KafkaSession;
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tauri::{AppHandle, Emitter};

use super::session::SConsumer;

/// UI buffer cap — older messages inside one flush window would be evicted anyway.
const TAIL_BATCH_CAP: usize = 100;
/// At most ~4 IPC events/s regardless of topic rate.
const TAIL_FLUSH: Duration = Duration::from_millis(250);

impl KafkaSession {
    pub async fn start_tail(
        &self,
        app: AppHandle,
        request: TailRequest,
    ) -> Result<(), String> {
        self.ensure_admin()?;
        self.stop_tail().await;

        // assign() to End of every partition — no rebalances, nothing committed.
        // assign() still requires `group.id`; reuse the stable consume id.
        let mut cfg = self.base_config.clone();
        cfg.set("group.id", self.consume_group_id());
        cfg.set("coordinator.query.interval.ms", "600000");
        cfg.set("enable.auto.commit", "false");
        cfg.set("auto.offset.reset", "latest");
        let consumer: SConsumer = cfg
            .create_with_context(super::consume_ctx::KafdeskCtx::default())
            .map_err(|e| e.to_string())?;
        let meta = consumer
            .fetch_metadata(Some(&request.topic), Duration::from_secs(5))
            .map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        if let Some(t) = meta.topics().iter().find(|t| t.name() == request.topic) {
            if let Some(err) = t.error() {
                return Err(format!("Topic {}: {:?}", request.topic, err));
            }
            for p in t.partitions() {
                tpl.add_partition_offset(&request.topic, p.id(), Offset::End)
                    .map_err(|e| e.to_string())?;
            }
        }
        if tpl.count() == 0 {
            return Err(format!("Topic not found: {}", request.topic));
        }
        consumer.assign(&tpl).map_err(|e| e.to_string())?;

        self.tailing.store(true, Ordering::SeqCst);

        {
            let mut p = self.tail_partitions.lock().map_err(|e| e.to_string())?;
            *p = request.partitions.clone();
        }

        let tailing = self.tailing.clone();
        let partitions = self.tail_partitions.clone();
        let registry = if self.sr_status == "ok" {
            self.registry.clone()
        } else {
            None
        };
        let handle = tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = consumer.stream();
            let mut schema_cache: HashMap<i32, String> = HashMap::new();
            let mut pending: VecDeque<KafkaMessageView> = VecDeque::new();
            let mut dropped: u64 = 0;
            let mut last_flush = Instant::now() - TAIL_FLUSH;
            while tailing.load(Ordering::SeqCst) {
                let wait = if pending.is_empty() {
                    Duration::from_millis(500)
                } else {
                    TAIL_FLUSH.saturating_sub(last_flush.elapsed())
                };
                match tokio::time::timeout(wait, stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        let partition = msg.partition();
                        {
                            let ps = match partitions.lock() {
                                Ok(g) => g.clone(),
                                Err(_) => continue,
                            };
                            if let Some(ref list) = ps {
                                if !list.contains(&partition) {
                                    continue;
                                }
                            }
                        }
                        let key = msg.key();
                        let headers = headers_from_message(&msg);
                        let ts = match msg.timestamp() {
                            rdkafka::Timestamp::CreateTime(ms)
                            | rdkafka::Timestamp::LogAppendTime(ms) => ms,
                            _ => 0,
                        };
                        let payload_len = msg.payload_len();
                        let mut view = decode_tail_message(
                            &registry,
                            &mut schema_cache,
                            msg.topic(),
                            partition,
                            msg.offset(),
                            ts,
                            key,
                            msg.payload(),
                            headers,
                        )
                        .await;
                        cap_message_value(&mut view, payload_len);
                        pending.push_back(view);
                        if pending.len() > TAIL_BATCH_CAP {
                            pending.pop_front();
                            dropped += 1;
                        }
                    }
                    Ok(Some(Err(e))) => {
                        let _ = app.emit("kafka://tail-error", e.to_string());
                        tailing.store(false, Ordering::SeqCst);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
                if !pending.is_empty() && last_flush.elapsed() >= TAIL_FLUSH {
                    // Drop anything decoded after Stop (race with abort).
                    if !tailing.load(Ordering::SeqCst) {
                        break;
                    }
                    let batch = TailBatch {
                        messages: pending.drain(..).collect(),
                        dropped,
                    };
                    let _ = app.emit("kafka://tail-batch", &batch);
                    dropped = 0;
                    last_flush = Instant::now();
                }
            }
        });

        *self.tail_task.lock().await = Some(handle);
        Ok(())
    }

    /// Hot-swap partition filter without restarting the consumer.
    /// Search/header filters are applied in the UI only (buffer stays intact).
    pub fn set_tail_filter(&self, request: &TailRequest) -> Result<(), String> {
        if !self.tailing.load(Ordering::SeqCst) {
            return Ok(());
        }
        let mut p = self.tail_partitions.lock().map_err(|e| e.to_string())?;
        *p = request.partitions.clone();
        Ok(())
    }

    pub async fn stop_tail(&self) {
        self.tailing.store(false, Ordering::SeqCst);
        if let Some(h) = self.tail_task.lock().await.take() {
            h.abort();
        }
    }
}

async fn decode_tail_message(
    registry: &Option<SchemaRegistryClient>,
    schema_cache: &mut HashMap<i32, String>,
    topic: &str,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers: HashMap<String, String>,
) -> KafkaMessageView {
    let Some(reg) = registry else {
        return decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
    };
    let Some(buf) = payload else {
        return decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
    };
    let Some((id, body)) = strip_confluent_wire(buf) else {
        return decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
    };

    let schema_str = if let Some(s) = schema_cache.get(&id) {
        s.clone()
    } else {
        match reg.get_schema_by_id(id).await {
            Ok(s) => {
                schema_cache.insert(id, s.clone());
                s
            }
            Err(err) => {
                let mut view =
                    decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
                view.format = "avro".into();
                view.schema_id = Some(id);
                view.value = serde_json::json!({ "_decodeError": err, "raw": view.raw_preview });
                return view;
            }
        }
    };

    match decode_datum_to_json(&schema_str, body) {
        Ok(json) => decode_with_avro_json(
            topic,
            partition,
            offset,
            timestamp_ms,
            key,
            payload,
            headers,
            Some(json),
        ),
        Err(err) => {
            let mut view =
                decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
            view.format = "avro".into();
            view.schema_id = Some(id);
            view.value = serde_json::json!({ "_decodeError": err, "raw": view.raw_preview });
            view
        }
    }
}
