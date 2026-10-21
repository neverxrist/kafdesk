use crate::avro_codec::decode_datum_to_json;
use crate::domain::{
    matches_decoded_message, matches_raw_pre_filter, BrokerMessageFilter,
};
use crate::sr::SchemaRegistryClient;
use crate::types::*;
use super::decode::{
    decode_owned_message, decode_with_avro_json, headers_from_message,
    strip_confluent_wire,
};
use super::session::KafkaSession;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tauri::{AppHandle, Emitter};

use super::session::SConsumer;

impl KafkaSession {
    pub async fn start_tail(
        &self,
        app: AppHandle,
        request: TailRequest,
    ) -> Result<(), String> {
        self.ensure_admin()?;
        self.stop_tail().await;

        let mut cfg = self.base_config.clone();
        cfg.set(
            "group.id",
            format!("kafdesk-tail-{}", uuid::Uuid::new_v4()),
        );
        cfg.set("enable.auto.commit", "false");
        cfg.set("auto.offset.reset", "latest");
        let consumer: SConsumer = cfg.create().map_err(|e| e.to_string())?;
        consumer
            .subscribe(&[&request.topic])
            .map_err(|e| e.to_string())?;
        self.tailing.store(true, Ordering::SeqCst);

        let filter = BrokerMessageFilter {
            search: request.search.clone(),
            search_key: request.search_key.clone(),
            header_key: request.search_header_key.clone(),
            header_value: request.search_header_value.clone(),
            time_from: None,
            time_to: None,
        };
        let partitions = request.partitions.clone();
        let tailing = self.tailing.clone();
        let registry = if self.sr_status == "ok" {
            self.registry.clone()
        } else {
            None
        };
        let handle = tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = consumer.stream();
            let mut schema_cache: HashMap<i32, String> = HashMap::new();
            while tailing.load(Ordering::SeqCst) {
                match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        let partition = msg.partition();
                        if let Some(ref ps) = partitions {
                            if !ps.contains(&partition) {
                                continue;
                            }
                        }
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
                        let view = decode_tail_message(
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
                        if !matches_decoded_message(&view, &filter) {
                            continue;
                        }
                        let _ = app.emit("kafka://tail-message", &view);
                    }
                    Ok(Some(Err(e))) => {
                        let _ = app.emit("kafka://tail-error", e.to_string());
                        tailing.store(false, Ordering::SeqCst);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => continue,
                }
            }
        });

        *self.tail_task.lock().await = Some(handle);
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

