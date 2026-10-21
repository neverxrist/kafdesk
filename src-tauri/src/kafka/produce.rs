use crate::avro_codec::encode_json_with_schema;
use crate::domain::PRODUCE_BATCH_MAX;
use crate::types::*;
use super::decode::{
    parse_json_loose, wrap_confluent_avro,
};
use super::session::KafkaSession;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use serde_json::Value;
use std::time::Duration;

impl KafkaSession {
    async fn encode_value(
        &self,
        format: &str,
        value: &str,
        subject: Option<&str>,
        local_schema: Option<&str>,
        local_schema_id: Option<i32>,
    ) -> Result<(Vec<u8>, Option<i32>), String> {
        match format {
            "avro" => {
                if self.sr_status != "ok" {
                    return Err(self
                        .sr_error
                        .clone()
                        .unwrap_or_else(|| "Schema Registry is not available".into()));
                }
                let reg = self
                    .registry
                    .as_ref()
                    .ok_or("Schema Registry is not configured")?;
                let subject = subject.ok_or("Avro subject is required")?;
                let schema_id = tokio::time::timeout(
                    Duration::from_secs(20),
                    reg.get_latest_schema_id(subject),
                )
                .await
                .map_err(|_| "Schema Registry getLatestSchemaId timed out".to_string())?
                .map_err(|e| e)?;
                let schema_str = tokio::time::timeout(
                    Duration::from_secs(20),
                    reg.get_schema_by_id(schema_id),
                )
                .await
                .map_err(|_| "Schema Registry getSchemaById timed out".to_string())?
                .map_err(|e| e)?;
                let v: Value = parse_json_loose(value);
                let bytes = encode_json_with_schema(&schema_str, &v)?;
                Ok((wrap_confluent_avro(schema_id, &bytes), Some(schema_id)))
            }
            "avro-local" => {
                let schema_str = local_schema
                    .filter(|s| !s.trim().is_empty())
                    .ok_or("Local .avsc schema is required")?;
                let schema_id = local_schema_id.unwrap_or(0);
                let v: Value = parse_json_loose(value);
                let bytes = encode_json_with_schema(schema_str, &v)?;
                Ok((wrap_confluent_avro(schema_id, &bytes), Some(schema_id)))
            }
            "json" => {
                let parsed = parse_json_loose(value);
                let s = if parsed.is_string() {
                    parsed.as_str().unwrap_or(value).to_string()
                } else {
                    serde_json::to_string(&parsed).unwrap_or_else(|_| value.to_string())
                };
                Ok((s.into_bytes(), None))
            }
            _ => Ok((value.as_bytes().to_vec(), None)),
        }
    }
    pub async fn produce(&self, request: ProduceRequest) -> Result<ProduceResult, String> {
        self.ensure_producer()?;
        let producer = self.producer.as_ref().unwrap();
        let (payload, schema_id) = self
            .encode_value(
                &request.format,
                &request.value,
                request.subject.as_deref(),
                request.local_schema.as_deref(),
                request.local_schema_id,
            )
            .await?;

        let mut record = FutureRecord::to(&request.topic).payload(&payload);
        let key_owned = request.key.clone();
        if let Some(ref k) = key_owned {
            record = record.key(k);
        }
        if let Some(p) = request.partition {
            record = record.partition(p);
        }
        if let Some(ref headers) = request.headers {
            let mut owned = OwnedHeaders::new();
            for (k, v) in headers {
                owned = owned.insert(Header {
                    key: k,
                    value: Some(v.as_bytes()),
                });
            }
            record = record.headers(owned);
        }

        let delivery = producer
            .send(record, Duration::from_secs(45))
            .await
            .map_err(|(e, _)| e.to_string())?;

        Ok(ProduceResult {
            partition: delivery.partition,
            offset: Some(delivery.offset.to_string()),
            format: request.format,
            schema_id,
        })
    }
    pub async fn produce_batch(
        &self,
        request: ProduceBatchRequest,
    ) -> Result<ProduceBatchResult, String> {
        self.ensure_producer()?;
        if request.messages.is_empty() {
            return Ok(ProduceBatchResult {
                sent: 0,
                format: request.format.clone(),
                schema_id: None,
                last_partition: None,
                last_offset: None,
            });
        }
        if request.messages.len() > PRODUCE_BATCH_MAX {
            return Err(format!(
                "Bulk limit is {PRODUCE_BATCH_MAX} messages per run"
            ));
        }

        // Resolve Avro schema once (Electron encodeProduceBatch parity).
        let mut schema_id = None;
        let mut schema_str: Option<String> = None;
        match request.format.as_str() {
            "avro" => {
                if self.sr_status != "ok" {
                    return Err(self
                        .sr_error
                        .clone()
                        .unwrap_or_else(|| "Schema Registry is not available".into()));
                }
                let reg = self
                    .registry
                    .as_ref()
                    .ok_or("Schema Registry is not configured")?;
                let subject = request
                    .subject
                    .as_deref()
                    .ok_or("Avro subject is required")?;
                let id = tokio::time::timeout(
                    Duration::from_secs(20),
                    reg.get_latest_schema_id(subject),
                )
                .await
                .map_err(|_| "Schema Registry getLatestSchemaId timed out".to_string())??;
                let s = tokio::time::timeout(Duration::from_secs(20), reg.get_schema_by_id(id))
                    .await
                    .map_err(|_| "Schema Registry getSchemaById timed out".to_string())??;
                schema_id = Some(id);
                schema_str = Some(s);
            }
            "avro-local" => {
                let s = request
                    .local_schema
                    .as_deref()
                    .filter(|x| !x.trim().is_empty())
                    .ok_or("Local .avsc schema is required")?;
                schema_id = Some(request.local_schema_id.unwrap_or(0));
                schema_str = Some(s.to_string());
            }
            _ => {}
        }

        let mut payloads: Vec<(Option<String>, Vec<u8>)> = Vec::with_capacity(request.messages.len());
        for m in &request.messages {
            let bytes = match request.format.as_str() {
                "avro" | "avro-local" => {
                    let sid = schema_id.unwrap_or(0);
                    let schema = schema_str.as_deref().unwrap();
                    let v = parse_json_loose(&m.value);
                    let body = encode_json_with_schema(schema, &v)?;
                    wrap_confluent_avro(sid, &body)
                }
                "json" => {
                    let parsed = parse_json_loose(&m.value);
                    if parsed.is_string() {
                        parsed.as_str().unwrap_or(&m.value).as_bytes().to_vec()
                    } else {
                        serde_json::to_vec(&parsed).unwrap_or_else(|_| m.value.as_bytes().to_vec())
                    }
                }
                _ => m.value.as_bytes().to_vec(),
            };
            payloads.push((m.key.clone(), bytes));
        }

        let producer = self.producer.as_ref().unwrap();
        let mut last_partition = None;
        let mut last_offset = None;
        let mut sent = 0usize;
        const CHUNK: usize = 100;
        for chunk in payloads.chunks(CHUNK) {
            for (key, payload) in chunk {
                let mut record = FutureRecord::to(&request.topic).payload(payload.as_slice());
                if let Some(ref k) = key {
                    record = record.key(k);
                }
                if let Some(p) = request.partition {
                    record = record.partition(p);
                }
                let delivery = producer
                    .send(record, Duration::from_secs(45))
                    .await
                    .map_err(|(e, _)| e.to_string())?;
                last_partition = Some(delivery.partition);
                last_offset = Some(delivery.offset.to_string());
                sent += 1;
            }
        }

        Ok(ProduceBatchResult {
            sent,
            format: request.format,
            schema_id,
            last_partition,
            last_offset,
        })
    }
}
