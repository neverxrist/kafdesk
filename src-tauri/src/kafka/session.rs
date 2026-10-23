use crate::avro_codec::decode_datum_to_json;
use crate::domain::BrokerMessageFilter;
use super::decode::{
    decode_owned_message, decode_with_avro_json,
    strip_confluent_wire,
};
use crate::sr::SchemaRegistryClient;
use crate::types::*;
use super::consume_ctx::KafdeskCtx;
use rdkafka::admin::AdminClient;
#[allow(unused_imports)]
use rdkafka::config::{ClientConfig, FromClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub(crate) type Admin = AdminClient<KafdeskCtx>;
pub(crate) type Producer = FutureProducer<KafdeskCtx>;
pub(crate) type BConsumer = BaseConsumer<KafdeskCtx>;
pub(crate) type SConsumer = StreamConsumer<KafdeskCtx>;

pub struct KafkaSession {
    pub config: Option<ClusterConfig>,
    pub(crate) producer: Option<Producer>,
    pub(crate) admin: Option<Admin>,
    pub(crate) brokers: String,
    pub(crate) client_id: String,
    pub(crate) base_config: ClientConfig,
    pub(crate) registry: Option<SchemaRegistryClient>,
    pub(crate) sr_status: String,
    pub(crate) sr_error: Option<String>,
    /// Cached Confluent schema bodies by id — avoids N×SR round-trips per consume window.
    pub(crate) schema_cache: Arc<std::sync::Mutex<HashMap<i32, String>>>,
    pub(crate) tailing: Arc<AtomicBool>,
    pub(crate) tail_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Live-updated while the tail task runs (search / key / headers / partitions).
    pub(crate) tail_filter: Arc<StdMutex<BrokerMessageFilter>>,
    pub(crate) tail_partitions: Arc<StdMutex<Option<Vec<i32>>>>,
}

impl KafkaSession {
    pub fn new() -> Self {
        Self {
            config: None,
            producer: None,
            admin: None,
            brokers: String::new(),
            client_id: "kafdesk".into(),
            base_config: ClientConfig::new(),
            registry: None,
            sr_status: "none".into(),
            sr_error: None,
            schema_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            tailing: Arc::new(AtomicBool::new(false)),
            tail_task: Mutex::new(None),
            tail_filter: Arc::new(StdMutex::new(BrokerMessageFilter::default())),
            tail_partitions: Arc::new(StdMutex::new(None)),
        }
    }

    /// Snapshot for background ops — live session stays in the mutex slot so
    /// topic switch / offsets / disconnect are not blocked by long Kafka calls.
    /// No fresh Admin/Producer: each librdkafka client burns FDs; workers use
    /// short-lived metadata/consume clients only.
    pub(crate) fn clone_for_worker(&self) -> Result<Self, String> {
        if self.config.is_none() {
            return Err("Not connected to Kafka".into());
        }
        Ok(Self {
            config: self.config.clone(),
            producer: None,
            admin: None,
            brokers: self.brokers.clone(),
            client_id: self.client_id.clone(),
            base_config: self.base_config.clone(),
            registry: self.registry.clone(),
            sr_status: self.sr_status.clone(),
            sr_error: self.sr_error.clone(),
            schema_cache: Arc::clone(&self.schema_cache),
            tailing: Arc::clone(&self.tailing),
            tail_task: Mutex::new(None),
            tail_filter: Arc::clone(&self.tail_filter),
            tail_partitions: Arc::clone(&self.tail_partitions),
        })
    }
    pub(crate) fn make_consumer(&self, group_prefix: &str) -> Result<BConsumer, String> {
        let mut c = self.base_config.clone();
        c.set("group.id", format!("{group_prefix}-{}", uuid::Uuid::new_v4()));
        c.set("enable.auto.commit", "false");
        // Prefer fresh leaders — stale metadata → NotLeaderForPartition on watermarks.
        c.set("metadata.max.age.ms", "5000");
        c.set("topic.metadata.refresh.interval.ms", "10000");
        c.create_with_context(KafdeskCtx::default())
            .map_err(|e| e.to_string())
    }
    pub async fn connect(
        &mut self,
        config: ClusterConfig,
    ) -> Result<ConnectResult, String> {
        self.disconnect().await;

        let base = super::config::build_client_config(&config)?;
        let brokers = config.brokers.join(",");
        let client_id = config
            .client_id
            .clone()
            .unwrap_or_else(|| "kafdesk".into());

        let producer: Producer = base
            .create_with_context(KafdeskCtx::default())
            .map_err(|e| format!("producer: {e}"))?;
        let admin: Admin = base
            .create_with_context(KafdeskCtx::default())
            .map_err(|e| format!("admin: {e}"))?;

        let mut probe_cfg = base.clone();
        probe_cfg.set(
            "group.id",
            format!("kafdesk-probe-{}", uuid::Uuid::new_v4()),
        );
        probe_cfg.set("enable.auto.commit", "false");
        let consumer: BConsumer = probe_cfg
            .create_with_context(KafdeskCtx::default())
            .map_err(|e| format!("probe consumer: {e}"))?;
        consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| format!("Kafka metadata: {e}"))?;

        let broker_dns_warning =
            super::dns_check::dns_warning_for_connect(&consumer, &brokers);

        let mut sr_status = "none".to_string();
        let mut sr_error = None;
        let mut registry = None;
        if let Some(ref url) = config.schema_registry_url {
            if !url.trim().is_empty() {
                match SchemaRegistryClient::new(&config) {
                    Ok(client) => match client.probe().await {
                        Ok(_) => {
                            sr_status = "ok".into();
                            registry = Some(client);
                        }
                        Err(e) => {
                            sr_status = "fail".into();
                            sr_error = Some(e);
                            // Keep client so UI can retry list/probe; encode/decode gated on sr_status.
                            registry = Some(client);
                        }
                    },
                    Err(e) => {
                        sr_status = "fail".into();
                        sr_error = Some(e);
                    }
                }
            }
        }

        self.config = Some(config);
        self.producer = Some(producer);
        self.admin = Some(admin);
        self.brokers = brokers;
        self.client_id = client_id;
        self.base_config = base;
        self.registry = registry;
        self.sr_status = sr_status.clone();
        self.sr_error = sr_error.clone();

        Ok(ConnectResult {
            ok: true,
            schema_registry: sr_status,
            schema_registry_error: sr_error,
            broker_dns_warning,
        })
    }
    pub async fn disconnect(&mut self) {
        self.stop_tail().await;
        self.producer = None;
        self.admin = None;
        self.registry = None;
        self.config = None;
        self.sr_status = "none".into();
        self.sr_error = None;
        if let Ok(mut cache) = self.schema_cache.lock() {
            cache.clear();
        }
    }
    pub fn is_connected(&self) -> bool {
        self.admin.is_some() && self.producer.is_some()
    }
    pub fn sr_status(&self) -> String {
        self.sr_status.clone()
    }
    pub async fn ping(&self) -> Result<PingResult, String> {
        self.ensure_admin()?;
        let started = Instant::now();
        let consumer = self.make_consumer("kafdesk-ping")?;
        let kafka = match consumer.fetch_metadata(None, Duration::from_secs(8)) {
            Ok(_) => ProbePart {
                ok: true,
                ms: Some(started.elapsed().as_millis() as u64),
                error: None,
            },
            Err(e) => ProbePart {
                ok: false,
                ms: Some(started.elapsed().as_millis() as u64),
                error: Some(e.to_string()),
            },
        };
        let dns_warning = if kafka.ok {
            super::dns_check::dns_warning_for_connect(&consumer, &self.brokers)
        } else {
            None
        };
        let schema_registry = if let Some(ref reg) = self.registry {
            let t0 = Instant::now();
            match reg.probe().await {
                Ok(_) => SrProbe {
                    status: "ok".into(),
                    ms: Some(t0.elapsed().as_millis() as u64),
                    error: None,
                },
                Err(e) => SrProbe {
                    status: "fail".into(),
                    ms: Some(t0.elapsed().as_millis() as u64),
                    error: Some(e),
                },
            }
        } else {
            SrProbe {
                status: "none".into(),
                ms: None,
                error: None,
            }
        };
        Ok(PingResult {
            kafka,
            schema_registry,
            dns_warning,
        })
    }
    pub async fn probe(config: ClusterConfig) -> Result<PingResult, String> {
        let mut tmp = KafkaSession::new();
        match tmp.connect(config).await {
            Ok(connect) => {
                let mut r = tmp.ping().await?;
                if r.dns_warning.is_none() {
                    r.dns_warning = connect.broker_dns_warning;
                }
                tmp.disconnect().await;
                Ok(r)
            }
            Err(e) => Ok(PingResult {
                kafka: ProbePart {
                    ok: false,
                    ms: None,
                    error: Some(e),
                },
                schema_registry: SrProbe {
                    status: "none".into(),
                    ms: None,
                    error: None,
                },
                dns_warning: None,
            }),
        }
    }
    pub(crate) fn ensure_admin(&self) -> Result<(), String> {
        if self.admin.is_none() {
            return Err("Not connected to Kafka".into());
        }
        Ok(())
    }
    /// Connected config present (workers have no Admin — use this instead of ensure_admin).
    pub(crate) fn ensure_config(&self) -> Result<(), String> {
        if self.config.is_none() {
            return Err("Not connected to Kafka".into());
        }
        Ok(())
    }
    pub(crate) fn ensure_producer(&self) -> Result<(), String> {
        if self.producer.is_none() {
            return Err("Not connected to Kafka".into());
        }
        Ok(())
    }
    pub(crate) fn metadata_consumer(&self) -> Result<BConsumer, String> {
        self.make_consumer("kafdesk-meta")
    }
    pub fn list_topics(&self) -> Result<Vec<TopicInfo>, String> {
        let consumer = self.metadata_consumer()?;
        let meta = consumer
            .fetch_metadata(None, Duration::from_secs(15))
            .map_err(|e| e.to_string())?;
        let mut topics: Vec<TopicInfo> = meta
            .topics()
            .iter()
            .filter(|t| !t.name().starts_with("__"))
            .map(|t| TopicInfo {
                name: t.name().to_string(),
                partitions: {
                    let mut parts: Vec<_> = t
                        .partitions()
                        .iter()
                        .map(|p| PartitionInfo {
                            partition_id: p.id(),
                            leader: p.leader(),
                            replicas: p.replicas().to_vec(),
                            isr: p.isr().to_vec(),
                            offset_low: None,
                            offset_high: None,
                            approx_messages: None,
                        })
                        .collect();
                    parts.sort_by_key(|p| p.partition_id);
                    parts
                },
                approx_messages: None,
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }
    pub fn get_topic_offsets(&self, topic: &str) -> Result<TopicInfo, String> {
        let mut consumer = self.metadata_consumer()?;
        // No hard DNS ensure here — enrich calls this for every topic and must stay fast.
        // Consume still runs ensure_advertised_brokers_resolvable.
        // At most one client recreate per call — recreating per partition blows the FD limit.

        let meta = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .map_err(|e| e.to_string())?;
        let tmeta = meta
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| format!("Topic not found: {topic}"))?;
        let part_meta: Vec<(i32, i32, Vec<i32>, Vec<i32>)> = tmeta
            .partitions()
            .iter()
            .map(|p| {
                (
                    p.id(),
                    p.leader(),
                    p.replicas().to_vec(),
                    p.isr().to_vec(),
                )
            })
            .collect();
        if part_meta.is_empty() {
            return Ok(TopicInfo {
                name: topic.to_string(),
                partitions: vec![],
                approx_messages: Some(0),
            });
        }

        let mut partitions = Vec::new();
        let mut approx_total = 0i64;
        // Keep enrich snappy — soft-fail watermarks; consume does not need every high watermark.
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut recreated = false;

        for (pid, mut leader, mut replicas, mut isr) in part_meta {
            if Instant::now() >= deadline {
                // Return what we have — consume can fall back to Offset::End / Beginning.
                break;
            }

            let mut got: Option<(i64, i64)> = None;
            for attempt in 0..2 {
                match consumer.fetch_watermarks(topic, pid, Duration::from_secs(2)) {
                    Ok(wh) => {
                        got = Some(wh);
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let retryable = msg.contains("NotLeaderForPartition")
                            || msg.contains("LeaderNotAvailable")
                            || msg.contains("NotController")
                            || msg.contains("NetworkException")
                            || msg.contains("Broker transport")
                            || msg.contains("Timed out");
                        if !retryable || attempt == 1 {
                            break;
                        }
                        if !recreated {
                            if let Ok(c) = self.metadata_consumer() {
                                consumer = c;
                                recreated = true;
                            }
                        }
                        if let Ok(m) = consumer.fetch_metadata(Some(topic), Duration::from_secs(3)) {
                            if let Some(p) = m
                                .topics()
                                .iter()
                                .find(|t| t.name() == topic)
                                .and_then(|t| t.partitions().iter().find(|p| p.id() == pid))
                            {
                                leader = p.leader();
                                replicas = p.replicas().to_vec();
                                isr = p.isr().to_vec();
                            }
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }

            // Soft-fail: keep partition in the list even without watermarks (NotLeader etc.).
            let (offset_low, offset_high, approx_messages) = if let Some((low, high)) = got {
                let approx = (high - low).max(0);
                approx_total += approx;
                (
                    Some(low.to_string()),
                    Some(high.to_string()),
                    Some(approx),
                )
            } else {
                (None, None, None)
            };

            partitions.push(PartitionInfo {
                partition_id: pid,
                leader,
                replicas,
                isr,
                offset_low,
                offset_high,
                approx_messages,
            });
        }
        partitions.sort_by_key(|p| p.partition_id);
        Ok(TopicInfo {
            name: topic.to_string(),
            partitions,
            approx_messages: Some(approx_total),
        })
    }
    pub(crate) async fn decode_message_view(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp_ms: i64,
        key: Option<&[u8]>,
        payload: Option<&[u8]>,
        headers: HashMap<String, String>,
    ) -> KafkaMessageView {
        if self.sr_status != "ok" {
            return decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers);
        }
        if let (Some(reg), Some(buf)) = (&self.registry, payload) {
            if let Some((id, body)) = strip_confluent_wire(buf) {
                let cached = self
                    .schema_cache
                    .lock()
                    .ok()
                    .and_then(|c| c.get(&id).cloned());
                let schema_result = if let Some(schema_str) = cached {
                    Ok(schema_str)
                } else {
                    match tokio::time::timeout(Duration::from_secs(5), reg.get_schema_by_id(id)).await
                    {
                        Ok(Ok(schema_str)) => {
                            if let Ok(mut cache) = self.schema_cache.lock() {
                                cache.insert(id, schema_str.clone());
                            }
                            Ok(schema_str)
                        }
                        Ok(Err(err)) => Err(err),
                        Err(_) => Err(format!("Schema Registry timeout (id={id})")),
                    }
                };
                match schema_result {
                    Ok(schema_str) => match decode_datum_to_json(&schema_str, body) {
                        Ok(json) => {
                            return decode_with_avro_json(
                                topic,
                                partition,
                                offset,
                                timestamp_ms,
                                key,
                                payload,
                                headers,
                                Some(json),
                            );
                        }
                        Err(err) => {
                            let mut view = decode_owned_message(
                                topic,
                                partition,
                                offset,
                                timestamp_ms,
                                key,
                                payload,
                                headers,
                            );
                            view.format = "avro".into();
                            view.schema_id = Some(id);
                            view.value = serde_json::json!({
                                "_decodeError": err,
                                "raw": view.raw_preview,
                            });
                            return view;
                        }
                    },
                    Err(err) => {
                        let mut view = decode_owned_message(
                            topic,
                            partition,
                            offset,
                            timestamp_ms,
                            key,
                            payload,
                            headers,
                        );
                        view.format = "avro".into();
                        view.schema_id = Some(id);
                        view.value = serde_json::json!({
                            "_decodeError": err,
                            "raw": view.raw_preview,
                        });
                        return view;
                    }
                }
            }
        }
        decode_owned_message(topic, partition, offset, timestamp_ms, key, payload, headers)
    }
    pub async fn list_subjects(&self) -> Result<Vec<String>, String> {
        let reg = self
            .registry
            .as_ref()
            .ok_or("Schema Registry is not configured")?;
        reg.list_subjects().await
    }
    pub async fn get_latest_schema(&self, subject: &str) -> Result<SchemaSubjectInfo, String> {
        let reg = self
            .registry
            .as_ref()
            .ok_or("Schema Registry is not configured")?;
        reg.get_latest_schema(subject).await
    }
}
