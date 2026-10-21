use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    pub brokers: Vec<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub ssl: Option<bool>,
    #[serde(default)]
    pub auth_mode: Option<String>,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub schema_registry_url: Option<String>,
    #[serde(default)]
    pub schema_registry_auth: Option<BasicAuth>,
    #[serde(default)]
    pub schema_registry_tls: Option<SrTls>,
    #[serde(default)]
    pub sso: Option<ConnectionSsoConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslConfig {
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SrTls {
    #[serde(default)]
    pub ca_path: Option<String>,
    #[serde(default)]
    pub cert_path: Option<String>,
    #[serde(default)]
    pub key_path: Option<String>,
    #[serde(default)]
    pub reject_unauthorized: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSsoConfig {
    pub provider_id: String,
    #[serde(default)]
    pub keycloak: Option<KeycloakSsoConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeycloakSsoConfig {
    pub url: String,
    pub realm: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectResult {
    pub ok: bool,
    pub schema_registry: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_registry_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResult {
    pub kafka: ProbePart,
    #[serde(rename = "schemaRegistry")]
    pub schema_registry: SrProbe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbePart {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrProbe {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_low: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_high: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approx_messages: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approx_messages: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaMessageView {
    pub topic: String,
    pub partition: i32,
    pub offset: String,
    pub timestamp: String,
    pub key: Option<String>,
    pub value: Value,
    pub raw_preview: String,
    pub format: String,
    pub headers: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeRequest {
    pub topic: String,
    #[serde(default)]
    pub from_beginning: Option<bool>,
    #[serde(default)]
    pub start_from: Option<String>,
    #[serde(default)]
    pub start_offset: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub search_key: Option<String>,
    #[serde(default)]
    pub search_header_key: Option<String>,
    #[serde(default)]
    pub search_header_value: Option<String>,
    #[serde(default)]
    pub time_from: Option<i64>,
    #[serde(default)]
    pub time_to: Option<i64>,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceRequest {
    pub topic: String,
    #[serde(default)]
    pub key: Option<String>,
    pub value: String,
    pub format: String,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub local_schema: Option<String>,
    #[serde(default)]
    pub local_schema_id: Option<i32>,
    #[serde(default)]
    pub partition: Option<i32>,
    #[serde(default)]
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceResult {
    pub partition: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceBatchRequest {
    pub topic: String,
    pub format: String,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub local_schema: Option<String>,
    #[serde(default)]
    pub local_schema_id: Option<i32>,
    #[serde(default)]
    pub partition: Option<i32>,
    pub messages: Vec<ProduceBatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceBatchItem {
    #[serde(default)]
    pub key: Option<String>,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceBatchResult {
    pub sent: usize,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_offset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailRequest {
    pub topic: String,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub search_key: Option<String>,
    #[serde(default)]
    pub search_header_key: Option<String>,
    #[serde(default)]
    pub search_header_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub protocol_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_lag: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_partition_lag: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_topics: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_partitions: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupOffsetEntry {
    pub topic: String,
    pub partition: i32,
    pub offset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub high: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetGroupOffsetsRequest {
    pub group_id: String,
    pub topic: String,
    pub partitions: Vec<PartitionOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupLagSummary {
    pub group_id: String,
    pub total_lag: i64,
    pub max_partition_lag: i64,
    pub topics: usize,
    pub partitions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_partition: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LagSummaryResult {
    pub groups: Vec<GroupLagSummary>,
    pub total_lag: i64,
    pub max_lag: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaSubjectInfo {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<String>,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthProviderInfo {
    pub id: String,
    pub label: String,
    pub implemented: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthSessionPublic {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub logged_in: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthLoginRequest {
    pub provider_id: String,
    #[serde(default)]
    pub keycloak: Option<KeycloakSsoConfig>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub remember_password: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredCredentialsInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    pub has_password: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatus {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSettings {
    pub auto_update: bool,
}
