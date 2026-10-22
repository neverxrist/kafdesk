use crate::types::{ClusterConfig, SrTls};

/// Broker SSL PEMs: prefer `kafka_tls`, fall back to legacy shared `schema_registry_tls`.
pub fn kafka_tls<'a>(config: &'a ClusterConfig) -> Option<&'a SrTls> {
    config
        .kafka_tls
        .as_ref()
        .or(config.schema_registry_tls.as_ref())
}

/// Schema Registry HTTPS PEMs.
/// Reuse Kafka TLS only when explicitly opted in.
///
/// Legacy: PEMs lived only in `schema_registry_tls`. When broker `ssl` is on and
/// `kafka_tls` is unset, that store is Kafka's (via [`kafka_tls`]) — do not also
/// send it to SR. When `ssl` is off, treat it as SR-only PEMs.
pub fn schema_registry_tls<'a>(config: &'a ClusterConfig) -> Option<&'a SrTls> {
    let reuse = config.schema_registry_reuse_kafka_tls.unwrap_or(false);
    if reuse {
        return kafka_tls(config);
    }
    match (&config.kafka_tls, &config.schema_registry_tls) {
        (Some(_), sr) => sr.as_ref(),
        (None, _sr) if config.ssl.unwrap_or(false) => None,
        (None, sr) => sr.as_ref(),
    }
}
