//! Capture librdkafka global errors during consume (GroupCoordinator ACL / DNS / SASL).

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct ConsumeCtx {
    pub last_error: Arc<Mutex<Option<String>>>,
}

impl ConsumeCtx {
    pub fn peek_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|g| g.clone())
    }
}

impl ClientContext for ConsumeCtx {
    fn error(&self, error: KafkaError, reason: &str) {
        let interesting = reason.contains("Group authorization")
            || reason.contains("Failed to resolve")
            || reason.contains("Authentication")
            || reason.contains("SASL")
            || reason.contains("Transport")
            || reason.contains("SSL handshake")
            || reason.contains("Broker transport");
        if !interesting {
            return;
        }
        if let Ok(mut g) = self.last_error.lock() {
            *g = Some(format!("{error}: {reason}"));
        }
    }
}

impl ConsumerContext for ConsumeCtx {}
