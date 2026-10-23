//! Shared librdkafka client context: keep FAIL / global errors out of the
//! terminal; when Expand logs is on, mirror them into the Logs tab (deduped).
//! Also capture last interesting error for consume UX.

use crate::debug_log;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryResult, ProducerContext};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct KafdeskCtx {
    pub last_error: Arc<Mutex<Option<String>>>,
}

/// Alias used by consume paths that peek `last_error`.
pub type ConsumeCtx = KafdeskCtx;

impl KafdeskCtx {
    pub fn peek_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|g| g.clone())
    }

    fn remember_interesting(&self, error: &KafkaError, reason: &str) {
        let interesting = reason.contains("Group authorization")
            || reason.contains("Failed to resolve")
            || reason.contains("Authentication")
            || reason.contains("SASL")
            || reason.contains("Transport")
            || reason.contains("SSL handshake")
            || reason.contains("Broker transport")
            || reason.contains("PLAINTEXT");
        if !interesting {
            return;
        }
        if let Ok(mut g) = self.last_error.lock() {
            *g = Some(format!("{error}: {reason}"));
        }
    }
}

impl ClientContext for KafdeskCtx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        // Never forward to the `log` crate (that was the terminal spam).
        if !debug_log::is_enabled() {
            return;
        }
        let ui_level = match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => "error",
            RDKafkaLogLevel::Warning => "warn",
            _ => "info",
        };
        debug_log::emit_rdkafka(ui_level, format!("{fac} {log_message}"));
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.remember_interesting(&error, reason);
        // No `log::error!` → quiet terminal; UI only with Expand logs.
        debug_log::emit_rdkafka("error", format!("{error}: {reason}"));
    }
}

impl ConsumerContext for KafdeskCtx {}

impl ProducerContext for KafdeskCtx {
    type DeliveryOpaque = ();

    fn delivery(&self, _result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {}
}
