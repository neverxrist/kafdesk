//! Opt-in backend breadcrumbs for the Logs tab (UI checkbox «Expand logs»).
//! When disabled (default), `log!` / rdkafka IPC are no-ops — no cost.
//! librdkafka FAIL lines are suppressed in the terminal; with Expand logs ON
//! they appear in the Logs list (deduped), same content as before.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tauri::{AppHandle, Emitter};

static ENABLED: AtomicBool = AtomicBool::new(false);
static APP: OnceLock<AppHandle> = OnceLock::new();
static LAST_RDKAFKA: Mutex<Option<(String, Instant)>> = Mutex::new(None);

pub fn bind_app(app: AppHandle) {
    let _ = APP.set(app);
}

pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

pub fn emit(app: &AppHandle, level: &str, msg: String) {
    let _ = app.emit(
        "kafka://debug-log",
        serde_json::json!({ "level": level, "msg": msg }),
    );
}

/// librdkafka FAIL / Global error → Logs tab only when Expand logs is on (deduped).
pub fn emit_rdkafka(level: &str, msg: String) {
    if !is_enabled() {
        return;
    }
    let Some(app) = APP.get() else {
        return;
    };
    if let Ok(mut slot) = LAST_RDKAFKA.lock() {
        if let Some((prev, at)) = slot.as_ref() {
            if prev == &msg && at.elapsed() < Duration::from_secs(3) {
                return;
            }
        }
        *slot = Some((msg.clone(), Instant::now()));
    }
    emit(app, level, format!("rdkafka: {msg}"));
}

/// Format + emit only when debug is on.
#[macro_export]
macro_rules! debug_log {
    ($app:expr, $level:expr, $($arg:tt)*) => {{
        if $crate::debug_log::is_enabled() {
            $crate::debug_log::emit($app, $level, format!($($arg)*));
        }
    }};
}
