//! Opt-in backend breadcrumbs for the Logs tab.
//! When disabled (default), `log!` is a no-op — no format!, no IPC.

use std::sync::atomic::{AtomicBool, Ordering};
use tauri::{AppHandle, Emitter};

static ENABLED: AtomicBool = AtomicBool::new(false);

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

/// Format + emit only when debug is on.
#[macro_export]
macro_rules! debug_log {
    ($app:expr, $level:expr, $($arg:tt)*) => {{
        if $crate::debug_log::is_enabled() {
            $crate::debug_log::emit($app, $level, format!($($arg)*));
        }
    }};
}
