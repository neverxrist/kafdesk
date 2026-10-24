//! Opt-in backend breadcrumbs for the Logs tab (UI checkbox «Expand logs»).
//! When disabled (default), `log!` / rdkafka IPC are no-ops — no cost.
//! librdkafka FAIL lines are suppressed in the terminal; with Expand logs ON
//! they appear in the Logs list (deduped).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tauri::{AppHandle, Emitter};

static ENABLED: AtomicBool = AtomicBool::new(false);
static APP: OnceLock<AppHandle> = OnceLock::new();
/// Recent fingerprints → last emit. Alternating auth/transport lines used to
/// defeat single-slot exact dedup and flood the UI.
static RECENT_RDKAFKA: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();

const RDKAFKA_DEDUP: Duration = Duration::from_secs(12);
/// GroupCoordinator DOWN/SASL handshake noise is expected on some clusters
/// (lag still works via Admin API). Show at most once per 5 minutes.
const GC_NOISE_DEDUP: Duration = Duration::from_secs(300);

pub fn bind_app(app: AppHandle) {
    let _ = APP.set(app);
}

pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
    if !on {
        if let Some(slot) = RECENT_RDKAFKA.get() {
            if let Ok(mut map) = slot.lock() {
                map.clear();
            }
        }
    }
}

pub fn emit(app: &AppHandle, level: &str, msg: String) {
    let _ = app.emit(
        "kafka://debug-log",
        serde_json::json!({ "level": level, "msg": msg }),
    );
}

fn is_group_coordinator_noise(lower: &str) -> bool {
    let gc = lower.contains("groupcoordinator") || lower.contains("group coordinator");
    let downish = lower.contains("state down")
        || lower.contains("(n/a)")
        || lower.contains("transport failure")
        || lower.contains("saslauthenticaterequest");
    gc && downish
        || (lower.contains("sasl") && lower.contains("transport failure") && lower.contains("down"))
}

/// Collapse noisy variants of the same broker problem into one key + TTL.
fn rdkafka_fingerprint(msg: &str) -> (String, Duration) {
    let lower = msg.to_ascii_lowercase();
    if is_group_coordinator_noise(&lower) {
        return ("gc-noise".into(), GC_NOISE_DEDUP);
    }
    if lower.contains("authentication") || lower.contains("sasl") {
        return ("auth".into(), RDKAFKA_DEDUP);
    }
    if lower.contains("broker transport") || lower.contains("transport failure") {
        return ("transport".into(), RDKAFKA_DEDUP);
    }
    if lower.contains("groupcoordinator") {
        return ("groupcoordinator".into(), RDKAFKA_DEDUP);
    }
    if lower.contains("failed to resolve") || lower.contains("dns") {
        return ("dns".into(), RDKAFKA_DEDUP);
    }
    (msg.chars().take(120).collect(), RDKAFKA_DEDUP)
}

/// librdkafka FAIL / Global error → Logs tab only when Expand logs is on (deduped).
pub fn emit_rdkafka(level: &str, msg: String) {
    if !is_enabled() {
        return;
    }
    let Some(app) = APP.get() else {
        return;
    };
    let (key, ttl) = rdkafka_fingerprint(&msg);
    let slot = RECENT_RDKAFKA.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut map) = slot.lock() {
        let now = Instant::now();
        map.retain(|_, at| now.duration_since(*at) < GC_NOISE_DEDUP);
        if let Some(at) = map.get(&key) {
            if now.duration_since(*at) < ttl {
                return;
            }
        }
        map.insert(key.clone(), now);
    }
    let line = if key == "gc-noise" {
        // One clear line instead of raw librdkafka spam every ~12s.
        "rdkafka: GroupCoordinator unreachable (transport/SASL DOWN) — usually advertised.listeners/VPN, not ACL. Lag via Admin may still work. (suppressed 5m)".into()
    } else {
        format!("rdkafka: {msg}")
    };
    emit(app, level, line);
}

/// Format + emit only when Expand logs is on.
#[macro_export]
macro_rules! debug_log {
    ($app:expr, $level:expr, $($arg:tt)*) => {{
        if $crate::debug_log::is_enabled() {
            $crate::debug_log::emit($app, $level, format!($($arg)*));
        }
    }};
}
