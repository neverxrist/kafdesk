use crate::auth::AuthState;
use crate::debug_log;
use crate::kafka::config::security_protocol_label;
use crate::kafka::KafkaSession;
use crate::types::*;
use crate::updates::UpdateState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, State};
use tokio::sync::{Mutex, Semaphore};

/// Hot path (offsets + consume) — never share with GroupCoordinator lag walks.
const KAFKA_HOT_SLOTS: usize = 2;
/// Background user actions (list / describe / group offsets) — short, may wait.
const KAFKA_BG_SLOTS: usize = 1;
/// Long cluster walks (group snapshot / lag summary). try_acquire only, own lane so a
/// 25s walk never blocks a click on a group.
const KAFKA_WALK_SLOTS: usize = 1;

pub struct AppState {
    pub kafka: Mutex<KafkaSession>,
    pub kafka_hot: Arc<Semaphore>,
    pub kafka_bg: Arc<Semaphore>,
    pub kafka_walk: Arc<Semaphore>,
    /// Bumped on each consume start — older poll loops exit so topic switches don't queue.
    pub consume_gen: Arc<AtomicU64>,
    pub auth: AuthState,
    pub updates: UpdateState,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            kafka: Mutex::new(KafkaSession::new()),
            kafka_hot: Arc::new(Semaphore::new(KAFKA_HOT_SLOTS)),
            kafka_bg: Arc::new(Semaphore::new(KAFKA_BG_SLOTS)),
            kafka_walk: Arc::new(Semaphore::new(KAFKA_WALK_SLOTS)),
            consume_gen: Arc::new(AtomicU64::new(0)),
            auth: AuthState::new(),
            updates: UpdateState::new(),
        }
    }
}

#[tauri::command]
pub fn ping_shell() -> String {
    "kafdesk-tauri".into()
}

#[tauri::command]
pub fn set_backend_debug(app: AppHandle, enabled: bool) -> Result<OkMsg, String> {
    if enabled {
        debug_log::set_enabled(true);
        debug_log::emit(&app, "info", "backend debug ON".into());
    } else {
        // Emit while still enabled so the OFF line appears in the Logs tab.
        if debug_log::is_enabled() {
            debug_log::emit(&app, "info", "backend debug OFF".into());
        }
        debug_log::set_enabled(false);
    }
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub fn get_backend_debug() -> bool {
    debug_log::is_enabled()
}

#[tauri::command]
pub async fn kafka_connect(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    config: ClusterConfig,
) -> Result<ConnectResult, String> {
    crate::debug_log!(
        &app,
        "info",
        "connect → {} protocol={} tlsCa={}",
        config.brokers.join(","),
        security_protocol_label(&config),
        {
            // Only report CA when it is actually applied (SSL / SASL_SSL).
            let ssl = config.ssl.unwrap_or(false);
            let has_ca = crate::tls_resolve::kafka_tls(&config)
                .and_then(|t| t.ca_path.as_deref())
                .map(|p| !p.is_empty())
                .unwrap_or(false);
            if !ssl {
                "unused"
            } else if has_ca {
                "set"
            } else {
                "none"
            }
        }
    );
    // Take the slot first so a stuck enrich (detached) cannot block reconnect forever.
    let mut session = {
        let mut guard = state.kafka.lock().await;
        let mut empty = KafkaSession::new();
        std::mem::swap(&mut *guard, &mut empty);
        empty
    };
    session.disconnect().await;
    let result = session.connect(config).await;
    {
        let mut guard = state.kafka.lock().await;
        if result.is_ok() {
            *guard = session;
        } else {
            // Leave slot empty on failure (already disconnected above).
            session.disconnect().await;
        }
    }
    match &result {
        Ok(r) => crate::debug_log!(
            &app,
            "info",
            "connect ok sr={}{}",
            r.schema_registry,
            r.schema_registry_error
                .as_deref()
                .map(|e| format!(" srErr={e}"))
                .unwrap_or_default()
        ),
        Err(e) => crate::debug_log!(&app, "error", "connect failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_disconnect(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<OkMsg, String> {
    crate::debug_log!(&app, "info", "disconnect");
    // Swap out whatever is in the slot immediately — do not wait on a detached long op.
    let mut session = {
        let mut guard = state.kafka.lock().await;
        let mut empty = KafkaSession::new();
        std::mem::swap(&mut *guard, &mut empty);
        empty
    };
    session.disconnect().await;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_status(state: State<'_, Arc<AppState>>) -> Result<StatusMsg, String> {
    let k = state.kafka.lock().await;
    Ok(StatusMsg {
        connected: k.is_connected(),
        schema_registry: k.sr_status(),
    })
}

#[tauri::command]
pub async fn kafka_ping(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<PingResult, String> {
    crate::debug_log!(&app, "info", "ping (live session)");
    let result = state.kafka.lock().await.ping().await;
    match &result {
        Ok(p) => crate::debug_log!(
            &app,
            if p.kafka.ok { "info" } else { "warn" },
            "ping → kafkaOk={} ms={:?} sr={} err={:?}",
            p.kafka.ok,
            p.kafka.ms,
            p.schema_registry.status,
            p.kafka.error
        ),
        Err(e) => crate::debug_log!(&app, "error", "ping failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_probe(app: AppHandle, config: ClusterConfig) -> Result<PingResult, String> {
    crate::debug_log!(
        &app,
        "info",
        "probe → {} protocol={}",
        config.brokers.join(","),
        security_protocol_label(&config)
    );
    let result = KafkaSession::probe(config).await;
    match &result {
        Ok(p) => crate::debug_log!(
            &app,
            if p.kafka.ok { "info" } else { "warn" },
            "probe → kafkaOk={} ms={:?} sr={} err={:?}",
            p.kafka.ok,
            p.kafka.ms,
            p.schema_registry.status,
            p.kafka.error
        ),
        Err(e) => crate::debug_log!(&app, "error", "probe failed: {e}"),
    }
    result
}

/// Hot-path worker (topic offsets / list) — waits for a slot.
async fn with_session_worker<T, F>(state: &AppState, op: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(KafkaSession) -> Result<T, String> + Send + 'static,
{
    let permit = state
        .kafka_hot
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| "Kafka worker interrupted".to_string())?;
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        op(worker)
    })
    .await
    .map_err(|e| format!("kafka worker join: {e}"))?
}

/// Background worker — waits for the bg slot (user actions: open group / list groups).
async fn with_session_worker_bg<T, F>(state: &AppState, op: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(KafkaSession) -> Result<T, String> + Send + 'static,
{
    let permit = state
        .kafka_bg
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| "Kafka background interrupted".to_string())?;
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        op(worker)
    })
    .await
    .map_err(|e| format!("kafka worker join: {e}"))?
}

/// Cluster walk — never queue behind / block UI; skip if the walk lane is taken.
async fn with_session_worker_walk_try<T, F>(state: &AppState, op: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(KafkaSession) -> Result<T, String> + Send + 'static,
{
    let permit = state
        .kafka_walk
        .clone()
        .try_acquire_owned()
        .map_err(|_| "Kafka background busy".to_string())?;
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        op(worker)
    })
    .await
    .map_err(|e| format!("kafka worker join: {e}"))?
}

#[tauri::command]
pub async fn kafka_list_topics(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<TopicInfo>, String> {
    crate::debug_log!(&app, "info", "list_topics");
    let result = with_session_worker(&state, |session| session.list_topics()).await;
    match &result {
        Ok(t) => crate::debug_log!(&app, "info", "list_topics → {} topics", t.len()),
        Err(e) => crate::debug_log!(&app, "error", "list_topics failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_get_topic_offsets(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    topic: String,
) -> Result<TopicInfo, String> {
    // Hot path (sidebar enrich) — log failures only, not every topic poll.
    let topic_name = topic.clone();
    let result = with_session_worker(&state, move |session| session.get_topic_offsets(&topic)).await;
    if let Err(e) = &result {
        crate::debug_log!(&app, "error", "topic_offsets {topic_name} failed: {e}");
    }
    result
}

/// Batched watermarks for the sidebar (≈msgs per topic) — one call instead of N.
#[tauri::command]
pub async fn kafka_topics_offsets(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    topics: Vec<String>,
) -> Result<Vec<TopicInfo>, String> {
    let result = with_session_worker(&state, move |session| session.topics_with_offsets(&topics)).await;
    if let Err(e) = &result {
        crate::debug_log!(&app, "error", "topics_offsets failed: {e}");
    }
    result
}

/// Detach the live session for a long op so disconnect/ping are not blocked.
/// Always returns the session for restore (unless a newer connected session won).
async fn with_session_detached<T, F, Fut>(state: &AppState, op: F) -> Result<T, String>
where
    F: FnOnce(KafkaSession) -> Fut,
    Fut: std::future::Future<Output = (KafkaSession, Result<T, String>)>,
{
    let session = {
        let mut guard = state.kafka.lock().await;
        let mut empty = KafkaSession::new();
        std::mem::swap(&mut *guard, &mut empty);
        empty
    };
    if !session.is_connected() {
        let mut guard = state.kafka.lock().await;
        let mut session = session;
        std::mem::swap(&mut *guard, &mut session);
        return Err("Not connected to Kafka".into());
    }
    let (mut session, result) = op(session).await;
    {
        let mut guard = state.kafka.lock().await;
        if guard.is_connected() {
            // User reconnected while we were busy — drop the stale detached session.
            session.disconnect().await;
        } else {
            std::mem::swap(&mut *guard, &mut session);
        }
    }
    result
}

#[tauri::command]
pub async fn kafka_consume(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    request: ConsumeRequest,
) -> Result<Vec<KafkaMessageView>, String> {
    crate::debug_log!(
        &app,
        "info",
        "consume → topic={} limit={:?} start={:?}",
        request.topic,
        request.limit,
        request.start_from
    );
    let my_gen = state.consume_gen.fetch_add(1, Ordering::SeqCst) + 1;
    let gen = Arc::clone(&state.consume_gen);
    let permit = state
        .kafka_hot
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| "Kafka worker interrupted".to_string())?;
    // Drop if a newer consume already started while we waited for a hot slot.
    if gen.load(Ordering::SeqCst) != my_gen {
        crate::debug_log!(&app, "info", "consume cancelled (newer request)");
        return Ok(vec![]);
    }
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    // No nested tokio Runtime per click — that alone burns FDs until EMFILE.
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        futures::executor::block_on(worker.consume_cancellable(request, gen, my_gen))
    })
    .await
    .map_err(|e| format!("consume join: {e}"))?;
    match &result {
        Ok(msgs) => crate::debug_log!(&app, "info", "consume → {} messages", msgs.len()),
        Err(e) => crate::debug_log!(&app, "error", "consume failed: {e}"),
    }
    result
}

/// Full value for one message (list rows are capped at `MAX_VALUE_BYTES`).
#[tauri::command]
pub async fn kafka_fetch_message(
    state: State<'_, Arc<AppState>>,
    topic: String,
    partition: i32,
    offset: String,
) -> Result<KafkaMessageView, String> {
    let offset: i64 = offset.trim().parse().map_err(|_| format!("Bad offset: {offset}"))?;
    let permit = state
        .kafka_hot
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| "Kafka worker interrupted".to_string())?;
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        futures::executor::block_on(worker.fetch_message(&topic, partition, offset))
    })
    .await
    .map_err(|e| format!("fetch message join: {e}"))?
}

#[tauri::command]
pub async fn kafka_produce(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    request: ProduceRequest,
) -> Result<ProduceResult, String> {
    crate::debug_log!(
        &app,
        "info",
        "produce → topic={} format={}",
        request.topic,
        request.format
    );
    let result = state.kafka.lock().await.produce(request).await;
    match &result {
        Ok(r) => crate::debug_log!(
            &app,
            "info",
            "produce ok partition={} offset={:?}",
            r.partition,
            r.offset
        ),
        Err(e) => crate::debug_log!(&app, "error", "produce failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_produce_batch(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    request: ProduceBatchRequest,
) -> Result<ProduceBatchResult, String> {
    crate::debug_log!(
        &app,
        "info",
        "produce_batch → topic={} count={}",
        request.topic,
        request.messages.len()
    );
    let result = with_session_detached(&state, |session| async move {
        let result = session.produce_batch(request).await;
        (session, result)
    })
    .await;
    match &result {
        Ok(r) => crate::debug_log!(&app, "info", "produce_batch → sent={}", r.sent),
        Err(e) => crate::debug_log!(&app, "error", "produce_batch failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_list_subjects(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<String>, String> {
    crate::debug_log!(&app, "info", "sr list_subjects");
    let result = state.kafka.lock().await.list_subjects().await;
    match &result {
        Ok(s) => crate::debug_log!(&app, "info", "sr list_subjects → {}", s.len()),
        Err(e) => crate::debug_log!(&app, "error", "sr list_subjects failed: {e}"),
    }
    result
}

#[tauri::command]
pub async fn kafka_get_latest_schema(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    subject: String,
) -> Result<SchemaSubjectInfo, String> {
    crate::debug_log!(&app, "info", "sr get_schema → {subject}");
    let result = state.kafka.lock().await.get_latest_schema(&subject).await;
    if let Err(e) = &result {
        crate::debug_log!(&app, "error", "sr get_schema failed: {e}");
    }
    result
}

#[tauri::command]
pub async fn kafka_list_consumer_groups(
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<ConsumerGroupInfo>, String> {
    // GroupCoordinator — background only; never block consume/offsets.
    with_session_worker_bg(&state, |session| session.list_consumer_groups()).await
}

#[tauri::command]
pub async fn kafka_get_group_offsets(
    state: State<'_, Arc<AppState>>,
    group_id: String,
    topic: Option<String>,
) -> Result<Vec<GroupOffsetEntry>, String> {
    with_session_worker_bg(&state, move |session| {
        session.get_group_offsets(&group_id, topic.as_deref())
    })
    .await
}

#[tauri::command]
pub async fn kafka_describe_consumer_group(
    state: State<'_, Arc<AppState>>,
    group_id: String,
) -> Result<ConsumerGroupDetail, String> {
    with_session_worker_bg(&state, move |session| {
        session.describe_consumer_group(&group_id)
    })
    .await
}

/// Cluster-wide group snapshot. `None` = walk lane busy (keep the previous view).
#[tauri::command]
pub async fn kafka_refresh_group_snapshot(
    state: State<'_, Arc<AppState>>,
) -> Result<Option<GroupSnapshotView>, String> {
    match with_session_worker_walk_try(&state, |session| session.refresh_group_snapshot()).await {
        Err(e) if e.contains("background busy") => Ok(None),
        other => other.map(Some),
    }
}

/// Groups that committed on `topic`, from the last snapshot (no Kafka calls).
#[tauri::command]
pub async fn kafka_topic_consumers(
    state: State<'_, Arc<AppState>>,
    topic: String,
) -> Result<Option<TopicConsumersView>, String> {
    let guard = state.kafka.lock().await;
    Ok(guard.topic_consumers(&topic))
}

#[tauri::command]
pub async fn kafka_get_lag_summary(
    state: State<'_, Arc<AppState>>,
    topic: Option<String>,
    group_ids: Option<Vec<String>>,
) -> Result<LagSummaryResult, String> {
    // Lag walks groups via coordinator — try_acquire only (never modal "busy" on click).
    // group_ids = exact UI filter selection (keyword search), not a second list+substring pass.
    let ids = group_ids.unwrap_or_default();
    match with_session_worker_walk_try(&state, move |session| {
        session.get_lag_summary(topic.as_deref(), ids)
    })
    .await
    {
        Err(e) if e.contains("background busy") => Ok(LagSummaryResult {
            groups: vec![],
            total_lag: 0,
            max_lag: 0,
            updated_at: 0,
            listed: 0,
            scanned: 0,
            partial: true,
        }),
        other => other,
    }
}

#[tauri::command]
pub async fn kafka_set_group_offsets(
    state: State<'_, Arc<AppState>>,
    request: SetGroupOffsetsRequest,
) -> Result<OkMsg, String> {
    state.kafka.lock().await.set_group_offsets(request)?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_reset_group_offsets(
    state: State<'_, Arc<AppState>>,
    group_id: String,
    topic: String,
    earliest: bool,
) -> Result<OkMsg, String> {
    state
        .kafka
        .lock()
        .await
        .reset_group_offsets(&group_id, &topic, earliest)?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_tail_start(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    request: TailRequest,
) -> Result<OkMsg, String> {
    crate::debug_log!(&app, "info", "tail start → {}", request.topic);
    let result = state.kafka.lock().await.start_tail(app.clone(), request).await;
    match &result {
        Ok(()) => crate::debug_log!(&app, "info", "tail started"),
        Err(e) => crate::debug_log!(&app, "error", "tail start failed: {e}"),
    }
    result?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_tail_stop(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<OkMsg, String> {
    crate::debug_log!(&app, "info", "tail stop");
    state.kafka.lock().await.stop_tail().await;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_tail_set_filter(
    state: State<'_, Arc<AppState>>,
    request: TailRequest,
) -> Result<OkMsg, String> {
    state.kafka.lock().await.set_tail_filter(&request)?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn dialog_pick_pem_file(app: AppHandle) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;
    let path = app
        .dialog()
        .file()
        .add_filter("Certificates", &["pem", "crt", "cer", "key", "pub"])
        .blocking_pick_file();
    Ok(path.and_then(|p| p.into_path().ok()).map(|p| p.display().to_string()))
}

#[tauri::command]
pub async fn dialog_pick_avsc_file(app: AppHandle) -> Result<Option<AvscFile>, String> {
    use tauri_plugin_dialog::DialogExt;
    let path = app
        .dialog()
        .file()
        .add_filter("Avro schema", &["avsc", "json"])
        .blocking_pick_file();
    let Some(p) = path.and_then(|p| p.into_path().ok()) else {
        return Ok(None);
    };
    let path_str = p.display().to_string();
    let text = std::fs::read_to_string(&path_str).map_err(|e| e.to_string())?;
    Ok(Some(AvscFile {
        path: path_str,
        text,
    }))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveTextFileFilter {
    pub name: String,
    pub extensions: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveTextFileRequest {
    pub contents: String,
    #[serde(default)]
    pub default_path: Option<String>,
    #[serde(default)]
    pub filters: Option<Vec<SaveTextFileFilter>>,
}

/// Native save dialog; returns written path or null if cancelled.
#[tauri::command]
pub async fn dialog_save_text_file(
    app: AppHandle,
    request: SaveTextFileRequest,
) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;
    let mut dialog = app.dialog().file();
    if let Some(name) = request.default_path.as_ref().filter(|s| !s.is_empty()) {
        dialog = dialog.set_file_name(name);
    }
    if let Some(filters) = &request.filters {
        for f in filters {
            let exts: Vec<&str> = f.extensions.iter().map(|s| s.as_str()).collect();
            if !exts.is_empty() {
                dialog = dialog.add_filter(&f.name, &exts);
            }
        }
    }
    let path = dialog.blocking_save_file();
    let Some(p) = path.and_then(|p| p.into_path().ok()) else {
        return Ok(None);
    };
    std::fs::write(&p, request.contents.as_bytes()).map_err(|e| e.to_string())?;
    Ok(Some(p.display().to_string()))
}

#[tauri::command]
pub fn auth_list_providers(state: State<'_, Arc<AppState>>) -> Vec<AuthProviderInfo> {
    state.auth.list_providers()
}

#[tauri::command]
pub fn auth_session(state: State<'_, Arc<AppState>>) -> AuthSessionPublic {
    state.auth.session()
}

#[tauri::command]
pub async fn auth_login(
    state: State<'_, Arc<AppState>>,
    request: AuthLoginRequest,
) -> Result<AuthSessionPublic, String> {
    state.auth.login(request).await
}

#[tauri::command]
pub async fn auth_logout(state: State<'_, Arc<AppState>>) -> Result<OkMsg, String> {
    state.auth.logout().await?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub fn auth_stored_credentials(state: State<'_, Arc<AppState>>) -> StoredCredentialsInfo {
    state.auth.stored_credentials()
}

#[tauri::command]
pub fn auth_forget_credentials(state: State<'_, Arc<AppState>>) -> OkMsg {
    state.auth.forget_credentials();
    OkMsg { ok: true }
}

#[tauri::command]
pub async fn app_check_for_updates(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<UpdateStatus, String> {
    Ok(state.updates.check_for_updates(&app).await)
}

#[tauri::command]
pub fn app_update_status(state: State<'_, Arc<AppState>>) -> UpdateStatus {
    state.updates.status()
}

#[tauri::command]
pub fn app_update_settings(state: State<'_, Arc<AppState>>) -> UpdateSettings {
    state.updates.settings()
}

#[tauri::command]
pub fn app_set_auto_update(
    state: State<'_, Arc<AppState>>,
    enabled: bool,
) -> UpdateSettings {
    state.updates.set_auto_update(enabled)
}

#[tauri::command]
pub fn app_open_downloaded_update(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<(), String> {
    state.updates.open_downloaded_update(&app)
}

#[tauri::command]
pub fn app_open_releases(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
) -> Result<(), String> {
    state.updates.open_releases_page(&app)
}

#[tauri::command]
pub fn app_get_version(app: AppHandle) -> String {
    app.package_info().version.to_string()
}

#[derive(serde::Serialize)]
pub struct OkMsg {
    pub ok: bool,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusMsg {
    pub connected: bool,
    pub schema_registry: String,
}

#[derive(serde::Serialize)]
pub struct AvscFile {
    pub path: String,
    pub text: String,
}
