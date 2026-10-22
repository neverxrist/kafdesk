use crate::auth::AuthState;
use crate::kafka::KafkaSession;
use crate::types::*;
use crate::updates::UpdateState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, State};
use tokio::sync::{Mutex, Semaphore};

/// Hot path (offsets + consume) — never share with GroupCoordinator lag walks.
const KAFKA_HOT_SLOTS: usize = 2;
/// Background (lag / enrich-friendly). try_acquire only — never blocks hot path.
const KAFKA_BG_SLOTS: usize = 1;

pub struct AppState {
    pub kafka: Mutex<KafkaSession>,
    pub kafka_hot: Arc<Semaphore>,
    pub kafka_bg: Arc<Semaphore>,
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
pub async fn kafka_connect(
    state: State<'_, Arc<AppState>>,
    config: ClusterConfig,
) -> Result<ConnectResult, String> {
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
    result
}

#[tauri::command]
pub async fn kafka_disconnect(state: State<'_, Arc<AppState>>) -> Result<OkMsg, String> {
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
pub async fn kafka_ping(state: State<'_, Arc<AppState>>) -> Result<PingResult, String> {
    state.kafka.lock().await.ping().await
}

#[tauri::command]
pub async fn kafka_probe(config: ClusterConfig) -> Result<PingResult, String> {
    KafkaSession::probe(config).await
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

/// Lag walk — never queue behind / block UI; skip if bg slot taken.
async fn with_session_worker_bg_try<T, F>(state: &AppState, op: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(KafkaSession) -> Result<T, String> + Send + 'static,
{
    let permit = state
        .kafka_bg
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
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<TopicInfo>, String> {
    with_session_worker(&state, |session| session.list_topics()).await
}

#[tauri::command]
pub async fn kafka_get_topic_offsets(
    state: State<'_, Arc<AppState>>,
    topic: String,
) -> Result<TopicInfo, String> {
    with_session_worker(&state, move |session| session.get_topic_offsets(&topic)).await
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
    state: State<'_, Arc<AppState>>,
    request: ConsumeRequest,
) -> Result<Vec<KafkaMessageView>, String> {
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
        return Ok(vec![]);
    }
    let worker = {
        let guard = state.kafka.lock().await;
        guard.clone_for_worker()?
    };
    // No nested tokio Runtime per click — that alone burns FDs until EMFILE.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        futures::executor::block_on(worker.consume_cancellable(request, gen, my_gen))
    })
    .await
    .map_err(|e| format!("consume join: {e}"))?
}

#[tauri::command]
pub async fn kafka_produce(
    state: State<'_, Arc<AppState>>,
    request: ProduceRequest,
) -> Result<ProduceResult, String> {
    state.kafka.lock().await.produce(request).await
}

#[tauri::command]
pub async fn kafka_produce_batch(
    state: State<'_, Arc<AppState>>,
    request: ProduceBatchRequest,
) -> Result<ProduceBatchResult, String> {
    with_session_detached(&state, |session| async move {
        let result = session.produce_batch(request).await;
        (session, result)
    })
    .await
}

#[tauri::command]
pub async fn kafka_list_subjects(state: State<'_, Arc<AppState>>) -> Result<Vec<String>, String> {
    state.kafka.lock().await.list_subjects().await
}

#[tauri::command]
pub async fn kafka_get_latest_schema(
    state: State<'_, Arc<AppState>>,
    subject: String,
) -> Result<SchemaSubjectInfo, String> {
    state.kafka.lock().await.get_latest_schema(&subject).await
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
pub async fn kafka_get_lag_summary(
    state: State<'_, Arc<AppState>>,
    topic: Option<String>,
) -> Result<LagSummaryResult, String> {
    // Lag walks groups via coordinator — try_acquire only (never modal "busy" on click).
    match with_session_worker_bg_try(&state, move |session| {
        session.get_lag_summary(topic.as_deref())
    })
    .await
    {
        Err(e) if e.contains("background busy") => Ok(LagSummaryResult {
            groups: vec![],
            total_lag: 0,
            max_lag: 0,
            updated_at: 0,
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
    state.kafka.lock().await.start_tail(app, request).await?;
    Ok(OkMsg { ok: true })
}

#[tauri::command]
pub async fn kafka_tail_stop(state: State<'_, Arc<AppState>>) -> Result<OkMsg, String> {
    state.kafka.lock().await.stop_tail().await;
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
