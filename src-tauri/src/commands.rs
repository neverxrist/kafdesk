use crate::auth::AuthState;
use crate::kafka::KafkaSession;
use crate::types::*;
use crate::updates::UpdateState;
use std::sync::Arc;
use tauri::{AppHandle, State};
use tokio::sync::Mutex;

pub struct AppState {
    pub kafka: Mutex<KafkaSession>,
    pub auth: AuthState,
    pub updates: UpdateState,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            kafka: Mutex::new(KafkaSession::new()),
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
    state.kafka.lock().await.connect(config).await
}

#[tauri::command]
pub async fn kafka_disconnect(state: State<'_, Arc<AppState>>) -> Result<OkMsg, String> {
    state.kafka.lock().await.disconnect().await;
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

#[tauri::command]
pub async fn kafka_list_topics(
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<TopicInfo>, String> {
    state.kafka.lock().await.list_topics()
}

#[tauri::command]
pub async fn kafka_get_topic_offsets(
    state: State<'_, Arc<AppState>>,
    topic: String,
) -> Result<TopicInfo, String> {
    state.kafka.lock().await.get_topic_offsets(&topic)
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
    with_session_detached(&state, |session| async move {
        let result = session.consume(request).await;
        (session, result)
    })
    .await
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
    state.kafka.lock().await.list_consumer_groups()
}

#[tauri::command]
pub async fn kafka_get_group_offsets(
    state: State<'_, Arc<AppState>>,
    group_id: String,
    topic: Option<String>,
) -> Result<Vec<GroupOffsetEntry>, String> {
    state
        .kafka
        .lock()
        .await
        .get_group_offsets(&group_id, topic.as_deref())
}

#[tauri::command]
pub async fn kafka_get_lag_summary(
    state: State<'_, Arc<AppState>>,
    topic: Option<String>,
) -> Result<LagSummaryResult, String> {
    state.kafka.lock().await.get_lag_summary(topic.as_deref())
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
