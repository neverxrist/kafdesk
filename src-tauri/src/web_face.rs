//! Localhost face of the same client. One Kafka session.
//! The browser and the desktop window can both be open; only the side
//! that holds the lock may call Kafka commands.

use crate::commands::{self, AppState};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Listener, Manager};
use tauri_plugin_opener::OpenerExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

const DESKTOP: u8 = 0;
const WEB: u8 = 1;
/// Browser gone (no open page) for this long → lock returns to the desktop.
const RELEASE_AFTER_MS: u64 = 20_000;
const PORT_FIRST: u16 = 47321;
const PORT_LAST: u16 = 47340;

struct Hub {
    owner: AtomicU8,
    seen_ms: AtomicU64,
    port: AtomicU64,
    sse_clients: AtomicUsize,
    running: AtomicBool,
    token: parking_lot::Mutex<String>,
    client_state: parking_lot::Mutex<String>,
    events: broadcast::Sender<FaceEvent>,
    start: AsyncMutex<()>,
}

#[derive(Clone)]
struct FaceEvent {
    name: String,
    data: String,
}

fn hub() -> &'static Hub {
    static HUB: OnceLock<Hub> = OnceLock::new();
    HUB.get_or_init(|| {
        let (events, _) = broadcast::channel(256);
        Hub {
            owner: AtomicU8::new(DESKTOP),
            seen_ms: AtomicU64::new(0),
            port: AtomicU64::new(0),
            sse_clients: AtomicUsize::new(0),
            running: AtomicBool::new(false),
            token: parking_lot::Mutex::new(String::new()),
            client_state: parking_lot::Mutex::new("{}".into()),
            events,
            start: AsyncMutex::new(()),
        }
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn owner_name(owner: u8) -> &'static str {
    if owner == WEB { "web" } else { "desktop" }
}

fn touch() {
    hub().seen_ms.store(now_ms(), Ordering::Relaxed);
}

fn should_release(owner_is_web: bool, sse_clients: usize, silent_ms: u64) -> bool {
    owner_is_web && sse_clients == 0 && silent_ms > RELEASE_AFTER_MS
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FaceStatus {
    pub running: bool,
    pub url: Option<String>,
    pub owner: String,
}

fn status_now() -> FaceStatus {
    let h = hub();
    let port = h.port.load(Ordering::Relaxed) as u16;
    let running = h.running.load(Ordering::Relaxed) && port != 0;
    let token = h.token.lock().clone();
    let url = if running && !token.is_empty() {
        Some(format!("http://127.0.0.1:{port}/?k={token}"))
    } else {
        None
    };
    FaceStatus {
        running,
        url,
        owner: owner_name(h.owner.load(Ordering::Relaxed)).into(),
    }
}

#[tauri::command]
pub fn web_face_status() -> FaceStatus {
    status_now()
}

/// Snapshot of the desktop `localStorage` (`kafdesk.*`) so the browser opens the same connections.
#[tauri::command]
pub fn web_face_put_client_state(raw: String) -> Result<(), String> {
    if raw.len() > 1_500_000 {
        return Err("client state too large".into());
    }
    let value: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    if !value.is_object() {
        return Err("client state must be an object".into());
    }
    *hub().client_state.lock() = raw;
    let _ = hub().events.send(FaceEvent {
        name: "face://client-state".into(),
        data: "\"1\"".into(),
    });
    Ok(())
}

#[tauri::command]
pub async fn web_face_set_owner(
    app: AppHandle,
    owner: String,
    open_browser: bool,
) -> Result<FaceStatus, String> {
    match owner.as_str() {
        "web" => {
            ensure_started(&app).await?;
            hub().owner.store(WEB, Ordering::Relaxed);
            touch();
            if open_browser {
                let url = status_now().url.unwrap_or_default();
                if !url.is_empty() {
                    let _ = app.opener().open_url(url, None::<&str>);
                }
            }
        }
        "desktop" => {
            hub().owner.store(DESKTOP, Ordering::Relaxed);
        }
        _ => return Err("owner must be web or desktop".into()),
    }
    Ok(status_now())
}

async fn ensure_started(app: &AppHandle) -> Result<(), String> {
    let _guard = hub().start.lock().await;
    if hub().running.load(Ordering::Relaxed) {
        return Ok(());
    }
    let token = uuid::Uuid::new_v4().to_string();
    let listener = bind_loopback().await?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    *hub().token.lock() = token;
    hub().port.store(port as u64, Ordering::Relaxed);
    hub().owner.store(DESKTOP, Ordering::Relaxed);
    listen_events(app);
    let app_loop = app.clone();
    tauri::async_runtime::spawn(async move { accept_loop(listener, app_loop).await });
    tauri::async_runtime::spawn(watch_browser());
    hub().running.store(true, Ordering::Relaxed);
    Ok(())
}

async fn bind_loopback() -> Result<TcpListener, String> {
    let mut last = "bind failed".to_string();
    for port in PORT_FIRST..=PORT_LAST {
        match TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return Ok(listener),
            Err(e) => last = e.to_string(),
        }
    }
    Err(last)
}

fn listen_events(app: &AppHandle) {
    for name in [
        "kafka://tail-batch",
        "kafka://tail-error",
        "kafka://trace-progress",
        "kafka://debug-log",
        "app://update-status",
        "face://client-state",
    ] {
        let tx = hub().events.clone();
        let event_name = name.to_string();
        app.listen(name, move |event| {
            let _ = tx.send(FaceEvent {
                name: event_name.clone(),
                data: event.payload().to_string(),
            });
        });
    }
}

async fn watch_browser() {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let h = hub();
        if h.owner.load(Ordering::Relaxed) != WEB {
            continue;
        }
        let silent = now_ms().saturating_sub(h.seen_ms.load(Ordering::Relaxed));
        if should_release(true, h.sse_clients.load(Ordering::Relaxed), silent) {
            h.owner.store(DESKTOP, Ordering::Relaxed);
        }
    }
}

async fn accept_loop(listener: TcpListener, app: AppHandle) {
    loop {
        let Ok((sock, peer)) = listener.accept().await else {
            continue;
        };
        if !peer.ip().is_loopback() {
            continue;
        }
        let app = app.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(e) = handle_conn(sock, app).await {
                let _ = e;
            }
        });
    }
}

struct Req {
    method: String,
    path: String,
    query: String,
    token: String,
    body: Vec<u8>,
}

async fn handle_conn(mut sock: TcpStream, app: AppHandle) -> Result<(), String> {
    let req = read_req(&mut sock).await?;
    if !path_safe(&req.path) {
        return write_text(&mut sock, 400, "text/plain", b"bad path").await;
    }
    if req.path.starts_with("/api/") && !token_ok(&req.token) {
        return write_json(&mut sock, 401, &json!({"error": "bad token"})).await;
    }
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/api/heartbeat") => {
            touch();
            write_json(&mut sock, 200, &json!({"ok": true})).await
        }
        ("GET", "/api/client-state") => {
            let raw = hub().client_state.lock().clone();
            let value: Value = serde_json::from_str(&raw).unwrap_or_else(|_| json!({}));
            write_json(&mut sock, 200, &value).await
        }
        ("GET", "/api/face") => {
            write_json(&mut sock, 200, &serde_json::to_value(status_now()).unwrap_or(Value::Null)).await
        }
        ("POST", "/api/invoke") => dispatch_http(&mut sock, &app, &req.body).await,
        ("GET", "/api/events") => write_sse(&mut sock).await,
        ("GET", _) | ("HEAD", _) => serve_ui(&mut sock, &app, &req).await,
        _ => write_json(&mut sock, 404, &json!({"error": "not found"})).await,
    }
}

async fn dispatch_http(sock: &mut TcpStream, app: &AppHandle, body: &[u8]) -> Result<(), String> {
    let parsed: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let cmd = parsed
        .get("cmd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if cmd.is_empty() {
        return write_json(sock, 400, &json!({"error": "missing cmd"})).await;
    }
    let args = parsed.get("args").cloned().unwrap_or_else(|| json!({}));
    let web_holds = hub().owner.load(Ordering::Relaxed) == WEB;
    if !web_holds && !always_allowed(&cmd) {
        return write_json(sock, 423, &json!({"error": "FACE_LOCKED"})).await;
    }
    if cmd == "web_face_status" {
        return write_json(sock, 200, &serde_json::to_value(status_now()).unwrap_or(Value::Null)).await;
    }
    if cmd == "web_face_set_owner" {
        let owner = args.get("owner").and_then(|v| v.as_str()).unwrap_or("");
        let open_browser = args.get("openBrowser").and_then(|v| v.as_bool()).unwrap_or(false);
        match owner {
            "web" => {
                hub().owner.store(WEB, Ordering::Relaxed);
                touch();
                if open_browser {
                    let url = status_now().url.unwrap_or_default();
                    if !url.is_empty() {
                        let _ = app.opener().open_url(url, None::<&str>);
                    }
                }
            }
            "desktop" => hub().owner.store(DESKTOP, Ordering::Relaxed),
            _ => return write_json(sock, 400, &json!({"error": "owner must be web or desktop"})).await,
        }
        return write_json(sock, 200, &serde_json::to_value(status_now()).unwrap_or(Value::Null)).await;
    }
    match dispatch_command(app.clone(), &cmd, args).await {
        Ok(v) => write_json(sock, 200, &v).await,
        Err(e) if e == "FACE_LOCKED" => write_json(sock, 423, &json!({"error": e})).await,
        Err(e) => write_json(sock, 400, &json!({"error": e})).await,
    }
}

fn always_allowed(cmd: &str) -> bool {
    matches!(
        cmd,
        "web_face_status"
            | "web_face_set_owner"
            | "app_get_version"
            | "app_update_status"
            | "app_update_settings"
            | "app_set_auto_update"
            | "app_check_for_updates"
            | "app_open_releases"
            | "app_open_downloaded_update"
            | "get_backend_debug"
            | "set_backend_debug"
            | "ping_shell"
    )
}

fn token_ok(got: &str) -> bool {
    if got.is_empty() {
        return false;
    }
    let expected = hub().token.lock().clone();
    let a = expected.as_bytes();
    let b = got.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn path_safe(path: &str) -> bool {
    !path.contains("..") && !path.contains('\\') && !path.contains('\0')
}

async fn serve_ui(sock: &mut TcpStream, app: &AppHandle, req: &Req) -> Result<(), String> {
    if cfg!(debug_assertions) {
        if let Ok((code, ctype, body)) = proxy_vite(&req.method, &req.path, &req.query).await {
            if req.method == "HEAD" {
                return write_head(sock, code, &ctype, body.len()).await;
            }
            return write_bytes(sock, code, &ctype, &body).await;
        }
    }
    let rel = if req.path == "/" || req.path.is_empty() {
        "index.html".to_string()
    } else {
        req.path.trim_start_matches('/').to_string()
    };
    let asset = app.asset_resolver().get(rel.clone()).or_else(|| {
        if rel.contains('.') {
            None
        } else {
            app.asset_resolver().get("index.html".into())
        }
    });
    let Some(asset) = asset else {
        return write_text(sock, 404, "text/plain", b"not found").await;
    };
    if req.method == "HEAD" {
        return write_head(sock, 200, &asset.mime_type, asset.bytes.len()).await;
    }
    write_bytes(sock, 200, &asset.mime_type, &asset.bytes).await
}

async fn proxy_vite(method: &str, path: &str, query: &str) -> Result<(u16, String, Vec<u8>), String> {
    let mut stream = TcpStream::connect("127.0.0.1:5173")
        .await
        .map_err(|e| e.to_string())?;
    let q = if query.is_empty() { String::new() } else { format!("?{query}") };
    let req = format!(
        "{method} {path}{q} HTTP/1.0\r\nHost: 127.0.0.1:5173\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
    split_response(&buf).ok_or_else(|| "bad vite response".to_string())
}

fn split_response(buf: &[u8]) -> Option<(u16, String, Vec<u8>)> {
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&buf[..end]).ok()?;
    let mut lines = head.split("\r\n");
    let status = lines.next()?;
    let code: u16 = status.split_whitespace().nth(1)?.parse().ok()?;
    let mut ctype = "application/octet-stream".to_string();
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-type:") {
            ctype = v.trim().to_string();
        }
    }
    Some((code, ctype, buf[end + 4..].to_vec()))
}

async fn write_sse(sock: &mut TcpStream) -> Result<(), String> {
    hub().sse_clients.fetch_add(1, Ordering::Relaxed);
    touch();
    struct DropSse;
    impl Drop for DropSse {
        fn drop(&mut self) {
            hub().sse_clients.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _guard = DropSse;
    let mut rx = hub().events.subscribe();
    let header = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n: ok\n\n";
    sock.write_all(header).await.map_err(|e| e.to_string())?;
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let chunk = sse_frame(&ev.name, &ev.data);
                if sock.write_all(chunk.as_bytes()).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

fn sse_frame(event: &str, data: &str) -> String {
    let mut out = format!("event: {event}\n");
    if data.is_empty() {
        out.push_str("data: \n");
    } else {
        for line in data.split('\n') {
            out.push_str("data: ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out.push('\n');
    out
}

async fn read_req(sock: &mut TcpStream) -> Result<Req, String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        let n = sock.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > 64 * 1024 {
            return Err("headers too large".into());
        }
    };
    let head = std::str::from_utf8(&buf[..header_end]).map_err(|e| e.to_string())?;
    let mut lines = head.split("\r\n");
    let start = lines.next().unwrap_or("");
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut token = String::new();
    let mut len = 0usize;
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            len = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case("x-kafdesk-token")) {
            token = v.1.trim().to_string();
        }
    }
    if token.is_empty() {
        for pair in query.split('&') {
            if let Some(v) = pair.strip_prefix("k=") {
                token = v.to_string();
                break;
            }
        }
    }
    if len > 32 * 1024 * 1024 {
        return Err("body too large".into());
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < len {
        let n = sock.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(len);
    Ok(Req {
        method,
        path: path.to_string(),
        query: query.to_string(),
        token,
        body,
    })
}

async fn write_json(sock: &mut TcpStream, code: u16, value: &Value) -> Result<(), String> {
    let body = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    write_bytes(sock, code, "application/json", &body).await
}

async fn write_text(sock: &mut TcpStream, code: u16, ctype: &str, body: &[u8]) -> Result<(), String> {
    write_bytes(sock, code, ctype, body).await
}

async fn write_head(sock: &mut TcpStream, code: u16, ctype: &str, len: usize) -> Result<(), String> {
    let text = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: {ctype}\r\nContent-Length: {len}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
        reason(code)
    );
    sock.write_all(text.as_bytes()).await.map_err(|e| e.to_string())
}

async fn write_bytes(sock: &mut TcpStream, code: u16, ctype: &str, body: &[u8]) -> Result<(), String> {
    write_head(sock, code, ctype, body.len()).await?;
    sock.write_all(body).await.map_err(|e| e.to_string())
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        423 => "Locked",
        502 => "Bad Gateway",
        _ => "Error",
    }
}

fn need<T: for<'de> Deserialize<'de>>(args: &Value, key: &str) -> Result<T, String> {
    let v = args
        .get(key)
        .filter(|v| !v.is_null())
        .ok_or_else(|| format!("missing {key}"))?;
    serde_json::from_value(v.clone()).map_err(|e| format!("{key}: {e}"))
}

fn opt<T: for<'de> Deserialize<'de>>(args: &Value, key: &str) -> Result<Option<T>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(
            serde_json::from_value(v.clone()).map_err(|e| format!("{key}: {e}"))?,
        )),
    }
}

fn json_ok<T: Serialize>(v: T) -> Result<Value, String> {
    serde_json::to_value(v).map_err(|e| e.to_string())
}

fn json_res<T: Serialize>(r: Result<T, String>) -> Result<Value, String> {
    json_ok(r?)
}

#[allow(clippy::too_many_lines)]
async fn dispatch_command(app: AppHandle, cmd: &str, args: Value) -> Result<Value, String> {
    let state = app.state::<Arc<AppState>>();
    match cmd {
        "ping_shell" => json_ok(commands::ping_shell()),
        "set_backend_debug" => json_res(commands::set_backend_debug(app.clone(), need(&args, "enabled")?)),
        "get_backend_debug" => json_ok(commands::get_backend_debug()),
        "kafka_connect" => json_res(commands::kafka_connect(app.clone(), state, need(&args, "config")?).await),
        "kafka_disconnect" => json_res(commands::kafka_disconnect(app.clone(), state).await),
        "kafka_status" => json_res(commands::kafka_status(state).await),
        "kafka_ping" => json_res(commands::kafka_ping(app.clone(), state).await),
        "kafka_probe" => json_res(commands::kafka_probe(app.clone(), need(&args, "config")?).await),
        "kafka_list_topics" => json_res(commands::kafka_list_topics(app.clone(), state).await),
        "kafka_get_topic_offsets" => {
            json_res(commands::kafka_get_topic_offsets(app.clone(), state, need(&args, "topic")?).await)
        }
        "kafka_topics_offsets" => {
            json_res(commands::kafka_topics_offsets(app.clone(), state, need(&args, "topics")?).await)
        }
        "kafka_fetch_message" => json_res(
            commands::kafka_fetch_message(
                state,
                need(&args, "topic")?,
                need(&args, "partition")?,
                need(&args, "offset")?,
            )
            .await,
        ),
        "kafka_consume" => json_res(commands::kafka_consume(app.clone(), state, need(&args, "request")?).await),
        "kafka_produce" => json_res(commands::kafka_produce(app.clone(), state, need(&args, "request")?).await),
        "kafka_produce_batch" => {
            json_res(commands::kafka_produce_batch(app.clone(), state, need(&args, "request")?).await)
        }
        "kafka_list_subjects" => json_res(commands::kafka_list_subjects(app.clone(), state).await),
        "kafka_get_latest_schema" => {
            json_res(commands::kafka_get_latest_schema(app.clone(), state, need(&args, "subject")?).await)
        }
        "kafka_list_consumer_groups" => json_res(commands::kafka_list_consumer_groups(state).await),
        "kafka_get_group_offsets" => json_res(
            commands::kafka_get_group_offsets(state, need(&args, "groupId")?, opt(&args, "topic")?).await,
        ),
        "kafka_describe_consumer_group" => {
            json_res(commands::kafka_describe_consumer_group(state, need(&args, "groupId")?).await)
        }
        "kafka_refresh_group_snapshot" => json_res(commands::kafka_refresh_group_snapshot(state).await),
        "kafka_topic_consumers" => json_res(commands::kafka_topic_consumers(state, need(&args, "topic")?).await),
        "kafka_get_lag_summary" => json_res(
            commands::kafka_get_lag_summary(state, opt(&args, "topic")?, opt(&args, "groupIds")?).await,
        ),
        "kafka_set_group_offsets" => json_res(commands::kafka_set_group_offsets(state, need(&args, "request")?).await),
        "kafka_reset_group_offsets" => json_res(
            commands::kafka_reset_group_offsets(
                state,
                need(&args, "groupId")?,
                need(&args, "topic")?,
                need(&args, "earliest")?,
            )
            .await,
        ),
        "kafka_tail_start" => json_res(commands::kafka_tail_start(app.clone(), state, need(&args, "request")?).await),
        "kafka_tail_stop" => json_res(commands::kafka_tail_stop(app.clone(), state).await),
        "kafka_tail_set_filter" => json_res(commands::kafka_tail_set_filter(state, need(&args, "request")?).await),
        "kafka_trace_key" => json_res(commands::kafka_trace_key(app.clone(), state, need(&args, "request")?).await),
        "kafka_trace_cancel" => {
            commands::kafka_trace_cancel(state).await?;
            Ok(Value::Null)
        }
        "dialog_pick_pem_file" => json_res(commands::dialog_pick_pem_file(app.clone()).await),
        "dialog_pick_avsc_file" => json_res(commands::dialog_pick_avsc_file(app.clone()).await),
        "dialog_save_text_file" => {
            json_res(commands::dialog_save_text_file(app.clone(), need(&args, "request")?).await)
        }
        "auth_list_providers" => json_ok(commands::auth_list_providers(state)),
        "auth_session" => json_ok(commands::auth_session(state)),
        "auth_login" => json_res(commands::auth_login(state, need(&args, "request")?).await),
        "auth_logout" => json_res(commands::auth_logout(state).await),
        "auth_stored_credentials" => json_ok(commands::auth_stored_credentials(state)),
        "auth_forget_credentials" => json_ok(commands::auth_forget_credentials(state)),
        "app_check_for_updates" => json_res(commands::app_check_for_updates(app.clone(), state).await),
        "app_update_status" => json_ok(commands::app_update_status(state)),
        "app_update_settings" => json_ok(commands::app_update_settings(state)),
        "app_set_auto_update" => json_ok(commands::app_set_auto_update(state, need(&args, "enabled")?)),
        "app_open_downloaded_update" => {
            commands::app_open_downloaded_update(app.clone(), state)?;
            Ok(Value::Null)
        }
        "app_open_releases" => {
            commands::app_open_releases(app.clone(), state)?;
            Ok(Value::Null)
        }
        "app_get_version" => json_ok(commands::app_get_version(app.clone())),
        other => Err(format!("unknown command {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_returns_the_lock_only_when_the_page_is_gone() {
        assert!(should_release(true, 0, RELEASE_AFTER_MS + 1));
        assert!(!should_release(true, 1, RELEASE_AFTER_MS + 5_000));
        assert!(!should_release(true, 0, 1_000));
        assert!(!should_release(false, 0, RELEASE_AFTER_MS + 5_000));
    }

    #[test]
    fn paths_cannot_escape() {
        assert!(path_safe("/"));
        assert!(path_safe("/assets/index.js"));
        assert!(!path_safe("/../secret"));
        assert!(!path_safe("/a\\b"));
    }
}
