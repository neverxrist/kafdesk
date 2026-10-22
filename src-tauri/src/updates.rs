use crate::types::{UpdateSettings, UpdateStatus};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use std::process::Command;
use tauri::{AppHandle, Emitter};
use tauri_plugin_opener::OpenerExt;

const RELEASES_API: &str = "https://api.github.com/repos/neverxrist/kafdesk/releases/latest";
const RELEASES_PAGE: &str = "https://github.com/neverxrist/kafdesk/releases/latest";
#[cfg(target_os = "macos")]
const POST_INSTALL_CMDS: &str = "xattr -cr /Applications/KafDesk.app\nopen /Applications/KafDesk.app";

pub struct UpdateState {
    settings: Mutex<UpdateSettings>,
    status: Mutex<UpdateStatus>,
}

impl UpdateState {
    pub fn new() -> Self {
        let auto = load_settings().unwrap_or(false);
        Self {
            settings: Mutex::new(UpdateSettings {
                auto_update: auto,
            }),
            status: Mutex::new(UpdateStatus {
                state: "idle".into(),
                version: None,
                percent: None,
                message: None,
            }),
        }
    }

    pub fn settings(&self) -> UpdateSettings {
        self.settings.lock().clone()
    }

    pub fn set_auto_update(&self, enabled: bool) -> UpdateSettings {
        let mut s = self.settings.lock();
        s.auto_update = enabled;
        save_settings(enabled);
        s.clone()
    }

    pub fn status(&self) -> UpdateStatus {
        self.status.lock().clone()
    }

    fn emit(&self, app: &AppHandle, st: UpdateStatus) {
        *self.status.lock() = st.clone();
        let _ = app.emit("app://update-status", &st);
    }

    pub async fn check_for_updates(&self, app: &AppHandle) -> UpdateStatus {
        self.emit(
            app,
            UpdateStatus {
                state: "checking".into(),
                version: None,
                percent: None,
                message: None,
            },
        );

        let current = app.package_info().version.to_string();
        let latest = match find_latest_asset().await {
            Ok(v) => v,
            Err(e) => {
                let st = UpdateStatus {
                    state: "error".into(),
                    version: None,
                    percent: None,
                    message: Some(e),
                };
                self.emit(app, st.clone());
                return st;
            }
        };

        let Some((ver, url, name)) = latest else {
            let st = UpdateStatus {
                state: "error".into(),
                version: None,
                percent: None,
                message: Some(format!("No installer found for this platform ({})", platform_label())),
            };
            self.emit(app, st.clone());
            return st;
        };

        if !is_newer(&ver, &current) {
            write_pending(None);
            let st = UpdateStatus {
                state: "not-available".into(),
                version: Some(ver),
                percent: None,
                message: None,
            };
            self.emit(app, st.clone());
            return st;
        }

        self.emit(
            app,
            UpdateStatus {
                state: "available".into(),
                version: Some(ver.clone()),
                percent: None,
                message: None,
            },
        );
        self.emit(
            app,
            UpdateStatus {
                state: "downloading".into(),
                version: Some(ver.clone()),
                percent: Some(0.0),
                message: None,
            },
        );

        let downloads = dirs::download_dir().unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("Downloads")
        });
        let dest = downloads.join(&name);
        let _ = fs::remove_file(&dest);

        let download_result = download_file(&url, &dest, |percent| {
            self.emit(
                app,
                UpdateStatus {
                    state: "downloading".into(),
                    version: Some(ver.clone()),
                    percent: Some(percent),
                    message: None,
                },
            );
        })
        .await;

        if let Err(e) = download_result {
            let st = UpdateStatus {
                state: "error".into(),
                version: Some(ver),
                percent: None,
                message: Some(e),
            };
            self.emit(app, st.clone());
            return st;
        }

        #[cfg(target_os = "linux")]
        {
            if name.to_lowercase().ends_with(".appimage") {
                let _ = make_executable(&dest);
            }
        }

        write_pending(Some(PendingUpdate {
            path: dest.to_string_lossy().into_owned(),
            version: ver.clone(),
        }));

        let st = UpdateStatus {
            state: "downloaded".into(),
            version: Some(ver.clone()),
            percent: Some(100.0),
            message: Some(format!("Downloaded {name}")),
        };
        self.emit(app, st.clone());
        st
    }

    pub fn open_downloaded_update(&self, app: &AppHandle) -> Result<(), String> {
        let pending = read_pending().ok_or_else(|| "No downloaded update".to_string())?;
        let path = PathBuf::from(&pending.path);
        if !path.is_file() {
            write_pending(None);
            return Err(format!("Update file missing: {}", pending.path));
        }
        open_installer_and_quit(app, &path, &pending.version);
        Ok(())
    }

    pub fn open_releases_page(&self, app: &AppHandle) -> Result<(), String> {
        app.opener()
            .open_url(RELEASES_PAGE, None::<&str>)
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PendingUpdate {
    path: String,
    version: String,
}

fn open_installer_and_quit(app: &AppHandle, path: &PathBuf, version: &str) {
    write_pending(Some(PendingUpdate {
        path: path.to_string_lossy().into_owned(),
        version: version.to_string(),
    }));

    #[cfg(target_os = "macos")]
    show_post_install_help();

    #[cfg(target_os = "linux")]
    {
        if path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("appimage"))
            .unwrap_or(false)
        {
            let _ = make_executable(path);
        }
    }

    let _ = app.opener().open_path(path.to_string_lossy(), None::<&str>);
    let handle = app.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(400));
        let _ = handle.exit(0);
    });
}

#[cfg(target_os = "macos")]
fn show_post_install_help() {
    let _ = Command::new("pbcopy")
        .stdin(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(POST_INSTALL_CMDS.as_bytes());
            }
            child.wait()
        });

    let script = r#"display dialog "EN" & return & ¬
  "1. Drag KafDesk.app to Applications → Replace." & return & ¬
  "2. Try opening it normally — it often works." & return & ¬
  "3. If macOS blocks it or the first launch is very slow, paste in Terminal (⌘V, already copied):" & return & return & ¬
  "xattr -cr /Applications/KafDesk.app" & return & ¬
  "open /Applications/KafDesk.app" & return & return & ¬
  "RU" & return & ¬
  "1. Перетащи KafDesk.app в Applications → Replace." & return & ¬
  "2. Попробуй открыть как обычно — часто запускается и так." & return & ¬
  "3. Если macOS блокирует или первый запуск очень долгий — Terminal (⌘V):" & return & return & ¬
  "xattr -cr /Applications/KafDesk.app" & return & ¬
  "open /Applications/KafDesk.app" buttons {"OK"} default button 1 with title "KafDesk — after install / после установки""#;
    let tmp = std::env::temp_dir().join("kafdesk-post-install.applescript");
    if fs::write(&tmp, script).is_ok() {
        let _ = Command::new("osascript").arg(&tmp).spawn();
    }
}

#[cfg(target_os = "linux")]
fn make_executable(path: &PathBuf) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path).map_err(|e| e.to_string())?.permissions();
    perms.set_mode(perms.mode() | 0o111);
    fs::set_permissions(path, perms).map_err(|e| e.to_string())
}

async fn download_file(
    url: &str,
    dest: &PathBuf,
    mut on_progress: impl FnMut(f64),
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header("User-Agent", "KafDesk-Tauri")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("Download failed: HTTP {}", res.status()));
    }
    let total = res.content_length().unwrap_or(0);
    let mut stream = res.bytes_stream();
    let mut file = fs::File::create(dest).map_err(|e| e.to_string())?;
    let mut got: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        file.write_all(&chunk).map_err(|e| e.to_string())?;
        got += chunk.len() as u64;
        if total > 0 {
            on_progress(((got as f64 / total as f64) * 100.0).floor().min(99.0));
        }
    }
    on_progress(100.0);
    Ok(())
}

#[derive(Deserialize)]
struct GhRelease {
    tag_name: String,
    #[serde(default)]
    assets: Vec<GhAsset>,
}

#[derive(Deserialize)]
struct GhAsset {
    name: String,
    browser_download_url: String,
}

async fn fetch_latest_release() -> Result<GhRelease, String> {
    let client = reqwest::Client::new();
    let res = client
        .get(RELEASES_API)
        .header("User-Agent", "KafDesk-Tauri")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("GitHub API {}", res.status()));
    }
    res.json().await.map_err(|e| e.to_string())
}

fn platform_label() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "windows") {
        "Windows"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else {
        "unknown"
    }
}

/// Pick the installable asset for this OS/arch from the latest GitHub release.
async fn find_latest_asset() -> Result<Option<(String, String, String)>, String> {
    let rel = fetch_latest_release().await?;
    let ver = rel.tag_name.trim_start_matches('v').to_string();
    let asset = pick_asset(&rel.assets);
    Ok(asset.map(|a| (ver, a.browser_download_url.clone(), a.name.clone())))
}

fn pick_asset(assets: &[GhAsset]) -> Option<&GhAsset> {
    #[cfg(target_os = "macos")]
    {
        let arch = mac_arch();
        return assets.iter().find(|a| {
            let n = a.name.to_lowercase();
            n.contains("mac")
                && n.contains(arch)
                && n.ends_with(".dmg")
                && !n.contains("blockmap")
        });
    }

    #[cfg(target_os = "windows")]
    {
        // Prefer NSIS .exe; skip signatures / blockmaps.
        return assets
            .iter()
            .find(|a| {
                let n = a.name.to_lowercase();
                n.contains("windows") && n.ends_with(".exe") && !n.contains("blockmap")
            })
            .or_else(|| {
                assets.iter().find(|a| {
                    let n = a.name.to_lowercase();
                    n.contains("windows") && n.ends_with(".msi")
                })
            });
    }

    #[cfg(target_os = "linux")]
    {
        // AppImage is simplest to launch; .deb as fallback.
        return assets
            .iter()
            .find(|a| {
                let n = a.name.to_lowercase();
                n.contains("linux") && n.ends_with(".appimage")
            })
            .or_else(|| {
                assets.iter().find(|a| {
                    let n = a.name.to_lowercase();
                    n.contains("linux") && n.ends_with(".deb")
                })
            });
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    {
        let _ = assets;
        None
    }
}

#[cfg(target_os = "macos")]
fn mac_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        return "arm64";
    }
    if let Ok(out) = Command::new("sysctl")
        .args(["-n", "hw.optional.arm64"])
        .output()
    {
        if String::from_utf8_lossy(&out.stdout).trim() == "1" {
            return "arm64";
        }
    }
    "x64"
}

fn is_newer(remote: &str, current: &str) -> bool {
    parse_ver(remote) > parse_ver(current)
}

fn parse_ver(s: &str) -> (u64, u64, u64) {
    let mut parts = s.trim_start_matches('v').split('.').filter_map(|p| {
        p.split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|x| x.parse().ok())
    });
    (
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    )
}

fn pending_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("kafdesk")
        .join("pending-update.json")
}

fn write_pending(p: Option<PendingUpdate>) {
    let path = pending_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    match p {
        None => {
            let _ = fs::remove_file(path);
        }
        Some(v) => {
            let _ = fs::write(path, serde_json::to_string(&v).unwrap_or_default());
        }
    }
}

fn read_pending() -> Option<PendingUpdate> {
    let path = pending_path();
    if let Ok(raw) = fs::read_to_string(&path) {
        if let Ok(v) = serde_json::from_str(&raw) {
            return Some(v);
        }
    }
    // Migrate older Mac-only pending file name.
    let legacy = path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("pending-mac-update.json");
    let raw = fs::read_to_string(legacy).ok()?;
    serde_json::from_str(&raw).ok()
}

#[derive(Serialize, Deserialize)]
struct SettingsFile {
    auto_update: bool,
}

fn settings_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("kafdesk")
        .join("update-settings.json")
}

fn load_settings() -> Option<bool> {
    let raw = fs::read_to_string(settings_path()).ok()?;
    serde_json::from_str::<SettingsFile>(&raw)
        .ok()
        .map(|s| s.auto_update)
}

fn save_settings(auto_update: bool) {
    let p = settings_path();
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(
        p,
        serde_json::to_string(&SettingsFile { auto_update }).unwrap_or_default(),
    );
}
