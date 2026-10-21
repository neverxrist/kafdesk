use crate::types::{UpdateSettings, UpdateStatus};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use tauri::{AppHandle, Emitter};
use tauri_plugin_dialog::{DialogExt, MessageDialogButtons, MessageDialogKind};
use tauri_plugin_opener::OpenerExt;

const RELEASES_API: &str = "https://api.github.com/repos/neverxrist/kafdesk/releases/latest";
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
        let latest = match find_latest_mac_dmg().await {
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
                message: Some("No macOS DMG found for this arch".into()),
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

        write_pending(Some(PendingMacUpdate {
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

        let open = offer_open_dialog(app, &ver).await;
        if open {
            open_dmg_and_quit(app, &dest, &ver);
        }

        st
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PendingMacUpdate {
    path: String,
    version: String,
}

async fn offer_open_dialog(app: &AppHandle, version: &str) -> bool {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let message = format!("KafDesk {version} downloaded.");
    let detail = format!(
        "Open DMG → KafDesk quits → drag into Applications (Replace).\n\n\
         Then in Terminal (copied to clipboard):\n\n{POST_INSTALL_CMDS}\n\n\
         Connections and settings are kept."
    );
    app.dialog()
        .message(message)
        .kind(MessageDialogKind::Info)
        .title("KafDesk update")
        .buttons(MessageDialogButtons::OkCancelCustom(
            "Open DMG & quit".into(),
            "Later".into(),
        ))
        .show(move |yes| {
            let _ = tx.send(yes);
        });
    // Keep detail available via log — MessageDialogBuilder may not expose detail on all platforms.
    log::info!("{detail}");
    rx.await.unwrap_or(false)
}

fn open_dmg_and_quit(app: &AppHandle, dmg: &PathBuf, version: &str) {
    write_pending(Some(PendingMacUpdate {
        path: dmg.to_string_lossy().into_owned(),
        version: version.to_string(),
    }));
    show_post_install_help();
    let _ = app.opener().open_path(dmg.to_string_lossy(), None::<&str>);
    let handle = app.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(400));
        let _ = handle.exit(0);
    });
}

fn show_post_install_help() {
    #[cfg(target_os = "macos")]
    {
        let _ = Command::new("pbcopy")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                if let Some(mut stdin) = child.stdin.take() {
                    let _ = stdin.write_all(POST_INSTALL_CMDS.as_bytes());
                }
                child.wait()
            });

        let script = format!(
            r#"display dialog "1. Перетащи KafDesk.app в Applications → Replace" & return & return & ¬
  "2. НЕ открывай app двойным кликом — сначала Terminal (⌘V, уже в буфере):" & return & return & ¬
  "xattr -cr /Applications/KafDesk.app" & return & ¬
  "open /Applications/KafDesk.app" & return & return & ¬
  "Если открыть без xattr — первый старт будет очень долгим (Gatekeeper)." buttons {{"OK"}} default button 1 with title "KafDesk — после установки""#
        );
        let tmp = std::env::temp_dir().join("kafdesk-post-install.applescript");
        if fs::write(&tmp, script).is_ok() {
            let _ = Command::new("osascript")
                .arg(&tmp)
                .spawn();
        }
    }
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
            on_progress(((got as f64 / total as f64) * 100.0).min(99.0));
        }
    }
    on_progress(100.0);
    Ok(())
}

async fn find_latest_mac_dmg() -> Result<Option<(String, String, String)>, String> {
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
    #[derive(Deserialize)]
    struct Release {
        tag_name: String,
        assets: Vec<Asset>,
    }
    #[derive(Deserialize)]
    struct Asset {
        name: String,
        browser_download_url: String,
    }
    let rel: Release = res.json().await.map_err(|e| e.to_string())?;
    let ver = rel.tag_name.trim_start_matches('v').to_string();
    let arch = mac_arch();
    let asset = rel
        .assets
        .into_iter()
        .find(|a| {
            let n = a.name.to_lowercase();
            n.contains("mac") && n.contains(arch) && n.ends_with(".dmg") && !n.ends_with(".blockmap")
        });
    Ok(asset.map(|a| (ver, a.browser_download_url, a.name)))
}

fn mac_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        return "arm64";
    }
    // Rosetta: prefer arm64 DMG on Apple Silicon
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = Command::new("sysctl")
            .args(["-n", "hw.optional.arm64"])
            .output()
        {
            if String::from_utf8_lossy(&out.stdout).trim() == "1" {
                return "arm64";
            }
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
        .join("pending-mac-update.json")
}

fn write_pending(p: Option<PendingMacUpdate>) {
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
