//! Trim WebView2 working set when the window is unfocused (Windows only).
//!
//! `with_webview` yields Tauri's `PlatformWebview`, not wry's `WebView`, so we
//! call the WebView2 COM API via `controller()` instead of `WebViewExtWindows`.

use tauri::{AppHandle, RunEvent, WindowEvent};

#[cfg(windows)]
fn set_level(app: &AppHandle, low: bool) {
    use tauri::Manager;
    use webview2_com::Microsoft::Web::WebView2::Win32::{
        ICoreWebView2_19, COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL_LOW,
        COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL_NORMAL,
    };
    use windows::core::Interface;

    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    let target = if low {
        COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL_LOW
    } else {
        COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL_NORMAL
    };
    let _ = window.with_webview(move |webview| {
        let _ = unsafe {
            webview
                .controller()
                .CoreWebView2()
                .and_then(|core| core.cast::<ICoreWebView2_19>())
                .and_then(|wv| wv.SetMemoryUsageTargetLevel(target))
        };
    });
}

#[cfg(not(windows))]
fn set_level(_app: &AppHandle, _low: bool) {}

/// Call from `Builder::build` / `run` event loop: Low when unfocused, Normal when focused.
pub fn on_run_event(app: &AppHandle, event: &RunEvent) {
    match event {
        RunEvent::WindowEvent {
            label,
            event: WindowEvent::Focused(focused),
            ..
        } if label == "main" => {
            set_level(app, !*focused);
        }
        _ => {}
    }
}
