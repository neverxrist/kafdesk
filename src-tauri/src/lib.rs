mod auth;
mod avro_codec;
mod commands;
mod debug_log;
mod domain;
mod kafka;
mod tls_resolve;
mod sr;
mod types;
mod updates;
mod webview_memory;

use commands::AppState;
use std::sync::Arc;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = Arc::new(AppState::new());
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .setup(|app| {
            debug_log::bind_app(app.handle().clone());
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        // librdkafka / rdkafka go to Logs tab via KafdeskCtx — not stderr.
                        .level_for("librdkafka", log::LevelFilter::Off)
                        .level_for("rdkafka", log::LevelFilter::Off)
                        .level_for("rdkafka::client", log::LevelFilter::Off)
                        .build(),
                )?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::ping_shell,
            commands::set_backend_debug,
            commands::get_backend_debug,
            commands::kafka_connect,
            commands::kafka_disconnect,
            commands::kafka_status,
            commands::kafka_ping,
            commands::kafka_probe,
            commands::kafka_list_topics,
            commands::kafka_get_topic_offsets,
            commands::kafka_topics_offsets,
            commands::kafka_fetch_message,
            commands::kafka_trace_key,
            commands::kafka_trace_cancel,
            commands::kafka_consume,
            commands::kafka_produce,
            commands::kafka_produce_batch,
            commands::kafka_list_subjects,
            commands::kafka_get_latest_schema,
            commands::kafka_list_consumer_groups,
            commands::kafka_get_group_offsets,
            commands::kafka_describe_consumer_group,
            commands::kafka_refresh_group_snapshot,
            commands::kafka_topic_consumers,
            commands::kafka_get_lag_summary,
            commands::kafka_set_group_offsets,
            commands::kafka_reset_group_offsets,
            commands::kafka_tail_start,
            commands::kafka_tail_stop,
            commands::kafka_tail_set_filter,
            commands::dialog_pick_pem_file,
            commands::dialog_pick_avsc_file,
            commands::dialog_save_text_file,
            commands::auth_list_providers,
            commands::auth_session,
            commands::auth_login,
            commands::auth_logout,
            commands::auth_stored_credentials,
            commands::auth_forget_credentials,
            commands::app_check_for_updates,
            commands::app_update_status,
            commands::app_update_settings,
            commands::app_set_auto_update,
            commands::app_open_downloaded_update,
            commands::app_open_releases,
            commands::app_get_version,
        ])
        .build(tauri::generate_context!())
        .expect("error while building KafDesk")
        .run(|app, event| {
            webview_memory::on_run_event(app, &event);
        });
}
