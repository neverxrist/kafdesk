//! Admin API `ListOffsets(EARLIEST|LATEST)` — one batched request for many partitions
//! (librdkafka routes per leader), instead of `fetch_watermarks` per partition.

use rdkafka::admin::AdminClient;
use rdkafka::bindings::*;
use rdkafka::client::ClientContext;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::Duration;

#[derive(Clone, Copy)]
pub enum OffsetSpec {
    Earliest,
    Latest,
}

const CHUNK: usize = 1000;

/// `list_offsets` in chunks of `CHUNK`; a failed chunk is retried per topic so one
/// bad topic doesn't blank the rest. Missing partitions are omitted.
pub fn list_offsets_chunked<C: ClientContext>(
    admin: &AdminClient<C>,
    tps: &[(String, i32)],
    spec: OffsetSpec,
    timeout: Duration,
) -> HashMap<(String, i32), i64> {
    let mut out = HashMap::new();
    for chunk in tps.chunks(CHUNK) {
        match list_offsets(admin, chunk, spec, timeout) {
            Ok(m) => out.extend(m),
            Err(_) => {
                let mut by_topic: HashMap<&str, Vec<(String, i32)>> = HashMap::new();
                for tp in chunk {
                    by_topic.entry(tp.0.as_str()).or_default().push(tp.clone());
                }
                for list in by_topic.values() {
                    if let Ok(m) = list_offsets(admin, list, spec, timeout) {
                        out.extend(m);
                    }
                }
            }
        }
    }
    out
}

/// Offsets for `tps` at `spec` (no duplicate TPs). Errored partitions are omitted.
pub fn list_offsets<C: ClientContext>(
    admin: &AdminClient<C>,
    tps: &[(String, i32)],
    spec: OffsetSpec,
    timeout: Duration,
) -> Result<HashMap<(String, i32), i64>, String> {
    let spec = match spec {
        OffsetSpec::Earliest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_EARLIEST,
        OffsetSpec::Latest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_LATEST,
    };
    let mut out = HashMap::new();
    if tps.is_empty() {
        return Ok(out);
    }
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin.inner().native_ptr();

    struct Cleanup {
        list: *mut rd_kafka_topic_partition_list_t,
        options: *mut rd_kafka_AdminOptions_t,
        queue: *mut rd_kafka_queue_t,
        event: *mut rd_kafka_event_t,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            unsafe {
                if !self.event.is_null() {
                    rd_kafka_event_destroy(self.event);
                }
                if !self.queue.is_null() {
                    rd_kafka_queue_destroy(self.queue);
                }
                if !self.options.is_null() {
                    rd_kafka_AdminOptions_destroy(self.options);
                }
                if !self.list.is_null() {
                    rd_kafka_topic_partition_list_destroy(self.list);
                }
            }
        }
    }

    unsafe {
        let mut cleanup = Cleanup {
            list: ptr::null_mut(),
            options: ptr::null_mut(),
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
        };

        let list = rd_kafka_topic_partition_list_new(tps.len().min(i32::MAX as usize) as i32);
        if list.is_null() {
            return Err("ListOffsets: failed to create partition list".into());
        }
        cleanup.list = list;
        for (topic, partition) in tps {
            let c_topic = CString::new(topic.as_str()).map_err(|_| "topic contains NUL".to_string())?;
            let elem = rd_kafka_topic_partition_list_add(list, c_topic.as_ptr(), *partition);
            if !elem.is_null() {
                (*elem).offset = spec as i64;
            }
        }

        let options = rd_kafka_AdminOptions_new(rk, rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTOFFSETS);
        if options.is_null() {
            return Err("ListOffsets: failed to create AdminOptions".into());
        }
        cleanup.options = options;
        let mut errstr = [0 as c_char; 512];
        let err = rd_kafka_AdminOptions_set_request_timeout(
            options,
            timeout_ms,
            errstr.as_mut_ptr(),
            errstr.len(),
        );
        if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(format!(
                "ListOffsets: bad timeout: {}",
                CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
            ));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err("ListOffsets: failed to create queue".into());
        }
        cleanup.queue = queue;

        // List is copied by librdkafka; we still own and destroy it.
        rd_kafka_ListOffsets(rk, list, options, queue);

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err("ListOffsets timed out".into());
        }
        cleanup.event = event;
        if rd_kafka_event_type(event) != RD_KAFKA_EVENT_LISTOFFSETS_RESULT {
            return Err("ListOffsets: unexpected event type".into());
        }
        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let p = rd_kafka_event_error_string(event);
            let msg = if p.is_null() {
                "unknown".to_string()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            };
            return Err(format!("ListOffsets: {msg}"));
        }
        let result = rd_kafka_event_ListOffsets_result(event);
        if result.is_null() {
            return Err("ListOffsets: null result".into());
        }
        let mut cnt: usize = 0;
        let infos = rd_kafka_ListOffsets_result_infos(result, &mut cnt);
        if infos.is_null() {
            return Ok(out);
        }
        for i in 0..cnt {
            let info = *infos.add(i);
            if info.is_null() {
                continue;
            }
            let tp = rd_kafka_ListOffsetsResultInfo_topic_partition(info);
            if tp.is_null() || (*tp).topic.is_null() {
                continue;
            }
            if (*tp).err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR || (*tp).offset < 0 {
                continue;
            }
            let topic = CStr::from_ptr((*tp).topic).to_string_lossy().into_owned();
            out.insert((topic, (*tp).partition), (*tp).offset);
        }
        Ok(out)
    }
}
