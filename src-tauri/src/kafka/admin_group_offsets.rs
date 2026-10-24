//! Admin API `ListConsumerGroupOffsets` — same path AKHQ uses for lag.
//! Avoids creating a Consumer with the target `group.id` (that path storms
//! GroupCoordinator SASL handshakes and is easy to confuse with ACL failures).

use rdkafka::admin::AdminClient;
use rdkafka::bindings::*;
use rdkafka::client::ClientContext;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::Duration;

/// One committed offset for `(topic, partition)`.
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Fetch committed offsets for `group_id` (NULL partition list → broker returns
/// only partitions the group has committed).
pub fn list_committed_offsets<C: ClientContext>(
    admin: &AdminClient<C>,
    group_id: &str,
    timeout: Duration,
) -> Result<Vec<CommittedOffset>, String> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin.inner().native_ptr();
    let c_group = CString::new(group_id).map_err(|_| "group id contains NUL".to_string())?;

    struct Cleanup {
        request: *mut rd_kafka_ListConsumerGroupOffsets_t,
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
                if !self.request.is_null() {
                    rd_kafka_ListConsumerGroupOffsets_destroy(self.request);
                }
            }
        }
    }

    unsafe {
        let mut cleanup = Cleanup {
            request: ptr::null_mut(),
            options: ptr::null_mut(),
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
        };

        // NULL partitions → only committed TPs (KIP-88).
        let req = rd_kafka_ListConsumerGroupOffsets_new(c_group.as_ptr(), ptr::null_mut());
        if req.is_null() {
            return Err("ListConsumerGroupOffsets: failed to create request".into());
        }
        cleanup.request = req;

        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        );
        if options.is_null() {
            return Err("ListConsumerGroupOffsets: failed to create AdminOptions".into());
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
                "ListConsumerGroupOffsets: bad timeout: {}",
                CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
            ));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err("ListConsumerGroupOffsets: failed to create queue".into());
        }
        cleanup.queue = queue;

        let mut req_ptr = cleanup.request;
        rd_kafka_ListConsumerGroupOffsets(rk, &mut req_ptr, 1, options, queue);
        // Ownership of request moves into the call; avoid double-free.
        cleanup.request = ptr::null_mut();

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(
                "ListConsumerGroupOffsets timed out — GroupCoordinator unreachable (often advertised.listeners / VPN), not ACL"
                    .into(),
            );
        }
        cleanup.event = event;

        if rd_kafka_event_type(event) != RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT {
            return Err("ListConsumerGroupOffsets: unexpected event type".into());
        }

        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let msg = cstr_or(rd_kafka_event_error_string(event), "unknown");
            return Err(classify_coord_err(&msg));
        }

        let result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
        if result.is_null() {
            return Err("ListConsumerGroupOffsets: null result".into());
        }

        let mut n_groups: usize = 0;
        let groups_ptr = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n_groups);
        if groups_ptr.is_null() || n_groups == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for i in 0..n_groups {
            let group = *groups_ptr.add(i);
            let group_error = rd_kafka_group_result_error(group);
            if !group_error.is_null() {
                let code = rd_kafka_error_code(group_error);
                if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    let msg = cstr_or(rd_kafka_error_string(group_error), "group error");
                    return Err(classify_coord_err(&msg));
                }
            }
            let partitions = rd_kafka_group_result_partitions(group);
            if partitions.is_null() {
                continue;
            }
            let cnt = (*partitions).cnt;
            let elems = (*partitions).elems;
            if elems.is_null() {
                continue;
            }
            for j in 0..cnt {
                let elem = &*elems.add(j as usize);
                if elem.offset < 0 || elem.topic.is_null() {
                    continue;
                }
                let topic = CStr::from_ptr(elem.topic).to_string_lossy().into_owned();
                out.push(CommittedOffset {
                    topic,
                    partition: elem.partition,
                    offset: elem.offset,
                });
            }
        }
        Ok(out)
    }
}

fn cstr_or(p: *const c_char, fallback: &str) -> String {
    if p.is_null() {
        fallback.to_string()
    } else {
        unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
    }
}

/// Map broker/rdkafka wording so the UI can tell ACL from transport.
pub fn classify_coord_err(msg: &str) -> String {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("group authorization")
        || lower.contains("topic authorization")
        || lower.contains("cluster authorization")
        || lower.contains("authorization failed")
    {
        return format!("ACL: {msg}");
    }
    if lower.contains("authentication") || lower.contains("sasl") {
        if lower.contains("state down")
            || lower.contains("(n/a)")
            || lower.contains("transport")
        {
            return format!(
                "Coordinator transport/SASL handshake failed (not ACL — advertised.listeners / VPN / wrong listener). {msg}"
            );
        }
        return format!("SASL auth: {msg}");
    }
    if lower.contains("transport") || lower.contains("timed out") || lower.contains("disconnect") {
        return format!("Coordinator unreachable (not ACL): {msg}");
    }
    msg.to_string()
}
