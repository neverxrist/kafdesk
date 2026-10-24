//! Admin `DescribeConsumerGroups` — members + assignment (AKHQ/Conduktor detail path).

use crate::types::{GroupMemberAssignment, GroupMemberInfo, ConsumerGroupDetail};
use super::admin_group_offsets::classify_coord_err;
use rdkafka::admin::AdminClient;
use rdkafka::bindings::*;
use rdkafka::client::ClientContext;
use std::collections::BTreeSet;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::Duration;

pub fn describe_consumer_group<C: ClientContext>(
    admin: &AdminClient<C>,
    group_id: &str,
    timeout: Duration,
) -> Result<ConsumerGroupDetail, String> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin.inner().native_ptr();
    let c_group = CString::new(group_id).map_err(|_| "group id contains NUL".to_string())?;

    struct Cleanup {
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
            }
        }
    }

    unsafe {
        let mut cleanup = Cleanup {
            options: ptr::null_mut(),
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
        };

        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        );
        if options.is_null() {
            return Err("DescribeConsumerGroups: failed to create AdminOptions".into());
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
                "DescribeConsumerGroups: bad timeout: {}",
                CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
            ));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err("DescribeConsumerGroups: failed to create queue".into());
        }
        cleanup.queue = queue;

        let mut group_ptr: *const c_char = c_group.as_ptr();
        rd_kafka_DescribeConsumerGroups(rk, &mut group_ptr, 1, options, queue);

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(
                "DescribeConsumerGroups timed out — GroupCoordinator unreachable (not ACL)".into(),
            );
        }
        cleanup.event = event;

        if rd_kafka_event_type(event) != RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT {
            return Err("DescribeConsumerGroups: unexpected event type".into());
        }

        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let msg = cstr_or(rd_kafka_event_error_string(event), "unknown");
            return Err(classify_coord_err(&msg));
        }

        let result = rd_kafka_event_DescribeConsumerGroups_result(event);
        if result.is_null() {
            return Err("DescribeConsumerGroups: null result".into());
        }

        let mut n: usize = 0;
        let groups_ptr = rd_kafka_DescribeConsumerGroups_result_groups(result, &mut n);
        if groups_ptr.is_null() || n == 0 {
            return Err(format!("DescribeConsumerGroups: group '{group_id}' not found"));
        }

        let grp = *groups_ptr.add(0);
        let err = rd_kafka_ConsumerGroupDescription_error(grp);
        if !err.is_null() {
            let code = rd_kafka_error_code(err);
            if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let msg = cstr_or(rd_kafka_error_string(err), "describe error");
                return Err(classify_coord_err(&msg));
            }
        }

        let state = rd_kafka_ConsumerGroupDescription_state(grp);
        let state_name = cstr_or(rd_kafka_consumer_group_state_name(state), "Unknown");

        let mut coordinator_id = None;
        let coord = rd_kafka_ConsumerGroupDescription_coordinator(grp);
        if !coord.is_null() {
            coordinator_id = Some(rd_kafka_Node_id(coord));
        }

        let member_count = rd_kafka_ConsumerGroupDescription_member_count(grp);
        let mut members = Vec::with_capacity(member_count);
        let mut topic_set: BTreeSet<String> = BTreeSet::new();

        for i in 0..member_count {
            let m = rd_kafka_ConsumerGroupDescription_member(grp, i);
            if m.is_null() {
                continue;
            }
            let member_id = cstr_or(rd_kafka_MemberDescription_consumer_id(m), "");
            let client_id = cstr_or(rd_kafka_MemberDescription_client_id(m), "");
            let host = cstr_or(rd_kafka_MemberDescription_host(m), "");
            let mut assignment = Vec::new();
            let assign = rd_kafka_MemberDescription_assignment(m);
            if !assign.is_null() {
                let parts = rd_kafka_MemberAssignment_partitions(assign);
                if !parts.is_null() {
                    let cnt = (*parts).cnt;
                    let elems = (*parts).elems;
                    if !elems.is_null() {
                        for j in 0..cnt {
                            let elem = &*elems.add(j as usize);
                            if elem.topic.is_null() {
                                continue;
                            }
                            let topic = CStr::from_ptr(elem.topic).to_string_lossy().into_owned();
                            topic_set.insert(topic.clone());
                            assignment.push(GroupMemberAssignment {
                                topic,
                                partition: elem.partition,
                            });
                        }
                    }
                }
            }
            members.push(GroupMemberInfo {
                member_id,
                client_id,
                host,
                assignment,
            });
        }

        Ok(ConsumerGroupDetail {
            group_id: group_id.to_string(),
            state: Some(state_name),
            member_count,
            coordinator_id,
            members,
            topics: topic_set.into_iter().collect(),
        })
    }
}

fn cstr_or(p: *const c_char, fallback: &str) -> String {
    if p.is_null() {
        fallback.to_string()
    } else {
        unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
    }
}
