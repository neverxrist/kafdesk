use crate::types::KafkaMessageView;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct BrokerMessageFilter {
    pub search: Option<String>,
    pub search_key: Option<String>,
    /// "both" | "key" | "value" — default both for `search`.
    pub search_scope: Option<String>,
    pub header_key: Option<String>,
    pub header_value: Option<String>,
    pub time_from: Option<i64>,
    pub time_to: Option<i64>,
}

fn normalize_ts_ms(raw: i64) -> i64 {
    if raw < 1_000_000_000_000 {
        raw * 1000
    } else {
        raw
    }
}

pub fn matches_raw_pre_filter(
    key: Option<&[u8]>,
    timestamp: &str,
    headers: &HashMap<String, String>,
    f: &BrokerMessageFilter,
) -> bool {
    if let Some(ref sk) = f.search_key {
        let needle = sk.trim().to_lowercase();
        if !needle.is_empty() {
            let key_text = key
                .map(|k| String::from_utf8_lossy(k).to_lowercase())
                .unwrap_or_default();
            if !key_text.contains(&needle) {
                return false;
            }
        }
    }

    let hk = f
        .header_key
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    let hv = f
        .header_value
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    if hk.is_some() || hv.is_some() {
        let ok = headers.iter().any(|(k, v)| {
            let lk = k.to_lowercase();
            let lv = v.to_lowercase();
            if let Some(ref h) = hk {
                if !lk.contains(h) {
                    return false;
                }
            }
            if let Some(ref h) = hv {
                if !lv.contains(h) {
                    return false;
                }
            }
            true
        });
        if !ok {
            return false;
        }
    }

    if f.time_from.is_some() || f.time_to.is_some() {
        let raw: i64 = timestamp.parse().unwrap_or(i64::MIN);
        if raw == i64::MIN {
            return false;
        }
        let ts = normalize_ts_ms(raw);
        if let Some(from) = f.time_from {
            if ts < from {
                return false;
            }
        }
        if let Some(to) = f.time_to {
            if ts > to {
                return false;
            }
        }
    }

    true
}

pub fn matches_decoded_message(view: &KafkaMessageView, f: &BrokerMessageFilter) -> bool {
    let search = f
        .search
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    let search_key = f
        .search_key
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    let scope = f
        .search_scope
        .as_deref()
        .unwrap_or("both")
        .to_ascii_lowercase();

    let key_text = view.key.clone().unwrap_or_default().to_lowercase();
    let value_hay = format!("{} {}", view.raw_preview, view.value).to_lowercase();
    let both_hay = format!("{key_text} {value_hay}");

    // Explicit key needle (legacy AND / scope=key wire).
    if let Some(ref sk) = search_key {
        if !key_text.contains(sk) {
            return false;
        }
    }
    if let Some(ref s) = search {
        let ok = match scope.as_str() {
            "value" => value_hay.contains(s),
            "key" => key_text.contains(s),
            _ => both_hay.contains(s),
        };
        if !ok {
            return false;
        }
    }

    let hk = f
        .header_key
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    let hv = f
        .header_value
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    if hk.is_some() || hv.is_some() {
        let ok = view.headers.iter().any(|(k, v)| {
            let lk = k.to_lowercase();
            let lv = v.to_lowercase();
            if let Some(ref h) = hk {
                if !lk.contains(h) {
                    return false;
                }
            }
            if let Some(ref h) = hv {
                if !lv.contains(h) {
                    return false;
                }
            }
            true
        });
        if !ok {
            return false;
        }
    }

    if f.time_from.is_some() || f.time_to.is_some() {
        let raw: i64 = view.timestamp.parse().unwrap_or(i64::MIN);
        if raw == i64::MIN {
            return false;
        }
        let ts = normalize_ts_ms(raw);
        if let Some(from) = f.time_from {
            if ts < from {
                return false;
            }
        }
        if let Some(to) = f.time_to {
            if ts > to {
                return false;
            }
        }
    }

    true
}

pub fn sort_messages_by_time(mut messages: Vec<KafkaMessageView>, newest_first: bool) -> Vec<KafkaMessageView> {
    messages.sort_by(|a, b| {
        let ta: i64 = a.timestamp.parse().unwrap_or(0);
        let tb: i64 = b.timestamp.parse().unwrap_or(0);
        let ord = if newest_first {
            tb.cmp(&ta)
        } else {
            ta.cmp(&tb)
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let pord = if newest_first {
            b.partition.cmp(&a.partition)
        } else {
            a.partition.cmp(&b.partition)
        };
        if pord != std::cmp::Ordering::Equal {
            return pord;
        }
        let oa: i64 = a.offset.parse().unwrap_or(0);
        let ob: i64 = b.offset.parse().unwrap_or(0);
        if newest_first {
            ob.cmp(&oa)
        } else {
            oa.cmp(&ob)
        }
    });
    messages
}
