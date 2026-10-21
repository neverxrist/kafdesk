use crate::types::KafkaMessageView;
use byteorder::{BigEndian, ReadBytesExt};
use rdkafka::message::{Headers, Message};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;

pub fn decode_owned_message(
    topic: &str,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers_in: HashMap<String, String>,
) -> KafkaMessageView {
    let (format, value, raw_preview, schema_id) = decode_payload(payload);
    KafkaMessageView {
        topic: topic.to_string(),
        partition,
        offset: offset.to_string(),
        timestamp: timestamp_ms.to_string(),
        key: key.map(|k| String::from_utf8_lossy(k).into_owned()),
        value,
        raw_preview,
        format,
        headers: headers_in,
        schema_id,
    }
}

pub fn headers_from_message<M: Message>(msg: &M) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(headers) = msg.headers() {
        for i in 0..headers.count() {
            if let Some(h) = headers.try_get(i) {
                let key = h.key.to_string();
                let val = match h.value {
                    Some(v) => String::from_utf8_lossy(v).into_owned(),
                    None => String::new(),
                };
                out.insert(key, val);
            }
        }
    }
    out
}

fn decode_payload(payload: Option<&[u8]>) -> (String, Value, String, Option<i32>) {
    let Some(buf) = payload else {
        return ("bytes".into(), Value::Null, String::new(), None);
    };
    let schema_id = extract_confluent_schema_id(buf);
    let format = detect_format(buf, schema_id);
    let raw_preview = to_raw_preview(buf, &format);

    let value = match format.as_str() {
        "avro" => {
            // Without SR at decode time, surface raw/wire hint; SR decode happens in session when registry configured.
            Value::Object(
                [("raw".into(), Value::String(raw_preview.clone()))]
                    .into_iter()
                    .collect(),
            )
        }
        "json" => parse_json_loose(std::str::from_utf8(buf).unwrap_or("")),
        "string" => Value::String(String::from_utf8_lossy(buf).into_owned()),
        _ => Value::String(raw_preview.clone()),
    };

    (format, value, raw_preview, schema_id)
}

pub fn decode_with_avro_json(
    topic: &str,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers_in: HashMap<String, String>,
    avro_json: Option<Value>,
) -> KafkaMessageView {
    let mut view = decode_owned_message(
        topic,
        partition,
        offset,
        timestamp_ms,
        key,
        payload,
        headers_in,
    );
    if let Some(v) = avro_json {
        view.value = v;
        view.format = "avro".into();
    }
    view
}

fn extract_confluent_schema_id(buf: &[u8]) -> Option<i32> {
    if buf.len() >= 5 && buf[0] == 0 {
        let mut cur = Cursor::new(&buf[1..5]);
        cur.read_i32::<BigEndian>().ok()
    } else {
        None
    }
}

fn detect_format(buf: &[u8], schema_id: Option<i32>) -> String {
    if schema_id.is_some() {
        return "avro".into();
    }
    if let Ok(s) = std::str::from_utf8(buf) {
        let t = s.trim();
        if (t.starts_with('{') && t.ends_with('}')) || (t.starts_with('[') && t.ends_with(']')) {
            if serde_json::from_str::<Value>(t).is_ok() {
                return "json".into();
            }
        }
        if s.is_ascii() || std::str::from_utf8(buf).is_ok() {
            return "string".into();
        }
    }
    "bytes".into()
}

fn to_raw_preview(buf: &[u8], format: &str) -> String {
    if format == "bytes" {
        let take = buf.len().min(64);
        let hex: String = buf[..take]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        if buf.len() > take {
            format!("{hex}…")
        } else {
            hex
        }
    } else {
        let text = String::from_utf8_lossy(buf);
        if text.len() > 2000 {
            format!("{}…", &text[..2000])
        } else {
            text.into_owned()
        }
    }
}

pub fn parse_json_loose(s: &str) -> Value {
    serde_json::from_str(s.trim()).unwrap_or_else(|_| Value::String(s.to_string()))
}

/// Confluent wire: magic(0) + schema_id(i32 BE) + avro payload
pub fn wrap_confluent_avro(schema_id: i32, avro_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + avro_bytes.len());
    out.push(0);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(avro_bytes);
    out
}

pub fn strip_confluent_wire(buf: &[u8]) -> Option<(i32, &[u8])> {
    if buf.len() < 5 || buf[0] != 0 {
        return None;
    }
    let id = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    Some((id, &buf[5..]))
}
