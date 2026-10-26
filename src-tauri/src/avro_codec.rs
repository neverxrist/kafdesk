//! Schema-aware Avro encode/decode — Confluent wire + schema resolve.
//!
//! JSON → Avro uses `TryFrom` then `resolve(schema)` so unions / nullables /
//! records / enums match Confluent behaviour.

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::Schema;
use serde_json::Value as JsonValue;

pub fn encode_json_with_schema(schema_str: &str, json: &JsonValue) -> Result<Vec<u8>, String> {
    let schema = Schema::parse_str(schema_str).map_err(|e| format!("Avro schema: {e}"))?;
    let unresolved = AvroValue::try_from(json.clone())
        .map_err(|e| format!("Avro encode (JSON→Value): {e}"))?;
    let resolved = unresolved
        .resolve(&schema)
        .map_err(|e| format!("Avro encode (resolve): {e}"))?;
    GenericDatumWriter::builder(&schema)
        .build()
        .map_err(|e| format!("Avro encode (writer): {e}"))?
        .write_value_to_vec(resolved)
        .map_err(|e| format!("Avro encode: {e}"))
}

pub fn decode_datum_to_json(schema_str: &str, body: &[u8]) -> Result<JsonValue, String> {
    let schema = Schema::parse_str(schema_str).map_err(|e| format!("Avro schema: {e}"))?;
    decode_with_schema(&schema, body)
}

pub fn decode_with_schema(schema: &Schema, body: &[u8]) -> Result<JsonValue, String> {
    let mut cursor = std::io::Cursor::new(body);
    let value = GenericDatumReader::builder(schema)
        .build()
        .map_err(|e| format!("Avro decode (reader): {e}"))?
        .read_value(&mut cursor)
        .map_err(|e| format!("Avro decode: {e}"))?;
    JsonValue::try_from(value).map_err(|e| format!("Avro → JSON: {e}"))
}
