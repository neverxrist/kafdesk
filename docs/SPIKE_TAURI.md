# Tauri stack notes

Branch history: started as `spike/tauri-rust`; **Tauri is now the only desktop backend** (Electron removed).

See [AGENTS.md](../AGENTS.md) for module map and contracts.

## Run

```bash
source "$HOME/.cargo/env"
npm install
npm run tauri:dev
```

```bash
npm run tauri:build   # or npm run build:mac
```

cmake: Homebrew or portable under `src-tauri/.tools/cmake/` (`scripts/with-cmake.mjs`).

## Kafka auth / TLS

rdkafka with vendored OpenSSL/SASL. Modes: PLAINTEXT, SSL, SASL PLAIN/SCRAM, SASL_SSL.  
Corp CA/mTLS PEMs: `schemaRegistryTls` (also applied to broker when `ssl: true`).

## Schema Registry / Avro

Basic auth + PEM TLS (fail hard on bad paths). Schema-aware Avro via `apache_avro` resolve. Encode/decode when SR probe is `ok`.
