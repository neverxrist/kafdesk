use crate::types::{ClusterConfig, SchemaSubjectInfo, SrTls};
use reqwest::{Certificate, Client, Identity};
use serde::Deserialize;
use std::fs;

#[derive(Clone)]
pub struct SchemaRegistryClient {
    host: String,
    client: Client,
    basic: Option<(String, String)>,
}

impl SchemaRegistryClient {
    pub fn new(config: &ClusterConfig) -> Result<Self, String> {
        let host = config
            .schema_registry_url
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        if host.is_empty() {
            return Err("Schema Registry URL is empty".into());
        }
        let basic = config
            .schema_registry_auth
            .as_ref()
            .map(|a| (a.username.clone(), a.password.clone()));
        let client = build_http_client(crate::tls_resolve::schema_registry_tls(config))?;
        Ok(Self { host, client, basic })
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some((u, p)) = &self.basic {
            req.basic_auth(u, Some(p))
        } else {
            req
        }
    }

    pub async fn probe(&self) -> Result<u64, String> {
        let t0 = std::time::Instant::now();
        let req = self.client.get(format!("{}/subjects", self.host));
        let res = self
            .apply_auth(req)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("HTTP {}", res.status()));
        }
        Ok(t0.elapsed().as_millis() as u64)
    }

    pub async fn list_subjects(&self) -> Result<Vec<String>, String> {
        let req = self.client.get(format!("{}/subjects", self.host));
        let res = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("Schema Registry error: {}", res.status()));
        }
        res.json().await.map_err(|e| e.to_string())
    }

    pub async fn get_latest_schema_id(&self, subject: &str) -> Result<i32, String> {
        Ok(self.get_latest_schema(subject).await?.id)
    }

    pub async fn get_latest_schema(&self, subject: &str) -> Result<SchemaSubjectInfo, String> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.host,
            urlencoding_subject(subject)
        );
        let req = self.client.get(url);
        let res = self
            .apply_auth(req)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("Schema Registry error: {}", res.status()));
        }
        #[derive(Deserialize)]
        struct Raw {
            subject: String,
            version: i32,
            id: i32,
            #[serde(default, rename = "schemaType")]
            schema_type: Option<String>,
            schema: String,
        }
        let data: Raw = res.json().await.map_err(|e| e.to_string())?;
        Ok(SchemaSubjectInfo {
            subject: data.subject,
            version: data.version,
            id: data.id,
            schema_type: data.schema_type.or(Some("AVRO".into())),
            schema: data.schema,
        })
    }

    pub async fn get_schema_by_id(&self, id: i32) -> Result<String, String> {
        let req = self.client.get(format!("{}/schemas/ids/{id}", self.host));
        let res = self
            .apply_auth(req)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("Schema Registry error: {}", res.status()));
        }
        #[derive(Deserialize)]
        struct Raw {
            schema: String,
        }
        let data: Raw = res.json().await.map_err(|e| e.to_string())?;
        Ok(data.schema)
    }
}

fn build_http_client(tls: Option<&SrTls>) -> Result<Client, String> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(20));
    let Some(tls) = tls else {
        return builder
            .build()
            .map_err(|e| format!("Schema Registry HTTP client: {e}"));
    };

    if tls.reject_unauthorized == Some(false) {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(ca) = tls.ca_path.as_deref().filter(|p| !p.is_empty()) {
        let pem = fs::read(ca).map_err(|e| format!("Cannot read TLS file: {ca} ({e})"))?;
        let cert = Certificate::from_pem(&pem).map_err(|e| format!("SR CA PEM ({ca}): {e}"))?;
        builder = builder.add_root_certificate(cert);
    }

    let cert_path = tls.cert_path.as_deref().filter(|p| !p.is_empty());
    let key_path = tls.key_path.as_deref().filter(|p| !p.is_empty());
    match (cert_path, key_path) {
        (Some(cert), Some(key)) => {
            let c = fs::read(cert).map_err(|e| format!("Cannot read TLS file: {cert} ({e})"))?;
            let k = fs::read(key).map_err(|e| format!("Cannot read TLS file: {key} ({e})"))?;
            let mut pem = c;
            if !pem.ends_with(b"\n") {
                pem.push(b'\n');
            }
            pem.extend_from_slice(&k);
            let id = Identity::from_pem(&pem)
                .map_err(|e| format!("SR client identity (cert+key): {e}"))?;
            builder = builder.identity(id);
        }
        (Some(_), None) => {
            return Err("Client certificate set but private key path is empty".into());
        }
        (None, Some(_)) => {
            return Err("Client private key set but certificate path is empty".into());
        }
        (None, None) => {}
    }

    builder
        .build()
        .map_err(|e| format!("Schema Registry HTTP client: {e}"))
}

fn urlencoding_subject(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

mod urlencoding {
    pub fn encode(s: &str) -> std::borrow::Cow<'_, str> {
        let needs = s.bytes().any(|b| {
            !(b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~')
        });
        if !needs {
            return std::borrow::Cow::Borrowed(s);
        }
        let mut out = String::new();
        for b in s.bytes() {
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                out.push(b as char);
            } else {
                out.push_str(&format!("%{b:02X}"));
            }
        }
        std::borrow::Cow::Owned(out)
    }
}
