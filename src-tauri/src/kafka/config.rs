use crate::types::ClusterConfig;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};

pub(crate) fn build_client_config(config: &ClusterConfig) -> Result<ClientConfig, String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", config.brokers.join(","));
    cfg.set(
        "client.id",
        config.client_id.clone().unwrap_or_else(|| "kafdesk".into()),
    );
    cfg.set("log_level", "3");
    cfg.set_log_level(RDKafkaLogLevel::Error);
    // Match Electron connection/request budgets so hung brokers don't freeze the UI forever.
    cfg.set("socket.timeout.ms", "10000");
    cfg.set("metadata.request.timeout.ms", "10000");
    cfg.set("api.version.request.timeout.ms", "10000");

    let auth = config.auth_mode.clone().unwrap_or_else(|| {
        if config
            .sasl
            .as_ref()
            .map(|s| !s.username.is_empty())
            .unwrap_or(false)
        {
            "sasl".into()
        } else {
            "none".into()
        }
    });
    // oauthbearer intentionally not supported on Tauri spike
    let auth = if auth == "oauthbearer" {
        "none".into()
    } else {
        auth
    };

    let want_ssl = config.ssl.unwrap_or(false);
    let want_sasl = auth == "sasl";

    if want_ssl && want_sasl {
        cfg.set("security.protocol", "sasl_ssl");
    } else if want_sasl {
        cfg.set("security.protocol", "sasl_plaintext");
    } else if want_ssl {
        cfg.set("security.protocol", "ssl");
    } else {
        cfg.set("security.protocol", "plaintext");
    }

    if auth == "sasl" {
        if let Some(ref sasl) = config.sasl {
            let mech = match sasl.mechanism.as_str() {
                "scram-sha-256" => "SCRAM-SHA-256",
                "scram-sha-512" => "SCRAM-SHA-512",
                _ => "PLAIN",
            };
            cfg.set("sasl.mechanisms", mech);
            cfg.set("sasl.username", &sasl.username);
            cfg.set("sasl.password", &sasl.password);
        } else {
            return Err("SASL auth selected but username/password missing".into());
        }
    }

    // Vendored OpenSSL has no corp trust store — apply Kafka TLS PEMs when SSL is on.
    if want_ssl {
        if let Some(tls) = crate::tls_resolve::kafka_tls(config) {
            if let Some(ca) = tls.ca_path.as_deref().filter(|p| !p.is_empty()) {
                cfg.set("ssl.ca.location", ca);
            }
            if let Some(cert) = tls.cert_path.as_deref().filter(|p| !p.is_empty()) {
                cfg.set("ssl.certificate.location", cert);
            }
            if let Some(key) = tls.key_path.as_deref().filter(|p| !p.is_empty()) {
                cfg.set("ssl.key.location", key);
            }
            if tls.reject_unauthorized == Some(false) {
                cfg.set("enable.ssl.certificate.verification", "false");
            }
        }

        // Broker certs often only list DNS SANs. Connecting by IP fails hostname check
        // even when the CA chain is valid — disable endpoint identification for IP bootstraps.
        if brokers_include_ip(&config.brokers) {
            cfg.set("ssl.endpoint.identification.algorithm", "none");
        }
    }

    Ok(cfg)
}

/// Human label for logs (no secrets).
pub(crate) fn security_protocol_label(config: &ClusterConfig) -> &'static str {
    let mut auth = config.auth_mode.clone().unwrap_or_else(|| {
        if config
            .sasl
            .as_ref()
            .map(|s| !s.username.is_empty())
            .unwrap_or(false)
        {
            "sasl".into()
        } else {
            "none".into()
        }
    });
    if auth == "oauthbearer" {
        auth = "none".into();
    }
    let want_ssl = config.ssl.unwrap_or(false);
    let want_sasl = auth == "sasl";
    match (want_ssl, want_sasl) {
        (true, true) => "sasl_ssl",
        (false, true) => "sasl_plaintext",
        (true, false) => "ssl",
        (false, false) => "plaintext",
    }
}

fn brokers_include_ip(brokers: &[String]) -> bool {
    brokers.iter().any(|b| {
        let host = b
            .rsplit_once(':')
            .map(|(h, p)| {
                if p.chars().all(|c| c.is_ascii_digit()) {
                    h
                } else {
                    b.as_str()
                }
            })
            .unwrap_or(b.as_str())
            .trim_start_matches('[')
            .trim_end_matches(']');
        host.parse::<std::net::IpAddr>().is_ok()
    })
}
