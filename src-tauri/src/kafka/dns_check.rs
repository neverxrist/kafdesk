//! Detect advertised broker DNS problems that break consume (NotLeader / hang).

use rdkafka::consumer::{Consumer, ConsumerContext};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct AdvertisedDnsReport {
    pub unresolved: Vec<String>,
    /// hostname → resolved IPs (when lookup succeeded)
    pub resolved: BTreeMap<String, BTreeSet<IpAddr>>,
}

impl AdvertisedDnsReport {
    /// Every broker hostname has exactly one A record, and they are all the same IP.
    pub fn collapsed_to_one_ip(&self) -> Option<IpAddr> {
        if self.resolved.len() < 2 {
            return None;
        }
        let mut sole: Option<IpAddr> = None;
        for set in self.resolved.values() {
            if set.len() != 1 {
                return None;
            }
            let ip = *set.iter().next()?;
            match sole {
                None => sole = Some(ip),
                Some(s) if s == ip => {}
                Some(_) => return None,
            }
        }
        sole
    }
}

pub fn inspect_advertised_brokers<C: ConsumerContext>(
    consumer: &rdkafka::consumer::BaseConsumer<C>,
) -> Result<AdvertisedDnsReport, String> {
    let meta = consumer
        .fetch_metadata(None, Duration::from_secs(8))
        .map_err(|e| format!("metadata for DNS check: {e}"))?;

    let mut report = AdvertisedDnsReport::default();
    for broker in meta.brokers() {
        let host = broker.host().trim();
        if host.is_empty() {
            continue;
        }
        if host.parse::<IpAddr>().is_ok() {
            continue;
        }
        let port = broker.port();
        let sock = if port > 0 {
            format!("{host}:{port}")
        } else {
            format!("{host}:9092")
        };
        match sock.to_socket_addrs() {
            Ok(addrs) => {
                let ips: BTreeSet<IpAddr> = addrs.map(|a| a.ip()).collect();
                if ips.is_empty() {
                    report.unresolved.push(host.to_string());
                } else {
                    report.resolved.insert(host.to_string(), ips);
                }
            }
            Err(_) => report.unresolved.push(host.to_string()),
        }
    }
    report.unresolved.sort();
    report.unresolved.dedup();
    Ok(report)
}

fn format_unresolved(unresolved: &[String]) -> String {
    let n = unresolved.len();
    let preview: Vec<_> = unresolved.iter().take(3).map(|s| s.as_str()).collect();
    let list = preview.join(", ");
    if n > 3 {
        format!(
            "Broker DNS: {n} hostnames do not resolve (e.g. {list}, …). Check VPN / DNS / advertised.listeners."
        )
    } else {
        format!(
            "Broker DNS: hostnames do not resolve ({list}). Check VPN / DNS / advertised.listeners."
        )
    }
}

fn format_collapsed(same_ip: IpAddr, host_count: usize) -> String {
    format!(
        "Broker DNS: {host_count} hostnames all resolve to {same_ip} (need distinct IPs). Check VPN / DNS / fake-IP / advertised.listeners."
    )
}

pub fn dns_warning_for_connect<C: ConsumerContext>(
    consumer: &rdkafka::consumer::BaseConsumer<C>,
    _brokers: &str,
) -> Option<String> {
    let report = inspect_advertised_brokers(consumer).ok()?;
    if !report.unresolved.is_empty() {
        return Some(format_unresolved(&report.unresolved));
    }
    if let Some(ip) = report.collapsed_to_one_ip() {
        return Some(format_collapsed(ip, report.resolved.len()));
    }
    // Multi-A (hosts + DNS) is noisy and environment-specific — do not surface in UI.
    None
}

pub fn ensure_advertised_brokers_resolvable<C: ConsumerContext>(
    consumer: &rdkafka::consumer::BaseConsumer<C>,
    _brokers: &str,
) -> Result<(), String> {
    let report = inspect_advertised_brokers(consumer)?;
    if !report.unresolved.is_empty() {
        return Err(format_unresolved(&report.unresolved));
    }
    if let Some(ip) = report.collapsed_to_one_ip() {
        return Err(format_collapsed(ip, report.resolved.len()));
    }
    Ok(())
}
