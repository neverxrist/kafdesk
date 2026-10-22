//! Detect advertised broker DNS problems that break consume (NotLeader / hang).

use rdkafka::consumer::{Consumer, ConsumerContext};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
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

pub fn bootstrap_ip_hint(brokers: &str) -> Option<String> {
    for part in brokers.split(',') {
        let host = part
            .trim()
            .rsplit_once(':')
            .and_then(|(h, p)| {
                if p.chars().all(|c| c.is_ascii_digit()) {
                    Some(h)
                } else {
                    None
                }
            })
            .unwrap_or(part.trim())
            .trim_start_matches('[')
            .trim_end_matches(']');
        if host.parse::<IpAddr>().is_ok() {
            return Some(host.to_string());
        }
    }
    None
}

fn broker_sort_key(host: &str) -> (u32, String) {
    let label = host.split('.').next().unwrap_or(host);
    let num = label
        .rsplit(|c: char| !c.is_ascii_digit())
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(999);
    (num, host.to_string())
}

pub fn suggested_hosts_lines(hosts: &[String], brokers: &str) -> Vec<String> {
    let tip = bootstrap_ip_hint(brokers).unwrap_or_else(|| "<broker-ip>".into());
    let Ok(IpAddr::V4(base)) = tip.parse::<IpAddr>() else {
        return hosts.iter().map(|h| format!("{tip}  {h}")).collect();
    };
    let octets = base.octets();
    let mut ordered = hosts.to_vec();
    ordered.sort_by_key(|h| broker_sort_key(h));
    let first_num = ordered
        .first()
        .map(|h| broker_sort_key(h).0)
        .filter(|&n| n < 900)
        .unwrap_or(1);

    ordered
        .iter()
        .map(|h| {
            let n = broker_sort_key(h).0;
            let offset = if n < 900 {
                n.saturating_sub(first_num.min(n))
            } else {
                0
            };
            let last = octets[3].saturating_add(offset as u8);
            let ip = Ipv4Addr::new(octets[0], octets[1], octets[2], last);
            format!("{ip}  {h}")
        })
        .collect()
}

pub fn format_hosts_guidance(unresolved: &[String], brokers: &str) -> String {
    let hosts = suggested_hosts_lines(unresolved, brokers).join("\n");
    format!(
        "Broker hostnames do not resolve: {}.\n\
         Map them in /etc/hosts to reachable broker IPs, then reconnect.\n{}",
        unresolved.join(", "),
        hosts
    )
}

pub fn format_collapsed_dns_guidance(
    hosts: &[String],
    same_ip: IpAddr,
    brokers: &str,
) -> String {
    let lines = suggested_hosts_lines(hosts, brokers).join("\n");
    format!(
        "All advertised broker hostnames resolve to {same_ip}. \
         Consume needs distinct broker IPs (otherwise NotLeaderForPartition).\n\
         Add /etc/hosts entries (one IP per broker hostname), flush DNS, reconnect:\n{lines}"
    )
}

pub fn dns_warning_for_connect<C: ConsumerContext>(
    consumer: &rdkafka::consumer::BaseConsumer<C>,
    brokers: &str,
) -> Option<String> {
    let report = inspect_advertised_brokers(consumer).ok()?;
    if !report.unresolved.is_empty() {
        return Some(format_hosts_guidance(&report.unresolved, brokers));
    }
    if let Some(ip) = report.collapsed_to_one_ip() {
        let hosts: Vec<_> = report.resolved.keys().cloned().collect();
        return Some(format_collapsed_dns_guidance(&hosts, ip, brokers));
    }
    // Multi-A (hosts + DNS) is noisy and environment-specific — do not surface in UI.
    None
}

pub fn ensure_advertised_brokers_resolvable<C: ConsumerContext>(
    consumer: &rdkafka::consumer::BaseConsumer<C>,
    brokers: &str,
) -> Result<(), String> {
    let report = inspect_advertised_brokers(consumer)?;
    if !report.unresolved.is_empty() {
        return Err(format_hosts_guidance(&report.unresolved, brokers));
    }
    if let Some(ip) = report.collapsed_to_one_ip() {
        let hosts: Vec<_> = report.resolved.keys().cloned().collect();
        return Err(format_collapsed_dns_guidance(&hosts, ip, brokers));
    }
    Ok(())
}
