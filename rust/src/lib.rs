//! Offline, bounded primitives for the SakuraPool Rust worker.
//!
//! All worker state is in-memory by contract: the durable budget ledger
//! lives exclusively on the Python side. [JobBudget] is a per-process guard
//! whose counters are intentionally lost on crash (a crash refunds nothing;
//! the Python durable ledger keeps the reservation pending).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{self, Read};

/// Version of the NDJSON control protocol spoken on stdin/stdout.
pub const PROTOCOL_VERSION: u32 = 1;
/// Hard cap for a single control line in either direction.
pub const MAX_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetLimits {
    /// Cumulative raw body bytes the job may consume.
    pub body: u64,
    /// Maximum per-request disk (staging/output) bytes.
    pub disk: u64,
    /// Maximum per-request in-flight (network + IPC) bytes.
    pub inflight: u64,
    /// Cumulative number of attempts the job may start.
    pub attempts: u64,
}

/// In-memory job budget. Never persisted; lost on crash on purpose.
#[derive(Debug, Clone)]
pub struct JobBudget {
    limit: BudgetLimits,
    attempts_used: u64,
    body_consumed: u64,
}

impl JobBudget {
    pub fn new(limit: BudgetLimits) -> Self {
        Self {
            limit,
            attempts_used: 0,
            body_consumed: 0,
        }
    }
    pub fn limit(&self) -> BudgetLimits {
        self.limit
    }
    pub fn attempts_used(&self) -> u64 {
        self.attempts_used
    }
    pub fn body_consumed(&self) -> u64 {
        self.body_consumed
    }
    /// Fail-closed admission check for one request reservation.
    pub fn admit(&self, need: &BudgetLimits) -> Result<(), &'static str> {
        if need.attempts > self.limit.attempts.saturating_sub(self.attempts_used) {
            return Err("budget_exceeded");
        }
        if need.body > self.limit.body.saturating_sub(self.body_consumed) {
            return Err("budget_exceeded");
        }
        if need.disk > self.limit.disk {
            return Err("budget_exceeded");
        }
        if need.inflight > self.limit.inflight {
            return Err("budget_exceeded");
        }
        Ok(())
    }
    pub fn commit_attempt(&mut self) {
        self.attempts_used = self.attempts_used.saturating_add(1);
    }
    pub fn commit_body(&mut self, bytes: u64) {
        self.body_consumed = self.body_consumed.saturating_add(bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}
impl ByteRange {
    pub fn new(start: u64, end: u64, total: u64) -> Result<Self, &'static str> {
        if start > end || end >= total {
            return Err("range outside resource");
        }
        Ok(Self { start, end })
    }
    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }
    pub fn is_empty(self) -> bool {
        self.end < self.start
    }
}

pub fn validate_content_range(
    request: ByteRange,
    total: u64,
    value: &str,
    body_len: u64,
) -> Result<(), &'static str> {
    let (unit, rest) = value.split_once(' ').ok_or("invalid content range")?;
    if unit != "bytes" {
        return Err("unsupported range unit");
    }
    let (span, advertised_total) = rest.split_once('/').ok_or("invalid content range")?;
    let (start, end) = span.split_once('-').ok_or("invalid content range")?;
    let parsed = ByteRange::new(
        start.parse().map_err(|_| "invalid content range")?,
        end.parse().map_err(|_| "invalid content range")?,
        total,
    )?;
    if parsed != request
        || advertised_total
            .parse::<u64>()
            .map_err(|_| "invalid content range")?
            != total
        || body_len != request.len()
    {
        return Err("content range mismatch");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopbackTarget {
    pub host: String,
    pub port: u16,
    pub path: String,
}

/// Only loopback http URLs are acceptable; everything else is rejected before any I/O.
pub fn parse_loopback_url(url: &str) -> Result<LoopbackTarget, &'static str> {
    let rest = url
        .split_once("://")
        .ok_or("missing scheme")
        .and_then(|(scheme, rest)| {
            (scheme == "http")
                .then_some(rest)
                .ok_or("only http is allowed")
        })?;
    let (hostport, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let (host, port) = hostport.rsplit_once(':').ok_or("missing port")?;
    let port = port.parse::<u16>().map_err(|_| "invalid port")?;
    if host != "127.0.0.1" && host != "localhost" && host != "::1" {
        return Err("non-loopback target");
    }
    Ok(LoopbackTarget {
        host: host.to_owned(),
        port,
        path: path.to_owned(),
    })
}

pub struct StreamingSha256(Sha256);
impl Default for StreamingSha256 {
    fn default() -> Self {
        Self::new()
    }
}
impl StreamingSha256 {
    pub fn new() -> Self {
        Self(Sha256::new())
    }
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    pub fn finish(self) -> String {
        format!("{:x}", self.0.finalize())
    }
    pub fn digest_reader<R: Read>(mut reader: R) -> io::Result<(String, u64)> {
        let mut hasher = Self::new();
        let mut buffer = [0u8; 64 * 1024];
        let mut total = 0u64;
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            total += count as u64;
            hasher.update(&buffer[..count]);
        }
        Ok((hasher.finish(), total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn budget_admits_within_limits_and_rejects_overbudget() {
        let limit = BudgetLimits {
            body: 100,
            disk: 10,
            inflight: 10,
            attempts: 2,
        };
        let mut budget = JobBudget::new(limit);
        assert!(budget
            .admit(&BudgetLimits {
                body: 60,
                disk: 4,
                inflight: 4,
                attempts: 1
            })
            .is_ok());
        budget.commit_attempt();
        budget.commit_body(60);
        // Body remainder is now 40; a 60-byte request must fail.
        assert_eq!(
            budget.admit(&BudgetLimits {
                body: 60,
                disk: 4,
                inflight: 4,
                attempts: 1
            }),
            Err("budget_exceeded")
        );
        // Attempts are exhausted after one more admission.
        assert!(budget
            .admit(&BudgetLimits {
                body: 40,
                disk: 4,
                inflight: 4,
                attempts: 1
            })
            .is_ok());
        budget.commit_attempt();
        assert_eq!(
            budget.admit(&BudgetLimits {
                body: 0,
                disk: 0,
                inflight: 0,
                attempts: 1
            }),
            Err("budget_exceeded")
        );
        // Per-request disk/inflight caps are independent of consumption.
        assert_eq!(
            JobBudget::new(limit).admit(&BudgetLimits {
                body: 0,
                disk: 11,
                inflight: 0,
                attempts: 0
            }),
            Err("budget_exceeded")
        );
        assert_eq!(
            JobBudget::new(limit).admit(&BudgetLimits {
                body: 0,
                disk: 0,
                inflight: 11,
                attempts: 0
            }),
            Err("budget_exceeded")
        );
    }

    #[test]
    fn range_is_exact() {
        let range = ByteRange::new(10, 19, 100).unwrap();
        assert!(validate_content_range(range, 100, "bytes 10-19/100", 10).is_ok());
        assert!(validate_content_range(range, 100, "bytes 10-20/100", 10).is_err());
    }

    #[test]
    fn hash_is_streaming() {
        let mut hasher = StreamingSha256::new();
        hasher.update(b"a");
        hasher.update(b"bc");
        let (direct, bytes) = (hasher.finish(), 3);
        let (streamed, streamed_bytes) =
            StreamingSha256::digest_reader(Cursor::new(b"abc")).unwrap();
        assert_eq!(direct, streamed);
        assert_eq!(bytes, streamed_bytes);
    }

    #[test]
    fn loopback_url_rejects_everything_but_loopback_http() {
        let ok = parse_loopback_url("http://127.0.0.1:8080/data/obj.bin").unwrap();
        assert_eq!(
            ok,
            LoopbackTarget {
                host: "127.0.0.1".to_owned(),
                port: 8080,
                path: "/data/obj.bin".to_owned()
            }
        );
        assert!(parse_loopback_url("http://localhost:9/x").is_ok());
        assert!(parse_loopback_url("http://example.com:1/").is_err());
        assert!(parse_loopback_url("https://127.0.0.1:1/").is_err());
        assert!(parse_loopback_url("127.0.0.1:1/").is_err());
        assert!(parse_loopback_url("http://127.0.0.1").is_err());
        assert!(parse_loopback_url("http://127.0.0.1:70000/").is_err());
    }
}
