//! Offline, bounded primitives for the SakuraPool Rust worker.
//!
//! All worker state is in-memory by contract: the durable budget ledger
//! lives exclusively on the Python side. [JobBudget] is a per-process guard
//! whose counters are intentionally lost on crash (a crash refunds nothing;
//! the Python durable ledger keeps the reservation pending).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

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

// ---------------------------------------------------------------------------
// Sequential uncompressed TAR scanner (mature `tar` crate, no custom parsing)
// ---------------------------------------------------------------------------

/// Member kinds the scanner will emit. Everything else (links, sparse,
/// devices, fifos, unknown) is rejected before any payload is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberKind {
    File,
    Dir,
}

/// One archive member with its exact payload location in the raw file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TarMember {
    pub path: String,
    pub kind: MemberKind,
    /// Byte offset of the payload start (directories: the offset of the
    /// (empty) payload position, which equals the next entry's header start).
    pub offset: u64,
    /// Declared payload size in bytes (directories are 0).
    pub size: u64,
    /// Streaming SHA-256 of the payload, files only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Result of a bounded sequential scan of one uncompressed archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TarScan {
    /// SHA-256 of the entire raw archive file.
    pub whole_sha256: String,
    /// Raw archive size in bytes.
    pub size: u64,
    pub members: Vec<TarMember>,
    /// Bytes after the final entry's payload (the closing zero blocks plus
    /// any padding). Always all-zero; nonzero tails are rejected upstream.
    pub trailing_bytes: u64,
}

/// Hard bounds for a single scan; exceeding any of them is a clean rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanLimits {
    pub max_members: u64,
    pub max_bytes: u64,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            max_members: 100_000,
            max_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

/// Reads any source exactly once, front to back, remembering how many
/// bytes flowed (`bytes`) and the SHA-256 of every byte (`hasher`). When
/// the cumulative count passes the limit the flag is set and the scan
/// fails closed at its next checkpoint - no extra I/O is required.
struct HashCountReader<R> {
    inner: R,
    hasher: StreamingSha256,
    bytes: Arc<AtomicU64>,
    max_bytes: u64,
    over_limit: Arc<AtomicBool>,
}

impl<R: Read> Read for HashCountReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            let total = self.bytes.fetch_add(n as u64, Ordering::SeqCst) + n as u64;
            self.hasher.update(&buf[..n]);
            if total > self.max_bytes {
                self.over_limit.store(true, Ordering::SeqCst);
            }
        }
        Ok(n)
    }
}

impl<R> HashCountReader<R> {
    fn new(inner: R, max_bytes: u64) -> (Self, Arc<AtomicU64>, Arc<AtomicBool>) {
        let bytes = Arc::new(AtomicU64::new(0));
        let over_limit = Arc::new(AtomicBool::new(false));
        (
            Self {
                inner,
                hasher: StreamingSha256::new(),
                bytes: bytes.clone(),
                max_bytes,
                over_limit: over_limit.clone(),
            },
            bytes,
            over_limit,
        )
    }
}

fn member_path<R: Read>(entry: &tar::Entry<'_, R>) -> Result<String, &'static str> {
    // `path_bytes` is the lossless accessor; validate UTF-8 ourselves so the
    // failure is platform-independent (Windows `Path` rejects 0xFF bytes).
    let bytes = entry.path_bytes();
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text.to_owned(),
        Err(_) => return Err("non_utf8_member"),
    };
    if text.is_empty() {
        return Err("unsafe_member_path");
    }
    // Tar member paths are POSIX; check the raw text directly (host OS path
    // semantics would misclassify e.g. "/etc/x" on Windows).
    if text.starts_with('/') {
        return Err("unsafe_member_path");
    }
    for component in text.split('/') {
        if component == ".." {
            return Err("unsafe_member_path");
        }
    }
    Ok(text)
}

/// Single-pass scan of an uncompressed archive from any byte source.
///
/// The source is consumed exactly once, front to back: the wrapper hashes
/// every byte as it flows, so whole-file SHA and member offsets come from
/// the same read. Non-seekable streams (pipes, sockets, 1-byte readers) are
/// fine - nothing is ever re-read. Fail-closed on truncation, checksum
/// errors, nonzero tails, any non-regular/non-directory member, and any
/// bound exceeded (checked as bytes flow).
pub fn scan_tar_reader<R: Read>(reader: R, limits: &ScanLimits) -> Result<TarScan, &'static str> {
    let (reader, bytes, over_limit) = HashCountReader::new(reader, limits.max_bytes);
    let mut archive = tar::Archive::new(reader);

    let mut members: Vec<TarMember> = Vec::new();
    {
        let entries = archive.entries().map_err(|_| "corrupt_archive")?;
        for entry in entries {
            let entry = entry.map_err(|_| "corrupt_archive")?;
            if (members.len() as u64) >= limits.max_members {
                return Err("limit_exceeded");
            }
            let path = member_path(&entry)?;
            let kind = entry.header().entry_type();
            // After `entries.next()` the underlying reader has consumed exactly
            // header + extension headers, so its byte count is the payload start.
            let offset = bytes.load(Ordering::SeqCst);
            match kind {
                tar::EntryType::Regular => {
                    let declared = entry.header().size().map_err(|_| "corrupt_archive")?;
                    if declared > limits.max_bytes {
                        return Err("limit_exceeded");
                    }
                    let mut member_hasher = StreamingSha256::new();
                    let mut read: u64 = 0;
                    let mut buffer = [0u8; 64 * 1024];
                    let mut reader = entry;
                    loop {
                        let n = reader.read(&mut buffer).map_err(|_| "corrupt_archive")?;
                        if n == 0 {
                            break;
                        }
                        read += n as u64;
                        if read > declared {
                            return Err("corrupt_archive");
                        }
                        member_hasher.update(&buffer[..n]);
                    }
                    if over_limit.load(Ordering::SeqCst) {
                        return Err("limit_exceeded");
                    }
                    if read != declared {
                        return Err("truncated_member");
                    }
                    members.push(TarMember {
                        path,
                        kind: MemberKind::File,
                        offset,
                        size: declared,
                        sha256: Some(member_hasher.finish()),
                    });
                }
                tar::EntryType::Directory => {
                    members.push(TarMember {
                        path,
                        kind: MemberKind::Dir,
                        offset,
                        size: 0,
                        sha256: None,
                    });
                }
                // Links, sparse, char/block devices, fifos, and anything unknown.
                _ => return Err("unsupported_member"),
            }
            if over_limit.load(Ordering::SeqCst) {
                return Err("limit_exceeded");
            }
        }
    }
    let mut tail = archive.into_inner();
    let mut trailing_bytes = 0u64;
    let mut buffer = [0u8; 4096];
    loop {
        let n = tail.read(&mut buffer).map_err(|_| "corrupt_archive")?;
        if n == 0 {
            break;
        }
        if buffer[..n].iter().any(|byte| *byte != 0) {
            return Err("invalid_tail");
        }
        trailing_bytes += n as u64;
        if trailing_bytes > 64 * 1024 {
            return Err("limit_exceeded");
        }
    }
    if over_limit.load(Ordering::SeqCst) || trailing_bytes > limits.max_bytes {
        return Err("limit_exceeded");
    }
    let size = bytes.load(Ordering::SeqCst);
    Ok(TarScan {
        whole_sha256: tail.hasher.finish(),
        size,
        members,
        trailing_bytes,
    })
}

/// Sequentially scan an uncompressed archive file (one pass).
pub fn scan_tar_file(path: &Path, limits: &ScanLimits) -> Result<TarScan, &'static str> {
    let file = std::fs::File::open(path).map_err(|_| "io_error")?;
    scan_tar_reader(file, limits)
}

/// Compatibility alias for [scan_tar_file].
pub fn scan_tar(path: &Path, limits: &ScanLimits) -> Result<TarScan, &'static str> {
    scan_tar_file(path, limits)
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
