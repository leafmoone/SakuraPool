//! Offline, bounded primitives for the first SakuraPool Rust stage.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerEntry {
    pub id: String,
    pub reserved: u64,
    pub settled: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LedgerState {
    budget: u64,
    spent: u64,
    entries: Vec<LedgerEntry>,
}

#[derive(Debug)]
pub struct BudgetLedger {
    path: PathBuf,
    state: LedgerState,
}

impl BudgetLedger {
    pub fn open(path: impl AsRef<Path>, budget: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let exists = path.exists();
        let state = if exists {
            serde_json::from_reader(File::open(&path)?).map_err(invalid_data)?
        } else {
            LedgerState {
                budget,
                ..Default::default()
            }
        };
        if state.budget != budget || state.spent > state.budget {
            return Err(invalid_data("invalid budget state"));
        }
        Ok(Self { path, state })
    }
    pub fn available(&self) -> u64 {
        self.state.budget - self.state.spent
    }
    pub fn reserve(&mut self, id: impl Into<String>, amount: u64) -> io::Result<()> {
        let id = id.into();
        if self.state.entries.iter().any(|e| e.id == id) {
            return Err(invalid_data("duplicate reservation"));
        }
        if amount > self.available() {
            return Err(io::Error::other("budget exceeded"));
        }
        self.state.spent += amount;
        self.state.entries.push(LedgerEntry {
            id,
            reserved: amount,
            settled: None,
        });
        self.persist()
    }
    pub fn settle(&mut self, id: &str, actual: u64) -> io::Result<()> {
        let entry = self
            .state
            .entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| invalid_data("unknown reservation"))?;
        if entry.settled.is_some() {
            return Err(invalid_data("already settled"));
        }
        if actual > entry.reserved {
            return Err(invalid_data("settlement exceeds reservation"));
        }
        self.state.spent -= entry.reserved - actual;
        entry.settled = Some(actual);
        self.persist()
    }
    pub fn entry(&self, id: &str) -> Option<&LedgerEntry> {
        self.state.entries.iter().find(|e| e.id == id)
    }
    fn persist(&self) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        serde_json::to_writer(&mut file, &self.state).map_err(invalid_data)?;
        file.sync_all()?;
        fs::rename(tmp, &self.path)
    }
}

fn invalid_data<E: std::fmt::Display>(error: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
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
        self.end - self.start + 1
    }
    /// A valid range always spans at least one byte (`start <= end`).
    pub fn is_empty(self) -> bool {
        false
    }
}

pub fn validate_content_range(
    request: ByteRange,
    total: u64,
    value: &str,
    body_len: u64,
) -> Result<(), &'static str> {
    let (unit, rest) = value.split_once(' ').ok_or("invalid Content-Range")?;
    if unit != "bytes" {
        return Err("unsupported range unit");
    }
    let (span, advertised_total) = rest.split_once('/').ok_or("invalid Content-Range")?;
    let (start, end) = span.split_once('-').ok_or("invalid Content-Range")?;
    let parsed = ByteRange::new(
        start.parse().map_err(|_| "invalid start")?,
        end.parse().map_err(|_| "invalid end")?,
        total,
    )?;
    if parsed != request
        || advertised_total
            .parse::<u64>()
            .map_err(|_| "invalid total")?
            != total
        || body_len != request.len()
    {
        return Err("Content-Range does not match request");
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
    pub fn digest_reader<R: Read>(mut reader: R) -> io::Result<String> {
        let mut hasher = Self::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        Ok(hasher.finish())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Created,
    Responding,
    Completed,
    Cancelled,
}
#[derive(Debug)]
pub struct ResponseLifecycle {
    state: Lifecycle,
}
impl Default for ResponseLifecycle {
    fn default() -> Self {
        Self::new()
    }
}
impl ResponseLifecycle {
    pub fn new() -> Self {
        Self {
            state: Lifecycle::Created,
        }
    }
    pub fn state(&self) -> Lifecycle {
        self.state
    }
    pub fn begin(&mut self) -> Result<(), &'static str> {
        if self.state != Lifecycle::Created {
            return Err("response already started");
        }
        self.state = Lifecycle::Responding;
        Ok(())
    }
    pub fn complete(&mut self) -> Result<(), &'static str> {
        if self.state != Lifecycle::Responding {
            return Err("response is not active");
        }
        self.state = Lifecycle::Completed;
        Ok(())
    }
    pub fn cancel(&mut self) -> Result<(), &'static str> {
        if matches!(self.state, Lifecycle::Completed | Lifecycle::Cancelled) {
            return Err("response already closed");
        }
        self.state = Lifecycle::Cancelled;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn temp() -> PathBuf {
        std::env::temp_dir().join(format!(
            "sakurapool-r1-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    #[test]
    fn ledger_reserve_settle_and_reopen() {
        let path = temp();
        let mut ledger = BudgetLedger::open(&path, 10).unwrap();
        ledger.reserve("a", 7).unwrap();
        assert_eq!(ledger.available(), 3);
        assert!(ledger.reserve("b", 4).is_err());
        ledger.settle("a", 5).unwrap();
        assert_eq!(ledger.available(), 5);
        let reopened = BudgetLedger::open(&path, 10).unwrap();
        assert_eq!(reopened.entry("a").unwrap().settled, Some(5));
        let _ = fs::remove_file(path);
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
        assert_eq!(
            hasher.finish(),
            StreamingSha256::digest_reader(Cursor::new(b"abc")).unwrap()
        );
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
    #[test]
    fn lifecycle_rejects_invalid_transitions() {
        let mut lifecycle = ResponseLifecycle::new();
        lifecycle.begin().unwrap();
        lifecycle.complete().unwrap();
        assert!(lifecycle.cancel().is_err());
    }
}
