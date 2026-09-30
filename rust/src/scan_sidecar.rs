//! Bounded observer for the existing TAR scanner. Never retains image payloads.
use crate::{MemberKind, ScanObserver, TarMember, TarScanSummary};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Write;

pub const RECORD_CAP: u64 = 32 * 1024 * 1024;
pub const METADATA_CAP: u64 = 32 * 1024 * 1024;
pub const FOOTER_CAP: u64 = 4096;
pub const LINE_CAP: usize = 32 * 1024;
pub const PATH_CAP: usize = 4096;
pub const JSON_CAP: u64 = 1024 * 1024;
pub const FORMAT: &str = "sakurapool-production-scan-v1";

struct CountedFile {
    file: File,
    hash: Sha256,
    bytes: u64,
    cap: u64,
}
impl CountedFile {
    fn new(file: File, cap: u64) -> Self {
        Self {
            file,
            hash: Sha256::new(),
            bytes: 0,
            cap,
        }
    }
    fn write(&mut self, data: &[u8]) -> Result<(), &'static str> {
        if (data.len() as u64) > self.cap.saturating_sub(self.bytes) {
            return Err("sidecar_limit");
        }
        self.file.write_all(data).map_err(|_| "sidecar_io")?;
        self.hash.update(data);
        self.bytes += data.len() as u64;
        Ok(())
    }
    fn finish(self) -> Result<(u64, String), &'static str> {
        self.file.sync_all().map_err(|_| "sidecar_io")?;
        Ok((self.bytes, format!("{:x}", self.hash.finalize())))
    }
}

#[derive(Serialize)]
struct Record {
    #[serde(flatten)]
    member: TarMember,
    metadata_offset: Option<u64>,
}

pub struct SidecarObserver {
    records: CountedFile,
    metadata: CountedFile,
    metadata_offset: Option<u64>,
    expected_json: u64,
    json_written: u64,
    json_limit: u64,
}
impl SidecarObserver {
    pub fn new(records: File, metadata: File, json_limit: u64) -> Result<Self, &'static str> {
        if json_limit == 0 || json_limit > JSON_CAP {
            return Err("metadata_limit");
        }
        Ok(Self {
            records: CountedFile::new(records, RECORD_CAP + FOOTER_CAP),
            metadata: CountedFile::new(metadata, METADATA_CAP),
            metadata_offset: None,
            expected_json: 0,
            json_written: 0,
            json_limit,
        })
    }
    pub fn finish(mut self, summary: &TarScanSummary) -> Result<serde_json::Value, &'static str> {
        let footer = serde_json::to_vec(&serde_json::json!({"complete":true,"format":FORMAT,
            "whole_sha256":summary.whole_sha256,"size":summary.size,
            "member_count":summary.member_count,"trailing_bytes":summary.trailing_bytes,
            "metadata_bytes":self.metadata.bytes}))
        .map_err(|_| "sidecar_encoding")?;
        if footer.len() as u64 + 1 > FOOTER_CAP {
            return Err("sidecar_limit");
        }
        self.records.write(&footer)?;
        self.records.write(b"\n")?;
        let (report_bytes, report_sha256) = self.records.finish()?;
        let (metadata_bytes, metadata_sha256) = self.metadata.finish()?;
        Ok(
            serde_json::json!({"format":FORMAT,"report_bytes":report_bytes,
            "report_sha256":report_sha256,"metadata_bytes":metadata_bytes,
            "metadata_sha256":metadata_sha256,"member_count":summary.member_count,
            "trailing_bytes":summary.trailing_bytes}),
        )
    }
}
impl ScanObserver for SidecarObserver {
    fn begin(
        &mut self,
        path: &str,
        kind: MemberKind,
        _offset: u64,
        size: u64,
    ) -> Result<(), &'static str> {
        if path.len() > PATH_CAP {
            return Err("member_path_limit");
        }
        self.metadata_offset = None;
        self.expected_json = 0;
        self.json_written = 0;
        if kind == MemberKind::File && path.to_ascii_lowercase().ends_with(".json") {
            if size == 0
                || size > self.json_limit
                || size > METADATA_CAP.saturating_sub(self.metadata.bytes)
            {
                return Err("metadata_limit");
            }
            self.metadata_offset = Some(self.metadata.bytes);
            self.expected_json = size;
        }
        Ok(())
    }
    fn chunk(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        if self.metadata_offset.is_some() {
            if bytes.len() as u64 > self.expected_json.saturating_sub(self.json_written) {
                return Err("metadata_limit");
            }
            self.metadata.write(bytes)?;
            self.json_written += bytes.len() as u64;
        }
        Ok(())
    }
    fn end(&mut self, member: TarMember) -> Result<(), &'static str> {
        if self.json_written != self.expected_json {
            return Err("metadata_length");
        }
        // Path is capped before serialization; even six-byte JSON escaping fits LINE_CAP.
        let record = serde_json::to_vec(&Record {
            member,
            metadata_offset: self.metadata_offset,
        })
        .map_err(|_| "sidecar_encoding")?;
        if record.len() + 1 > LINE_CAP
            || record.len() as u64 + 1 > RECORD_CAP.saturating_sub(self.records.bytes)
        {
            return Err("sidecar_limit");
        }
        self.records.write(&record)?;
        self.records.write(b"\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Owned(std::path::PathBuf);
    impl Owned {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "r2c1-sidecar-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }
        fn observer(&self) -> SidecarObserver {
            SidecarObserver::new(
                File::create(self.0.join("records")).unwrap(),
                File::create(self.0.join("metadata")).unwrap(),
                JSON_CAP,
            )
            .unwrap()
        }
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn member(path: String, size: u64) -> TarMember {
        TarMember {
            path,
            kind: MemberKind::File,
            offset: 512,
            size,
            sha256: Some("a".repeat(64)),
        }
    }
    #[test]
    fn typical_100k_members_do_not_collect_manifest() {
        let owned = Owned::new();
        let mut sink = owned.observer();
        for n in 0..100_000 {
            let path = format!("part/{}.png", n);
            sink.begin(&path, MemberKind::File, 512, 1).unwrap();
            sink.chunk(b"x").unwrap();
            sink.end(member(path, 1)).unwrap();
        }
        assert!(sink.records.bytes < RECORD_CAP);
        assert_eq!(sink.metadata.bytes, 0);
        let summary = TarScanSummary {
            whole_sha256: "a".repeat(64),
            size: 100_000 * 1024 + 1024,
            member_count: 100_000,
            trailing_bytes: 512,
        };
        let report = sink.finish(&summary).unwrap();
        assert!(report["report_bytes"].as_u64().unwrap() < RECORD_CAP + FOOTER_CAP);
    }
    #[test]
    fn aggregate_serialized_cap_includes_escaped_paths_before_write() {
        let owned = Owned::new();
        let mut sink = owned.observer();
        let path = "\u{1}".repeat(PATH_CAP);
        for _ in 0..100_000 {
            sink.begin(&path, MemberKind::File, 512, 1).unwrap();
            if sink.end(member(path.clone(), 1)).is_err() {
                break;
            }
        }
        assert!(sink.records.bytes <= RECORD_CAP);
        let before = sink.records.bytes;
        assert_eq!(sink.end(member(path, 1)), Err("sidecar_limit"));
        assert_eq!(sink.records.bytes, before);
    }
    #[test]
    fn metadata_declared_and_aggregate_limit_before_allocation_or_write() {
        let owned = Owned::new();
        let mut sink = owned.observer();
        assert_eq!(
            sink.begin("too.json", MemberKind::File, 512, JSON_CAP + 1),
            Err("metadata_limit")
        );
        let buffer = [b' '; 8192];
        for n in 0..32 {
            let path = format!("{}.json", n);
            sink.begin(&path, MemberKind::File, 512, JSON_CAP).unwrap();
            for _ in 0..JSON_CAP / buffer.len() as u64 {
                sink.chunk(&buffer).unwrap();
            }
            sink.end(member(path, JSON_CAP)).unwrap();
        }
        assert_eq!(sink.metadata.bytes, METADATA_CAP);
        assert_eq!(
            sink.begin("overflow.json", MemberKind::File, 512, 1),
            Err("metadata_limit")
        );
        assert_eq!(sink.metadata.bytes, METADATA_CAP);
    }
}
