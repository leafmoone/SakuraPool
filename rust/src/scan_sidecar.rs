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
