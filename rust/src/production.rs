//! Explicit provider byte plane. No discovery API, credentials/URLs never leave memory.
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
pub const NEGATIVE_CONDITION_BODY_CAP: u64 = 65_536;
fn public_scan_error(error: &'static str) -> &'static str {
    if error == "metadata_limit" {
        "metadata_limit"
    } else {
        "scan_failed"
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub repo_id: String,
    pub repo_type: String,
    pub origin: String,
    pub revision: String,
    pub object_path: String,
    pub object_size: u64,
    pub validator: Option<String>,
    pub cdn_host: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub profile: String,
    pub object: Object,
    pub token: Option<String>,
    pub cookie: Option<String>,
    pub start: u64,
    pub length: u64,
    pub condition: String,
    pub output_root: PathBuf,
    pub output_name: String,
    pub report_name: Option<String>,
    pub mode: String,
    #[serde(default = "default_json_limit")]
    pub json_limit: u64,
    #[serde(default = "default_revision")]
    pub payload_revision: u32,
    #[serde(default = "default_range")]
    pub range_chunk_bytes: u64,
    #[serde(default = "default_header")]
    pub http_header_bytes: usize,
}
fn default_revision() -> u32 {
    1
}
fn default_range() -> u64 {
    crate::HTTP_MAX_RANGE_BYTES
}
fn default_header() -> usize {
    crate::HTTP_MAX_HEADER_BYTES
}
fn default_json_limit() -> u64 {
    crate::scan_sidecar::JSON_CAP
}
/// Shared Python/Rust protocol allocation contract; see production_resources.py.
pub fn protocolmemory(rpc: usize, header: usize) -> Result<u64, &'static str> {
    let line = (rpc as u64).max(65_536);
    let headers = (header as u64)
        .max(417_792)
        .checked_mul(4)
        .and_then(|n| n.checked_add(128 * 256))
        .and_then(|n| n.checked_mul(2))
        .ok_or("production_budget")?;
    line.checked_mul(13)
        .and_then(|n| n.checked_add(line.min(65_536) * 256))
        .and_then(|n| n.checked_add(1 << 20))
        .and_then(|n| n.checked_add(headers))
        .ok_or("production_budget")
}
pub fn footprint(mode: &str, bytes: u64) -> Result<(u64, u64), &'static str> {
    footprint_with_capacity(mode, bytes, crate::MAX_LINE_BYTES, default_header())
}
pub fn footprint_with_capacity(
    mode: &str,
    bytes: u64,
    rpc: usize,
    header: usize,
) -> Result<(u64, u64), &'static str> {
    let protocol = protocolmemory(rpc, header)?;
    let artifacts = crate::scan_sidecar::RECORD_CAP
        + crate::scan_sidecar::METADATA_CAP
        + crate::scan_sidecar::FOOTER_CAP;
    let (memory, disk) = match mode {
        "range" => (
            bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(32 * 1024 * 1024))
                .ok_or("production_budget")?,
            bytes,
        ),
        "download-then-scan" => (
            128 * 1024 * 1024,
            bytes.checked_add(artifacts).ok_or("production_budget")?,
        ),
        "remote-stream-scan" => (128 * 1024 * 1024, artifacts),
        _ => return Err("production_budget"),
    };
    disk.checked_add(16 << 10).ok_or("production_budget")?;
    Ok((
        memory.checked_add(protocol).ok_or("production_budget")?,
        disk,
    ))
}
#[derive(Default, Serialize)]
pub struct Accounting {
    pub attempts: u64,
    pub body: u64,
    pub complete: bool,
    #[serde(skip)]
    pub phase: &'static str,
    #[serde(skip)]
    pub http_status: Option<u16>,
    #[serde(skip)]
    pub content_length_present: bool,
    #[serde(skip)]
    pub content_range_present: bool,
    #[serde(skip)]
    pub etag_present: bool,
    #[serde(skip)]
    pub etag_is_strong: bool,
    #[serde(skip)]
    pub content_encoding_present: bool,
    #[serde(skip)]
    pub origin_http_status: Option<u16>,
    #[serde(skip)]
    pub cdn_http_status: Option<u16>,
    #[serde(skip)]
    pub cdn_content_length: Option<u64>,
}
impl Accounting {
    fn observe_headers(&mut self, response: &Response) {
        self.http_status = Some(response.status().as_u16());
        let h = response.headers();
        if self.phase == "origin" {
            self.origin_http_status = self.http_status;
        }
        if self.phase == "cdn" {
            self.cdn_http_status = self.http_status;
            self.cdn_content_length = h
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
        }
        self.content_length_present = h.contains_key("content-length");
        self.content_range_present = h.contains_key("content-range");
        self.etag_present = h.contains_key("etag");
        self.etag_is_strong = h
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .is_some_and(validator);
        self.content_encoding_present = h.contains_key("content-encoding");
    }
    pub fn observation(&self) -> serde_json::Value {
        serde_json::json!({"origin_http_status":self.origin_http_status,"cdn_http_status":self.cdn_http_status,"content_length":self.cdn_content_length})
    }
    pub fn diagnostic(&self) -> serde_json::Value {
        serde_json::json!({"phase":self.phase,"http_status":self.http_status,"attempts":self.attempts,"body_bytes_observed":self.body,
            "accounting_complete":self.complete,"content_length_present":self.content_length_present,
            "content_range_present":self.content_range_present,"etag_present":self.etag_present,"etag_is_strong":self.etag_is_strong,
            "content_encoding_present":self.content_encoding_present})
    }
}
pub struct Outcome {
    pub result: serde_json::Value,
    pub error: Option<&'static str>,
    pub accounting: Accounting,
}
fn valid_atom(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn validator(s: &str) -> bool {
    s.len() >= 3
        && s.len() <= 256
        && s.starts_with('"')
        && s.ends_with('"')
        && s[1..s.len() - 1]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn credential_echo(t: &Transfer, text: &str) -> bool {
    t.token.as_ref().is_some_and(|v| text.contains(v))
        || t.cookie.as_ref().is_some_and(|cookie| {
            text.contains(cookie)
                || cookie.split(';').any(|part| {
                    part.trim()
                        .split_once('=')
                        .is_some_and(|(_, value)| !value.is_empty() && text.contains(value))
                })
        })
}
fn loopback(url: &Url) -> bool {
    url.scheme() == "http" && url.host_str() == Some("127.0.0.1") && url.port().is_some()
}
fn origin(t: &Transfer) -> Result<Url, &'static str> {
    let o = &t.object;
    let mut u = Url::parse(&o.origin).map_err(|_| "origin_invalid")?;
    let test = t.profile == "twohop_test";
    if !matches!(t.profile.as_str(), "modelscope_https_v1" | "twohop_test")
        || u.path() != "/"
        || u.query().is_some()
        || u.fragment().is_some()
        || !u.username().is_empty()
        || u.password().is_some()
        || (!test
            && (u.scheme() != "https"
                || !matches!(u.host_str(), Some("www.modelscope.cn" | "modelscope.cn"))
                || u.port_or_known_default() != Some(443)))
        || (test && !loopback(&u))
        || o.repo_type != "modelscope_dataset_legacy"
        || o.object_size == 0
        || !matches!(o.revision.len(), 40 | 64)
        || !o.revision.bytes().all(|b| b.is_ascii_hexdigit())
        || o.repo_id.split('/').count() != 2
        || !o.repo_id.split('/').all(valid_atom)
        || o.object_path
            .split('/')
            .any(|p| !valid_atom(p) || p == "." || p == "..")
        || !matches!(
            t.mode.as_str(),
            "range" | "remote-stream-scan" | "download-then-scan"
        )
        || !matches!(t.condition.as_str(), "observe" | "match" | "wrong")
        || (t.condition != "observe" && !o.validator.as_deref().is_some_and(validator))
        || (t.mode != "range" && (t.condition != "match" || t.report_name.is_none()))
        || (t.mode == "range"
            && (t.length == 0
                || t.length > t.range_chunk_bytes
                || t.start
                    .checked_add(t.length)
                    .is_none_or(|n| n > o.object_size)))
    {
        return Err("object_profile_invalid");
    }
    if !matches!(t.payload_revision, 1 | 2)
        || t.range_chunk_bytes == 0
        || t.http_header_bytes == 0
        || t.http_header_bytes > 16 * 1024 * 1024
        || (t.payload_revision == 1
            && (t.range_chunk_bytes != default_range() || t.http_header_bytes != default_header()))
    {
        return Err("object_profile_invalid");
    }
    if t.cookie.as_ref().is_some_and(|v| {
        v.is_empty() || v.len() > 512 || v.bytes().any(|b| !(0x20..0x7f).contains(&b))
    }) {
        return Err("credential_invalid");
    }
    if let Some(token) = &t.token {
        if token.is_empty()
            || token.len() > 4096
            || token.bytes().any(|b| !(0x21..0x7f).contains(&b))
            || o.validator.as_ref().is_some_and(|v| v.contains(token))
        {
            return Err("credential_invalid");
        }
    }
    u.set_path(&format!("/api/v1/datasets/{}/repo", o.repo_id));
    u.query_pairs_mut()
        .append_pair("Revision", &o.revision)
        .append_pair("FilePath", &o.object_path);
    Ok(u)
}
fn client() -> Result<Client, &'static str> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::ACCEPT,
        HeaderValue::from_static("application/json, application/octet-stream"),
    );
    // Pinned reqwest does not expose hyper's HTTP/1 allocation knobs. The shared
    // admission model charges its 417792-byte parser ceiling even for tiny limits.
    Client::builder()
        .user_agent("SakuraMoon/1")
        .default_headers(headers)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| "client_failed")
}
fn single<'a>(h: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, &'static str> {
    if h.get_all(name).iter().count() > 1 {
        return Err("duplicate_header");
    }
    h.get(name)
        .map(|v| v.to_str().map_err(|_| "header_invalid"))
        .transpose()
}
fn headers(r: &Response, limit: usize) -> Result<(), &'static str> {
    let mut total = 0usize;
    for (k, v) in r.headers() {
        total = total
            .checked_add(k.as_str().len())
            .and_then(|n| n.checked_add(v.as_bytes().len()))
            .and_then(|n| n.checked_add(4))
            .ok_or("headers_limit")?;
    }
    if total > limit {
        return Err("headers_limit");
    }
    for k in [
        "location",
        "content-range",
        "content-length",
        "content-encoding",
        "etag",
        "transfer-encoding",
        "content-type",
    ] {
        single(r.headers(), k)?;
    }
    Ok(())
}
fn decode(s: &str) -> Result<String, &'static str> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() {
                return Err("location_encoding");
            }
            let v = u8::from_str_radix(&s[i + 1..i + 3], 16).map_err(|_| "location_encoding")?;
            if !b"/+=%".contains(&v) {
                return Err("location_encoding");
            }
            out.push(v);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "location_encoding")
}
fn location(raw: &str, t: &Transfer) -> Result<Url, &'static str> {
    if raw.is_empty() || raw.len() > 8192 || !raw.is_ascii() {
        return Err("location_invalid");
    }
    let u = Url::parse(raw).map_err(|_| "location_invalid")?;
    let host = u.host_str().ok_or("location_invalid")?;
    let mut layer = raw.to_string();
    for depth in 0..5 {
        if layer.bytes().any(|b| b <= 0x20 || b == 0x7f || b == b'\\') {
            return Err("location_invalid");
        }
        let d = Url::parse(&layer).map_err(|_| "location_invalid")?;
        if d.host_str() != Some(host)
            || !d.username().is_empty()
            || d.password().is_some()
            || d.fragment().is_some()
            || d.path().contains('%')
            || layer.split('/').nth(2).is_some_and(|a| a.contains('%'))
            || layer.contains("/../")
            || layer.contains("/./")
            || (depth == 0 && d.query().is_some_and(|q| q.contains('+')))
            || (t.profile == "twohop_test" && !loopback(&d))
            || (t.profile != "twohop_test"
                && (d.scheme() != "https"
                    || d.port_or_known_default() != Some(443)
                    || matches!(host, "www.modelscope.cn" | "modelscope.cn")
                    || host.parse::<std::net::IpAddr>().is_ok()
                    || !host.contains('.')
                    || !host.split('.').all(|p| {
                        valid_atom(p)
                            && !p.contains('_')
                            && !p.starts_with('-')
                            && !p.ends_with('-')
                    })))
            || credential_echo(t, &layer)
        {
            return Err("location_invalid");
        }
        if !layer.contains('%') {
            break;
        }
        layer = decode(&layer)?;
        if layer.len() > 8192 || (depth == 4 && layer.contains('%')) {
            return Err("location_invalid");
        }
    }
    if host.len() > 253 {
        return Err("location_invalid");
    }
    if t.object.cdn_host.as_ref().is_some_and(|h| h != host) {
        return Err("cdn_binding_mismatch");
    }
    let pairs: Vec<_> = u.query_pairs().collect();
    let signed = pairs
        .iter()
        .any(|(k, _)| k.to_ascii_lowercase().contains("signature"));
    let expires: Vec<_> = pairs
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("expires"))
        .collect();
    if signed || !expires.is_empty() {
        if expires.len() != 1 {
            return Err("expiry_unknown");
        }
        let exp = expires[0].1.parse::<u64>().map_err(|_| "expiry_unknown")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "clock_unknown")?
            .as_secs();
        if exp <= now + 5 || exp > now + 86400 {
            return Err("expiry_rejected");
        }
    }
    Ok(u)
}
fn file(root: &Path, name: &str) -> Result<File, &'static str> {
    if !root.is_absolute() || !valid_atom(name) || name == "." || name == ".." {
        return Err("output_invalid");
    }
    let mut cur = PathBuf::new();
    for c in root.components() {
        if c == Component::ParentDir {
            return Err("output_invalid");
        }
        cur.push(c);
        let m = std::fs::symlink_metadata(&cur).map_err(|_| "output_invalid")?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if m.file_attributes() & 0x400 != 0 {
                return Err("output_reparse");
            }
        }
        if m.file_type().is_symlink() || !m.is_dir() {
            return Err("output_invalid");
        }
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(name))
        .map_err(|_| "output_exists")
}
struct CountTee<'a> {
    response: Response,
    file: Option<File>,
    max: u64,
    count: &'a mut u64,
    complete: &'a mut bool,
}
impl Read for CountTee<'_> {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        if b.is_empty() {
            return Ok(0);
        }
        let cap =
            (self.max.saturating_add(1).saturating_sub(*self.count)).min(b.len() as u64) as usize;
        if cap == 0 {
            return Err(std::io::Error::other("body_limit"));
        }
        let n = self.response.read(&mut b[..cap])?;
        *self.count += n as u64;
        if *self.count > self.max {
            return Err(std::io::Error::other("body_limit"));
        }
        if let Some(file) = self.file.as_mut() {
            file.write_all(&b[..n])?;
        }
        if n == 0 {
            *self.complete = true;
        }
        Ok(n)
    }
}
#[derive(Default)]
pub struct ExecutionContext {
    origin_client: Option<Client>,
    origin_identity: Option<String>,
    cdn_client: Option<Client>,
    cdn_hosts: std::collections::BTreeSet<String>,
}
impl ExecutionContext {
    fn origin_client(&mut self, identity: &str) -> Result<Client, &'static str> {
        if self.origin_identity.as_deref() != Some(identity) {
            self.origin_client = Some(client()?);
            self.origin_identity = Some(identity.to_owned());
        }
        self.origin_client.clone().ok_or("client_failed")
    }
    fn cdn_client(&mut self, target: &Url) -> Result<Client, &'static str> {
        let authority = format!(
            "{}:{}",
            target.host_str().ok_or("location_invalid")?,
            target.port_or_known_default().ok_or("location_invalid")?
        );
        if !self.cdn_hosts.contains(&authority) && self.cdn_hosts.len() >= 8 {
            self.cdn_client = None;
            self.cdn_hosts.clear();
        }
        if self.cdn_client.is_none() {
            self.cdn_client = Some(client()?);
        }
        self.cdn_hosts.insert(authority);
        self.cdn_client.clone().ok_or("client_failed")
    }
}
fn transfer(
    t: &Transfer,
    a: &mut Accounting,
    context: &mut ExecutionContext,
) -> Result<serde_json::Value, &'static str> {
    let url = origin(t)?;
    let mut output = if t.mode == "remote-stream-scan" {
        None
    } else {
        Some(file(&t.output_root, &t.output_name)?)
    };
    let mut sidecars = if t.mode == "range" {
        None
    } else {
        Some(crate::scan_sidecar::SidecarObserver::new(
            file(
                &t.output_root,
                t.report_name.as_deref().ok_or("report_missing")?,
            )?,
            file(&t.output_root, "metadata.bin")?,
            t.json_limit,
        )?)
    };
    let origin_client = context.origin_client(&t.object.origin)?;
    let mut req = origin_client.get(url).header("accept-encoding", "identity");
    let range = format!("bytes={}-{}", t.start, t.start + t.length.saturating_sub(1));
    if t.mode == "range" {
        req = req.header("range", &range);
    }
    if let Some(v) = &t.token {
        req = req.header(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {v}")).map_err(|_| "credential_invalid")?,
        );
    }
    if let Some(v) = &t.cookie {
        req = req.header(
            "cookie",
            HeaderValue::from_str(v).map_err(|_| "credential_invalid")?,
        );
    }
    a.phase = "origin";
    a.http_status = None;
    a.attempts += 1;
    a.complete = false;
    let r = req.send().map_err(|_| "origin_transport")?;
    a.observe_headers(&r);
    headers(&r, t.http_header_bytes)?;
    a.complete = single(r.headers(), "content-length")? == Some("0")
        && single(r.headers(), "transfer-encoding")?.is_none();
    if !a.complete {
        return Err("origin_body_unknown");
    }
    if r.status().as_u16() != 302 {
        return Err("origin_status");
    }
    let target = location(
        single(r.headers(), "location")?.ok_or("location_missing")?,
        t,
    )?;
    drop(r);
    drop(origin_client);
    let cdn = context.cdn_client(&target)?;
    let mut req = cdn
        .get(target.clone())
        .header("accept-encoding", "identity");
    if t.mode == "range" {
        req = req.header("range", range);
    }
    if t.condition != "observe" {
        let tag = if t.condition == "wrong" {
            "\"sakurapool-deliberately-wrong-r2\""
        } else {
            t.object.validator.as_deref().ok_or("validator_missing")?
        };
        req = req.header("if-match", tag);
    }
    a.phase = "cdn";
    a.http_status = None;
    a.content_length_present = false;
    a.content_range_present = false;
    a.etag_present = false;
    a.etag_is_strong = false;
    a.content_encoding_present = false;
    a.attempts += 1;
    a.complete = false;
    let mut r = req.send().map_err(|_| "cdn_transport")?;
    a.observe_headers(&r);
    headers(&r, t.http_header_bytes)?;
    a.complete = single(r.headers(), "content-length")? == Some("0")
        && single(r.headers(), "transfer-encoding")?.is_none();
    let status = r.status().as_u16();
    if t.condition == "wrong" {
        a.complete = false;
        if status != 412 {
            return Err("conditional_unsupported");
        }
        let cl = single(r.headers(), "content-length")?;
        let te = single(r.headers(), "transfer-encoding")?;
        if cl.is_some() && te.is_some()
            || single(r.headers(), "content-encoding")?.unwrap_or("identity") != "identity"
            || te.is_some_and(|v| !v.eq_ignore_ascii_case("chunked"))
        {
            return Err("body_framing");
        }
        let declared = cl
            .map(|v| {
                if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                    return Err("body_framing");
                }
                v.parse::<u64>().map_err(|_| "body_framing")
            })
            .transpose()?;
        if declared.is_some_and(|n| n > NEGATIVE_CONDITION_BODY_CAP) {
            return Err("body_length");
        }
        let mut chunk = [0u8; 4096];
        loop {
            let take = ((NEGATIVE_CONDITION_BODY_CAP + 1 - a.body) as usize).min(chunk.len());
            let n = r.read(&mut chunk[..take]).map_err(|_| "body_io")?;
            a.body += n as u64;
            if a.body > NEGATIVE_CONDITION_BODY_CAP {
                return Err("body_length");
            }
            if n == 0 {
                if declared.is_some_and(|n| n != a.body) {
                    return Err("body_length");
                }
                a.complete = true;
                break;
            }
        }
        output
            .as_mut()
            .ok_or("output_io")?
            .sync_all()
            .map_err(|_| "output_io")?;
        return Ok(serde_json::json!({"status":412,"cdn_host":target.host_str(),"bytes":0}));
    }
    if status != if t.mode == "range" { 206 } else { 200 } {
        return Err("cdn_status");
    }
    let size = if t.mode == "range" {
        t.length
    } else {
        t.object.object_size
    };
    let length = single(r.headers(), "content-length")?.ok_or("length_missing")?;
    if length != size.to_string()
        || single(r.headers(), "transfer-encoding")?.is_some()
        || single(r.headers(), "content-encoding")?.unwrap_or("identity") != "identity"
        || single(r.headers(), "content-type")?
            .is_some_and(|s| s.to_lowercase().contains("multipart"))
    {
        return Err("body_framing");
    }
    if t.mode == "range"
        && single(r.headers(), "content-range")?
            != Some(
                format!(
                    "bytes {}-{}/{}",
                    t.start,
                    t.start + t.length - 1,
                    t.object.object_size
                )
                .as_str(),
            )
    {
        return Err("content_range");
    }
    let etag = single(r.headers(), "etag")?
        .ok_or("validator_missing")?
        .to_string();
    if !validator(&etag)
        || credential_echo(t, &etag)
        || t.object.validator.as_ref().is_some_and(|v| v != &etag)
    {
        return Err("validator_mismatch");
    }
    a.complete = false;
    a.phase = "body";
    if t.mode == "range" {
        let mut hash = Sha256::new();
        let mut chunk = [0u8; 65536];
        while a.body < size + 1 {
            let cap = ((size + 1 - a.body) as usize).min(chunk.len());
            let n = r.read(&mut chunk[..cap]).map_err(|_| "body_io")?;
            if n == 0 {
                break;
            }
            a.body += n as u64;
            hash.update(&chunk[..n]);
            output
                .as_mut()
                .ok_or("output_io")?
                .write_all(&chunk[..n])
                .map_err(|_| "output_io")?;
        }
        a.complete = true;
        if a.body != size {
            return Err("body_length");
        }
        output
            .as_mut()
            .ok_or("output_io")?
            .sync_all()
            .map_err(|_| "output_io")?;
        return Ok(
            serde_json::json!({"bytes":a.body,"sha256":format!("{:x}",hash.finalize()),"etag":etag,"status":status,"cdn_host":target.host_str()}),
        );
    }
    let limits = crate::ScanLimits {
        max_bytes: size,
        max_members: 100_000,
    };
    let observer = sidecars.as_mut().ok_or("report_missing")?;
    let report = if t.mode == "remote-stream-scan" {
        a.phase = "scan";
        let mut counted = CountTee {
            response: r,
            file: None,
            max: size,
            count: &mut a.body,
            complete: &mut a.complete,
        };
        crate::scan_tar_reader_observed(&mut counted, &limits, observer)
            .map_err(public_scan_error)?
    } else {
        a.phase = "body";
        let mut tee = CountTee {
            response: r,
            file: output,
            max: size,
            count: &mut a.body,
            complete: &mut a.complete,
        };
        std::io::copy(&mut tee, &mut std::io::sink()).map_err(|_| "body_io")?;
        tee.file
            .as_mut()
            .ok_or("output_io")?
            .sync_all()
            .map_err(|_| "output_io")?;
        drop(tee);
        a.phase = "scan";
        crate::scan_tar_reader_observed(
            File::open(t.output_root.join(&t.output_name)).map_err(|_| "output_io")?,
            &limits,
            observer,
        )
        .map_err(public_scan_error)?
    };
    if a.body != size || report.size != size || !a.complete {
        return Err("body_length");
    }
    let mut result = sidecars.take().ok_or("report_missing")?.finish(&report)?;
    result["bytes"] = serde_json::json!(a.body);
    result["sha256"] = serde_json::json!(report.whole_sha256);
    result["etag"] = serde_json::json!(etag);
    result["status"] = serde_json::json!(status);
    result["cdn_host"] = serde_json::json!(target.host_str());
    Ok(result)
}
pub fn run(t: Transfer) -> Outcome {
    run_with_context(t, &mut ExecutionContext::default())
}
pub fn run_with_context(t: Transfer, context: &mut ExecutionContext) -> Outcome {
    let mut accounting = Accounting {
        phase: "origin",
        complete: true,
        ..Accounting::default()
    };
    match transfer(&t, &mut accounting, context) {
        Ok(result) => Outcome {
            result,
            error: None,
            accounting,
        },
        Err(error) => Outcome {
            result: serde_json::json!({}),
            error: Some(error),
            accounting,
        },
    }
}
#[cfg(test)]
mod diagnostic_tests {
    #[test]
    fn reusable_clients_have_bounded_host_set() {
        let mut context = super::ExecutionContext::default();
        for port in 10000..10020 {
            context
                .cdn_client(
                    &reqwest::Url::parse(&format!("http://127.0.0.1:{port}/object")).unwrap(),
                )
                .unwrap();
            assert!(context.cdn_hosts.len() <= 8);
        }
        assert_eq!(context.cdn_hosts.len(), 4);
        assert!(context.origin_client.is_none());
    }
    #[test]
    fn metadata_capacity_is_public_unknown_is_redacted() {
        assert_eq!(super::public_scan_error("metadata_limit"), "metadata_limit");
        assert_eq!(
            super::public_scan_error("private path or body"),
            "scan_failed"
        );
        assert_eq!(super::public_scan_error("metadata_length"), "scan_failed");
    }
    #[test]
    fn protocol_footprint_checked_and_capacity_sensitive() {
        assert!(super::protocolmemory(16 << 20, 16 << 20).unwrap() > 300 << 20);
        assert!(super::protocolmemory(usize::MAX, usize::MAX).is_err());
        assert!(super::footprint_with_capacity("range", u64::MAX, 65536, 65536).is_err());
    }
}
