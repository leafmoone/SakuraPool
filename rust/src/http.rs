//! Loopback-only HTTP transport built on reqwest's mature blocking client.
//!
//! Proxy discovery, automatic redirects, automatic retries and compression are
//! disabled. Every hop is validated here; caller headers never follow redirects.
//! FullStream owns a capped Response implementing Read, NOT a whole-body Vec.
//! Once a stream is handed to the consumer, read failures are terminal (no replay).

use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderName, HeaderValue};
use std::io::{self, Cursor, Read};
use std::net::SocketAddr;
use std::time::Duration;

use crate::{parse_loopback_url, validate_content_range, ByteRange, LoopbackTarget};

pub const HTTP_MAX_HEADER_BYTES: usize = 64 * 1024;
/// Range materialization is for small retrievals only, never whole TARs.
pub const HTTP_MAX_RANGE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct HttpPolicy {
    pub max_redirects: u32,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    /// reqwest blocking timeout, including response-body reads (not per-read).
    pub read_timeout: Duration,
    pub extra_headers: Vec<(String, String)>,
}

impl Default for HttpPolicy {
    fn default() -> Self {
        Self {
            max_redirects: 5,
            max_retries: 2,
            retry_base_ms: 25,
            read_timeout: Duration::from_secs(30),
            extra_headers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HttpOp {
    Range { range: ByteRange, total: u64 },
    FullStream,
    Probe,
}

/// The transport shape is explicit: only Range/Probe can have buffered bytes.
#[derive(Debug)]
pub enum HttpBody {
    Bounded(Cursor<Vec<u8>>),
    Stream(BoundedStream),
}

impl HttpBody {
    pub fn bounded_bytes(&self) -> Result<&[u8], &'static str> {
        match self {
            Self::Bounded(bytes) => Ok(bytes.get_ref()),
            Self::Stream(_) => Err("stream_is_not_buffered"),
        }
    }

    pub fn error_code(&self) -> Option<&'static str> {
        match self {
            Self::Stream(stream) => stream.error,
            Self::Bounded(_) => None,
        }
    }
}

impl Read for HttpBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Bounded(bytes) => bytes.read(buf),
            Self::Stream(stream) => stream.read(buf),
        }
    }
}

/// Owns the live HTTP response. Constant-size bookkeeping; reqwest handles wire
/// framing (including chunked encoding). At the cap, read at most ONE extra byte
/// to distinguish exact EOF from overflow. Dropping closes an unfinished body.
#[derive(Debug)]
pub struct BoundedStream {
    response: Response,
    limit: u64,
    read: u64,
    declared: Option<u64>,
    error: Option<&'static str>,
}

impl BoundedStream {
    fn fail(&mut self, code: &'static str) -> io::Error {
        self.error = Some(code);
        io::Error::other(code)
    }
}

impl Read for BoundedStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if let Some(code) = self.error {
            return Err(io::Error::other(code));
        }
        if self.read == self.limit {
            let mut probe = [0u8; 1];
            return match self.response.read(&mut probe) {
                Ok(0) => Ok(0),
                Ok(_) => Err(self.fail("body_too_large")),
                Err(_) => Err(self.fail("io_error")),
            };
        }
        let take = (self.limit - self.read).min(buf.len() as u64) as usize;
        let count = match self.response.read(&mut buf[..take]) {
            Ok(count) => count,
            Err(_) => {
                let code = if self.declared.is_some_and(|length| self.read < length) {
                    "body_too_short"
                } else {
                    "io_error"
                };
                return Err(self.fail(code));
            }
        };
        if count == 0 && self.declared.is_some_and(|length| self.read != length) {
            return Err(self.fail("body_too_short"));
        }
        self.read += count as u64;
        Ok(count)
    }
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: HttpBody,
    pub final_url: String,
    pub hops: u32,
    pub retries: u32,
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn validated_url(url: &str) -> Result<reqwest::Url, &'static str> {
    // Keep the existing narrow offline contract (explicit port + http only).
    parse_loopback_url(url)?;
    if url.chars().any(char::is_control) {
        return Err("invalid_url");
    }
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid_url")?;
    if parsed.scheme() != "http"
        || !matches!(parsed.host_str(), Some("127.0.0.1" | "localhost"))
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err("non-loopback target");
    }
    Ok(parsed)
}

fn resolve_location(current: &LoopbackTarget, location: &str) -> Result<String, &'static str> {
    let location = location.trim();
    let next = if location.starts_with("http://") {
        location.to_owned()
    } else if location.starts_with('/') && !location.starts_with("//") {
        format!("http://{}:{}{}", current.host, current.port, location)
    } else {
        return Err("invalid_location");
    };
    validated_url(&next)?;
    Ok(next)
}

fn client(policy: &HttpPolicy) -> Result<Client, &'static str> {
    Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        // Even 'localhost' must not depend on an altered hosts file / DNS.
        .resolve("localhost", SocketAddr::from(([127, 0, 0, 1], 0)))
        .connect_timeout(policy.read_timeout)
        .timeout(policy.read_timeout)
        .build()
        .map_err(|_| "client_build_failed")
}

fn caller_headers(policy: &HttpPolicy) -> Result<Vec<(HeaderName, HeaderValue)>, &'static str> {
    policy
        .extra_headers
        .iter()
        .map(|(name, value)| {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| "invalid_request_header")?;
            if matches!(
                name.as_str(),
                "host"
                    | "range"
                    | "connection"
                    | "accept-encoding"
                    | "content-length"
                    | "transfer-encoding"
            ) {
                return Err("reserved_request_header");
            }
            let value = HeaderValue::from_str(value).map_err(|_| "invalid_request_header")?;
            Ok((name, value))
        })
        .collect()
}

fn response_headers(response: &Response) -> Result<Vec<(String, String)>, &'static str> {
    // reqwest/hyper parses a bounded header block first. This is the stricter
    // application bound on decoded fields; not a custom HTTP parser.
    let mut size = 0usize;
    let mut headers = Vec::new();
    for (name, value) in response.headers() {
        size = size.saturating_add(name.as_str().len() + value.as_bytes().len() + 4);
        if size > HTTP_MAX_HEADER_BYTES {
            return Err("response_headers_too_large");
        }
        headers.push((
            name.as_str().to_owned(),
            value.to_str().map_err(|_| "headers_not_utf8")?.to_owned(),
        ));
    }
    if header(&headers, "content-encoding")
        .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
    {
        return Err("unsupported_content_encoding");
    }
    Ok(headers)
}

fn backoff(policy: &HttpPolicy, retries: u32) {
    std::thread::sleep(Duration::from_millis(
        policy
            .retry_base_ms
            .saturating_mul(1u64.checked_shl(retries).unwrap_or(u64::MAX)),
    ));
}

/// Send/redirect/retry through a single mature client. Head/connect/5xx retries
/// occur before handing off a stream. Range may retry a short bounded body;
/// FullStream never silently restarts after any bytes reach its consumer.
pub fn http_request(
    op: &HttpOp,
    url: &str,
    max_body_bytes: u64,
    policy: &HttpPolicy,
) -> Result<HttpResponse, &'static str> {
    let mut current_url = validated_url(url)?;
    if let HttpOp::Range { range, total } = op {
        ByteRange::new(range.start, range.end, *total)?;
        if range.len() > max_body_bytes || range.len() > HTTP_MAX_RANGE_BYTES {
            return Err("body_too_large");
        }
    }
    let client = client(policy)?;
    let extra = caller_headers(policy)?;
    let mut hops = 0u32;
    let mut all_retries = 0u32;
    'hop: loop {
        let mut retries = 0u32;
        loop {
            let mut request = match op {
                HttpOp::Probe => client.head(current_url.clone()),
                _ => client.get(current_url.clone()),
            }
            .header("Accept-Encoding", "identity")
            .header("Connection", "close");
            if let HttpOp::Range { range, .. } = op {
                request = request.header("Range", format!("bytes={}-{}", range.start, range.end));
            }
            if hops == 0 {
                for (name, value) in &extra {
                    request = request.header(name.clone(), value.clone());
                }
            }
            let result = (|| {
                let response = request.send().map_err(|error| {
                    if error.is_connect() {
                        "connect_failed"
                    } else if error.is_timeout() {
                        "io_error"
                    } else {
                        "malformed_status"
                    }
                })?;
                let status = response.status().as_u16();
                let headers = response_headers(&response)?;
                if matches!(status, 301 | 302 | 303 | 307 | 308) {
                    let location = header(&headers, "location").ok_or("missing_location")?;
                    // Url canonicalization drops an explicit default :80. Derive
                    // the validated target from Url rather than reparsing text.
                    let target = LoopbackTarget {
                        host: current_url.host_str().ok_or("invalid_url")?.to_owned(),
                        port: current_url.port_or_known_default().ok_or("invalid_url")?,
                        path: current_url.path().to_owned(),
                    };
                    let next = resolve_location(&target, location)?;
                    return Ok((status, headers, None, Some(next)));
                }
                if (500..600).contains(&status) {
                    return Err("server_error");
                }
                let expected = if matches!(op, HttpOp::Range { .. }) {
                    206
                } else {
                    200
                };
                if status != expected {
                    return Err("unexpected_status");
                }
                let declared = header(&headers, "content-length")
                    .map(|value| value.parse::<u64>().map_err(|_| "malformed_status"))
                    .transpose()?;
                let body = match op {
                    HttpOp::Probe => {
                        if declared.is_none() {
                            return Err("probe_without_content_length");
                        }
                        HttpBody::Bounded(Cursor::new(Vec::new()))
                    }
                    HttpOp::FullStream => {
                        if declared.is_some_and(|length| length > max_body_bytes) {
                            return Err("body_too_large");
                        }
                        HttpBody::Stream(BoundedStream {
                            response,
                            limit: max_body_bytes,
                            read: 0,
                            declared,
                            error: None,
                        })
                    }
                    HttpOp::Range { range, total } => {
                        // Validate head BEFORE reading any payload.
                        let value =
                            header(&headers, "content-range").ok_or("missing_content_range")?;
                        // Length mismatch gets a deterministic bounded-body code.
                        if let Some(length) = declared {
                            if length > range.len() {
                                return Err("body_too_long");
                            }
                            if length < range.len() {
                                return Err("body_too_short");
                            }
                        }
                        validate_content_range(*range, *total, value, range.len())?;
                        let mut stream = BoundedStream {
                            response,
                            limit: range.len(),
                            read: 0,
                            declared,
                            error: None,
                        };
                        let mut bytes = Vec::new();
                        stream
                            .read_to_end(&mut bytes)
                            .map_err(|_| match stream.error {
                                Some("body_too_large") => "body_too_long",
                                Some(code) => code,
                                None => "io_error",
                            })?;
                        if bytes.len() as u64 != range.len() {
                            return Err("body_too_short");
                        }
                        HttpBody::Bounded(Cursor::new(bytes))
                    }
                };
                Ok((status, headers, Some(body), None))
            })();
            match result {
                Err(code) if is_transient(code) && retries < policy.max_retries => {
                    retries += 1;
                    all_retries += 1;
                    backoff(policy, retries);
                }
                Err("server_error") => return Err("unexpected_status"),
                Err(code) => return Err(code),
                Ok((_, _, _, Some(next))) => {
                    if hops >= policy.max_redirects {
                        return Err("too_many_redirects");
                    }
                    hops += 1;
                    current_url = validated_url(&next)?;
                    continue 'hop;
                }
                Ok((status, headers, Some(body), None)) => {
                    return Ok(HttpResponse {
                        status,
                        headers,
                        body,
                        final_url: current_url.to_string(),
                        hops,
                        retries: all_retries,
                    })
                }
                _ => unreachable!(),
            }
        }
    }
}

fn is_transient(code: &'static str) -> bool {
    matches!(
        code,
        "connect_failed" | "malformed_status" | "body_too_short" | "io_error" | "server_error"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_classification_is_closed() {
        assert!(is_transient("connect_failed"));
        assert!(is_transient("body_too_short"));
        assert!(!is_transient("body_too_large"));
        assert!(!is_transient("unexpected_status"));
    }

    #[test]
    fn location_resolution_keeps_loopback_and_rejects_garbage() {
        let base = LoopbackTarget {
            host: "127.0.0.1".into(),
            port: 8080,
            path: "/a".into(),
        };
        assert_eq!(
            resolve_location(&base, "/b?q=1").unwrap(),
            "http://127.0.0.1:8080/b?q=1"
        );
        assert!(resolve_location(&base, "http://127.0.0.1:9999/c").is_ok());
        for location in [
            "https://127.0.0.1:1/x",
            "http://example.com:80/x",
            "//127.0.0.1:1/x",
            "",
        ] {
            assert!(resolve_location(&base, location).is_err());
        }
    }
}
