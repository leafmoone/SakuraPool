//! Unified loopback HTTP/1.1 client components for the worker.
//!
//! One shared implementation serves the three transport shapes the pipeline
//! needs - exact `Range` fetch, bounded `FullStream` download and `Probe`
//! (HEAD size discovery) - with a single policy governing redirects,
//! retries, header bounds and the response lifecycle:
//!
//! - loopback-only: every hop (including redirect targets) is re-parsed
//!   against the loopback allowlist; https and foreign hosts never connect.
//! - header isolation: caller-supplied headers ride the initial request
//!   only; after any redirect they are not forwarded (cross-origin token
//!   leakage is impossible by construction).
//! - retries: connect failures, malformed statuses, mid-head/short-body
//!   closes and 5xx responses retry with exponential backoff; 4xx,
//!   unexpected statuses and oversized responses never do.
//! - lifecycle: identity encoding only, `Connection: close`, header block
//!   bounded at 64 KiB, bodies bounded exactly (range) or by a cap
//!   (full stream). Anything out of bounds is a clean error code.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::{parse_loopback_url, validate_content_range, ByteRange, LoopbackTarget};

/// Hard bound for a response header block.
pub const HTTP_MAX_HEADER_BYTES: usize = 64 * 1024;

/// Policy for one HTTP component call.
#[derive(Debug, Clone)]
pub struct HttpPolicy {
    /// Maximum redirects to follow (0 = none).
    pub max_redirects: u32,
    /// Extra attempts after the first for transient failures (0 = none).
    pub max_retries: u32,
    /// Base backoff; attempt n waits `base * 2^n`.
    pub retry_base_ms: u64,
    /// Per-read timeout on the socket.
    pub read_timeout: Duration,
    /// Caller headers for the initial request only (see header isolation).
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

/// The three transport shapes; validation is exact and fail-closed.
#[derive(Debug, Clone, Copy)]
pub enum HttpOp {
    /// `GET Range: bytes=start-end`; requires 206 + matching Content-Range
    /// and exactly `range.len()` body bytes.
    Range {
        range: ByteRange,
        /// Advertised total size used to validate Content-Range.
        total: u64,
    },
    /// `GET` full body; requires 200 and reads to EOF, failing closed if
    /// more than `max_body_bytes` would flow.
    FullStream,
    /// `HEAD`; requires 200 and a Content-Length header (the total size).
    Probe,
}

/// Completed response; the body is fully materialized under its bound.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Reconstructed URL of the hop that produced the response.
    pub final_url: String,
    /// Redirects followed to reach `final_url` (0 = direct).
    pub hops: u32,
    /// Retried transient attempts that failed before this response.
    pub retries: u32,
}

fn url_of(target: &LoopbackTarget) -> String {
    format!("http://{}:{}{}", target.host, target.port, target.path)
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

struct ParsedHead {
    status: u16,
    headers: Vec<(String, String)>,
    /// Bytes already buffered past the header block.
    prefix: Vec<u8>,
}

/// Read and parse one HTTP/1.1 response head; bounded and fail-closed.
fn read_head(stream: &mut TcpStream) -> Result<ParsedHead, &'static str> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut header_end: Option<usize> = None;
    loop {
        if header_end.is_none() {
            if let Some(index) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = Some(index + 4);
            }
        }
        if header_end.is_some() {
            break;
        }
        let count = stream.read(&mut chunk).map_err(|_| "io_error")?;
        if count == 0 {
            return Err("connection closed before headers");
        }
        buffer.extend_from_slice(&chunk[..count]);
        if buffer.len() > HTTP_MAX_HEADER_BYTES {
            return Err("response_headers_too_large");
        }
    }
    let header_end = header_end.unwrap();
    let head_str = std::str::from_utf8(&buffer[..header_end]).map_err(|_| "headers_not_utf8")?;
    let mut lines = head_str.lines();
    let status_line = lines.next().ok_or("malformed_status")?;
    let status_str = match status_line.split_whitespace().nth(1) {
        Some(status_str) if status_line.starts_with("HTTP/1.1 ") => status_str,
        _ => return Err("malformed_status"),
    };
    let status = status_str.parse::<u16>().map_err(|_| "malformed_status")?;
    let mut headers = Vec::new();
    for line in lines {
        // The trailing CRLF pair yields one empty line; skip empties.
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':').ok_or("malformed_status")?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }
    let prefix = buffer.split_off(header_end);
    Ok(ParsedHead {
        status,
        headers,
        prefix,
    })
}

fn connect(target: &LoopbackTarget, policy: &HttpPolicy) -> Result<TcpStream, &'static str> {
    let stream =
        TcpStream::connect((target.host.as_str(), target.port)).map_err(|_| "connect_failed")?;
    stream
        .set_read_timeout(Some(policy.read_timeout))
        .map_err(|_| "io_error")?;
    stream
        .set_write_timeout(Some(policy.read_timeout))
        .map_err(|_| "io_error")?;
    Ok(stream)
}

/// One attempt: connect, send, parse the head, materialize the body under
/// its bound. Returns the parsed head + body; the caller validates status.
fn attempt_once(
    op: &HttpOp,
    target: &LoopbackTarget,
    extra_headers: &[(String, String)],
    max_body_bytes: u64,
    policy: &HttpPolicy,
) -> Result<(ParsedHead, Vec<u8>), &'static str> {
    let method = match op {
        HttpOp::Probe => "HEAD",
        _ => "GET",
    };
    let mut stream = connect(target, policy)?;
    let mut request = format!(
        "{method} {} HTTP/1.1\r\nHost: {}:{}\r\n",
        target.path, target.host, target.port
    );
    if let HttpOp::Range { range, .. } = op {
        request.push_str(&format!("Range: bytes={}-{}\r\n", range.start, range.end));
    }
    for (name, value) in extra_headers {
        // Header isolation: these ride only the very first hop (the caller
        // passes an empty slice afterwards), so no header can cross origins.
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|_| "io_error")?;

    let mut head = read_head(&mut stream)?;

    // Redirect responses carry no payload; do not hold them to the op's
    // exact-body expectation (that check applies to the final response).
    let is_redirect = matches!(head.status, 301 | 302 | 303 | 307 | 308);
    let expected: Option<u64> = if is_redirect {
        None
    } else {
        match op {
            HttpOp::Range { range, .. } => Some(range.len()),
            HttpOp::FullStream => None,
            HttpOp::Probe => None,
        }
    };
    let mut body = std::mem::take(&mut head.prefix);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let done = match expected {
            Some(total) => body.len() as u64 >= total,
            None if matches!(op, HttpOp::Probe) => true,
            None => false,
        };
        if done {
            if let Some(total) = expected {
                if body.len() as u64 > total {
                    return Err("body_too_long");
                }
                body.truncate(total as usize);
            }
            break;
        }
        let count = stream.read(&mut chunk).map_err(|_| "io_error")?;
        if count == 0 {
            // Full-stream EOF is the terminator; a range body must be exact.
            if expected.is_some() {
                return Err("body_too_short");
            }
            break;
        }
        body.extend_from_slice(&chunk[..count]);
        if let Some(total) = expected {
            if body.len() as u64 > total {
                return Err("body_too_long");
            }
        } else if body.len() as u64 > max_body_bytes {
            return Err("body_too_large");
        }
    }
    Ok((head, body))
}

fn resolve_location(
    current: &LoopbackTarget,
    location: &str,
) -> Result<LoopbackTarget, &'static str> {
    let location = location.trim();
    // Absolute loopback URL (re-validated by the parser).
    if location.starts_with("http://") {
        return parse_loopback_url(location);
    }
    // Protocol-relative ("//host/...") would switch origins - rejected.
    if location.starts_with("//") {
        return Err("invalid_location");
    }
    // Relative location: same host:port.
    if location.starts_with('/') {
        return Ok(LoopbackTarget {
            host: current.host.clone(),
            port: current.port,
            path: location.to_owned(),
        });
    }
    Err("invalid_location")
}

/// Unified entry point for the three transport components.
pub fn http_request(
    op: &HttpOp,
    url: &str,
    max_body_bytes: u64,
    policy: &HttpPolicy,
) -> Result<HttpResponse, &'static str> {
    let mut current_url = url.to_owned();
    let mut hops: u32 = 0;
    'hop: loop {
        let target = parse_loopback_url(&current_url)?;
        // Header isolation: extra headers only on the first hop.
        let extra = if hops == 0 {
            policy.extra_headers.clone()
        } else {
            Vec::new()
        };

        // Transient retry loop for one hop: connect/parse failures, short
        // bodies and 5xx responses are retried with backoff; 4xx and other
        // statuses are final.
        let mut retries: u32 = 0;
        loop {
            let attempt = attempt_once(op, &target, &extra, max_body_bytes, policy);
            match attempt {
                Err(code) if is_transient(code) => {
                    if retries >= policy.max_retries {
                        return Err(code);
                    }
                    retries += 1;
                    std::thread::sleep(Duration::from_millis(
                        policy.retry_base_ms.saturating_mul(1u64 << retries),
                    ));
                }
                Err(code) => {
                    return Err(code);
                }
                Ok((head, body)) => match head.status {
                    301 | 302 | 303 | 307 | 308 => {
                        let location = header(&head.headers, "location")
                            .ok_or("missing_location")?
                            .to_owned();
                        current_url = url_of(&resolve_location(&target, &location)?);
                        hops += 1;
                        if hops > policy.max_redirects {
                            return Err("too_many_redirects");
                        }
                        // A redirect is a completed hop; resume the outer loop.
                        continue 'hop;
                    }
                    500 | 502 | 503 | 504 => {
                        if retries >= policy.max_retries {
                            return Err("unexpected_status");
                        }
                        retries += 1;
                        std::thread::sleep(Duration::from_millis(
                            policy.retry_base_ms.saturating_mul(1u64 << retries),
                        ));
                    }
                    status => {
                        let expected_status: u16 = match op {
                            HttpOp::Range { .. } => 206,
                            _ => 200,
                        };
                        if status != expected_status {
                            return Err("unexpected_status");
                        }
                        if let HttpOp::Range { range, total } = op {
                            let content_range = header(&head.headers, "content-range")
                                .ok_or("missing_content_range")?
                                .to_owned();
                            validate_content_range(
                                *range,
                                *total,
                                &content_range,
                                body.len() as u64,
                            )?;
                        }
                        if matches!(op, HttpOp::Probe)
                            && header(&head.headers, "content-length").is_none()
                        {
                            return Err("probe_without_content_length");
                        }
                        if matches!(op, HttpOp::FullStream) {
                            if let Some(content_length) = header(&head.headers, "content-length") {
                                if content_length
                                    .parse::<u64>()
                                    .map_err(|_| "malformed_status")?
                                    != body.len() as u64
                                {
                                    return Err("body_mismatch");
                                }
                            }
                        }
                        return Ok(HttpResponse {
                            status,
                            headers: head.headers,
                            body,
                            final_url: current_url,
                            hops,
                            retries,
                        });
                    }
                },
            }
        }
    }
}

/// Transient failures worth a bounded retry; everything else is final.
fn is_transient(code: &'static str) -> bool {
    matches!(
        code,
        "connect_failed"
            | "connection closed before headers"
            | "malformed_status"
            | "body_too_short"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_classification_is_closed() {
        assert!(is_transient("connect_failed"));
        assert!(is_transient("malformed_status"));
        assert!(!is_transient("body_too_large"));
        assert!(!is_transient("unexpected_status"));
        assert!(!is_transient("io_error"));
    }

    #[test]
    fn location_resolution_keeps_loopback_and_rejects_garbage() {
        let base = LoopbackTarget {
            host: "127.0.0.1".into(),
            port: 8080,
            path: "/a".into(),
        };
        let same = resolve_location(&base, "/b?q=1").unwrap();
        assert_eq!(same.port, 8080);
        assert_eq!(same.path, "/b?q=1");
        let other = resolve_location(&base, "http://127.0.0.1:9999/c").unwrap();
        assert_eq!(other.port, 9999);
        assert!(resolve_location(&base, "https://127.0.0.1:1/x").is_err());
        assert!(resolve_location(&base, "http://example.com/x").is_err());
        assert!(resolve_location(&base, "//127.0.0.1:1/x").is_err());
        assert!(resolve_location(&base, "").is_err());
    }
}
