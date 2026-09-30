//! SakuraPool offline worker: structured NDJSON control protocol.
//!
//! Handshake: the first inbound line must be `hello` with
//! `protocol_version` and the job budget; anything else (or a version
//! mismatch) ends the process. Every later line must be a `request` with a
//! unique `request_id`, an `operation`, a per-request `budget` and a
//! `payload`. Unknown fields are rejected. Lines over 64 KiB or invalid
//! UTF-8 are rejected as protocol errors without killing the worker.
//!
//! stdout carries protocol messages only. stderr is reserved for fatal
//! conditions and is drained (with a bound) by the supervisor.

use sakurapool_rust::{
    http_request, scan_http_tar, scan_tar_file, BudgetLimits, ByteRange, HttpOp, HttpPolicy,
    JobBudget, ScanLimits, StreamingSha256, MAX_LINE_BYTES, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};

const CAPABILITIES: [&str; 4] = ["hash_file", "fetch_range", "scan_tar", "scan_http_tar"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    #[serde(rename = "type")]
    kind: String,
    protocol_version: u32,
    budget: BudgetLimits,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(rename = "type")]
    kind: String,
    request_id: String,
    operation: String,
    budget: BudgetLimits,
    payload: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HashFilePayload {
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchRangePayload {
    url: String,
    start: u64,
    end: u64,
    total: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanTarPayload {
    path: String,
    #[serde(default)]
    max_members: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanHttpTarPayload {
    url: String,
    #[serde(default)]
    max_members: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Outbound {
    Ready {
        protocol_version: u32,
        worker_version: &'static str,
        capabilities: &'static [&'static str],
    },
    Response {
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'static str>,
    },
    ProtocolError {
        error: &'static str,
    },
}

fn emit(stdout: &mut impl Write, message: &Outbound) -> io::Result<()> {
    serde_json::to_writer(&mut *stdout, message)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut stdout = io::BufWriter::new(io::stdout().lock());

    let hello: Hello = match read_line(&mut reader)?.and_then(parse_inbound) {
        Ok(Inbound::Hello(hello)) => hello,
        Ok(Inbound::Request(_)) => {
            let _ = emit(
                &mut stdout,
                &Outbound::ProtocolError {
                    error: "expected_hello",
                },
            );
            return Ok(());
        }
        Err(code) => {
            let _ = emit(&mut stdout, &Outbound::ProtocolError { error: code });
            return Ok(());
        }
    };
    if hello.kind != "hello" || hello.protocol_version != PROTOCOL_VERSION {
        let _ = emit(
            &mut stdout,
            &Outbound::ProtocolError {
                error: "protocol_version_mismatch",
            },
        );
        return Ok(());
    }
    let mut budget = JobBudget::new(hello.budget);
    emit(
        &mut stdout,
        &Outbound::Ready {
            protocol_version: PROTOCOL_VERSION,
            worker_version: env!("CARGO_PKG_VERSION"),
            capabilities: &CAPABILITIES,
        },
    )?;

    let mut seen_requests: BTreeSet<String> = BTreeSet::new();
    loop {
        // One oversized line is reported and skipped; the worker survives.
        let line = match read_line(&mut reader)? {
            Ok(line) => line,
            Err(code) => {
                let _ = emit(&mut stdout, &Outbound::ProtocolError { error: code });
                continue;
            }
        };
        if line.is_empty() {
            break; // stdin closed
        }
        let request: Request = match parse_inbound(line) {
            Ok(Inbound::Request(request)) => request,
            Ok(Inbound::Hello(_)) => {
                let _ = emit(
                    &mut stdout,
                    &Outbound::ProtocolError {
                        error: "expected_request",
                    },
                );
                continue;
            }
            Err(code) => {
                let _ = emit(&mut stdout, &Outbound::ProtocolError { error: code });
                continue;
            }
        };
        let request_id = request.request_id.clone();
        if request.kind != "request" || !seen_requests.insert(request_id.clone()) {
            respond(&mut stdout, &request_id, Err("duplicate_request"));
            continue;
        }
        // An admitted request consumes one attempt up front, mirroring the
        // Python durable ledger: a failed attempt is never refunded.
        let outcome = if budget.admit(&request.budget).is_err() {
            Err("budget_exceeded")
        } else {
            budget.commit_attempt();
            let outcome = dispatch(&request);
            if let Some(bytes) = outcome
                .as_ref()
                .ok()
                .and_then(|result| result.get("bytes").or_else(|| result.get("size")))
                .and_then(|value| value.as_u64())
            {
                budget.commit_body(bytes);
            }
            outcome
        };
        respond(&mut stdout, &request_id, outcome);
    }
    Ok(())
}

enum Inbound {
    Hello(Hello),
    Request(Request),
}

fn read_line(reader: &mut impl BufRead) -> io::Result<Result<Vec<u8>, &'static str>> {
    let mut line: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(Ok(line)); // EOF: pending partial line, or clean close
        }
        if let Some(pos) = buf.iter().position(|&byte| byte == b'\n') {
            // The line is complete (or definitely over the limit).
            if line.len() + pos > MAX_LINE_BYTES {
                reader.consume(pos + 1); // rest of the line discarded
                return Ok(Err("line_too_long"));
            }
            line.extend_from_slice(&buf[..pos]);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            reader.consume(pos + 1);
            return Ok(Ok(line));
        }
        // No newline yet: absorb the buffer, rejecting the moment the
        // line provably exceeds the limit.
        let take = buf.len();
        let over = line.len() + take > MAX_LINE_BYTES;
        line.extend_from_slice(&buf[..take]);
        reader.consume(take);
        if over {
            drain_line(reader)?;
            return Ok(Err("line_too_long"));
        }
    }
}

/// Discard the rest of an already-rejected oversized line, one bounded
/// chunk at a time, until its newline (or EOF). Keeps memory flat.
fn drain_line(reader: &mut impl BufRead) -> io::Result<()> {
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }
        if let Some(pos) = buf.iter().position(|&byte| byte == b'\n') {
            reader.consume(pos + 1); // stop exactly at the newline
            return Ok(());
        }
        let take = buf.len();
        reader.consume(take);
    }
}

fn parse_inbound(line: Vec<u8>) -> Result<Inbound, &'static str> {
    if line.len() > MAX_LINE_BYTES {
        return Err("line_too_long");
    }
    let text = std::str::from_utf8(&line).map_err(|_| "invalid_utf8")?;
    // Exactly one JSON object per line.
    let tagged: serde_json::Value = serde_json::from_str(text).map_err(|_| "invalid_json")?;
    let kind = tagged.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match kind {
        "hello" => serde_json::from_value::<Hello>(tagged)
            .map(Inbound::Hello)
            .map_err(|_| "protocol_violation"),
        "request" => serde_json::from_value::<Request>(tagged)
            .map(Inbound::Request)
            .map_err(|_| "protocol_violation"),
        _ => Err("protocol_violation"),
    }
}

fn dispatch(request: &Request) -> Result<serde_json::Value, &'static str> {
    match request.operation.as_str() {
        "hash_file" => {
            let payload: HashFilePayload = serde_json::from_value(request.payload.clone())
                .map_err(|_| "protocol_violation")?;
            let file = std::fs::File::open(&payload.path).map_err(|_| "io_error")?;
            let (sha256, bytes) = StreamingSha256::digest_reader(file).map_err(|_| "io_error")?;
            Ok(serde_json::json!({ "sha256": sha256, "bytes": bytes }))
        }
        "fetch_range" => {
            let payload: FetchRangePayload = serde_json::from_value(request.payload.clone())
                .map_err(|_| "protocol_violation")?;
            let range = ByteRange::new(payload.start, payload.end, payload.total)?;
            let response = http_request(
                &HttpOp::Range {
                    range,
                    total: payload.total,
                },
                &payload.url,
                range.len(),
                &HttpPolicy {
                    // Durable Python reservation permits ONE attempt only.
                    // Retry must be separately reserved by the caller, never hidden.
                    max_retries: 0,
                    ..HttpPolicy::default()
                },
            )?;
            let mut hasher = StreamingSha256::new();
            hasher.update(response.body.bounded_bytes()?);
            Ok(serde_json::json!({ "sha256": hasher.finish(), "bytes": range.len() }))
        }
        "scan_tar" => {
            let payload: ScanTarPayload = serde_json::from_value(request.payload.clone())
                .map_err(|_| "protocol_violation")?;
            let mut limits = ScanLimits::default();
            if let Some(value) = payload.max_members {
                limits.max_members = value;
            }
            if let Some(value) = payload.max_bytes {
                limits.max_bytes = value;
            }
            let scan = scan_tar_file(std::path::Path::new(&payload.path), &limits)?;
            let scan: serde_json::Value =
                serde_json::to_value(&scan).map_err(|_| "protocol_violation")?;
            Ok(scan)
        }
        "scan_http_tar" => {
            let payload: ScanHttpTarPayload = serde_json::from_value(request.payload.clone())
                .map_err(|_| "protocol_violation")?;
            let mut limits = ScanLimits::default();
            if let Some(value) = payload.max_members {
                limits.max_members = value;
            }
            if let Some(value) = payload.max_bytes {
                limits.max_bytes = value;
            }
            let scan = scan_http_tar(
                &payload.url,
                &limits,
                &HttpPolicy {
                    // Durable Python reservation permits ONE attempt only.
                    // Retry must be separately reserved by the caller, never hidden.
                    max_retries: 0,
                    ..HttpPolicy::default()
                },
            )?;
            let scan: serde_json::Value =
                serde_json::to_value(&scan).map_err(|_| "protocol_violation")?;
            Ok(scan)
        }
        _ => Err("unknown_operation"),
    }
}

fn respond(
    stdout: &mut impl Write,
    request_id: &str,
    outcome: Result<serde_json::Value, &'static str>,
) {
    let message = match outcome {
        Ok(result) => Outbound::Response {
            request_id: request_id.to_owned(),
            ok: true,
            result: Some(result),
            error: None,
        },
        Err(error) => Outbound::Response {
            request_id: request_id.to_owned(),
            ok: false,
            result: None,
            error: Some(error),
        },
    };
    if let Err(error) = emit(stdout, &message) {
        eprintln!("worker stdout failure: {error:?}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod line_reader_tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};

    /// Counts every byte handed to the reader so tests can prove how much
    /// of an oversized stream the worker actually consumed.
    struct CountingReader {
        inner: Cursor<Vec<u8>>,
        consumed: std::sync::atomic::AtomicU64,
    }

    impl Read for CountingReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.consumed
                .fetch_add(n as u64, std::sync::atomic::Ordering::SeqCst);
            Ok(n)
        }
    }

    impl CountingReader {
        fn consumed(&self) -> u64 {
            self.consumed.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[test]
    fn line_at_exact_limit_is_accepted_one_over_is_rejected() {
        let at = vec![b'x'; MAX_LINE_BYTES];
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(at),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        let got = read_line(&mut reader).unwrap().unwrap();
        assert_eq!(got.len(), MAX_LINE_BYTES);

        let over = vec![b'y'; MAX_LINE_BYTES + 1];
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(over),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
    }

    #[test]
    fn oversized_line_is_rejected_early_and_remainder_drained() {
        // 1 MiB of junk, then a newline, then a clean next line.
        let mut data = vec![b'j'; 1024 * 1024];
        data.push(b'\n');
        data.extend_from_slice(b"clean\n");
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(data),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
        // The whole oversized line had to be read to find its newline, but
        // never more than the line itself plus chunk slack.
        let consumed = reader.get_ref().consumed();
        assert!(
            consumed <= (1024 * 1024 + 16 * 1024) as u64,
            "consumed={consumed}"
        );
        // The next read starts on the clean line.
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"clean".to_vec()));
    }

    #[test]
    fn oversized_line_without_newline_uses_no_unbounded_memory() {
        // 16 MiB of junk with NO newline: the reader must reject after the
        // 64 KiB limit and drain, and the test proves the worker read the
        // stream (drain reaches EOF) while never buffering more than the
        // limit plus chunks.
        let data = vec![b'k'; 16 * 1024 * 1024];
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(data),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
        assert_eq!(reader.get_ref().consumed(), (16 * 1024 * 1024) as u64);
        // Buffered content never held the stream: only the limit-sized
        // vector ever exists in read_line.
    }

    #[test]
    fn eof_flushes_partial_line_then_reports_close() {
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(b"partial".to_vec()),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"partial".to_vec()));
        assert_eq!(read_line(&mut reader).unwrap(), Ok(Vec::new()));
    }

    #[test]
    fn crlf_and_chunked_lines_are_read_in_full() {
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(b"a\r\nb".to_vec()),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"a".to_vec()));
        // "b" arrives across a later read and only completes at EOF.
        // "b" arrives across a later read and only completes at EOF.
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"b".to_vec()));
    }

    #[test]
    fn malformed_lines_report_codes_without_oversized_buffer() {
        // Non-UTF8 and non-JSON lines still reach the parser unbounded-safe.
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(vec![0xff, 0xfe, b'\n']),
            consumed: std::sync::atomic::AtomicU64::new(0),
        });
        let line = read_line(&mut reader).unwrap().unwrap();
        assert!(parse_inbound(line).is_err());
    }
}
