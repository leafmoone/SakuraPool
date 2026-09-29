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
    parse_loopback_url, validate_content_range, BudgetLimits, ByteRange, JobBudget,
    StreamingSha256, MAX_LINE_BYTES, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{self, BufRead, Read, Write};

const CAPABILITIES: [&str; 2] = ["hash_file", "fetch_range"];

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

    let line = read_line(&mut reader)?;
    let hello: Hello = match parse_inbound(line) {
        Ok(value) => match value {
            Inbound::Hello(hello) => hello,
            Inbound::Request(_) => {
                let _ = emit(
                    &mut stdout,
                    &Outbound::ProtocolError {
                        error: "expected_hello",
                    },
                );
                return Ok(());
            }
        },
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
        let line = read_line(&mut reader)?;
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
                .and_then(|result| result.get("bytes"))
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

fn read_line(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    Ok(line)
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
            let value = fetch_range(&payload.url, payload.start, payload.end, payload.total)?;
            Ok(serde_json::json!({ "sha256": value.0, "bytes": value.1 }))
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

/// Loopback-only HTTP/1.1 range fetch with exact Content-Range validation and
/// streaming SHA-256. Rejected targets never open a connection.
fn fetch_range(url: &str, start: u64, end: u64, total: u64) -> Result<(String, u64), &'static str> {
    use std::net::TcpStream;
    use std::time::Duration;
    let target = parse_loopback_url(url)?;
    let range = ByteRange::new(start, end, total)?;
    let mut stream =
        TcpStream::connect((target.host.as_str(), target.port)).map_err(|_| "io_error")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|_| "io_error")?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nRange: bytes={}-{}\r\nConnection: close\r\n\r\n",
        target.path, target.host, target.port, range.start, range.end
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|_| "io_error")?;
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
        if buffer.len() > 65536 {
            return Err("response headers too large");
        }
    }
    let header_end = header_end.unwrap();
    let head_str = std::str::from_utf8(&buffer[..header_end]).map_err(|_| "headers not utf-8")?;
    let mut lines = head_str.lines();
    let status = lines.next().ok_or("empty response")?;
    if status.split_whitespace().nth(1) != Some("206") {
        return Err("expected http 206");
    }
    let mut content_range = String::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-range") {
                content_range = value.trim().to_owned();
            }
        }
    }
    validate_content_range(range, total, &content_range, range.len())?;
    let mut body = buffer.split_off(header_end);
    let expected = range.len() as usize;
    let mut hasher = StreamingSha256::new();
    loop {
        if body.len() > expected {
            return Err("body longer than requested range");
        }
        if body.len() == expected {
            break;
        }
        let count = stream.read(&mut chunk).map_err(|_| "io_error")?;
        if count == 0 {
            return Err("body shorter than requested range");
        }
        body.extend_from_slice(&chunk[..count]);
    }
    hasher.update(&body);
    Ok((hasher.finish(), range.len()))
}
