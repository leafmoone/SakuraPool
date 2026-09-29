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
    http_request, scan_tar_file, BudgetLimits, ByteRange, HttpOp, HttpPolicy, JobBudget,
    ScanLimits, StreamingSha256, MAX_LINE_BYTES, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};

const CAPABILITIES: [&str; 3] = ["hash_file", "fetch_range", "scan_tar"];

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
            let range = ByteRange::new(payload.start, payload.end, payload.total)?;
            let response = http_request(
                &HttpOp::Range {
                    range,
                    total: payload.total,
                },
                &payload.url,
                range.len(),
                &HttpPolicy::default(),
            )?;
            let mut hasher = StreamingSha256::new();
            hasher.update(&response.body);
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
