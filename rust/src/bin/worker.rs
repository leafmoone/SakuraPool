//! Bounded NDJSON worker. Bootstrap is fixed; later lines use negotiated capacity.
use sakurapool_rust::{
    http_request, scan_http_tar, scan_tar_file, BudgetLimits, ByteRange, HttpOp, HttpPolicy,
    JobBudget, ScanLimits, StreamingSha256, MAX_LINE_BYTES, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};

const CAPABILITIES: [&str; 7] = [
    "hash_file",
    "fetch_range",
    "scan_tar",
    "scan_http_tar",
    "bounded_session_v1",
    "production_transfer_v2",
    "production_http_status_v1",
];
const MAX_SESSION_REQUESTS: usize = 256;
const MAX_REQUEST_ID_BYTES: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    #[serde(rename = "type")]
    kind: String,
    protocol_version: u32,
    budget: BudgetLimits,
    #[serde(default)]
    stream_capacity: Option<StreamCapacity>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StreamCapacity {
    range_chunk_bytes: u64,
    http_header_bytes: usize,
    rpc_line_bytes: usize,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_capacity: Option<StreamCapacity>,
        protocol_resident_bytes: u64,
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

// Serialize directly through a checked writer, never build a second full JSON line.
struct LimitedWriter<'a, W> {
    inner: &'a mut W,
    remaining: usize,
}
impl<W: Write> Write for LimitedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("response_line_limit"));
        }
        let n = self.inner.write(bytes)?;
        self.remaining -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
fn emit(stdout: &mut impl Write, message: &Outbound, limit: usize) -> io::Result<()> {
    let mut writer = LimitedWriter {
        inner: stdout,
        remaining: limit,
    };
    serde_json::to_writer(&mut writer, message)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let hello: Hello = match read_line_bounded(&mut reader, MAX_LINE_BYTES)?.and_then(parse_inbound)
    {
        Ok(Inbound::Hello(hello)) => hello,
        Ok(Inbound::Request(_)) => {
            emit(
                &mut stdout,
                &Outbound::ProtocolError {
                    error: "expected_hello",
                },
                MAX_LINE_BYTES,
            )?;
            return Ok(());
        }
        Err(code) => {
            emit(
                &mut stdout,
                &Outbound::ProtocolError { error: code },
                MAX_LINE_BYTES,
            )?;
            return Ok(());
        }
    };
    if hello.kind != "hello" || hello.protocol_version != PROTOCOL_VERSION {
        emit(
            &mut stdout,
            &Outbound::ProtocolError {
                error: "protocol_version_mismatch",
            },
            MAX_LINE_BYTES,
        )?;
        return Ok(());
    }
    if hello.stream_capacity.as_ref().is_some_and(|c| {
        c.range_chunk_bytes == 0
            || c.range_chunk_bytes > isize::MAX as u64
            || c.http_header_bytes == 0
            || c.http_header_bytes > 417_760
            || c.rpc_line_bytes == 0
            || c.rpc_line_bytes > 16 * 1024 * 1024
    }) {
        emit(
            &mut stdout,
            &Outbound::ProtocolError {
                error: "capacity_invalid",
            },
            MAX_LINE_BYTES,
        )?;
        return Ok(());
    }
    let line_limit = hello
        .stream_capacity
        .as_ref()
        .map_or(MAX_LINE_BYTES, |c| c.rpc_line_bytes);
    let header_limit = hello
        .stream_capacity
        .as_ref()
        .map_or(sakurapool_rust::HTTP_MAX_HEADER_BYTES, |c| {
            c.http_header_bytes
        });
    let resident = sakurapool_rust::production::protocolmemory(line_limit, header_limit)
        .map_err(io::Error::other)?;
    let stream_capacity = hello.stream_capacity.clone();
    let mut budget = JobBudget::new(hello.budget);
    emit(
        &mut stdout,
        &Outbound::Ready {
            protocol_version: PROTOCOL_VERSION,
            worker_version: env!("CARGO_PKG_VERSION"),
            capabilities: &CAPABILITIES,
            stream_capacity: hello.stream_capacity,
            protocol_resident_bytes: resident,
        },
        MAX_LINE_BYTES,
    )?;
    let mut seen_requests: BTreeSet<String> = BTreeSet::new();
    let mut execution = sakurapool_rust::production::ExecutionContext::default();
    loop {
        let line = match read_line_bounded(&mut reader, line_limit)? {
            Ok(line) => line,
            Err(code) => {
                emit(
                    &mut stdout,
                    &Outbound::ProtocolError { error: code },
                    line_limit,
                )?;
                continue;
            }
        };
        if line.is_empty() {
            break;
        }
        let request: Request = match parse_inbound(line) {
            Ok(Inbound::Request(request)) => request,
            Ok(Inbound::Hello(_)) => {
                emit(
                    &mut stdout,
                    &Outbound::ProtocolError {
                        error: "expected_request",
                    },
                    line_limit,
                )?;
                continue;
            }
            Err(code) => {
                emit(
                    &mut stdout,
                    &Outbound::ProtocolError { error: code },
                    line_limit,
                )?;
                continue;
            }
        };
        let request_id = request.request_id.clone();
        if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_BYTES {
            respond(&mut stdout, "", Err("request_id_invalid"), line_limit);
            continue;
        }
        if seen_requests.len() >= MAX_SESSION_REQUESTS {
            respond(
                &mut stdout,
                &request_id,
                Err("session_exhausted"),
                line_limit,
            );
            break;
        }
        if request.kind != "request" || !seen_requests.insert(request_id.clone()) {
            respond(
                &mut stdout,
                &request_id,
                Err("duplicate_request"),
                line_limit,
            );
            continue;
        }
        let outcome = if budget.admit(&request.budget).is_err() {
            Err("budget_exceeded")
        } else {
            budget.commit_attempt();
            if request.payload.get("production").is_some() {
                if let Some(p) = request.payload.get("production") {
                    if p.get("payload_revision").and_then(|v| v.as_u64()) == Some(2) {
                        let valid = stream_capacity.as_ref().is_some_and(|c| {
                            p.get("range_chunk_bytes").and_then(|v| v.as_u64())
                                == Some(c.range_chunk_bytes)
                                && p.get("http_header_bytes").and_then(|v| v.as_u64())
                                    == Some(c.http_header_bytes as u64)
                        });
                        if !valid {
                            respond(
                                &mut stdout,
                                &request_id,
                                Err("capacity_mismatch"),
                                line_limit,
                            );
                            continue;
                        }
                    }
                }
                budget.commit_attempt();
            }
            let outcome = dispatch(&request, &mut execution, stream_capacity.as_ref());
            if let Some(accounting) = outcome.as_ref().ok().and_then(|v| v.get("accounting")) {
                let charge = if accounting.get("complete").and_then(|v| v.as_bool()) == Some(true) {
                    accounting
                        .get("body")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(request.budget.body)
                } else {
                    request.budget.body
                };
                budget.commit_body(charge);
            }
            if let Some(bytes) = outcome
                .as_ref()
                .ok()
                .filter(|result| result.get("accounting").is_none())
                .and_then(|result| result.get("bytes").or_else(|| result.get("size")))
                .and_then(|value| value.as_u64())
            {
                budget.commit_body(bytes);
            }
            outcome
        };
        let legacy_scan = matches!(request.operation.as_str(), "scan_tar" | "scan_http_tar")
            && request.payload.get("production").is_none();
        respond(
            &mut stdout,
            &request_id,
            outcome,
            if legacy_scan {
                64 * 1024 * 1024
            } else {
                line_limit
            },
        );
    }
    Ok(())
}

enum Inbound {
    Hello(Hello),
    Request(Request),
}
#[cfg(test)]
fn read_line(reader: &mut impl BufRead) -> io::Result<Result<Vec<u8>, &'static str>> {
    read_line_bounded(reader, MAX_LINE_BYTES)
}
fn read_line_bounded(
    reader: &mut impl BufRead,
    limit: usize,
) -> io::Result<Result<Vec<u8>, &'static str>> {
    let mut line: Vec<u8> = Vec::with_capacity(1024.min(limit));
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(Ok(line));
        }
        if let Some(pos) = buf.iter().position(|&byte| byte == b'\n') {
            if line.len().checked_add(pos).is_none_or(|n| n >= limit) {
                reader.consume(pos + 1);
                return Ok(Err("line_too_long"));
            }
            line.extend_from_slice(&buf[..pos]);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            reader.consume(pos + 1);
            return Ok(Ok(line));
        }
        let take = buf.len();
        let over = line.len().checked_add(take).is_none_or(|n| n > limit);
        if !over {
            line.extend_from_slice(buf);
        }
        reader.consume(take);
        if over {
            drain_line(reader)?;
            return Ok(Err("line_too_long"));
        }
    }
}
fn drain_line(reader: &mut impl BufRead) -> io::Result<()> {
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }
        if let Some(pos) = buf.iter().position(|&byte| byte == b'\n') {
            reader.consume(pos + 1);
            return Ok(());
        }
        let take = buf.len();
        reader.consume(take);
    }
}

// Bound serde Value and typed payload allocation before deserializing.
fn lexical_bounds(text: &str) -> Result<(), &'static str> {
    let bytes = text.as_bytes();
    let mut stack = [0u8; 64];
    let (mut i, mut depth, mut nodes) = (0, 0, 0usize);
    let mut first = true;
    while i < bytes.len() {
        let c = bytes[i];
        if b" \t\r\n,:".contains(&c) {
            i += 1;
            continue;
        }
        if first {
            if c != b'{' {
                return Err("protocol_violation");
            }
            first = false;
        }
        match c {
            b'{' | b'[' => {
                if depth == stack.len() {
                    return Err("json_depth");
                }
                stack[depth] = c;
                depth += 1;
                nodes += 1;
                i += 1;
            }
            b'}' | b']' => {
                let expected = if c == b'}' { b'{' } else { b'[' };
                if depth == 0 || stack[depth - 1] != expected {
                    return Err("invalid_json");
                }
                depth -= 1;
                i += 1;
            }
            b'"' => {
                nodes += 1;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] < 32 {
                        return Err("invalid_json");
                    }
                    if bytes[i] == b'\\' {
                        i += 1;
                        if i >= bytes.len() {
                            return Err("invalid_json");
                        }
                    }
                    i += 1;
                }
                if i == bytes.len() {
                    return Err("invalid_json");
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                nodes += 1;
                let start = i;
                while i < bytes.len() && b"-+0123456789.eE".contains(&bytes[i]) {
                    i += 1;
                    if i - start > 32 {
                        return Err("json_number");
                    }
                }
                let token = &text[start..i];
                if token.contains(['.', 'e', 'E']) {
                    if !token.parse::<f64>().map_err(|_| "json_number")?.is_finite() {
                        return Err("json_number");
                    }
                } else if token.starts_with('-') {
                    token.parse::<i64>().map_err(|_| "json_number")?;
                } else {
                    token.parse::<u64>().map_err(|_| "json_number")?;
                }
            }
            b't' | b'f' | b'n' => {
                let token: &[u8] = if c == b't' {
                    b"true"
                } else if c == b'f' {
                    b"false"
                } else {
                    b"null"
                };
                if !bytes[i..].starts_with(token) {
                    return Err("invalid_json");
                }
                i += token.len();
                nodes += 1;
            }
            _ => return Err("invalid_json"),
        }
        if nodes > 65_536.min(bytes.len()) {
            return Err("json_nodes");
        }
    }
    if first || depth != 0 {
        return Err("invalid_json");
    }
    Ok(())
}
fn parse_inbound(line: Vec<u8>) -> Result<Inbound, &'static str> {
    let text = std::str::from_utf8(&line).map_err(|_| "invalid_utf8")?;
    lexical_bounds(text)?;
    let tagged: serde_json::Value = serde_json::from_str(text).map_err(|_| "invalid_json")?;
    match tagged.get("type").and_then(|v| v.as_str()).unwrap_or("") {
        "hello" => serde_json::from_value::<Hello>(tagged)
            .map(Inbound::Hello)
            .map_err(|_| "protocol_violation"),
        "request" => serde_json::from_value::<Request>(tagged)
            .map(Inbound::Request)
            .map_err(|_| "protocol_violation"),
        _ => Err("protocol_violation"),
    }
}

fn dispatch(
    request: &Request,
    execution: &mut sakurapool_rust::production::ExecutionContext,
    capacity: Option<&StreamCapacity>,
) -> Result<serde_json::Value, &'static str> {
    if let Some(payload) = request.payload.get("production") {
        if !matches!(request.operation.as_str(), "fetch_range" | "scan_http_tar") {
            return Err("production_operation");
        }
        let transfer: sakurapool_rust::production::Transfer =
            serde_json::from_value(payload.clone()).map_err(|_| "bad_production_payload")?;
        let bytes = if transfer.mode == "range" {
            transfer.length
        } else {
            transfer.object.object_size
        };
        let rpc = capacity.map_or(MAX_LINE_BYTES, |c| c.rpc_line_bytes);
        let (required_memory, required_disk) =
            sakurapool_rust::production::footprint_with_capacity(
                &transfer.mode,
                bytes,
                rpc,
                transfer.http_header_bytes,
            )?;
        let required_body = sakurapool_rust::production::network_body_budget(
            bytes, transfer.mode == "range" && transfer.condition == "wrong",
        )?;
        if request.budget.body < required_body
            || request.budget.attempts < 2
            || request.budget.disk < required_disk
            || request.budget.inflight < required_memory
            || ((request.operation == "fetch_range") != (transfer.mode == "range"))
        {
            return Err("production_budget");
        }
        let outcome = sakurapool_rust::production::run_with_context(transfer, execution);
        let mut value = outcome.result;
        value["diagnostic"] = outcome.accounting.diagnostic();
        value["observation"] = outcome.accounting.observation();
        value["accounting"] = serde_json::to_value(outcome.accounting).map_err(|_| "accounting")?;
        if let Some(error) = outcome.error {
            value["production_error"] = serde_json::json!(error);
        }
        return Ok(value);
    }
    match request.operation.as_str() {
        "hash_file" => {
            let payload: HashFilePayload = serde_json::from_value(request.payload.clone())
                .map_err(|_| "protocol_violation")?;
            let file = std::fs::File::open(&payload.path).map_err(|_| "io_error")?;
            let (sha256, bytes) = StreamingSha256::digest_reader(file).map_err(|_| "io_error")?;
            Ok(serde_json::json!({"sha256": sha256, "bytes": bytes}))
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
                    max_retries: 0,
                    range_chunk_bytes: capacity
                        .map_or(sakurapool_rust::HTTP_MAX_RANGE_BYTES, |c| {
                            c.range_chunk_bytes
                        }),
                    http_header_bytes: capacity
                        .map_or(sakurapool_rust::HTTP_MAX_HEADER_BYTES, |c| {
                            c.http_header_bytes
                        }),
                    ..HttpPolicy::default()
                },
            )?;
            let mut hasher = StreamingSha256::new();
            hasher.update(response.body.bounded_bytes()?);
            Ok(serde_json::json!({"sha256": hasher.finish(), "bytes": range.len()}))
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
            serde_json::to_value(&scan).map_err(|_| "protocol_violation")
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
                    max_retries: 0,
                    range_chunk_bytes: capacity
                        .map_or(sakurapool_rust::HTTP_MAX_RANGE_BYTES, |c| {
                            c.range_chunk_bytes
                        }),
                    http_header_bytes: capacity
                        .map_or(sakurapool_rust::HTTP_MAX_HEADER_BYTES, |c| {
                            c.http_header_bytes
                        }),
                    ..HttpPolicy::default()
                },
            )?;
            serde_json::to_value(&scan).map_err(|_| "protocol_violation")
        }
        _ => Err("unknown_operation"),
    }
}
fn respond(
    stdout: &mut impl Write,
    request_id: &str,
    outcome: Result<serde_json::Value, &'static str>,
    limit: usize,
) {
    let message = match outcome {
        Ok(result) => {
            let code = result
                .get("production_error")
                .and_then(|v| v.as_str())
                .map(|_| "production_rejected");
            Outbound::Response {
                request_id: request_id.to_owned(),
                ok: code.is_none(),
                result: Some(result),
                error: code,
            }
        }
        Err(error) => Outbound::Response {
            request_id: request_id.to_owned(),
            ok: false,
            result: None,
            error: Some(error),
        },
    };
    if let Err(error) = emit(stdout, &message, limit) {
        eprintln!("worker stdout failure: {error:?}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod line_reader_tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};
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
        let mut reader = BufReader::new(Cursor::new(vec![b'x'; MAX_LINE_BYTES]));
        assert_eq!(
            read_line(&mut reader).unwrap().unwrap().len(),
            MAX_LINE_BYTES
        );
        let mut reader = BufReader::new(Cursor::new(vec![b'x'; MAX_LINE_BYTES + 1]));
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
    }
    #[test]
    fn oversized_line_is_rejected_early_and_remainder_drained() {
        let mut data = vec![b'j'; 1024 * 1024];
        data.extend_from_slice(b"\nclean\n");
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(data),
            consumed: 0.into(),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
        assert!(reader.get_ref().consumed() <= (1024 * 1024 + 16 * 1024) as u64);
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"clean".to_vec()));
    }
    #[test]
    fn oversized_line_without_newline_uses_no_unbounded_memory() {
        let mut reader = BufReader::new(CountingReader {
            inner: Cursor::new(vec![b'k'; 16 * 1024 * 1024]),
            consumed: 0.into(),
        });
        assert_eq!(read_line(&mut reader).unwrap(), Err("line_too_long"));
        assert_eq!(reader.get_ref().consumed(), (16 * 1024 * 1024) as u64);
    }
    #[test]
    fn eof_flushes_partial_line_then_reports_close() {
        let mut reader = BufReader::new(Cursor::new(b"partial".to_vec()));
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"partial".to_vec()));
        assert_eq!(read_line(&mut reader).unwrap(), Ok(Vec::new()));
    }
    #[test]
    fn crlf_and_chunked_lines_are_read_in_full() {
        let mut reader = BufReader::new(Cursor::new(b"a\r\nb".to_vec()));
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"a".to_vec()));
        assert_eq!(read_line(&mut reader).unwrap(), Ok(b"b".to_vec()));
    }
    #[test]
    fn malformed_lines_report_codes_without_oversized_buffer() {
        assert!(parse_inbound(vec![0xff, 0xfe]).is_err());
    }
    #[test]
    fn lexical_allocation_attacks_are_rejected() {
        for text in [
            "[]".to_owned(),
            format!("{{\"x\":{}}}", "9".repeat(1000)),
            format!("{{\"x\":{}0{}}}", "[".repeat(64), "]".repeat(64)),
            format!("{{\"x\":[{}0]}}", "0,".repeat(65_536)),
            "{\"x\":1e999}".to_owned(),
        ] {
            assert!(lexical_bounds(&text).is_err());
        }
    }
    #[test]
    fn checked_writer_never_exceeds_extent() {
        let mut bytes = Vec::new();
        assert!(emit(
            &mut bytes,
            &Outbound::ProtocolError {
                error: "invalid_json"
            },
            16
        )
        .is_err());
        assert!(bytes.len() <= 16);
    }
}
