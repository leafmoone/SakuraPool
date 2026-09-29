//! End-to-end protocol tests against the real worker binary.

use serde_json::json;
use sha2::Digest;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Session {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
}

impl Session {
    fn new() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sakurapool-worker"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let stdin = child.stdin.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Self { child, stdin, rx }
    }
    fn send_raw(&mut self, line: &[u8]) {
        self.stdin.write_all(line).unwrap();
        self.stdin.flush().unwrap();
    }
    fn hello(&mut self, budget: serde_json::Value) {
        let message = json!({ "type": "hello", "protocol_version": 1, "budget": budget });
        self.send_raw(message.to_string().as_bytes());
        self.send_raw(b"\n");
    }
    fn ready(&mut self) -> serde_json::Value {
        let line = self.next();
        let value: serde_json::Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(value["type"], "ready");
        value
    }
    fn next(&mut self) -> Vec<u8> {
        self.rx.recv_timeout(Duration::from_secs(30)).unwrap()
    }
    fn request(
        &mut self,
        id: &str,
        operation: &str,
        payload: serde_json::Value,
    ) -> serde_json::Value {
        let message = json!({
            "type": "request",
            "request_id": id,
            "operation": operation,
            "budget": { "body": 1_000_000, "disk": 100, "inflight": 1_000_000, "attempts": 1 },
            "payload": payload,
        });
        self.send_raw(message.to_string().as_bytes());
        self.send_raw(b"\n");
        let line = self.next();
        serde_json::from_slice(&line).unwrap()
    }
    fn close(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        let _ = self.child.wait();
    }
}

fn hello_budget() -> serde_json::Value {
    json!({ "body": 10_000_000, "disk": 1_000, "inflight": 10_000_000, "attempts": 10 })
}

#[test]
fn handshake_reports_version_and_capabilities() {
    let mut session = Session::new();
    session.send_raw(
        json!({ "type": "hello", "protocol_version": 1, "budget": hello_budget() })
            .to_string()
            .as_bytes(),
    );
    session.send_raw(b"\n");
    let ready = session.ready();
    assert_eq!(ready["protocol_version"], 1);
    assert!(!ready["worker_version"].as_str().unwrap().is_empty());
    let capabilities = ready["capabilities"].as_array().unwrap();
    assert!(capabilities.contains(&json!("hash_file")));
    assert!(capabilities.contains(&json!("fetch_range")));
    session.close();
}

#[test]
fn version_mismatch_terminates_the_worker() {
    let mut session = Session::new();
    session.send_raw(
        json!({ "type": "hello", "protocol_version": 2, "budget": hello_budget() })
            .to_string()
            .as_bytes(),
    );
    session.send_raw(b"\n");
    let line = session.next();
    let value: serde_json::Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(value["type"], "protocol_error");
    assert_eq!(value["error"], "protocol_version_mismatch");
    // The worker terminates after a version mismatch.
    assert_eq!(session.child.wait().unwrap().code(), Some(0));
}

#[test]
fn first_message_must_be_hello() {
    let mut session = Session::new();
    session.send_raw(
        json!({
            "type": "request",
            "request_id": "x",
            "operation": "hash_file",
            "budget": hello_budget(),
            "payload": {}
        })
        .to_string()
        .as_bytes(),
    );
    session.send_raw(b"\n");
    let line = session.next();
    let value: serde_json::Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(value["type"], "protocol_error");
    session.kill();
}

#[test]
fn unknown_fields_are_rejected_as_protocol_violations() {
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let message = json!({
        "type": "request",
        "request_id": "r1",
        "operation": "hash_file",
        "budget": hello_budget(),
        "payload": { "path": "x" },
        "unexpected_field": true,
    });
    session.send_raw(message.to_string().as_bytes());
    session.send_raw(b"\n");
    let line = session.next();
    let value: serde_json::Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(value["type"], "protocol_error");
    assert_eq!(value["error"], "protocol_violation");
    // The worker survives protocol violations.
    let reply = session.request("r2", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(reply["ok"], false);
    session.close();
}

#[test]
fn oversized_and_non_utf8_lines_are_rejected_without_killing_the_worker() {
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    // 64 KiB + 1 of 'a' in a string field.
    let big = format!("\"{}\"", "a".repeat(64 * 1024 + 10));
    session.send_raw(big.as_bytes());
    session.send_raw(b"\n");
    let value: serde_json::Value = serde_json::from_slice(&session.next()).unwrap();
    assert_eq!(value["type"], "protocol_error");
    assert!(matches!(
        value["error"].as_str(),
        Some("line_too_long") | Some("invalid_json")
    ));
    session.send_raw(b"\xff\xfe\n");
    let value: serde_json::Value = serde_json::from_slice(&session.next()).unwrap();
    assert_eq!(value["type"], "protocol_error");
    assert_eq!(value["error"], "invalid_utf8");
    // Still alive: a normal request is served.
    let reply = session.request("ok1", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(reply["ok"], false);
    session.close();
}

#[test]
fn duplicate_request_ids_are_rejected() {
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let first = session.request("dup", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(first["ok"], false);
    let second = session.request("dup", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(second["ok"], false);
    assert_eq!(second["error"], "duplicate_request");
    session.close();
}

#[test]
fn unknown_operations_are_rejected() {
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let reply = session.request("u1", "nope", json!({}));
    assert_eq!(reply["ok"], false);
    assert_eq!(reply["error"], "unknown_operation");
    session.close();
}

#[test]
fn in_memory_budget_rejects_overbudget_after_exhaustion() {
    let mut session = Session::new();
    // Only one attempt is allowed for this job.
    session
        .hello(json!({ "body": 10_000_000, "disk": 100, "inflight": 10_000_000, "attempts": 1 }));
    session.ready();
    let first = session.request("b1", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(first["ok"], false); // rejected work, but the attempt was admitted
    let second = session.request("b2", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(second["ok"], false);
    assert_eq!(second["error"], "budget_exceeded");
    session.close();
}

#[test]
fn budget_counters_reset_on_restart_but_never_persist() {
    // First worker exhausts its attempts.
    {
        let mut session = Session::new();
        session.hello(
            json!({ "body": 10_000_000, "disk": 100, "inflight": 10_000_000, "attempts": 1 }),
        );
        session.ready();
        let reply = session.request("a1", "hash_file", json!({ "path": "missing-xyz" }));
        assert_eq!(reply["ok"], false);
        session.kill(); // crash: in-memory state is lost
    }
    // A restarted worker starts fresh (its budget comes from the new hello;
    // durability is the Python ledger's job, never the worker's).
    let mut session = Session::new();
    session
        .hello(json!({ "body": 10_000_000, "disk": 100, "inflight": 10_000_000, "attempts": 1 }));
    session.ready();
    let reply = session.request("a1", "hash_file", json!({ "path": "missing-xyz" }));
    assert_eq!(reply["ok"], false);
    assert_ne!(
        reply.get("error").and_then(|v| v.as_str()),
        Some("budget_exceeded")
    );
    session.close();
}

#[test]
fn hash_file_streams_sha256() {
    let data: Vec<u8> = (0..(1 << 17)).map(|i| (i % 251) as u8).collect();
    let path = std::env::temp_dir().join(format!(
        "sakurapool-worker-hash-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &data).unwrap();
    let expected = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&data);
        format!("{:x}", hasher.finalize())
    };
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let reply = session.request(
        "h1",
        "hash_file",
        json!({ "path": path.display().to_string() }),
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["result"]["sha256"], expected);
    assert_eq!(reply["result"]["bytes"], data.len() as u64);
    session.close();
    std::fs::remove_file(path).unwrap();
}

#[test]
fn fetch_range_validates_206_and_streaming_sha() {
    let data: Vec<u8> = (0..(64 * 1024 + 123)).map(|i| (i % 251) as u8).collect();
    let whole_sha = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&data);
        format!("{:x}", hasher.finalize())
    };
    let expected = data[1000..2000].to_vec();
    let range_sha = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&expected);
        format!("{:x}", hasher.finalize())
    };
    let data_clone = data.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut header = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let count = match std::io::Read::read(&mut stream, &mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => count,
                };
                header.extend_from_slice(&chunk[..count]);
                if header.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = &data_clone[1000..2000];
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 1000-1999/{total}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len(),
                total = data_clone.len(),
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        }
    });
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let url = format!("http://127.0.0.1:{port}/obj");
    let reply = session.request(
        "f1",
        "fetch_range",
        json!({ "url": url, "start": 1000, "end": 1999, "total": data.len() }),
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["result"]["sha256"], range_sha);
    assert_eq!(reply["result"]["bytes"], 1000);
    let _ = (whole_sha, expected);
    session.close();
}

#[test]
fn fetch_range_rejects_200_responses_and_non_loopback_targets() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut header = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let count = match std::io::Read::read(&mut stream, &mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => count,
                };
                header.extend_from_slice(&chunk[..count]);
                if header.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"not a partial";
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
            stream.write_all(body).unwrap();
        }
    });
    let mut session = Session::new();
    session.hello(hello_budget());
    session.ready();
    let url = format!("http://127.0.0.1:{port}/obj");
    let reply = session.request(
        "f2",
        "fetch_range",
        json!({ "url": url, "start": 0, "end": 12, "total": 13 }),
    );
    assert_eq!(reply["ok"], false);
    let external = session.request(
        "f3",
        "fetch_range",
        json!({ "url": "http://example.com:80/obj", "start": 0, "end": 1, "total": 10 }),
    );
    assert_eq!(external["ok"], false);
    assert_eq!(external["error"], "non-loopback target");
    session.close();
}
