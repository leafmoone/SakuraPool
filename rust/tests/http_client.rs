//! Offline loopback tests for the unified HTTP components
//! (range / full_stream / probe): redirect chains with header isolation,
//! bounded retries, response lifecycle and every rejection class.
//!
//! The server below is a single-threaded std TCP listener with the minimal
//! HTTP/1.1 surface the client speaks (identity, `Connection: close`).

use sakurapool_rust::{http_request, ByteRange, HttpBody, HttpOp, HttpPolicy};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const DATA: &[u8] = b"sakurapool-loopback-payload-0123456789";

/// One recorded request header list.
type HeaderList = Vec<(String, String)>;
/// (path, method, headers) per request, arrival order.
type RequestLog = Arc<Mutex<Vec<(String, String, HeaderList)>>>;

#[derive(Clone)]
struct ServerState {
    hits: Arc<Mutex<HashMap<String, u32>>>,
    requests: RequestLog,
    stop: Arc<AtomicBool>,
}

/// Minimal threaded loopback server. Endpoints:
///   /data            200 full (Range -> 206), HEAD -> 200 + Content-Length
///   /redirect        302 -> cdn /data
///   /redirect307     307 -> cdn /data (range-preserving)
///   /retry503        503 twice, then 200 /data
///   /always503       503 forever
///   /notfound        404
///   /hugeheader      200 with a 70 KiB header block
///   /badstatus       malformed status line
///   /shortbody       200, Content-Length lies high, stream closes early
///   /longrange       206 correct range but extra trailing bytes
///   /missingloc      302 without Location
///   /loop            302 -> itself
///   /nosize          200 without Content-Length
struct TestServer {
    port: u16,
    state: ServerState,
}

impl TestServer {
    fn new(state: ServerState, cdn_port: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = state.stop.clone();
        let state2 = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                if let Ok(stream) = stream {
                    handle_connection(stream, &state2, cdn_port, port);
                }
            }
        });
        TestServer { port, state }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }

    fn hits(&self, path: &str) -> u32 {
        self.state
            .hits
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or(0)
    }

    /// Header lists of every recorded request to `path`, arrival order.
    fn request_log(&self, path: &str) -> Vec<Vec<(String, String)>> {
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(recorded_path, _, _)| recorded_path == path)
            .map(|(_, _, headers)| headers.clone())
            .collect()
    }
}

fn handle_connection(stream: TcpStream, state: &ServerState, cdn_port: u16, self_port: u16) {
    let mut stream = stream;
    let mut head = String::new();
    let mut chunk = [0u8; 4096];
    loop {
        if head.contains("\r\n\r\n") {
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => head.push_str(std::str::from_utf8(&chunk[..n]).unwrap_or("")),
            Err(_) => return,
        }
    }
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    {
        let mut hits = state.hits.lock().unwrap();
        *hits.entry(path.to_owned()).or_insert(0) += 1;
        let hit = *hits.get(path).unwrap();
        state
            .requests
            .lock()
            .unwrap()
            .push((path.to_owned(), method.to_owned(), headers.clone()));
        let response = match path {
            "/data" => data_response(method, &headers),
            "/redirect" | "/redirect307" => {
                let status = if path == "/redirect307" {
                    "307 Temporary Redirect"
                } else {
                    "302 Found"
                };
                let location = format!("http://127.0.0.1:{}/data", cdn_port);
                (
                    format!(
                        "HTTP/1.1 {status}\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    ),
                    Vec::new(),
                )
            }
            "/retry503" if hit <= 2 => (
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned(),
                Vec::new(),
            ),
            "/retry503" => data_response(method, &headers),
            "/always503" => (
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned(),
                Vec::new(),
            ),
            "/notfound" => (
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
                Vec::new(),
            ),
            "/hugeheader" => {
                let big = format!("X-Big: {}\r\n", "x".repeat(70 * 1024));
                (
                    format!(
                        "HTTP/1.1 200 OK\r\n{big}Content-Length: {}\r\nConnection: close\r\n\r\n",
                        DATA.len()
                    ),
                    DATA.to_vec(),
                )
            }
            "/badstatus" => ("GARBAGE NO VERSION\r\n\r\n".to_owned(), Vec::new()),
            "/shortbody" => (
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    DATA.len() + 64
                ),
                DATA[..8].to_vec(),
            ),
            "/longrange" => {
                // 206 for 0-7 but the stream keeps sending - extra bytes.
                (
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-7/38\r\nContent-Length: 16\r\nConnection: close\r\n\r\n"
                        .to_owned(),
                    b"0123456789abcdef".to_vec(),
                )
            }
            "/missingloc" => (
                "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
                Vec::new(),
            ),
            "/loop" => (
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{self_port}/loop\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                ),
                Vec::new(),
            ),
            "/nosize" => (
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_owned(),
                DATA.to_vec(),
            ),
            _ => (
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
                Vec::new(),
            ),
        };
        drop(hits);
        let _ = stream.write_all(response.0.as_bytes());
        if !response.1.is_empty() && method != "HEAD" {
            let _ = stream.write_all(&response.1);
        }
        let _ = stream.flush();
    }
}

fn data_response(method: &str, headers: &[(String, String)]) -> (String, Vec<u8>) {
    let range = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("range"))
        .and_then(|(_, value)| value.strip_prefix("bytes="));
    match range {
        Some(spec) => {
            let (start_str, end_str) = spec.split_once('-').unwrap();
            let start: usize = start_str.parse().unwrap();
            let end: usize = end_str.parse().unwrap();
            let slice = &DATA[start..=end];
            (
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    DATA.len(),
                    slice.len()
                ),
                slice.to_vec(),
            )
        }
        None => (
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                DATA.len()
            ),
            if method == "HEAD" {
                Vec::new()
            } else {
                DATA.to_vec()
            },
        ),
    }
}

fn fast_policy() -> HttpPolicy {
    HttpPolicy {
        max_redirects: 5,
        max_retries: 0,
        retry_base_ms: 1,
        read_timeout: std::time::Duration::from_secs(5),
        extra_headers: Vec::new(),
    }
}

#[test]
fn range_component_fetches_exact_206() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    let range = ByteRange::new(10, 19, DATA.len() as u64).unwrap();
    let response = http_request(
        &HttpOp::Range {
            range,
            total: DATA.len() as u64,
        },
        &server.url("/data"),
        DATA.len() as u64,
        &fast_policy(),
    )
    .unwrap();
    assert_eq!(response.status, 206);
    assert_eq!(response.body.bounded_bytes().unwrap(), &DATA[10..20]);
    assert_eq!(response.hops, 0);
}

#[test]
fn full_stream_component_fetches_200_to_eof() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    let mut response = http_request(
        &HttpOp::FullStream,
        &server.url("/data"),
        DATA.len() as u64,
        &fast_policy(),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert!(matches!(response.body, HttpBody::Stream(_)));
    let mut collected = Vec::new(); // test-only observation of a tiny payload
    response.body.read_to_end(&mut collected).unwrap();
    assert_eq!(collected, DATA);
}

#[test]
fn probe_component_reports_total_size() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    let response = http_request(&HttpOp::Probe, &server.url("/data"), 0, &fast_policy()).unwrap();
    assert_eq!(response.status, 200);
    assert!(response.body.bounded_bytes().unwrap().is_empty());
    let content_length = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.as_str())
        .unwrap();
    assert_eq!(content_length, DATA.len().to_string());
}

#[test]
fn redirect_origin_302_to_cdn_206_with_header_isolation() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let cdn = TestServer::new(state.clone(), 0);
    let origin = TestServer::new(state.clone(), cdn.port);

    let mut policy = fast_policy();
    policy
        .extra_headers
        .push(("X-Client-Token".to_owned(), "secret".to_owned()));

    let range = ByteRange::new(0, 9, DATA.len() as u64).unwrap();
    let response = http_request(
        &HttpOp::Range {
            range,
            total: DATA.len() as u64,
        },
        &origin.url("/redirect"),
        DATA.len() as u64,
        &policy,
    )
    .unwrap();
    assert_eq!(response.status, 206);
    assert_eq!(response.body.bounded_bytes().unwrap(), &DATA[..10]);
    assert_eq!(response.hops, 1);
    assert!(
        response
            .final_url
            .starts_with(&format!("http://127.0.0.1:{}/", cdn.port)),
        "final_url must be the CDN hop: {}",
        response.final_url
    );

    // Header isolation: the token reached the origin, never the CDN.
    let origin_requests = origin.request_log("/redirect");
    assert!(
        origin_requests.iter().any(|headers| headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("x-client-token") && value == "secret")),
        "origin must receive the caller header"
    );
    let cdn_requests = cdn.request_log("/data");
    assert!(
        cdn_requests.iter().all(|headers| !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-client-token"))),
        "CDN must never see the caller header"
    );
    // And the CDN did receive the Range header on hop 2.
    assert!(
        cdn_requests.iter().any(|headers| headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("range") && value == "bytes=0-9")),
        "CDN must receive the Range header"
    );
}

#[test]
fn retry_succeeds_after_transient_503s_and_exhausts_cleanly() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    let mut policy = fast_policy();
    policy.max_retries = 2;
    policy.retry_base_ms = 1;

    // Two 503s then success: succeeds on the third attempt.
    let mut response = http_request(
        &HttpOp::FullStream,
        &server.url("/retry503"),
        DATA.len() as u64,
        &policy,
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert!(matches!(response.body, HttpBody::Stream(_)));
    let mut collected = Vec::new(); // test-only observation of a tiny payload
    response.body.read_to_end(&mut collected).unwrap();
    assert_eq!(collected, DATA);
    assert_eq!(response.retries, 2);
    assert_eq!(server.hits("/retry503"), 3);

    // Always-503: exhausts exactly max_retries+1 attempts, clean error.
    let err = http_request(
        &HttpOp::FullStream,
        &server.url("/always503"),
        DATA.len() as u64,
        &policy,
    )
    .unwrap_err();
    assert_eq!(err, "unexpected_status");
    assert_eq!(server.hits("/always503"), 3);
}

#[test]
fn reject_class_non_loopback_https_and_missing_location() {
    let policy = fast_policy();
    // No connection is even attempted for these.
    assert!(http_request(
        &HttpOp::FullStream,
        "http://example.com:80/x",
        1024,
        &policy,
    )
    .is_err());
    assert!(http_request(&HttpOp::FullStream, "https://127.0.0.1:1/x", 1024, &policy,).is_err());

    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    assert_eq!(
        http_request(
            &HttpOp::FullStream,
            &server.url("/missingloc"),
            1024,
            &policy
        )
        .unwrap_err(),
        "missing_location"
    );
    // Redirect loop proves the redirect cap.
    assert_eq!(
        http_request(&HttpOp::FullStream, &server.url("/loop"), 1024, &policy).unwrap_err(),
        "too_many_redirects"
    );
}

#[test]
fn reject_class_response_lifecycle() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let server = TestServer::new(state, 0);
    let policy = fast_policy();

    // Malformed status line (no retries configured).
    assert_eq!(
        http_request(
            &HttpOp::FullStream,
            &server.url("/badstatus"),
            1024,
            &policy
        )
        .unwrap_err(),
        "malformed_status"
    );
    // Oversized header block.
    assert_eq!(
        http_request(
            &HttpOp::FullStream,
            &server.url("/hugeheader"),
            1024,
            &policy
        )
        .unwrap_err(),
        "response_headers_too_large"
    );
    // Range server sent more bytes than the range (extra trailing payload).
    let range = ByteRange::new(0, 7, DATA.len() as u64).unwrap();
    assert_eq!(
        http_request(
            &HttpOp::Range {
                range,
                total: DATA.len() as u64,
            },
            &server.url("/longrange"),
            DATA.len() as u64,
            &policy,
        )
        .unwrap_err(),
        "body_too_long"
    );
    // 404 is final, not retried.
    assert_eq!(
        http_request(&HttpOp::FullStream, &server.url("/notfound"), 1024, &policy).unwrap_err(),
        "unexpected_status"
    );
    assert_eq!(server.hits("/notfound"), 1);
    // FullStream is live: a short body fails during Read, not before handoff.
    let mut short = http_request(
        &HttpOp::FullStream,
        &server.url("/shortbody"),
        1024,
        &policy,
    )
    .unwrap();
    let mut tiny = Vec::new();
    assert!(short.body.read_to_end(&mut tiny).is_err());
    assert_eq!(short.body.error_code(), Some("body_too_short"));
    assert_eq!(server.hits("/shortbody"), 1); // no replay after stream handoff
                                              // Probe without Content-Length is rejected.
    assert_eq!(
        http_request(&HttpOp::Probe, &server.url("/nosize"), 0, &policy).unwrap_err(),
        "probe_without_content_length"
    );
    // Full stream past the byte cap is rejected.
    assert_eq!(
        http_request(
            &HttpOp::FullStream,
            &server.url("/data"),
            DATA.len() as u64 - 1,
            &policy,
        )
        .unwrap_err(),
        "body_too_large"
    );
}

#[test]
fn redirect_307_preserves_range_to_cdn() {
    let state = ServerState {
        hits: Arc::new(Mutex::new(HashMap::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
        stop: Arc::new(AtomicBool::new(false)),
    };
    let cdn = TestServer::new(state.clone(), 0);
    let origin = TestServer::new(state.clone(), cdn.port);
    let policy = fast_policy();
    let range = ByteRange::new(5, 14, DATA.len() as u64).unwrap();
    let response = http_request(
        &HttpOp::Range {
            range,
            total: DATA.len() as u64,
        },
        &origin.url("/redirect307"),
        DATA.len() as u64,
        &policy,
    )
    .unwrap();
    assert_eq!(response.status, 206);
    assert_eq!(response.body.bounded_bytes().unwrap(), &DATA[5..15]);
    assert_eq!(response.hops, 1);
}

// ---------------------------------------------------------------------------
// scan_http_tar: HTTP body must agree field-for-field with the file scan.
// ---------------------------------------------------------------------------

fn scan_tar_bytes() -> Vec<u8> {
    let a = [0xA5u8; 100];
    let b: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(a.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, "a.txt", std::io::Cursor::new(a.as_slice()))
        .unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_path("sub").unwrap();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_cksum();
    builder.append(&header, &b""[..]).unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_size(b.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, "sub/b.bin", std::io::Cursor::new(&b[..]))
        .unwrap();
    builder.into_inner().unwrap()
}

#[test]
fn scan_http_tar_matches_file_scan_field_for_field() {
    let dir = {
        let id = std::process::id();
        let path = std::env::temp_dir().join(format!("sakurapool-http-scan-{}", id));
        std::fs::create_dir_all(&path).unwrap();
        path
    };
    let bytes = scan_tar_bytes();
    let file_path = dir.join("archive.tar");
    std::fs::write(&file_path, &bytes).unwrap();
    let file_scan =
        sakurapool_rust::scan_tar_file(&file_path, &sakurapool_rust::ScanLimits::default())
            .unwrap();

    // Serve the bytes under a data endpoint (200 full + Range 206 + HEAD).
    // Reuse the shared server by exposing the bytes through /data is not
    // possible (DATA is fixed), so spin a dedicated minimal server.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = bytes.clone();
    let serve = std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut request = String::new();
            let mut chunk = [0u8; 4096];
            while !request.contains("\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => request.push_str(std::str::from_utf8(&chunk[..n]).unwrap_or("")),
                    Err(_) => break,
                }
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(&body);
            let _ = stream.flush();
        }
    });
    let url = format!("http://127.0.0.1:{}/archive.tar", port);
    let http_scan = sakurapool_rust::scan_http_tar(
        &url,
        &sakurapool_rust::ScanLimits::default(),
        &HttpPolicy::default(),
    )
    .unwrap();
    assert_eq!(
        http_scan, file_scan,
        "HTTP scan and file scan must agree on every field"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = serve;
}

#[test]
fn scan_http_tar_rejects_over_limit_stream() {
    let bytes = scan_tar_bytes();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let body = bytes.clone();
    let serve = std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut request = String::new();
            let mut chunk = [0u8; 4096];
            while !request.contains("\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => request.push_str(std::str::from_utf8(&chunk[..n]).unwrap_or("")),
                    Err(_) => break,
                }
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(&body);
            let _ = stream.flush();
        }
    });
    let url = format!("http://127.0.0.1:{}/archive.tar", port);
    let err = sakurapool_rust::scan_http_tar(
        &url,
        &sakurapool_rust::ScanLimits {
            max_members: 100,
            max_bytes: bytes.len() as u64 - 1,
        },
        &HttpPolicy::default(),
    )
    .unwrap_err();
    assert_eq!(err, "body_too_large");
    let _ = serve;
}
