//! Synthetic loopback proofs of lazy handoff / incremental scan / body caps.
//!
//! No fixture here allocates a whole large archive. The large payload generator
//! and scanner each use fixed chunks; the scan result contains ONE member.
use sakurapool_rust::{http_request, scan_http_tar, HttpBody, HttpOp, HttpPolicy, ScanLimits};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

fn policy() -> HttpPolicy {
    HttpPolicy {
        max_retries: 0,
        read_timeout: Duration::from_secs(5),
        ..HttpPolicy::default()
    }
}

fn consume_head(stream: &mut impl Read) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        head.push(byte[0]);
    }
}

#[test]
fn full_stream_returns_before_server_releases_any_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/lazy", listener.local_addr().unwrap());
    let (release, wait) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
            .unwrap();
        wait.recv_timeout(Duration::from_secs(4)).unwrap();
        stream.write_all(b"lazy").unwrap();
    });
    let (done, result) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        done.send(http_request(&HttpOp::FullStream, &url, 4, &policy()))
            .unwrap();
    });
    // A materializing client cannot hand off until release; this would time out.
    let mut response = result
        .recv_timeout(Duration::from_secs(2))
        .expect("must return at headers")
        .unwrap();
    assert!(matches!(response.body, HttpBody::Stream(_)));
    assert!(response.body.bounded_bytes().is_err());
    release.send(()).unwrap();
    let mut bytes = [0u8; 4];
    response.body.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"lazy");
    assert_eq!(response.body.read(&mut [0u8; 1]).unwrap(), 0);
    caller.join().unwrap();
    server.join().unwrap();
}

#[test]
fn scanner_rejects_unsafe_first_header_before_remainder_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/unsafe.tar", listener.local_addr().unwrap());
    let (release, wait) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_path("safe").unwrap();
        header.as_mut_bytes()[..5].copy_from_slice(b"../x\0");
        header.set_size(1);
        header.set_mode(0o644);
        header.set_cksum();
        stream.write_all(header.as_bytes()).unwrap();
        // No payload/tail will arrive until after the scan has already failed.
        let _ = wait.recv_timeout(Duration::from_secs(4));
    });
    let (done, result) = mpsc::channel();
    let scanner = std::thread::spawn(move || {
        done.send(scan_http_tar(&url, &ScanLimits::default(), &policy()))
            .unwrap();
    });
    let outcome = result
        .recv_timeout(Duration::from_secs(2))
        .expect("incremental header rejection");
    assert_eq!(outcome.unwrap_err(), "unsafe_member_path");
    release.send(()).unwrap();
    scanner.join().unwrap();
    server.join().unwrap();
}

#[test]
fn sixteen_mib_member_flows_in_fixed_chunks_without_whole_tar_vec() {
    const SIZE: u64 = 16 * 1024 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://{}/large-synthetic.tar",
        listener.local_addr().unwrap()
    );
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            SIZE + 1536
        )
        .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_path("synthetic.bin").unwrap();
        header.set_size(SIZE);
        header.set_mode(0o644);
        header.set_cksum();
        stream.write_all(header.as_bytes()).unwrap();
        let chunk = [0xA5u8; 64 * 1024];
        let mut member_hash = Sha256::new();
        let mut whole_hash = Sha256::new();
        whole_hash.update(header.as_bytes());
        for _ in 0..(SIZE / chunk.len() as u64) {
            stream.write_all(&chunk).unwrap();
            member_hash.update(chunk);
            whole_hash.update(chunk);
        }
        stream.write_all(&[0u8; 1024]).unwrap();
        whole_hash.update([0u8; 1024]);
        (
            format!("{:x}", member_hash.finalize()),
            format!("{:x}", whole_hash.finalize()),
        )
    });
    let scan = scan_http_tar(
        &url,
        &ScanLimits {
            max_bytes: SIZE + 1536,
            max_members: 1,
        },
        &policy(),
    )
    .unwrap();
    let (member, whole) = server.join().unwrap();
    assert_eq!(scan.size, SIZE + 1536);
    assert_eq!(scan.whole_sha256, whole);
    assert_eq!(scan.members[0].offset, 512);
    assert_eq!(scan.members[0].size, SIZE);
    assert_eq!(scan.members[0].sha256.as_deref(), Some(member.as_str()));
}

#[test]
fn unknown_length_chunked_stream_enforces_cap_during_read() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/chunked", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\n12345\r\n0\r\n\r\n").unwrap();
    });
    let mut response = http_request(&HttpOp::FullStream, &url, 4, &policy()).unwrap();
    let mut buffer = [0u8; 64];
    assert_eq!(response.body.read(&mut buffer).unwrap(), 4);
    assert!(response.body.read(&mut buffer).is_err());
    assert_eq!(response.body.error_code(), Some("body_too_large"));
    server.join().unwrap();
}

#[test]
fn range_above_small_body_cap_fails_before_connect() {
    use sakurapool_rust::{ByteRange, HTTP_MAX_RANGE_BYTES};
    let size = HTTP_MAX_RANGE_BYTES + 1;
    let outcome = http_request(
        &HttpOp::Range {
            range: ByteRange::new(0, size - 1, size).unwrap(),
            total: size,
        },
        "http://127.0.0.1:9/no-connection",
        size,
        &policy(),
    );
    assert_eq!(outcome.unwrap_err(), "body_too_large");
}

#[test]
fn redirect_to_foreign_host_rejected_without_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/redirect", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: http://example.invalid:80/archive.tar\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    });
    assert_eq!(
        http_request(&HttpOp::FullStream, &url, 100, &policy()).unwrap_err(),
        "non-loopback target"
    );
    server.join().unwrap();
}

#[test]
fn partial_scan_body_fails_closed_with_no_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/truncated.tar", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        consume_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_path("member.bin").unwrap();
        header.set_size(1024);
        header.set_mode(0o644);
        header.set_cksum();
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&[42u8; 10]).unwrap();
        // Close instead of sending the declared payload/tail.
    });
    let mut retry_policy = policy();
    retry_policy.max_retries = 2;
    assert_eq!(
        scan_http_tar(&url, &ScanLimits::default(), &retry_policy).unwrap_err(),
        "body_too_short"
    );
    server.join().unwrap();
}
