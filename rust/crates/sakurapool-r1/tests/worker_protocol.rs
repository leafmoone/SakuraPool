use sha2::Digest;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Persistent-reader client: one stdout reader thread, one request at a time.
struct Session {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    rx: std::sync::mpsc::Receiver<String>,
}

impl Session {
    fn new() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sakurapool-r1-worker"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let stdin = child.stdin.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout).lines();
            for line in reader {
                if tx.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        Self { child, stdin, rx }
    }
    fn request(&mut self, json: &str) -> serde_json::Value {
        writeln!(self.stdin, "{json}").unwrap();
        let line = self.rx.recv_timeout(Duration::from_secs(30)).unwrap();
        serde_json::from_str(&line).unwrap()
    }
    fn close(mut self) {
        drop(self.stdin);
        self.child.wait().unwrap();
    }
}

#[test]
fn worker_handles_ndjson_range_and_lifecycle() {
    let mut session = Session::new();
    let valid = session.request(
        r#"{"op":"validate_range","start":2,"end":4,"total":10,"content_range":"bytes 2-4/10","body_len":3}"#,
    );
    assert_eq!(valid["ok"], true);
    let begin = session.request(r#"{"op":"lifecycle","action":"begin"}"#);
    assert_eq!(begin["ok"], true);
    assert!(begin["result"].as_str().unwrap().contains("Responding"));
    let cancel = session.request(r#"{"op":"lifecycle","action":"cancel"}"#);
    assert_eq!(cancel["ok"], true);
    assert!(cancel["result"].as_str().unwrap().contains("Cancelled"));
    let bad = session.request(
        r#"{"op":"validate_range","start":0,"end":9,"total":10,"content_range":"bytes 0-8/10","body_len":9}"#,
    );
    assert_eq!(bad["ok"], false);
    let unknown = session.request(r#"{"op":"nope"}"#);
    assert_eq!(unknown["ok"], false);
    session.close();
}

#[test]
fn worker_hash_file_streams_sha256() {
    let data: Vec<u8> = (0..(1 << 17)).map(|i| (i % 251) as u8).collect();
    let path = std::env::temp_dir().join(format!(
        "sakurapool-r1-hash-{}",
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
    let payload = serde_json::json!({"op": "hash_file", "path": path.display().to_string()});
    let reply = session.request(&payload.to_string());
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["result"].as_str().unwrap(), expected);
    let missing = session.request(r#"{"op":"hash_file","path":"missing-file-xyz"}"#);
    assert_eq!(missing["ok"], false);
    session.close();
    std::fs::remove_file(path).unwrap();
}
