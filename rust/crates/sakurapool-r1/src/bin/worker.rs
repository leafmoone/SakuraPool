use sakurapool_r1::{ByteRange, ResponseLifecycle};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Read, Write};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    ValidateRange {
        start: u64,
        end: u64,
        total: u64,
        content_range: String,
        body_len: u64,
    },
    Lifecycle {
        action: String,
    },
    Cancel {
        id: String,
    },
    HashFile {
        path: String,
    },
    FetchRange {
        url: String,
        start: u64,
        end: u64,
        total: u64,
    },
}
#[derive(Serialize)]
struct Response {
    ok: bool,
    result: Option<String>,
    error: Option<String>,
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut lifecycle = ResponseLifecycle::new();
    for line in stdin.lock().lines() {
        let line = line?;
        let reply = match serde_json::from_str::<Request>(&line) {
            Ok(Request::ValidateRange {
                start,
                end,
                total,
                content_range,
                body_len,
            }) => {
                let result = ByteRange::new(start, end, total)
                    .map_err(str::to_owned)
                    .and_then(|r| {
                        sakurapool_r1::validate_content_range(r, total, &content_range, body_len)
                            .map(|_| "valid".to_owned())
                            .map_err(str::to_owned)
                    });
                response(result)
            }
            Ok(Request::Lifecycle { action }) => {
                let result = match action.as_str() {
                    "begin" => lifecycle.begin(),
                    "complete" => lifecycle.complete(),
                    "cancel" => lifecycle.cancel(),
                    _ => Err("unknown lifecycle action"),
                };
                response(
                    result
                        .map(|_| format!("{:?}", lifecycle.state()))
                        .map_err(str::to_owned),
                )
            }
            Ok(Request::Cancel { id }) => response(Ok(format!("cancelled:{id}"))),
            Ok(Request::HashFile { path }) => {
                let result = std::fs::File::open(&path)
                    .map_err(|e| e.to_string())
                    .and_then(|file| {
                        sakurapool_r1::StreamingSha256::digest_reader(file)
                            .map_err(|e| e.to_string())
                    });
                response(result)
            }
            Ok(Request::FetchRange {
                url,
                start,
                end,
                total,
            }) => response(fetch_range(&url, start, end, total).map_err(|e| e.to_string())),
            Err(error) => response(Err(error.to_string())),
        };
        serde_json::to_writer(&mut stdout, &reply)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}
/// Loopback-only HTTP/1.1 range fetch with exact Content-Range validation and
/// streaming SHA-256. Rejected targets never open a connection.
fn fetch_range(url: &str, start: u64, end: u64, total: u64) -> Result<String, String> {
    use sakurapool_r1::{parse_loopback_url, validate_content_range, StreamingSha256};
    use std::net::TcpStream;
    use std::time::Duration;
    let target = parse_loopback_url(url)?;
    let range = ByteRange::new(start, end, total)?;
    let mut stream =
        TcpStream::connect((target.host.as_str(), target.port)).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nRange: bytes={}-{}\r\nConnection: close\r\n\r\n",
        target.path, target.host, target.port, range.start, range.end
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
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
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before headers".to_owned());
        }
        buffer.extend_from_slice(&chunk[..count]);
        if buffer.len() > 65536 {
            return Err("response headers too large".to_owned());
        }
    }
    let header_end = header_end.unwrap();
    let head_str = std::str::from_utf8(&buffer[..header_end]).map_err(|_| "headers not utf-8")?;
    let mut lines = head_str.lines();
    let status = lines.next().ok_or("empty response")?;
    if status.split_whitespace().nth(1) != Some("206") {
        return Err("expected HTTP 206".to_owned());
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
            return Err("body longer than requested range".to_owned());
        }
        if body.len() == expected {
            break;
        }
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("body shorter than requested range".to_owned());
        }
        body.extend_from_slice(&chunk[..count]);
    }
    hasher.update(&body);
    Ok(format!("sha256:{}:bytes:{}", hasher.finish(), expected))
}

fn response(result: Result<String, String>) -> Response {
    match result {
        Ok(value) => Response {
            ok: true,
            result: Some(value),
            error: None,
        },
        Err(error) => Response {
            ok: false,
            result: None,
            error: Some(error),
        },
    }
}
