use sakurapool_r1::{ByteRange, ResponseLifecycle};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};

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
            Err(error) => response(Err(error.to_string())),
        };
        serde_json::to_writer(&mut stdout, &reply)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
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
