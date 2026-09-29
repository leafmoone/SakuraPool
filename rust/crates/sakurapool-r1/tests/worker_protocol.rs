use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

#[test]
fn worker_handles_ndjson_range_and_lifecycle() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sakurapool-r1-worker"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, r#"{{"op":"validate_range","start":2,"end":4,"total":10,"content_range":"bytes 2-4/10","body_len":3}}"#).unwrap();
    writeln!(input, r#"{{"op":"lifecycle","action":"begin"}}"#).unwrap();
    writeln!(input, r#"{{"op":"lifecycle","action":"cancel"}}"#).unwrap();
    drop(input);
    let stdout = child.stdout.take().unwrap();
    let lines: Vec<String> = BufReader::new(stdout).lines().map(Result::unwrap).collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains(r#""ok":true"#));
    assert!(lines[1].contains("Responding"));
    assert!(lines[2].contains("Cancelled"));
    assert!(child.wait().unwrap().success());
}
