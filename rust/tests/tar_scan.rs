//! Offline tests for the bounded sequential TAR scanner (lib level).
//!
//! Fixtures are synthetic, built here with the `tar` crate plus raw ustar
//! headers (hard link / sparse / device / fifo / absolute path) that the
//! writer API cannot produce. Nothing touches production data.

use sakurapool_rust::{scan_tar, ScanLimits, TarScan};
use sha2::Digest;

// ---------------------------------------------------------------------------
// Minimal temp-dir helper (no external crate needed).
// ---------------------------------------------------------------------------
mod tmpdir {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct Dir {
        pub path: PathBuf,
    }

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub fn new() -> Dir {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "sakurapool-worker-tar-{}-{}",
            std::process::id(),
            id
        ));
        std::fs::create_dir_all(&path).unwrap();
        Dir { path }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    pub fn write_tar(dir: &Dir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn append_file(builder: &mut tar::Builder<Vec<u8>>, name: &str, data: &[u8]) {
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append_data(&mut header, name, data).unwrap();
}

fn append_dir(builder: &mut tar::Builder<Vec<u8>>, name: &str) {
    let mut header = tar::Header::new_gnu();
    header.set_path(name).unwrap();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_cksum();
    builder.append(&header, &b""[..]).unwrap();
}

/// Independent cross-check: the raw bytes at each reported file member's
/// offset must equal its declared size and hash to the reported sha.
fn verify_members_against_raw(tar: &[u8], scan: &TarScan) {
    assert_eq!(scan.whole_sha256, sha256_hex(tar));
    assert_eq!(scan.size as usize, tar.len());
    for member in &scan.members {
        if member.kind == sakurapool_rust::MemberKind::File {
            let end = (member.offset + member.size) as usize;
            assert!(end <= tar.len(), "member exceeds file end");
            let slice = &tar[member.offset as usize..end];
            assert_eq!(slice.len(), member.size as usize);
            assert_eq!(
                member.sha256.as_deref(),
                Some(sha256_hex(slice).as_str()),
                "{}",
                member.path
            );
        }
    }
}

#[test]
fn scans_basic_archive_with_dirs_and_empty_file() {
    let dir = tmpdir::new();
    let a = vec![0xA5u8; 100];
    let b: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
    let mut builder = tar::Builder::new(Vec::new());
    append_file(&mut builder, "a.txt", &a);
    append_dir(&mut builder, "sub");
    append_file(&mut builder, "sub/b.bin", &b);
    append_file(&mut builder, "sub/empty.txt", &[]);
    let bytes = builder.into_inner().unwrap();
    let path = tmpdir::write_tar(&dir, "basic.tar", &bytes);

    let scan = scan_tar(&path, &ScanLimits::default()).unwrap();
    verify_members_against_raw(&bytes, &scan);
    assert_eq!(scan.members.len(), 4);
    assert_eq!(scan.members[0].path, "a.txt");
    assert_eq!(scan.members[0].size, 100);
    assert_eq!(
        scan.members[0].sha256.as_deref(),
        Some(sha256_hex(&a).as_str())
    );
    assert_eq!(scan.members[1].path, "sub");
    assert_eq!(scan.members[1].kind, sakurapool_rust::MemberKind::Dir);
    assert!(scan.members[1].sha256.is_none());
    assert_eq!(scan.members[2].path, "sub/b.bin");
    assert_eq!(scan.members[2].size, 1000);
    let empty = &scan.members[3];
    assert_eq!(empty.path, "sub/empty.txt");
    assert_eq!(empty.size, 0);
    assert_eq!(empty.sha256.as_deref(), Some(sha256_hex(&[]).as_str()));
    // Payload offsets are block-aligned (512) and strictly monotonic.
    for pair in scan.members.windows(2) {
        assert!(pair[0].offset < pair[1].offset);
        assert_eq!(pair[0].offset % 512, 0);
    }
    assert_eq!(scan.trailing_bytes, 512, "one closing zero block remains");
}

#[test]
fn scans_gnu_long_name_and_pax_long_name() {
    let dir = tmpdir::new();
    // --- GNU long name (writer-generated longlink extension) ---
    let long = format!("gnu/{}.txt", "x".repeat(150));
    let payload = vec![0x3Cu8; 77];
    let mut builder = tar::Builder::new(Vec::new());
    append_file(&mut builder, &long, &payload);
    let gnu_bytes = builder.into_inner().unwrap();
    let gnu_path = tmpdir::write_tar(&dir, "gnu.tar", &gnu_bytes);
    let scan = scan_tar(&gnu_path, &ScanLimits::default()).unwrap();
    assert_eq!(scan.members.len(), 1);
    assert_eq!(scan.members[0].path, long);
    assert_eq!(scan.members[0].size, 77);
    verify_members_against_raw(&gnu_bytes, &scan);

    // --- PAX long name (hand-crafted 'x' extension header) ---
    let pax_name = format!("pax/{}.bin", "y".repeat(160));
    let pax_payload = vec![0x77u8; 33];
    let record = pax_record(&format!("path={}", pax_name));
    let ext_header = raw_ustar_header(b"./@PaxHeader", record.len() as u64, b'x', b"");
    let main_name: String = pax_name.chars().take(100).collect();
    let main_header = raw_ustar_header(main_name.as_bytes(), pax_payload.len() as u64, b'0', b"");
    let mut pax_bytes = Vec::new();
    pax_bytes.extend_from_slice(&ext_header);
    pax_bytes.extend_from_slice(&record);
    pax_bytes.extend_from_slice(&pad_to(record.len(), 512));
    pax_bytes.extend_from_slice(&main_header);
    pax_bytes.extend_from_slice(&pax_payload);
    pax_bytes.extend_from_slice(&pad_to(pax_payload.len(), 512));
    pax_bytes.extend_from_slice(&[0u8; 1024]); // closing zero blocks
    let pax_path = tmpdir::write_tar(&dir, "pax.tar", &pax_bytes);
    let scan = scan_tar(&pax_path, &ScanLimits::default()).unwrap();
    assert_eq!(scan.members.len(), 1);
    assert_eq!(scan.members[0].path, pax_name);
    assert_eq!(scan.members[0].size, 33);
    verify_members_against_raw(&pax_bytes, &scan);
}

/// PAX record: "<len> <body>\n" where len is the total record length.
fn pax_record(body: &str) -> Vec<u8> {
    let mut len = 1 + body.len() + 2;
    loop {
        let candidate = len.to_string().len() + body.len() + 2;
        if candidate == len {
            return format!("{len} {body}\n").into_bytes();
        }
        len = candidate;
    }
}

fn pad_to(len: usize, block: usize) -> Vec<u8> {
    let rem = len % block;
    vec![0u8; if rem == 0 { 0 } else { block - rem }]
}

/// Hand-crafted ustar header (for members the writer API cannot produce).
fn raw_ustar_header(name: &[u8], size: u64, typeflag: u8, linkname: &[u8]) -> [u8; 512] {
    let mut header = [0u8; 512];
    let name_bytes = &name[..name.len().min(100)];
    header[..name_bytes.len()].copy_from_slice(name_bytes);
    header[100..108].copy_from_slice(b"0000644\0");
    header[108..116].copy_from_slice(b"0000000\0");
    header[116..124].copy_from_slice(b"0000000\0");
    header[124..136].copy_from_slice(format!("{:011o}\0", size).as_bytes());
    header[136..148].copy_from_slice(format!("{:011o}\0", 0).as_bytes());
    header[156] = typeflag;
    let link_bytes = &linkname[..linkname.len().min(100)];
    header[157..157 + link_bytes.len()].copy_from_slice(link_bytes);
    // GNU magic so the reader parses GNU extensions (e.g. sparse 'S').
    header[257..263].copy_from_slice(b"ustar ");
    header[263..265].copy_from_slice(b" \0");
    // Checksum: computed with the field blanked to spaces.
    header[148..156].copy_from_slice(b"        ");
    let sum: u32 = header.iter().map(|b| *b as u32).sum();
    header[148..154].copy_from_slice(format!("{:06o}", sum).as_bytes());
    header[154] = b' ';
    header[155] = 0;
    header
}

/// GNU sparse header: typeflag 'S' plus one all-zero extent so the header
/// parses (chunk bytes must sum to the size field). The three unused slots
/// stay all-NUL so the reader treats them as empty. The scanner must reject
/// the entry type before any payload is read.
fn raw_sparse_header(name: &[u8], logical_size: u64) -> [u8; 512] {
    let mut header = raw_ustar_header(name, logical_size, b'S', b"");
    // Record 0 at 386: offset=0, numbytes=logical_size (octal).
    header[386..398].copy_from_slice(b"00000000000\0");
    header[398..410].copy_from_slice(format!("{:011o}\0", logical_size).as_bytes());
    // Records 1..3 (410..482) remain all NUL = empty.
    header[482] = 0; // isextended
    header[483..495].copy_from_slice(format!("{:011o}\0", logical_size).as_bytes()); // realsize
    header[148..156].copy_from_slice(b"        ");
    let sum: u32 = header.iter().map(|b| *b as u32).sum();
    header[148..154].copy_from_slice(format!("{:06o}", sum).as_bytes());
    header[154] = b' ';
    header[155] = 0;
    header
}

#[test]
fn rejects_symlink_hardlink_device_and_fifo_members() {
    let dir = tmpdir::new();
    // Symlink via the writer.
    let mut builder = tar::Builder::new(Vec::new());
    append_file(&mut builder, "real.txt", b"hello");
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_cksum();
    builder
        .append_link(&mut header, "link.txt", "real.txt")
        .unwrap();
    let bytes = builder.into_inner().unwrap();
    let path = tmpdir::write_tar(&dir, "link.tar", &bytes);
    assert_eq!(
        scan_tar(&path, &ScanLimits::default()),
        Err("unsupported_member")
    );

    // Hard link via a raw ustar header (typeflag '1').
    let header = raw_ustar_header(b"first.bin", 0, b'1', b"real.bin");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&[0u8; 1024]);
    let path = tmpdir::write_tar(&dir, "hard.tar", &bytes);
    assert_eq!(
        scan_tar(&path, &ScanLimits::default()),
        Err("unsupported_member")
    );

    // Block device and fifo via raw headers.
    for (name, flag, file) in [("blockdev", b'4', "block.tar"), ("fifo", b'6', "fifo.tar")] {
        let header = raw_ustar_header(name.as_bytes(), 0, flag, b"");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&[0u8; 1024]);
        let path = tmpdir::write_tar(&dir, file, &bytes);
        assert_eq!(
            scan_tar(&path, &ScanLimits::default()),
            Err("unsupported_member"),
            "flag {flag:?}"
        );
    }
    // Sparse via a valid GNU sparse header.
    let header = raw_sparse_header(b"sparse", 1024);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&[0u8; 1024]);
    let path = tmpdir::write_tar(&dir, "sparse.tar", &bytes);
    assert_eq!(
        scan_tar(&path, &ScanLimits::default()),
        Err("unsupported_member")
    );
}

#[test]
fn rejects_truncation_checksum_and_bad_tail() {
    let dir = tmpdir::new();
    let payload: Vec<u8> = (0..5000).map(|i| (i % 199) as u8).collect();
    let mut builder = tar::Builder::new(Vec::new());
    append_file(&mut builder, "blob.bin", &payload);
    let full = builder.into_inner().unwrap();

    // Truncated mid-payload.
    let truncated = &full[..full.len() / 2];
    let path = tmpdir::write_tar(&dir, "truncated.tar", truncated);
    let err = scan_tar(&path, &ScanLimits::default()).unwrap_err();
    assert!(
        matches!(err, "corrupt_archive" | "truncated_member"),
        "unexpected {err:?}"
    );

    // Corrupted header checksum (flip a byte in the name field).
    let mut corrupted = full.clone();
    corrupted[3] ^= 0xFF;
    let path = tmpdir::write_tar(&dir, "corrupt.tar", &corrupted);
    assert_eq!(
        scan_tar(&path, &ScanLimits::default()),
        Err("corrupt_archive")
    );

    // Nonzero trailing garbage after the closing zero blocks.
    let mut bad_tail = full.clone();
    bad_tail.push(0x42);
    let path = tmpdir::write_tar(&dir, "tail.tar", &bad_tail);
    let err = scan_tar(&path, &ScanLimits::default()).unwrap_err();
    assert!(
        matches!(err, "corrupt_archive" | "invalid_tail"),
        "unexpected {err:?}"
    );
}

#[test]
fn rejects_unsafe_and_non_utf8_member_paths() {
    let dir = tmpdir::new();
    for (name, expected) in [
        (b"/etc/absolute".as_slice(), "unsafe_member_path"),
        (b"../traversal".as_slice(), "unsafe_member_path"),
        (b"ok/../x".as_slice(), "unsafe_member_path"),
    ] {
        let header = raw_ustar_header(name, 0, b'0', b"");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&[0u8; 1024]);
        let path = tmpdir::write_tar(&dir, "unsafe.tar", &bytes);
        assert_eq!(scan_tar(&path, &ScanLimits::default()), Err(expected));
    }
    // Non-UTF-8 name byte.
    let mut name = b"bad-".to_vec();
    name.push(0xFF);
    let header = raw_ustar_header(&name, 0, b'0', b"");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&[0u8; 1024]);
    let path = tmpdir::write_tar(&dir, "nonutf8.tar", &bytes);
    assert_eq!(
        scan_tar(&path, &ScanLimits::default()),
        Err("non_utf8_member")
    );
}

#[test]
fn enforces_member_and_byte_limits() {
    let dir = tmpdir::new();
    let payload = vec![0x21u8; 3000];
    let mut builder = tar::Builder::new(Vec::new());
    append_file(&mut builder, "one.bin", &payload);
    append_file(&mut builder, "two.bin", &payload);
    let bytes = builder.into_inner().unwrap();
    let path = tmpdir::write_tar(&dir, "limits.tar", &bytes);

    // Two members but only one allowed.
    assert_eq!(
        scan_tar(
            &path,
            &ScanLimits {
                max_members: 1,
                max_bytes: 1 << 30
            }
        ),
        Err("limit_exceeded")
    );
    // Archive larger than the byte budget.
    assert_eq!(
        scan_tar(
            &path,
            &ScanLimits {
                max_members: 100,
                max_bytes: bytes.len() as u64 - 1
            }
        ),
        Err("limit_exceeded")
    );
    // Within budget: fine.
    let scan = scan_tar(
        &path,
        &ScanLimits {
            max_members: 100,
            max_bytes: bytes.len() as u64,
        },
    )
    .unwrap();
    assert_eq!(scan.members.len(), 2);
}

#[test]
fn missing_file_is_io_error() {
    let dir = tmpdir::new();
    let missing = dir.path.join("no-such.tar");
    assert_eq!(scan_tar(&missing, &ScanLimits::default()), Err("io_error"));
}
