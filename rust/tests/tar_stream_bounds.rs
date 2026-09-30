//! Raw mature-parser metadata limits and strict closing-block validation.
use sakurapool_rust::{scan_tar_reader, ScanLimits, MAX_TAR_EXTENSION_BYTES};
use std::io::{self, Cursor, Read};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct HeaderOnly {
    header: Cursor<[u8; 512]>,
    reads: Arc<AtomicU64>,
}
impl Read for HeaderOnly {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let count = self.header.read(out)?;
        if count == 0 {
            panic!("extension payload MUST NOT be read or allocated before size rejection");
        }
        self.reads.fetch_add(count as u64, Ordering::SeqCst);
        Ok(count)
    }
}

#[test]
fn oversized_gnu_and_pax_rejected_at_header_before_payload_read() {
    for kind in [tar::EntryType::GNULongName, tar::EntryType::XHeader] {
        let mut header = tar::Header::new_gnu();
        header.set_path("extension").unwrap();
        header.set_entry_type(kind);
        header.set_size(MAX_TAR_EXTENSION_BYTES + 1);
        header.set_cksum();
        let reads = Arc::new(AtomicU64::new(0));
        let source = HeaderOnly {
            header: Cursor::new(*header.as_bytes()),
            reads: reads.clone(),
        };
        assert_eq!(
            scan_tar_reader(source, &ScanLimits::default()).unwrap_err(),
            "extension_too_large"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 512);
    }
}

fn one_file() -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(3);
    header.set_cksum();
    builder
        .append_data(&mut header, "small.bin", &b"abc"[..])
        .unwrap();
    builder.into_inner().unwrap()
}

#[test]
fn closing_requires_two_full_zero_blocks_and_zero_tail() {
    let valid = one_file();
    assert!(scan_tar_reader(Cursor::new(&valid), &ScanLimits::default()).is_ok());
    // Header + padded payload occupies 1024; after that are closing blocks.
    for end in [0, 1024, 1536, 2047] {
        assert!(
            scan_tar_reader(Cursor::new(&valid[..end]), &ScanLimits::default()).is_err(),
            "end={end}"
        );
    }
    let mut garbage = valid;
    *garbage.last_mut().unwrap() = 1;
    assert_eq!(
        scan_tar_reader(Cursor::new(garbage), &ScanLimits::default()).unwrap_err(),
        "invalid_tail"
    );
}

#[test]
fn legal_pax_path_and_matching_size_preserved_but_conflicting_geometry_rejected() {
    for size in ["3", "4"] {
        let mut builder = tar::Builder::new(Vec::new());
        builder
            .append_pax_extensions([
                ("path", &b"long/path/from/pax.bin"[..]),
                ("size", size.as_bytes()),
            ])
            .unwrap();
        let mut header = tar::Header::new_ustar();
        header.set_size(3);
        header.set_cksum();
        builder
            .append_data(&mut header, "short.bin", &b"abc"[..])
            .unwrap();
        let bytes = builder.into_inner().unwrap();
        let result = scan_tar_reader(Cursor::new(bytes), &ScanLimits::default());
        if size == "3" {
            let scan = result.unwrap();
            assert_eq!(scan.members[0].path, "long/path/from/pax.bin");
            assert_eq!(scan.members[0].size, 3);
        } else {
            assert_eq!(result.unwrap_err(), "unsupported_pax_size");
        }
    }
}

#[test]
fn dangling_extension_or_truncated_metadata_fails_closed() {
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_pax_extensions([("path", &b"future.bin"[..])])
        .unwrap();
    let bytes = builder.into_inner().unwrap();
    assert!(scan_tar_reader(Cursor::new(&bytes), &ScanLimits::default()).is_err());
    assert!(scan_tar_reader(Cursor::new(&bytes[..520]), &ScanLimits::default()).is_err());
}
