"""Additional synthetic TAR negative cases: no target network, no publication."""

import gzip
import io
import tarfile

import pytest
from test_p4_transport import ETAG, Handler
from test_p4_transport import http_and_budget as _synthetic_http

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.remote_index import stage_tar
from sakurapool.storage.transport import BoundObject, RemoteIOError


@pytest.fixture
def local_fixture():
    yield from _synthetic_http.__wrapped__()


@pytest.mark.parametrize("payload", [
    gzip.compress(b"a synthetic TAR-looking payload"),
    b"NOT_A_TAR_HEADER" + bytes(1024),
])
def test_compressed_disguise_and_malformed_tar_never_publish(local_fixture, payload):
    base, ledger, transport = local_fixture
    Handler.fixture_data = payload
    destination = ledger.root / "invalid-archive"
    bound = BoundObject(base + "/full-custom", len(payload), strong_etag=ETAG)
    with pytest.raises(RemoteIOError):
        stage_tar(transport, ledger, bound, destination, DatasetAdapter("d", "s"),
                  max_records=1)
    assert ledger.status()["attempts"] == 1
    assert not (destination / "stage.complete").exists()


def test_tar_checksum_tampering_does_not_publish(local_fixture):
    base, ledger, transport = local_fixture
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:", format=tarfile.GNU_FORMAT) as archive:
        info = tarfile.TarInfo("nested/1.jpg")
        content = b"tiny"
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    damaged = bytearray(output.getvalue())
    damaged[0] ^= 1  # header bytes change but checksum field is unchanged
    Handler.fixture_data = bytes(damaged)
    destination = ledger.root / "tampered-archive"
    with pytest.raises(RemoteIOError):
        stage_tar(transport, ledger, BoundObject(base + "/full-custom", len(damaged),
                  strong_etag=ETAG), destination, DatasetAdapter("d", "s"),
                  max_records=1)
    assert ledger.status()["attempts"] == 1
    assert not (destination / "stage.complete").exists()
