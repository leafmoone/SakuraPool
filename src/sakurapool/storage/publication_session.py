"""Serial publication reader; no concurrent SQLite access or scheduler."""

from threading import get_ident

from .publication import PublicationCorrupt, load_publication
from .publication_fetch import fetch_publication_sample


class PublicationSession:
    def __init__(self, root, transport, *, control=None, scope=None):
        self._owner = get_ident()
        self.transport = transport
        self.ledger = transport.ledger
        self.control, self.scope = control, scope
        self.publication = load_publication(root, full_verify=True)
        self._publication = self.publication
        self._transport, self._ledger = transport, transport.ledger
        self._identity = (self.publication.content_digest, self.publication.runtime.snapshot_id)
        self._closed = False

    def _check(self):
        if (
            self._closed
            or get_ident() != self._owner
            or self.transport is not self._transport
            or self.ledger is not self._ledger
            or self.publication is not self._publication
            or self.transport.ledger is not self._ledger
            or self._identity
            != (self.publication.content_digest, self.publication.runtime.snapshot_id)
        ):
            raise PublicationCorrupt("serial session lifecycle/ledger mismatch")

    def fetch(self, record_id, output, *, metadata=False):
        self._check()
        return fetch_publication_sample(
            self.publication,
            record_id,
            self.transport,
            output,
            metadata=metadata,
            control=self.control,
            scope=self.scope,
        )

    def close(self):
        if self._closed:
            return
        if get_ident() != self._owner:
            raise PublicationCorrupt("serial session owner thread required")
        self._publication.close()
        self._closed = True

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *args):
        self.close()
