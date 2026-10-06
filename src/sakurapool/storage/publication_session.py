"""Serial lightweight publication session bound to one transport and capacity."""

from threading import get_ident

from ..capacity import CapacityConfig
from .publication import PublicationCorrupt, load_publication
from .publication_fetch import fetch_publication_sample


class PublicationSession:
    def __init__(
        self, root, transport, *, control=None, scope=None, capacity=None, image_extensions=None,
        filename_template="{tag}_{index}", filename_prefix=None
    ):
        from ..download_naming import FilenameConfig
        from ..image_formats import image_extensions as validate_extensions

        self.filename_config = FilenameConfig.resolve(
            template=filename_template, prefix=filename_prefix
        )
        self.image_extensions = validate_extensions(image_extensions)
        self._owner = get_ident()
        self.transport = transport
        if getattr(transport, "ledger", None) is not None:
            raise ValueError("download consumption ledger unsupported")
        self.capacity = (
            capacity if capacity is not None else getattr(transport, "capacity", CapacityConfig())
        )
        if not isinstance(self.capacity, CapacityConfig):
            raise ValueError("typed capacity required")
        if getattr(transport, "capacity", self.capacity) != self.capacity:
            raise ValueError("session/transport capacity mismatch")
        if control is not None and getattr(control, "capacity", self.capacity) != self.capacity:
            raise ValueError("session/control capacity mismatch")
        self.control, self.scope = control, scope
        self.publication = load_publication(root, full_verify=True)
        self._publication = self.publication
        self._transport = transport
        self._identity = (self.publication.content_digest, self.publication.runtime.snapshot_id)
        self._closed = False

    def _check(self):
        if (
            self._closed
            or get_ident() != self._owner
            or self.transport is not self._transport
            or self.publication is not self._publication
            or getattr(self.transport, "ledger", None) is not None
            or self._identity
            != (self.publication.content_digest, self.publication.runtime.snapshot_id)
        ):
            raise PublicationCorrupt("serial session lifecycle/transport mismatch")

    def fetch(self, record_id, output, *, filename_index, metadata=False, attempt_hook=None):
        self._check()
        return fetch_publication_sample(
            self.publication,
            record_id,
            self.transport,
            output,
            metadata=metadata,
            control=self.control,
            scope=self.scope,
            attempt_hook=attempt_hook,
            capacity=self.capacity,
            image_extensions=self.image_extensions,
            filename_template=self.filename_config.template,
            filename_prefix=self.filename_config.prefix,
            filename_index=filename_index,
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

    def __exit__(self, exc_type, primary, traceback):
        try:
            self.close()
        except BaseException:
            if primary is None:
                raise
            primary.task_secondary = (
                *getattr(primary, "task_secondary", ()),
                "PUBLICATION_SESSION_CLOSE_FAILED",
            )
