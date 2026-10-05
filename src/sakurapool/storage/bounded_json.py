"""Non-materializing, strict UTF-8 JSON-object validation.

Duplicate keys and escaped unpaired surrogates are tolerated like json.loads.
BOM/NaN/Infinity are rejected. Keys/values/numbers/decoded strings are never built.
Nodes count keys and values, including containers; string bytes count raw contents
including escapes. Counters are extent-bounded, with a fixed node ceiling. Depth
is a fixed technical limit, not a new capacity configuration field.

Inflight workspace: one readinto buffer, one byte per stack slot, and a fixed
control allowance for the reader, unbuffered file, scalar counters and frames.
This is validator workspace, not total interpreter RSS. No decoder buffer,
growing token, object graph or recursion is used.
"""

from pathlib import Path

READ_BYTES = 4096
MAX_DEPTH = 128
MAX_NODES = 1_000_000
CONTROL_BYTES = 4096
VALIDATION_INFLIGHT_BYTES = READ_BYTES + MAX_DEPTH + CONTROL_BYTES


class BoundedJSONError(ValueError):
    """Invalid syntax, extent, or structural limit; never includes input."""


class _Reader:
    def __init__(self, stream, extent):
        self.stream = stream
        self.extent = extent
        self.buffer = bytearray(READ_BYTES)
        self.pos = self.end = self.total = 0

    def peek(self):
        if self.pos == self.end:
            if self.total == self.extent:
                return -1
            self.end = self.stream.readinto(
                memoryview(self.buffer)[:min(READ_BYTES, self.extent - self.total)])
            if not self.end:
                raise BoundedJSONError("JSON truncated extent")
            self.total += self.end
            self.pos = 0
        return self.buffer[self.pos]

    def take(self):
        value = self.peek()
        if value < 0:
            raise BoundedJSONError("JSON truncated token")
        self.pos += 1
        return value

    def whitespace(self):
        while self.peek() in (32, 9, 10, 13):
            self.take()


def _string(reader):
    count = 0
    while True:
        value = reader.take()
        if value == 34:
            return count
        count += 1
        if value < 32:
            raise BoundedJSONError("JSON string control character")
        if value == 92:
            value = reader.take()
            count += 1
            if value == 117:
                for _ in range(4):
                    if reader.take() not in b"0123456789abcdefABCDEF":
                        raise BoundedJSONError("JSON unicode escape")
                count += 4
            elif value not in b'"\\/bfnrt':
                raise BoundedJSONError("JSON string escape")
        elif value >= 128:
            # Strict UTF-8: reject overlongs, surrogate encodings and > U+10FFFF.
            if 194 <= value <= 223:
                remaining, low, high = 1, 128, 191
            elif 224 <= value <= 239:
                remaining = 2
                low = 160 if value == 224 else 128
                high = 159 if value == 237 else 191
            elif 240 <= value <= 244:
                remaining = 3
                low = 144 if value == 240 else 128
                high = 143 if value == 244 else 191
            else:
                raise BoundedJSONError("JSON UTF-8 lead byte")
            if not low <= reader.take() <= high:
                raise BoundedJSONError("JSON UTF-8 continuation")
            for _ in range(remaining - 1):
                if not 128 <= reader.take() <= 191:
                    raise BoundedJSONError("JSON UTF-8 continuation")
            count += remaining


def _number(reader):
    if reader.peek() == 45:
        reader.take()
    value = reader.take()
    if value == 48:
        pass
    elif 49 <= value <= 57:
        while 48 <= reader.peek() <= 57:
            reader.take()
    else:
        raise BoundedJSONError("JSON number integer")
    if reader.peek() == 46:
        reader.take()
        if not 48 <= reader.take() <= 57:
            raise BoundedJSONError("JSON number fraction")
        while 48 <= reader.peek() <= 57:
            reader.take()
    if reader.peek() in (69, 101):
        reader.take()
        if reader.peek() in (43, 45):
            reader.take()
        if not 48 <= reader.take() <= 57:
            raise BoundedJSONError("JSON number exponent")
        while 48 <= reader.peek() <= 57:
            reader.take()


def validate_file(path, extent, *, max_bytes):
    """Validate exactly extent bytes as a JSON object; return None, never payload.

    max_bytes is metadata_max_bytes. Empty flagged metadata is invalid JSON;
    an absent metadata flag must skip this function.
    """
    if (type(extent) is not int or type(max_bytes) is not int
            or not 0 <= extent <= max_bytes):
        raise BoundedJSONError("JSON extent capacity")
    with Path(path).open("rb", buffering=0) as stream:
        reader = _Reader(stream, extent)
        stack = bytearray(MAX_DEPTH)
        try:
            _validate(reader, stack)
            if stream.readinto(memoryview(reader.buffer)[:1]):
                raise BoundedJSONError("JSON excess file extent")
        finally:
            # Tracebacks may escape; release large scratch storage explicitly.
            reader.buffer.clear()
            stack.clear()


def _validate(reader, stack):
    # 0 object first key/end, 1 key, 2 colon, 3 value,
    # 4 object comma/end, 5 array first value/end, 6 value, 7 array comma/end.
    reader.whitespace()
    if reader.take() != 123:
        raise BoundedJSONError("JSON root must be object")
    depth, nodes, string_bytes = 1, 1, 0
    while depth:
        reader.whitespace()
        state, value = stack[depth - 1], reader.peek()
        if state in (0, 1):
            if state == 0 and value == 125:
                reader.take()
                depth -= 1
                continue
            if reader.take() != 34:
                raise BoundedJSONError("JSON object key")
            string_bytes += _string(reader)
            nodes += 1
            stack[depth - 1] = 2
        elif state == 2:
            if reader.take() != 58:
                raise BoundedJSONError("JSON object colon")
            stack[depth - 1] = 3
        elif state in (4, 7):
            if value == (125 if state == 4 else 93):
                reader.take()
                depth -= 1
            elif reader.take() == 44:
                stack[depth - 1] = 1 if state == 4 else 6
            else:
                raise BoundedJSONError("JSON container separator")
        else:
            if state == 5 and value == 93:
                reader.take()
                depth -= 1
                continue
            stack[depth - 1] = 4 if state == 3 else 7
            nodes += 1
            if value in (123, 91):
                reader.take()
                if depth == MAX_DEPTH:
                    raise BoundedJSONError("JSON depth limit")
                stack[depth] = 0 if value == 123 else 5
                depth += 1
            elif value == 34:
                reader.take()
                string_bytes += _string(reader)
            elif value == 45 or 48 <= value <= 57:
                _number(reader)
            elif value in (116, 102, 110):
                token = b"true" if value == 116 else b"false" if value == 102 else b"null"
                for expected in token:
                    if reader.take() != expected:
                        raise BoundedJSONError("JSON literal")
            else:
                raise BoundedJSONError("JSON value")
        if nodes > min(MAX_NODES, reader.extent) or string_bytes > reader.extent:
            raise BoundedJSONError("JSON node/string byte limit")
    reader.whitespace()
    if reader.peek() != -1:
        raise BoundedJSONError("JSON trailing content")
