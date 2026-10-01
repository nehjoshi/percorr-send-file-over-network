from __future__ import annotations

import asyncio
import struct

from .messages import Blob, BlobReply, Message, RestartedQuery, RestartedReply

HEADER = struct.Struct("!BQ")  # message kind, body length in bytes
BLOB_REPLY_BODY = struct.Struct("!i")

RESTARTED_QUERY = 1
RESTARTED_REPLY = 2
BLOB = 3
BLOB_REPLY = 4

FIXED_BODY_LENGTHS = {
    RESTARTED_QUERY: 0,
    RESTARTED_REPLY: 1,
    BLOB_REPLY: BLOB_REPLY_BODY.size,
}


class ProtocolError(Exception):
    pass


def encode(message: Message) -> tuple[bytes, bytes]:
    match message:
        case RestartedQuery():
            kind, body = RESTARTED_QUERY, b""
        case RestartedReply(restarted=restarted):
            kind, body = RESTARTED_REPLY, bytes([int(restarted)])
        case Blob(payload=payload):
            kind, body = BLOB, payload
        case BlobReply(result=result):
            kind, body = BLOB_REPLY, BLOB_REPLY_BODY.pack(result)
        case _:
            raise TypeError(f"cannot encode {message!r}")
    # Header and body stay separate so a large payload is never copied to prepend it.
    return HEADER.pack(kind, len(body)), body


def decode(kind: int, body: bytes) -> Message:
    if kind == RESTARTED_QUERY:
        return RestartedQuery()
    if kind == RESTARTED_REPLY:
        if body not in (b"\x00", b"\x01"):
            raise ProtocolError(f"invalid restarted flag {body!r}")
        return RestartedReply(restarted=body == b"\x01")
    if kind == BLOB:
        return Blob(payload=body)
    if kind == BLOB_REPLY:
        (result,) = BLOB_REPLY_BODY.unpack(body)
        return BlobReply(result=result)
    raise ProtocolError(f"unknown message kind {kind}")


def check_length(kind: int, length: int, max_blob_bytes: int) -> None:
    if kind == BLOB:
        if length > max_blob_bytes:
            raise ProtocolError(f"Blob of {length} bytes exceeds {max_blob_bytes}")
        return
    expected = FIXED_BODY_LENGTHS.get(kind)
    if expected is None:
        raise ProtocolError(f"unknown message kind {kind}")
    if length != expected:
        raise ProtocolError(f"kind {kind} has length {length}, expected {expected}")


async def read_message(reader: asyncio.StreamReader, max_blob_bytes: int) -> Message:
    kind, length = HEADER.unpack(await reader.readexactly(HEADER.size))
    check_length(kind, length, max_blob_bytes)
    return decode(kind, await reader.readexactly(length))


def write_message(writer: asyncio.StreamWriter, message: Message) -> None:
    header, body = encode(message)
    writer.write(header)
    if body:
        writer.write(body)
