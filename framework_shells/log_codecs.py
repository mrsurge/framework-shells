"""Per-stream observation codecs, independent of pipe and dashboard transports."""
from typing import cast

from .log_projection import Codec


def log_codecs(value: object, *, templates: bool = False) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("log_codecs must be a mapping")
    result: dict[str, str] = {}
    for stream, codec in cast(dict[object, object], value).items():
        if stream not in ("stdout", "stderr"):
            raise ValueError("log_codecs keys must be stdout or stderr")
        if not isinstance(codec, str) or (
            codec not in ("text", "json", "messagepack")
            and not (templates and "${" in codec)
        ):
            raise ValueError("log codec must be text, json, or messagepack")
        result[stream] = codec
    return result


def stream_codec(codecs: dict[str, str], stream: str) -> Codec:
    if stream not in ("stdout", "stderr"):
        raise ValueError("stream must be stdout or stderr")
    return cast(Codec, log_codecs(codecs).get(stream, "text"))
