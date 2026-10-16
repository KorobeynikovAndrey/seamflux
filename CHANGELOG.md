# Changelog

## 2.1.2

- Low-level incoming frames now expose the original 10-byte `head` buffer.
- `SeamFluxHttp` forwards `frame.head` directly instead of rebuilding the SeamFlux header from parsed fields.

## 2.1.1

- Added `LICENSE` and npm license metadata.

## 2.1.0

- Added `decodeError` for per-frame codec decode failures. Decode errors no longer emit the stream-level `error` event or break an otherwise valid connection.
- Added controlled protocol failure handling for invalid framing. `SocketClient` and `SocketServer` destroy only the affected socket instead of allowing malformed remote input to escape as an uncaught exception.
- `SocketServer` now handles errors from accepted sockets, emits `socketError`, and calls `resetConnection()` when a socket closes.
- `request()`, `send()`, `response()` and `meta()` now use `null` when called without a data argument, so codecs such as JSON do not receive an empty payload for a normal no-argument call.
- `resetConnection()` now finishes locally created requests with `fault` followed by `done`, and emits `cancel` for remotely created requests.
- `SeamFluxHttp` now forwards complete SeamFlux frames in the HTTP response body. This supports multiple `RESPONSE` frames before `DONE` and forwards `META`, `ERROR` and `DONE` without converting their SeamFlux semantics into HTTP headers or status codes.
- `CODEC.RAW` and `CODEC.GZIP_RAW` now convert strings to UTF-8 `Buffer` values before frame length is calculated, preventing byte-length/frame-length mismatches for non-ASCII strings.
