# Changelog

## 4.0.6

- `SFEventClient(options, meta)` now takes connection metadata as a separate second argument instead of `options.meta`.
- Updated documentation, examples, and tests.

## 4.0.5

- Added connection-level `META` frames with `id = 0` and `connectionMeta` event in Core.
- `SFEventClient` sends initial metadata before requests and again after reconnect; `updateMeta()` sends updates.
- `SFEventServer` receives and updates `conn.meta`, invoking the connection listener after initial metadata.
- Request-scoped `request.meta()` remains unchanged.

## 4.0.4

- Added `SFEventClient` and `SFEventServer` with direct `emit()` / `on()` / `off()` / `once()`.
- Reworked `SeamFluxEvents` to use internal `EventEmitter`, return handler values (including async), and resolve `await emit()` from the first response at `DONE`.
- Updated tests, documentation and TCP examples.

## 4.0.3

- `SeamFluxEvents` now inherits from Node.js `EventEmitter`.
- Added event acknowledgements using existing `request()` / `response()` / `done()`.
- Added complete named-events TCP client/server examples and documentation.

## 4.0.2

- Added optional `SeamFluxEvents` wrapper for named `emit()`, `on()` and `off()` over `send()` / `message`.

## 4.0.1

- Removed browser-oriented byte normalization from `SeamFluxCore`; Core now works directly with Node.js `Buffer`.
- Removed codec forwarding from `SeamFluxStream`; codec configuration belongs to `SeamFluxCore`.
- Removed the redundant `SeamFluxStream.input()` forwarding method.
- Kept `SeamFluxStream` focused on Node.js `Duplex` integration.

## 4.0.0

- Rebuilt SeamFlux as a Node.js-only package.
- Added standalone `SeamFluxCore` for framing, codecs and request lifecycle.
- Made V8 serialization the default codec.
- Kept RAW and JSON codecs.
- Added Node.js `Duplex` adapter `SeamFluxStream` with normal `.pipe()` support and backpressure.
- Added `SocketClient` and `SocketServer` for TCP and local IPC sockets.
- Kept reconnect and connection-reset lifecycle.
- Removed HTTP, browser, WebSocket and platform abstraction code from the package.
