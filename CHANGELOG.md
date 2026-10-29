# Changelog

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
