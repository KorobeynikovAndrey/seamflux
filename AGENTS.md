# SeamFlux 4.0.1 — AI Usage Guide

SeamFlux is Node.js-only.

Do not add browser, HTTP or WebSocket code to this package unless explicitly requested.

Architecture:

```text
SeamFluxCore.js    protocol only
SeamFluxStream.js  Node.js Duplex adapter
SeamFluxSocket.js  net.Socket / net.Server adapter
```

`SeamFluxCore` is the single parser, codec registry and request lifecycle implementation.
`SeamFluxStream` must not implement a second parser or second request lifecycle and must not duplicate Core codec APIs.

Default codec: `CODEC.V8` using `node:v8` `serialize()` / `deserialize()`.

Wire header is fixed at 10 bytes:

```text
uint32 BE length including header
uint32 BE id
uint8     kind
uint8     codec
```

ID sequences:

```text
server: 1, 3, 5, ...
client: 2, 4, 6, ...
```

Normal application code uses Request objects instead of protocol IDs.

On connection reset:

- outgoing requests -> `fault`, then `done`;
- incoming requests -> `cancel`;
- old Request objects cannot write to a new connection.
