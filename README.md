# SeamFlux 4.0.5

SeamFlux is a compact framed request/response protocol for Node.js.

The transport and protocol layers are:

```text
SeamFluxCore    protocol, framing, codecs and request lifecycle
SeamFluxStream  Node.js Duplex wrapper around SeamFluxCore
SeamFluxSocket  TCP / Named Pipe client and server
SeamFluxEvents  event dispatcher over a SeamFlux instance
SFEventClient   ready-to-use event client over SocketClient
SFEventServer   socket server yielding event connections
```

## Install

```bash
npm install ./SeamFlux-4.0.5.tgz
```

## API

```js
const {
    SeamFluxCore,
    SeamFluxStream,
    SocketClient,
    SocketServer,
    SeamFluxEvents,
    SFEventClient,
    SFEventServer,
    KIND,
    CODEC
} = require('seamflux');
```

## Request / response

Client:

```js
const request = sf.request({ value: 123 });

request.on('meta', data => {});
request.on('response', data => {});
request.on('fault', error => {});
request.on('done', () => {});

request.cancel();
```

Server:

```js
sf.on('request', (request, data) => {
    request.meta({ rows: 1 });
    request.response({ value: data.value });
    request.done();
});
```

A request may receive multiple `response` events before `done`.

## Message

```js
sf.send({ hello: 'world' });
sf.on('message', data => {});
```

## Connection metadata

`SFEventClient` sends a connection-level `META` frame (`id = 0`) automatically before ordinary requests. Its `meta` option may contain arbitrary values:

```js
const client = new SFEventClient({
    host: '127.0.0.1',
    port: 5000,
    meta: { auth: { token: 'abc123' }, clientId: 42 }
});
client.connect();
```

`SFEventServer` calls its connection listener after the first `META` arrives, and exposes the current value as `conn.meta`:

```js
const server = new SFEventServer(conn => {
    console.log(conn.meta);
    conn.on('getStatus', data => ({ id: data.id, status: 'ok' }));
});
```

The client sends the latest `meta` again after reconnection. `client.updateMeta(data)` sends new connection metadata and uses it for future reconnections. The server replaces `conn.meta` when it receives a new connection-level `META` frame. When the client has no `meta` option it sends `null`, so connection setup still completes.

For direct Core usage, `core.sendConnectionMeta(data)` sends `META` with `id = 0` and the receiver emits `connectionMeta(data)`. Existing `request.meta(data)` retains its request ID and continues to emit `meta` on that request. `SeamFluxStream` is unchanged; socket wrappers access `sf.core` for connection metadata.

## Named events

`SFEventClient` and `SFEventServer` provide `on()`, `off()`, `once()` and remote `emit()` directly. They use the existing socket transport and request protocol.

Server (`example/events-server.js`):

```js
const { SFEventServer } = require('seamflux');

const server = new SFEventServer(conn => {
    conn.on('vehicle:update', data => console.log(data));
    conn.on('getStatus', async data => ({ id: data.id, status: 'ok' }));
});

server.listen(5000);
```

Client (`example/events-client.js`):

```js
const { SFEventClient } = require('seamflux');

const client = new SFEventClient({ host: '127.0.0.1', port: 5000 });
client.connect();

client.emit('vehicle:update', { id: 123, speed: 45 });
const status = await client.emit('getStatus', { id: 123 });
console.log(status);
client.close();
```

Each `emit(event, data)` sends a request and returns a Promise. Without `await`, it sends the event without using its result. With `await`, it resolves with the first response after `DONE`, or `undefined` when no handler returns a value. A connection fault rejects the Promise.

Handlers return values directly; `async` handlers are supported. If multiple handlers are registered, the first one to finish with a value other than `undefined` supplies the response; other results are ignored. Each handled request sends one `RESPONSE` followed by `DONE` (or only `DONE` when there is no response). A handler error is returned as a fault if no handler produces a response.

`SeamFluxEvents` can also wrap a standalone `SeamFluxCore`, `SeamFluxStream` or `SocketClient`:

```js
const events = new SeamFluxEvents(sf);
events.on('ping', () => 'pong');
```

The event layer uses an internal `EventEmitter` for listeners, without inheriting or overriding its `emit()` method. `SocketClient` and `SocketServer` remain available separately as transport classes. For client transport lifecycle events, use `client.sf.on('connect', ...)` or `client.sf.on('socketClose', ...)`.

## Codecs

```js
CODEC.RAW  = 0
CODEC.JSON = 1
CODEC.V8   = 2
```

V8 is the default codec:

```js
v8.serialize(data)
v8.deserialize(buffer)
```

Change codec:

```js
sf.setCodec(CODEC.JSON);
```

Register another codec:

```js
sf.useCodec(10, {
    encode(data) { return Buffer.from(...); },
    decode(buffer) { return ...; }
});
```

`RAW` accepts `Buffer` and strings. Strings are encoded as UTF-8.

## SeamFluxCore

`SeamFluxCore` works only with buffers and protocol state.

Incoming bytes:

```js
core.input(buffer);
```

Outgoing bytes:

```js
core.on('output', buffer => transport.write(buffer));
```

It does not depend on Node streams or sockets.

## SeamFluxStream

`SeamFluxStream` is a Node.js `Duplex` wrapper around one `SeamFluxCore`:

```js
const seam = new SeamFluxStream();

transport.pipe(seam, { end: false });
seam.pipe(transport, { end: false });
```

Parsing, codecs and request lifecycle remain in Core. Codec configuration is done through `seam.core`.

## SocketClient

```js
const client = new SocketClient({ host: '127.0.0.1', port: 5000 });

client.on('connect', () => {
    const request = client.request({ value: 123 });
    request.on('response', console.log);
});

client.connect();
```

`SocketClient` reconnects automatically with an exponential delay from 1 to 30 seconds. `close()` stops reconnecting.

`net.connect()` options are passed unchanged, so TCP and Named Pipes / Unix domain sockets are supported.

## SocketServer

```js
const server = new SocketServer(seam => {
    seam.on('request', (request, data) => {
        request.response(data);
        request.done();
    });
});

server.listen(5000);
```

## Connection reset

When a physical connection disappears, `resetConnection()` does the following:

- outgoing requests emit `fault`, then `done`;
- incoming requests emit `cancel`;
- parser state is cleared;
- queued `SeamFluxStream` output is cleared;
- old `Request` objects cannot send frames through a new connection.

## Errors

Payload decode failure:

```js
sf.on('decodeError', (error, frame) => {});
```

This does not invalidate framing. The next frame can still be processed.

Invalid framing:

```js
sf.on('protocolError', error => {});
```

Socket adapters destroy only the affected physical socket.

## Wire format

```text
offset  size  field
0       4     total frame length, uint32 BE
4       4     id, uint32 BE
8       1     kind, uint8
9       1     codec, uint8
10      ...   payload
```

Header size: 10 bytes.

IDs:

```text
server: 1, 3, 5, ...
client: 2, 4, 6, ...
```
