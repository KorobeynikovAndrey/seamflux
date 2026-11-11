# SeamFlux 4.0.3

SeamFlux is a compact framed request/response protocol for Node.js.

The transport and protocol layers are:

```text
SeamFluxCore    protocol, framing, codecs and request lifecycle
SeamFluxStream  Node.js Duplex wrapper around SeamFluxCore
SeamFluxSocket  TCP / Named Pipe client and server
SeamFluxEvents  optional named-event wrapper for any of the above
```

## Install

```bash
npm install seamflux
```

## API

```js
const {
    SeamFluxCore,
    SeamFluxStream,
    SocketClient,
    SocketServer,
    SeamFluxEvents,
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

## Named events

`SeamFluxEvents` extends Node.js `EventEmitter`. It wraps an existing `SeamFluxCore`, `SeamFluxStream` or `SocketClient` (or a server-side accepted stream), without changing the transport.

Send an event and handle it on the other side:

```js
const events = new SeamFluxEvents(sf);
const handler = data => console.log(data);

events.on('vehicle:update', handler);
events.emit('vehicle:update', { id: 123, speed: 45 });
events.off('vehicle:update', handler);
```

`emit(event, data)` uses `sf.send({ event, data })`. `on`, `off`, `once` and other listener methods are inherited from `EventEmitter`. `emit` is overridden to send a **remote** event.

To receive a single response, provide a callback to `emit`:

```js
// Client
const events = new SeamFluxEvents(sf);
events.emit('getStatus', { id: 123 }, status => {
    console.log(status);
});
```

```js
// Server
const events = new SeamFluxEvents(sf);
events.on('getStatus', (data, reply) => {
    reply({ id: data.id, status: 'ok' });
});
```

With a callback, `emit()` uses `sf.request({ event, data })`; `reply(value)` calls `request.response(value)` followed by `request.done()`. The reply function responds at most once. If there is no matching listener, the request ends without a response. If a listener does not invoke `reply`, no response is sent; the underlying request is completed on disconnect. The callback receives the reply value (not an error-first argument).

For a working TCP client/server example, see `example/events-server.js` and `example/events-client.js`.

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
