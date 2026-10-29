# SeamFlux 4.0.1

SeamFlux is a compact framed request/response protocol for Node.js.

The package contains three layers:

```text
SeamFluxCore    protocol, framing, codecs and request lifecycle
SeamFluxStream  Node.js Duplex wrapper around SeamFluxCore
SeamFluxSocket  TCP / Named Pipe client and server
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
