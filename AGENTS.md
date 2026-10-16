# SeamFlux 2.1.2 — AI Usage Guide

SeamFlux is a bidirectional framed protocol for Node.js built on top of `Duplex`.

The package exports:

```js
const {
    SeamFluxStream,
    SocketClient,
    SocketServer,
    SeamFluxHttp,
    KIND,
    CODEC,
    MAX_DELAY
} = require('seamflux');
```

## Core model

Regular application code does not work with the protocol `id`.

Client:

```js
const request = seam.request(data);

request.on('meta', data => {});
request.on('response', data => {});
request.on('fault', error => {});
request.on('done', () => {});

request.cancel();
```

Server:

```js
seam.on('request', (request, data) => {
    request.meta(meta);
    request.response(result);
    request.done();
});
```

The internal `id` associates the frames of one operation.

## Request

```js
const request = seam.request({ value: 123 });
```

Receive:

```js
seam.on('request', (request, data) => {
    console.log(data);
});
```

Reply:

```js
request.meta(data);
request.response(data);
request.done();
```

Fault:

```js
request.fault(error);
```

Cancel:

```js
request.cancel();
```

Incoming cancellation:

```js
request.on('cancel', () => {
    // stop associated work
});
```

A single request may receive multiple `response` events before `done`.

`request()`, `send()`, `response()` and `meta()` use `null` when called without a data argument.

## Connection loss

When the transport connection disappears:

- locally created requests emit `fault` and then `done`;
- remotely created requests emit `cancel`.

This behavior is implemented by `resetConnection()` and applies on both socket client and socket server sides.

## Message

```js
seam.send({ hello: 'world' });

seam.on('message', data => {
    console.log(data);
});
```

## Codecs

```js
CODEC.RAW       = 0
CODEC.JSON      = 1
CODEC.V8        = 2
CODEC.GZIP_RAW  = 32
CODEC.GZIP_JSON = 33
CODEC.GZIP_V8   = 34
```

```js
seam.setCodec(CODEC.GZIP_V8);
```

`RAW` keeps byte data unchanged and converts strings to UTF-8 `Buffer` values before framing.

## Decode errors

A codec decode failure belongs to one frame and does not mark the whole `Duplex` as failed:

```js
seam.on('decodeError', (error, frame) => {
    console.error(error, frame.kind, frame.codec);
});
```

The next valid frame can still be processed.

## Protocol errors

Invalid framing, such as a frame length smaller than the 10-byte header, emits:

```js
seam.on('protocolError', error => {});
```

For `SocketClient` and `SocketServer`, a protocol error destroys only the affected socket. The process is not terminated by malformed remote input.

## Low-level frame

```js
seam.on('frame', frame => {
    console.log(frame.id);
    console.log(frame.kind);
    console.log(frame.codec);
    console.log(frame.length);
});
```

`frame` contains:

```js
{
    id,
    kind,
    codec,
    length,
    head,
    stream,
    request,
    decode(callback)
}
```

`frame.head` is the original 10-byte header received from the transport. `frame.stream` is a `PassThrough` containing the encoded payload.

Forward without decoding:

```js
frame.stream.pipe(destination);
```

Decode:

```js
frame.decode((error, data) => {
    if (error)
        return console.error(error);

    console.log(data);
});
```

If the payload is not needed:

```js
frame.stream.resume();
```

## SeamFluxStream

`SeamFluxStream` extends `Duplex`.

```js
transport.pipe(seam, { end: false });
seam.pipe(transport, { end: false });
```

Server-side instance:

```js
const seam = new SeamFluxStream(true);
```

## SocketClient

```js
const seam = new SocketClient({ host: '127.0.0.1', port: 5000 });

seam.on('connect', () => {
    const request = seam.request({ value: 123 });
    request.on('response', data => console.log(data));
});

seam.on('socketError', error => {});
seam.on('socketClose', hadError => {});

seam.connect();
```

`SocketClient` uses Node.js `net`, including TCP and local IPC/Named Pipes.

## SocketServer

```js
const server = new SocketServer(seam => {
    seam.on('request', (request, data) => {
        request.response({ echo: data });
        request.done();
    });

    seam.on('socketError', error => {});
});

server.listen(5000);
```

`SocketServer` handles the accepted socket's `error` event internally and exposes it as `socketError`. On socket close it calls `resetConnection()`.

## SeamFluxHttp

`SeamFluxHttp` maps one HTTP request to one SeamFlux request.

```js
const http = SeamFluxHttp(dataServer);
app.post('/api/data', express.json(), http(req => req.body));
```

The HTTP response body is a stream of complete SeamFlux frames, not one decoded payload:

```text
10-byte SeamFlux header + encoded META payload
10-byte SeamFlux header + encoded RESPONSE payload
10-byte SeamFlux header + encoded RESPONSE payload
...
10-byte SeamFlux DONE header
```

The bridge does not decode `META`, `RESPONSE` or `ERROR`. It does not convert `ERROR` into HTTP 500. It writes the original `frame.head` to the HTTP body and then forwards `frame.stream`; the header is not reconstructed. The client determines `kind` and `codec` from each SeamFlux frame header.

Because more than one frame may be returned, the bridge does not use per-frame `Content-Length`, `X-Kind` or `X-Codec` HTTP headers.

The HTTP body can therefore represent:

```text
META
RESPONSE
RESPONSE
DONE
```

or:

```text
ERROR
DONE
```

If the total HTTP body size is unknown before streaming starts, only received-byte progress is available. A percentage requires an application-level known total, for example total rows.

If the HTTP client closes early, `SeamFluxHttp` calls `request.cancel()`.

## Kinds

```js
KIND.REQUEST  = 1
KIND.RESPONSE = 2
KIND.MESSAGE  = 3
KIND.DONE     = 4
KIND.ERROR    = 5
KIND.META     = 6
KIND.CANCEL   = 7
```

## Wire format

```text
offset  size  field
0       4     length   uint32 BE
4       4     id       uint32 BE
8       1     kind     uint8
9       1     codec    uint8
10      ...   payload
```

The header is 10 bytes. `length` includes the header.

The protocol `id` remains on the wire but is hidden from the regular request API.

Internal ID sequences:

```text
server: 1, 3, 5, ...
client: 2, 4, 6, ...
```
