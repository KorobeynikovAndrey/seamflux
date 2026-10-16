# SeamFlux 2.1.0

Compact framed multiplexed stream protocol for Node.js.

## Install

```text
npm install seamflux
```

## Request API

Protocol IDs stay inside SeamFlux. Application code works with a request object.

```js
const request = seam.request({ value: 123 });

request.on('meta', data => {});
request.on('response', data => {});
request.on('fault', error => {});
request.on('done', () => {});

request.cancel();
```

Server:

```js
seam.on('request', (request, data) => {
    request.meta({ columns: ['value'] });
    request.response({ ok: true });
    request.done();
});
```

`request()`, `send()`, `response()` and `meta()` use `null` when called without a data argument.

A request may receive multiple `response` events before `done`.

## Disconnect behavior

If a connection is lost, locally created requests receive:

```text
fault
DONE
```

Incoming requests receive `cancel`, so long-running work can stop when the remote side disappears.

## Message API

```js
seam.send({ hello: 'world' });

seam.on('message', data => {
    console.log(data);
});
```

## Codecs

```text
0   RAW
1   JSON
2   V8
32  GZIP_RAW
33  GZIP_JSON
34  GZIP_V8
```

```js
seam.setCodec(CODEC.GZIP_V8);
```

`RAW` accepts byte data directly and converts strings to UTF-8 `Buffer` values before the frame length is calculated.

## Decode and protocol errors

A payload decode failure does not break the transport:

```js
seam.on('decodeError', (error, frame) => {
    // one frame could not be decoded
});
```

A framing/protocol failure is fatal for the current connection:

```js
seam.on('protocolError', error => {
    // SocketClient/SocketServer close the affected socket automatically
});
```

## Low-level frame API

```js
seam.on('frame', frame => {
    console.log(frame.id, frame.kind, frame.codec, frame.length);
});
```

`frame.head` is the original 10-byte header received from the transport. `frame.stream` contains the encoded payload. `frame.decode(callback)` decodes it.

## Socket client/server

```js
const seam = new SocketClient({ host: '127.0.0.1', port: 5000 });

seam.on('connect', () => {
    const request = seam.request({ value: 123 });
    request.on('response', data => console.log(data));
});

seam.connect();
```

```js
new SocketServer(seam => {
    seam.on('request', (request, data) => {
        request.response({ echo: data });
        request.done();
    });
}).listen(5000);
```

`SocketClient` and `SocketServer` use Node.js `net`, so they work with TCP and local IPC/Named Pipes.

Accepted server sockets have their `error` event handled by `SocketServer`. A socket error is exposed as `socketError`, and socket close resets the corresponding SeamFlux connection state.

## SeamFluxHttp

`SeamFluxHttp` maps one HTTP request to one SeamFlux request, but the HTTP response body is a stream of complete SeamFlux frames.

```text
HTTP request data -> SeamFlux REQUEST

HTTP body:
    META
    RESPONSE
    RESPONSE
    ...
    ERROR or DONE
```

Express:

```js
const http = SeamFluxHttp(dataServer);

app.post('/api/data', express.json(), http(req => req.body));
```

The bridge does not decode `META`, `RESPONSE` or `ERROR` and does not map SeamFlux errors to HTTP status codes. It forwards the original `frame.head` followed by the encoded payload without rebuilding the SeamFlux header.

Because several SeamFlux frames may be returned, the HTTP response does not use a per-frame `Content-Length`, `X-Kind` or `X-Codec`. The client reads the HTTP body as a stream and parses normal SeamFlux framing.

If the total response size is not known before streaming starts, byte progress can show bytes received but not a true percentage. A logical percentage can be calculated when the application supplies a known total, for example `rows / totalRows`.

If the HTTP client closes early, `SeamFluxHttp` cancels the associated request.

If the upstream data-server connection is lost before HTTP headers are sent, the bridge returns HTTP 502. If streaming already started, the HTTP response is destroyed because the SeamFlux frame stream is incomplete.

## Worker data server

The main thread may accept a socket and transfer it to a worker. The worker owns `SeamFluxStream` and application processing.

```js
const net = require('net');
const { Worker } = require('worker_threads');

const worker = new Worker('./data-worker.js');

net.createServer(socket => {
    worker.postMessage({ socket }, [socket]);
}).listen(5001);
```

Worker:

```js
const { parentPort } = require('worker_threads');
const { SeamFluxStream } = require('seamflux');

parentPort.on('message', ({ socket }) => {
    const seam = new SeamFluxStream(true);

    socket.pipe(seam, { end: false });
    seam.pipe(socket, { end: false });

    seam.on('protocolError', () => socket.destroy());
    socket.on('error', error => seam.emit('socketError', error));

    socket.once('close', () => {
        socket.unpipe(seam);
        seam.unpipe(socket);
        seam.resetConnection();
    });

    seam.on('request', (request, data) => {
        request.response({ ok: true });
        request.done();
    });
});
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

The 10-byte header is included in `length`.
