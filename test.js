const assert = require('assert');
const http = require('http');
const net = require('net');
const { PassThrough } = require('stream');
const { SeamFluxStream, SocketServer, SeamFluxHttp, KIND, CODEC } = require('./');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function wireFrame(id, kind, codec, payload) {
    payload = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);

    const head = Buffer.allocUnsafe(10);
    head.writeUInt32BE(10 + payload.length, 0);
    head.writeUInt32BE(id, 4);
    head.writeUInt8(kind, 8);
    head.writeUInt8(codec, 9);

    return Buffer.concat([head, payload]);
}

function parseFrames(buffer) {
    const frames = [];
    let offset = 0;

    while (offset < buffer.length) {
        const length = buffer.readUInt32BE(offset);
        const id = buffer.readUInt32BE(offset + 4);
        const kind = buffer.readUInt8(offset + 8);
        const codec = buffer.readUInt8(offset + 9);

        frames.push({ id, kind, codec, payload: buffer.subarray(offset + 10, offset + length) });
        offset += length;
    }

    assert.strictEqual(offset, buffer.length);
    return frames;
}

async function testBasicApi() {
    const client = new SeamFluxStream(false);
    const server = new SeamFluxStream(true);

    client.pipe(server);
    server.pipe(client);

    let requestData;
    let responseData;
    let metaData;
    let done = false;
    let cancelled = false;
    let messageData;
    let faultMessage;
    let faultDone = false;

    server.on('request', (request, data) => {
        assert.strictEqual(request.id, undefined);
        assert.strictEqual(request._id, undefined);

        if (data.cancel) {
            request.on('cancel', () => cancelled = true);
            return;
        }

        if (data.fail) {
            request.fault(new Error('failed'));
            return;
        }

        requestData = data;
        request.meta({ columns: ['value'] });
        request.response({ ok: true });
        request.done();
    });

    server.on('message', data => messageData = data);

    const request = client.request({ value: 123 });

    assert.strictEqual(request.id, undefined);
    assert.strictEqual(request._id, undefined);

    request.on('meta', data => metaData = data);
    request.on('response', data => responseData = data);
    request.on('done', () => done = true);

    const cancelRequest = client.request({ cancel: true });
    cancelRequest.cancel();

    const failRequest = client.request({ fail: true });
    failRequest.on('fault', error => faultMessage = error.message);
    failRequest.on('done', () => faultDone = true);

    client.send({ hello: 'world' });

    await delay(30);

    assert.deepStrictEqual(requestData, { value: 123 });
    assert.deepStrictEqual(metaData, { columns: ['value'] });
    assert.deepStrictEqual(responseData, { ok: true });
    assert.strictEqual(done, true);
    assert.strictEqual(cancelled, true);
    assert.deepStrictEqual(messageData, { hello: 'world' });
    assert.strictEqual(faultMessage, 'failed');
    assert.strictEqual(faultDone, true);
}

async function testRequestWithoutArgument() {
    const client = new SeamFluxStream(false);
    const server = new SeamFluxStream(true);

    client.pipe(server);
    server.pipe(client);

    let received;

    server.on('request', (request, data) => {
        received = data;
        request.done();
    });

    client.request();
    await delay(20);

    assert.strictEqual(received, null);
}

async function testDecodeErrorDoesNotBreakPipe() {
    const source = new PassThrough();
    const server = new SeamFluxStream(true);

    source.pipe(server, { end: false });

    let decodeError;
    let received;

    server.on('decodeError', error => decodeError = error);
    server.on('request', (request, data) => {
        received = data;
        request.done();
    });

    source.write(wireFrame(2, KIND.REQUEST, CODEC.JSON, '{broken'));
    source.write(wireFrame(4, KIND.REQUEST, CODEC.JSON, JSON.stringify({ ok: true })));

    await delay(20);

    assert(decodeError instanceof Error);
    assert.deepStrictEqual(received, { ok: true });
    assert(source.isPaused() === false || source.readableFlowing !== false);
}

async function testRawUnicodeString() {
    const client = new SeamFluxStream(false);
    const server = new SeamFluxStream(true);

    client.setCodec(CODEC.RAW);
    client.pipe(server);
    server.pipe(client);

    const received = [];

    server.on('request', (request, data) => {
        received.push(data.toString());
        request.done();
    });

    client.request('héllo');
    client.request('next');

    await delay(20);

    assert.deepStrictEqual(received, ['héllo', 'next']);
}

async function testResetConnectionEvents() {
    const outgoing = new SeamFluxStream(false);
    const request = outgoing.request({ value: 1 });
    let fault;
    let done = false;

    request.on('fault', error => fault = error);
    request.on('done', () => done = true);

    outgoing.resetConnection();

    assert(fault instanceof Error);
    assert.strictEqual(fault.message, 'Connection closed');
    assert.strictEqual(done, true);

    const client = new SeamFluxStream(false);
    const server = new SeamFluxStream(true);

    client.pipe(server);
    server.pipe(client);

    let incomingCancelled = false;

    server.on('request', request => {
        request.on('cancel', () => incomingCancelled = true);
        server.resetConnection();
    });

    client.request({ long: true });
    await delay(20);

    assert.strictEqual(incomingCancelled, true);
}

async function testSocketServerSurvivesBadClients() {
    let connections = 0;
    let protocolErrors = 0;
    let socketErrors = 0;

    const server = new SocketServer(protocol => {
        connections++;
        protocol.on('protocolError', () => protocolErrors++);
        protocol.on('socketError', () => socketErrors++);
    });

    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;

    await new Promise((resolve, reject) => {
        const socket = net.connect({ host: '127.0.0.1', port }, () => {
            const invalid = Buffer.alloc(10);
            invalid.writeUInt32BE(9, 0);
            socket.write(invalid);
        });

        socket.once('close', resolve);
        socket.once('error', error => {
            if (error.code !== 'ECONNRESET')
                reject(error);
        });
    });

    await delay(10);
    assert.strictEqual(protocolErrors, 1);

    await new Promise((resolve, reject) => {
        const socket = net.connect({ host: '127.0.0.1', port }, () => {
            socket.resetAndDestroy();
        });

        socket.once('close', resolve);
        socket.once('error', error => {
            if (error.code !== 'ECONNRESET')
                reject(error);
        });
    });

    await delay(20);
    assert(connections >= 2);
    assert(socketErrors >= 0);

    await new Promise(resolve => server.close(resolve));
}


async function testFrameKeepsOriginalHead() {
    const source = new PassThrough();
    const seam = new SeamFluxStream(true);
    const wire = wireFrame(2, KIND.REQUEST, CODEC.JSON, JSON.stringify({ value: 1 }));
    let head;

    source.pipe(seam, { end: false });

    seam.on('frame', frame => {
        head = frame.head;
        frame.stream.resume();
    });

    source.write(wire);
    await delay(20);

    assert(Buffer.isBuffer(head));
    assert.deepStrictEqual(head, wire.subarray(0, 10));
}

async function testHttpMultipleResponses() {
    const connection = new SeamFluxStream(false);
    const dataServer = new SeamFluxStream(true);

    connection.socket = { readyState: 'open' };
    connection.pipe(dataServer);
    dataServer.pipe(connection);

    dataServer.on('request', (request, data) => {
        assert.deepStrictEqual(data, { value: 1 });
        request.meta(['value']);
        request.response([[1]]);
        request.response([[2]]);
        request.done();
    });

    const middleware = SeamFluxHttp(connection)(req => req.body);

    const web = http.createServer((req, res) => {
        res.status = code => {
            res.statusCode = code;
            return res;
        };

        res.set = headers => {
            for (const [name, value] of Object.entries(headers))
                res.setHeader(name, value);
            return res;
        };

        res.type = type => {
            res.setHeader('Content-Type', type === 'text' ? 'text/plain; charset=utf-8' : type);
            return res;
        };

        res.send = data => {
            res.end(data);
            return res;
        };

        const chunks = [];
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => {
            req.body = JSON.parse(Buffer.concat(chunks).toString());
            middleware(req, res);
        });
    });

    await new Promise(resolve => web.listen(0, '127.0.0.1', resolve));
    const port = web.address().port;

    const result = await new Promise((resolve, reject) => {
        const req = http.request({
            host: '127.0.0.1',
            port,
            method: 'POST',
            path: '/',
            headers: { 'Content-Type': 'application/json' }
        }, res => {
            const chunks = [];
            res.on('data', chunk => chunks.push(chunk));
            res.on('end', () => resolve({ res, body: Buffer.concat(chunks) }));
        });

        req.once('error', reject);
        req.end(JSON.stringify({ value: 1 }));
    });

    assert.strictEqual(result.res.statusCode, 200);
    assert.strictEqual(result.res.headers['content-length'], undefined);
    assert.strictEqual(result.res.headers['x-kind'], undefined);
    assert.strictEqual(result.res.headers['x-codec'], undefined);

    const frames = parseFrames(result.body);

    assert.deepStrictEqual(frames.map(frame => frame.kind), [KIND.META, KIND.RESPONSE, KIND.RESPONSE, KIND.DONE]);
    assert.deepStrictEqual(JSON.parse(frames[0].payload.toString()), ['value']);
    assert.deepStrictEqual(JSON.parse(frames[1].payload.toString()), [[1]]);
    assert.deepStrictEqual(JSON.parse(frames[2].payload.toString()), [[2]]);
    assert.strictEqual(frames[3].payload.length, 0);

    await new Promise(resolve => web.close(resolve));
}

(async () => {
    await testBasicApi();
    await testRequestWithoutArgument();
    await testDecodeErrorDoesNotBreakPipe();
    await testRawUnicodeString();
    await testResetConnectionEvents();
    await testSocketServerSurvivesBadClients();
    await testFrameKeepsOriginalHead();
    await testHttpMultipleResponses();

    console.log('SeamFlux 2.1.2: OK');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
