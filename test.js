const assert = require('node:assert/strict');
const v8 = require('node:v8');
const { once } = require('node:events');
const {
    SeamFluxCore,
    SeamFluxStream,
    SocketClient,
    SocketServer,
    KIND,
    CODEC
} = require('./');

function connectCore(a, b) {
    a.on('output', data => b.input(data));
    b.on('output', data => a.input(data));
}

function makeFrame(id, kind, codec, payload = Buffer.alloc(0)) {
    const head = Buffer.alloc(10);
    head.writeUInt32BE(10 + payload.length, 0);
    head.writeUInt32BE(id, 4);
    head.writeUInt8(kind, 8);
    head.writeUInt8(codec, 9);
    return Buffer.concat([head, payload]);
}

async function testCore() {
    const client = new SeamFluxCore(false);
    const server = new SeamFluxCore(true);
    connectCore(client, server);

    assert.equal(client.codec, CODEC.V8);
    assert.equal(server.codec, CODEC.V8);

    const clientIds = [];
    const serverIds = [];
    server.on('frame', frame => {
        if (frame.kind === KIND.REQUEST)
            clientIds.push(frame.id);
    });
    client.on('frame', frame => {
        if (frame.kind === KIND.REQUEST)
            serverIds.push(frame.id);
    });

    server.on('request', (request, data) => {
        assert.deepEqual(data, { value: 123 });
        request.meta({ columns: 2 });
        request.response([1, 2]);
        request.response([3, 4]);
        request.done();
    });

    const request = client.request({ value: 123 });
    const events = [];

    request.on('meta', data => events.push(['meta', data]));
    request.on('response', data => events.push(['response', data]));
    await once(request, 'done');

    assert.deepEqual(events, [
        ['meta', { columns: 2 }],
        ['response', [1, 2]],
        ['response', [3, 4]]
    ]);
    assert.deepEqual(clientIds, [2]);

    const serverRequestPromise = new Promise(resolve => {
        client.once('request', request => {
            request.response('server-request');
            request.done();
        });

        const serverRequest = server.request('ping');
        serverRequest.once('response', data => resolve(data));
    });

    assert.equal(await serverRequestPromise, 'server-request');
    assert.deepEqual(serverIds, [1]);

    const messagePromise = once(server, 'message');
    client.send({ hello: 'world' });
    assert.deepEqual((await messagePromise)[0], { hello: 'world' });
}

async function testCodecs() {
    const sender = new SeamFluxCore(false);
    const receiver = new SeamFluxCore(true);
    connectCore(sender, receiver);

    sender.setCodec(CODEC.RAW);
    const rawPromise = once(receiver, 'message');
    sender.send('Привет');
    assert.equal((await rawPromise)[0].toString(), 'Привет');

    sender.setCodec(CODEC.JSON);
    const jsonPromise = once(receiver, 'message');
    sender.send({ json: true });
    assert.deepEqual((await jsonPromise)[0], { json: true });

    sender.setCodec(CODEC.V8);
    const v8Promise = once(receiver, 'message');
    sender.send({ bigint: 123n, date: new Date(0) });
    const data = (await v8Promise)[0];
    assert.equal(data.bigint, 123n);
    assert.equal(data.date.getTime(), 0);
}

async function testDecodeError() {
    const receiver = new SeamFluxCore(true);
    const errors = [];
    const messages = [];

    receiver.on('decodeError', error => errors.push(error));
    receiver.on('message', data => messages.push(data));

    receiver.input(makeFrame(2, KIND.MESSAGE, CODEC.JSON, Buffer.from('{')));
    receiver.input(makeFrame(4, KIND.MESSAGE, CODEC.V8, v8.serialize({ ok: true })));

    assert.equal(errors.length, 1);
    assert.deepEqual(messages, [{ ok: true }]);
}

async function testProtocolError() {
    const core = new SeamFluxCore();
    const errorPromise = once(core, 'protocolError');
    const head = Buffer.alloc(10);
    head.writeUInt32BE(9, 0);
    core.input(head);
    assert.match((await errorPromise)[0].message, /Invalid frame length/);
}

async function testReset() {
    const core = new SeamFluxCore(false);
    let pendingOutput = 0;
    core.on('output', () => pendingOutput++);
    const outgoing = core.request({ value: 1 });
    const events = [];

    outgoing.on('fault', () => events.push('fault'));
    outgoing.on('done', () => events.push('done'));
    core.resetConnection(new Error('closed'));
    await Promise.resolve();
    assert.deepEqual(events, ['fault', 'done']);
    assert.equal(pendingOutput, 0);

    const receiver = new SeamFluxCore(true);
    let incoming;
    receiver.on('request', request => incoming = request);
    receiver.input(makeFrame(2, KIND.REQUEST, CODEC.V8, v8.serialize({ value: 2 })));

    let cancelled = false;
    incoming.on('cancel', () => cancelled = true);
    receiver.resetConnection();
    assert.equal(cancelled, true);

    let output = 0;
    receiver.on('output', () => output++);
    incoming.response('stale').done();
    assert.equal(output, 0);
}

async function testStream() {
    const client = new SeamFluxStream(false);
    const server = new SeamFluxStream(true);

    assert.equal(typeof client.pipe, 'function');
    assert.equal(client.setCodec, undefined);
    assert.equal(client.useCodec, undefined);
    assert.equal(client.input, undefined);
    client.pipe(server, { end: false });
    server.pipe(client, { end: false });

    server.on('request', (request, data) => {
        request.response({ echo: data });
        request.done();
    });

    const request = client.request('stream');
    const responsePromise = once(request, 'response');
    const donePromise = once(request, 'done');

    assert.deepEqual((await responsePromise)[0], { echo: 'stream' });
    await donePromise;

    client.unpipe(server);
    server.unpipe(client);
    client.destroy();
    server.destroy();
}

async function testSocket() {
    let connections = 0;
    const server = new SocketServer(seam => {
        connections++;

        if (connections === 1) {
            setTimeout(() => seam.socket.destroy(), 20);
            return;
        }

        seam.on('request', (request, data) => {
            request.response({ echo: data });
            request.done();
        });
    });

    server.listen(0, '127.0.0.1');
    await once(server, 'listening');

    const { port } = server.address();
    const client = new SocketClient({ host: '127.0.0.1', port });
    let connects = 0;

    const resultPromise = new Promise((resolve, reject) => {
        client.on('socketError', reject);
        client.on('connect', () => {
            connects++;
            if (connects !== 2)
                return;

            const request = client.request('socket');
            request.on('response', resolve);
            request.on('fault', reject);
        });
    });

    client.connect();
    assert.deepEqual(await resultPromise, { echo: 'socket' });
    assert.equal(connects, 2);

    client.close();
    await new Promise(resolve => server.close(resolve));
}

(async () => {
    await testCore();
    await testCodecs();
    await testDecodeError();
    await testProtocolError();
    await testReset();
    await testStream();
    await testSocket();
    console.log('SeamFlux 4.0.1 tests: OK');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
