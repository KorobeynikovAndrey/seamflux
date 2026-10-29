const { EventEmitter } = require('node:events');
const v8 = require('node:v8');

const HEADER_SIZE = 10;

const KIND = {
    REQUEST: 1,
    RESPONSE: 2,
    MESSAGE: 3,
    DONE: 4,
    ERROR: 5,
    META: 6,
    CANCEL: 7
};

const CODEC = {
    RAW: 0,
    JSON: 1,
    V8: 2
};

function encodeRaw(data) {
    if (data === null)
        return Buffer.alloc(0);

    if (typeof data === 'string')
        return Buffer.from(data);

    return data;
}

const codecs = new Map([
    [CODEC.RAW, {
        encode: encodeRaw,
        decode: data => data
    }],
    [CODEC.JSON, {
        encode(data) {
            return Buffer.from(JSON.stringify(data));
        },
        decode(data) {
            return JSON.parse(data.toString());
        }
    }],
    [CODEC.V8, {
        encode: v8.serialize,
        decode: v8.deserialize
    }]
]);

class Request extends EventEmitter {
    #core;
    #id;
    #generation;

    constructor(core, id, generation) {
        super();
        this.#core = core;
        this.#id = id;
        this.#generation = generation;
    }

    response(data = null) {
        this.#core._sendRequest(this.#generation, this.#id, KIND.RESPONSE, data);
        return this;
    }

    meta(data = null) {
        this.#core._sendRequest(this.#generation, this.#id, KIND.META, data);
        return this;
    }

    done() {
        this.#core._sendRequest(this.#generation, this.#id, KIND.DONE, undefined, true);
        return this;
    }

    cancel() {
        this.#core._sendRequest(this.#generation, this.#id, KIND.CANCEL, undefined, true);
        return this;
    }

    fault(error) {
        if (!this.#core._sendRequest(this.#generation, this.#id, KIND.ERROR, { message: error.message }))
            return this;

        this.#core._sendRequest(this.#generation, this.#id, KIND.DONE, undefined, true);
        return this;
    }

    error(error) {
        return this.fault(error);
    }
}

const kindHandlers = new Map([
    [KIND.REQUEST, (core, frame) => {
        if (!core.listenerCount('request'))
            return;

        core._decode(frame, (error, data) => {
            if (error) return core.emit('decodeError', error, frame);
            if (frame.generation === core._generation)
                core.emit('request', frame.request, data);
        });
    }],
    [KIND.MESSAGE, (core, frame) => {
        if (!core.listenerCount('message'))
            return;

        core._decode(frame, (error, data) => {
            if (error) return core.emit('decodeError', error, frame);
            if (frame.generation === core._generation)
                core.emit('message', data);
        });
    }],
    [KIND.RESPONSE, (core, frame) => core._decodeRequestEvent(frame, 'response')],
    [KIND.META, (core, frame) => core._decodeRequestEvent(frame, 'meta')],
    [KIND.ERROR, (core, frame) => core._decodeRequestEvent(frame, 'fault', data => new Error(data.message))],
    [KIND.DONE, (core, frame) => core._finishRequest(frame, 'done')],
    [KIND.CANCEL, (core, frame) => core._finishRequest(frame, 'cancel')]
]);

class SeamFluxCore extends EventEmitter {
    constructor(server = false) {
        super();

        this.codec = CODEC.V8;
        this._generation = 0;
        this._nextId = server ? 1 : 2;
        this._requests = new Map();
        this._out = [];
        this._flushScheduled = false;

        this._head = Buffer.allocUnsafe(HEADER_SIZE);
        this._headSize = 0;
        this._frame = null;
        this._payloadSize = 0;
        this._protocolFailed = false;
    }

    useCodec(id, codec) {
        codecs.set(id, codec);
        return this;
    }

    setCodec(codec) {
        this.codec = codec;
        return this;
    }

    request(data = null) {
        const id = this._allocId();
        const request = this._createRequest(id, true);

        this._send(id, KIND.REQUEST, data);
        return request;
    }

    send(data = null) {
        const id = this._allocId();

        this._send(id, KIND.MESSAGE, data);
        this._send(id, KIND.DONE);
        return this;
    }

    input(chunk) {
        if (this._protocolFailed)
            return this;

        const generation = this._generation;
        try {
            let offset = 0;

            while (offset < chunk.length && generation === this._generation) {
                if (!this._frame)
                    offset = this._readHeader(chunk, offset);
                else
                    offset = this._readPayload(chunk, offset);
            }
        } catch (error) {
            if (generation !== this._generation)
                return this;

            this._protocolFailed = true;
            this._resetFrame();
            this.emit('protocolError', error);
        }

        return this;
    }

    resetConnection(error = new Error('Connection closed')) {
        const requests = Array.from(this._requests.values());

        this._requests.clear();
        this._out.length = 0;
        this._generation++;
        this._protocolFailed = false;
        this._resetFrame();

        for (const state of requests) {
            if (state.outgoing) {
                state.request.emit('fault', error);
                state.request.emit('done');
            } else {
                state.request.emit('cancel');
            }
        }

        return this;
    }

    _createRequest(id, outgoing) {
        const request = new Request(this, id, this._generation);
        this._requests.set(id, { request, outgoing });
        return request;
    }

    _allocId() {
        const id = this._nextId;

        this._nextId = (this._nextId + 2) >>> 0;
        if (!this._nextId)
            this._nextId = id & 1 ? 1 : 2;

        return id;
    }

    _sendRequest(generation, id, kind, data, finish = false) {
        if (generation !== this._generation)
            return false;

        this._send(id, kind, data);
        if (finish)
            this._requests.delete(id);

        return true;
    }

    _send(id, kind, data) {
        this._out.push({ id, kind, codec: this.codec, data, generation: this._generation });

        if (!this._flushScheduled) {
            this._flushScheduled = true;
            queueMicrotask(() => this._flush());
        }

        return this;
    }

    _flush() {
        this._flushScheduled = false;

        while (this._out.length) {
            const frame = this._out.shift();
            if (frame.generation !== this._generation)
                continue;

            try {
                const codec = codecs.get(frame.codec);
                if (!codec)
                    throw new Error(`Unknown codec: ${frame.codec}`);

                const payload = frame.data === undefined ? Buffer.alloc(0) : codec.encode(frame.data);
                if (frame.generation !== this._generation)
                    continue;
                if (payload.length > 0xffffffff - HEADER_SIZE)
                    throw new RangeError('Frame payload is too large');

                const head = Buffer.allocUnsafe(HEADER_SIZE);
                head.writeUInt32BE(HEADER_SIZE + payload.length, 0);
                head.writeUInt32BE(frame.id, 4);
                head.writeUInt8(frame.kind, 8);
                head.writeUInt8(frame.codec, 9);

                this.emit('output', head);
                if (payload.length)
                    this.emit('output', payload);
            } catch (error) {
                this.emit('encodeError', error);
            }
        }
    }

    _readHeader(chunk, offset) {
        const n = Math.min(HEADER_SIZE - this._headSize, chunk.length - offset);

        chunk.copy(this._head, this._headSize, offset, offset + n);
        this._headSize += n;
        offset += n;

        if (this._headSize < HEADER_SIZE)
            return offset;

        const head = Buffer.from(this._head);
        const length = head.readUInt32BE(0);

        this._headSize = 0;
        if (length < HEADER_SIZE)
            throw new Error(`Invalid frame length: ${length}`);

        const size = length - HEADER_SIZE;
        const frame = this._frame = {
            id: head.readUInt32BE(4),
            kind: head.readUInt8(8),
            codec: head.readUInt8(9),
            length: size,
            head,
            payload: size ? Buffer.allocUnsafe(size) : Buffer.alloc(0),
            request: null,
            generation: this._generation
        };

        frame.decode = callback => this._decode(frame, callback);
        this._payloadSize = 0;

        if (!size)
            this._completeFrame(frame);

        return offset;
    }

    _readPayload(chunk, offset) {
        const frame = this._frame;
        const n = Math.min(frame.length - this._payloadSize, chunk.length - offset);

        chunk.copy(frame.payload, this._payloadSize, offset, offset + n);
        this._payloadSize += n;
        offset += n;

        if (this._payloadSize === frame.length)
            this._completeFrame(frame);

        return offset;
    }

    _completeFrame(frame) {
        if (frame.generation !== this._generation || this._frame !== frame)
            return;

        const state = frame.kind === KIND.REQUEST ? null : this._requests.get(frame.id);
        frame.request = frame.kind === KIND.REQUEST ? this._createRequest(frame.id, false) : state?.request;

        this.emit('frame', frame);
        kindHandlers.get(frame.kind)?.(this, frame);
        this._resetFrame(frame);
    }

    _decode(frame, callback) {
        try {
            const codec = codecs.get(frame.codec);
            if (!codec)
                throw new Error(`Unknown codec: ${frame.codec}`);

            callback(null, codec.decode(frame.payload));
        } catch (error) {
            callback(error);
        }
    }

    _decodeRequestEvent(frame, event, transform = data => data) {
        if (!frame.request?.listenerCount(event))
            return;

        this._decode(frame, (error, data) => {
            if (error) return this.emit('decodeError', error, frame);
            if (frame.generation === this._generation)
                frame.request.emit(event, transform(data));
        });
    }

    _finishRequest(frame, event) {
        if (frame.generation !== this._generation || !frame.request)
            return;

        frame.request.emit(event);
        this._requests.delete(frame.id);
    }

    _resetFrame(frame = null) {
        if (frame && this._frame !== frame)
            return;

        this._headSize = 0;
        this._frame = null;
        this._payloadSize = 0;
    }
}

module.exports = {
    SeamFluxCore,
    KIND,
    CODEC
};
