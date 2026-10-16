const { Duplex, PassThrough } = require('stream');
const { EventEmitter } = require('events');
const v8 = require('v8');
const zlib = require('zlib');

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
    V8: 2,
    GZIP_RAW: 32,
    GZIP_JSON: 33,
    GZIP_V8: 34
};

const EVENTS = {
    [KIND.RESPONSE]: 'response',
    [KIND.ERROR]: 'fault',
    [KIND.META]: 'meta'
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

        decode(data) {
            return data;
        }
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
    }],

    [CODEC.GZIP_RAW, {
        encode(data) {
            return zlib.gzipSync(encodeRaw(data));
        },

        decode(data) {
            return zlib.gunzipSync(data);
        }
    }],

    [CODEC.GZIP_JSON, {
        encode(data) {
            return zlib.gzipSync(Buffer.from(JSON.stringify(data)));
        },

        decode(data) {
            return JSON.parse(zlib.gunzipSync(data).toString());
        }
    }],

    [CODEC.GZIP_V8, {
        encode(data) {
            return zlib.gzipSync(v8.serialize(data));
        },

        decode(data) {
            return v8.deserialize(zlib.gunzipSync(data));
        }
    }]
]);

class Request extends EventEmitter {
    #stream;
    #id;

    constructor(stream, id) {
        super();
        this.#stream = stream;
        this.#id = id;
    }

    response(data = null) {
        this.#stream._queue(this.#id, KIND.RESPONSE, data);
        return this;
    }

    meta(data = null) {
        this.#stream._queue(this.#id, KIND.META, data);
        return this;
    }

    done() {
        this.#stream._queue(this.#id, KIND.DONE);
        this.#stream._requests.delete(this.#id);
        return this;
    }

    cancel() {
        this.#stream._queue(this.#id, KIND.CANCEL);
        this.#stream._requests.delete(this.#id);
        return this;
    }

    fault(error) {
        this.#stream._queue(this.#id, KIND.ERROR, { message: error.message });
        this.#stream._queue(this.#id, KIND.DONE);
        this.#stream._requests.delete(this.#id);
        return this;
    }
}

class SeamFluxStream extends Duplex {
    constructor(server = false) {
        super();

        this.codec = CODEC.JSON;
        this._nextId = server ? 1 : 2;
        this._requests = new Map();

        this._out = [];
        this._blocked = false;

        this._head = Buffer.allocUnsafe(HEADER_SIZE);
        this._headSize = 0;

        this._length = 0;
        this._id = 0;
        this._kind = 0;
        this._codec = 0;
        this._payloadSize = 0;
        this._frameStream = null;

        this._writeChunk = null;
        this._writeOffset = 0;
        this._writeCallback = null;
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

        this._queue(id, KIND.REQUEST, data);
        return request;
    }

    send(data = null) {
        const id = this._allocId();

        this._queue(id, KIND.MESSAGE, data);
        this._queue(id, KIND.DONE);

        return this;
    }

    resetConnection(error = new Error('Connection closed')) {
        const requests = Array.from(this._requests.values());

        this._out.length = 0;
        this._blocked = false;
        this._requests.clear();
        this._protocolFailed = false;

        while (this.read() !== null) {
        }

        if (this._frameStream)
            this._frameStream.destroy();

        const callback = this._writeCallback;

        this._writeChunk = null;
        this._writeOffset = 0;
        this._writeCallback = null;

        this._resetFrame();
        callback?.();

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
        const request = new Request(this, id);

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

    _queue(id, kind, data) {
        this._out.push({ id, kind, codec: this.codec, data });
        this._flush();
        return this;
    }

    _flush() {
        while (!this._blocked && this._out.length) {
            const frame = this._out.shift();
            const codec = codecs.get(frame.codec);

            if (!codec)
                throw new Error(`Unknown codec: ${frame.codec}`);

            const payload = frame.data === undefined ? Buffer.alloc(0) : codec.encode(frame.data);
            const head = Buffer.allocUnsafe(HEADER_SIZE);

            head.writeUInt32BE(HEADER_SIZE + payload.length, 0);
            head.writeUInt32BE(frame.id, 4);
            head.writeUInt8(frame.kind, 8);
            head.writeUInt8(frame.codec, 9);

            this.push(head);

            if (payload.length && !this.push(payload))
                this._blocked = true;
        }
    }

    _read() {
        this._blocked = false;
        this._flush();
    }

    _write(chunk, encoding, callback) {
        if (this._protocolFailed) {
            callback();
            return;
        }

        this._writeChunk = chunk;
        this._writeOffset = 0;
        this._writeCallback = callback;
        this._consumeWrite();
    }

    _consumeWrite() {
        if (!this._writeChunk)
            return;

        try {
            const chunk = this._writeChunk;
            let offset = this._writeOffset;

            while (offset < chunk.length) {
                if (!this._length) {
                    offset = this._readHeader(chunk, offset);
                    continue;
                }

                const result = this._readPayload(chunk, offset);
                offset = result.offset;

                if (result.blocked) {
                    this._writeOffset = offset;
                    return;
                }
            }

            const callback = this._writeCallback;

            this._writeChunk = null;
            this._writeOffset = 0;
            this._writeCallback = null;
            callback();
        } catch (error) {
            this._failProtocol(error);
        }
    }

    _failProtocol(error) {
        const callback = this._writeCallback;

        this._protocolFailed = true;
        this._writeChunk = null;
        this._writeOffset = 0;
        this._writeCallback = null;

        if (this._frameStream)
            this._frameStream.destroy();

        this._resetFrame();
        callback?.();
        this.emit('protocolError', error);
    }

    _readHeader(chunk, offset) {
        const n = Math.min(HEADER_SIZE - this._headSize, chunk.length - offset);

        chunk.copy(this._head, this._headSize, offset, offset + n);

        this._headSize += n;
        offset += n;

        if (this._headSize < HEADER_SIZE)
            return offset;

        const head = Buffer.from(this._head);

        this._length = head.readUInt32BE(0);
        this._id = head.readUInt32BE(4);
        this._kind = head.readUInt8(8);
        this._codec = head.readUInt8(9);
        this._headSize = 0;

        if (this._length < HEADER_SIZE)
            throw new Error(`Invalid frame length: ${this._length}`);

        const codec = codecs.get(this._codec);

        if (!codec)
            throw new Error(`Unknown codec: ${this._codec}`);

        const size = this._length - HEADER_SIZE;
        const stream = this._frameStream = new PassThrough();
        const state = this._kind === KIND.REQUEST ? null : this._requests.get(this._id);
        const request = this._kind === KIND.REQUEST ? this._createRequest(this._id, false) : state?.request;
        const frame = {
            id: this._id,
            kind: this._kind,
            codec: this._codec,
            length: size,
            head,
            stream,
            request,

            decode(callback) {
                const chunks = [];

                stream.on('data', chunk => chunks.push(chunk));
                stream.once('end', () => {
                    try {
                        callback(null, codec.decode(Buffer.concat(chunks, size)));
                    } catch (error) {
                        callback(error);
                    }
                });
            }
        };

        const hasFrameListener = this.listenerCount('frame') > 0;

        this.emit('frame', frame);
        this._dispatchFrame(frame, hasFrameListener);

        if (!size) {
            stream.end();
            this._resetFrame();
        }

        return offset;
    }

    _dispatchFrame(frame, hasFrameListener) {
        if (frame.kind === KIND.DONE) {
            if (frame.request) {
                frame.request.emit('done');
                this._requests.delete(frame.id);
            }
            return;
        }

        if (frame.kind === KIND.CANCEL) {
            if (frame.request) {
                frame.request.emit('cancel');
                this._requests.delete(frame.id);
            }
            return;
        }

        if (frame.kind === KIND.REQUEST) {
            if (this.listenerCount('request')) {
                frame.decode((error, data) => {
                    if (error)
                        return this.emit('decodeError', error, frame);

                    this.emit('request', frame.request, data);
                });
            } else if (!hasFrameListener) {
                frame.stream.resume();
            }
            return;
        }

        if (frame.kind === KIND.MESSAGE) {
            if (this.listenerCount('message')) {
                frame.decode((error, data) => {
                    if (error)
                        return this.emit('decodeError', error, frame);

                    this.emit('message', data);
                });
            } else if (!hasFrameListener) {
                frame.stream.resume();
            }
            return;
        }

        const event = EVENTS[frame.kind];

        if (event && frame.request?.listenerCount(event)) {
            frame.decode((error, data) => {
                if (error)
                    return this.emit('decodeError', error, frame);

                if (frame.kind === KIND.ERROR)
                    data = new Error(data.message);

                frame.request.emit(event, data);
            });
        } else if (!hasFrameListener) {
            frame.stream.resume();
        }
    }

    _readPayload(chunk, offset) {
        const size = this._length - HEADER_SIZE;
        const n = Math.min(size - this._payloadSize, chunk.length - offset);
        const stream = this._frameStream;
        const data = chunk.subarray(offset, offset + n);

        this._payloadSize += n;
        offset += n;

        const complete = this._payloadSize === size;
        const ok = stream.write(data);

        if (complete) {
            stream.once('end', () => {
                this._resetFrame();
                this._consumeWrite();
            });

            stream.end();
            return { offset, blocked: true };
        }

        if (!ok) {
            stream.once('drain', () => this._consumeWrite());
            return { offset, blocked: true };
        }

        return { offset, blocked: false };
    }

    _resetFrame() {
        this._headSize = 0;
        this._length = 0;
        this._id = 0;
        this._kind = 0;
        this._codec = 0;
        this._payloadSize = 0;
        this._frameStream = null;
    }
}

module.exports = {
    SeamFluxStream,
    KIND,
    CODEC
};
