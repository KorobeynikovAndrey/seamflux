import { v8DeserializeDB } from './v8-db-decoder.js';

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

const textDecoder = new TextDecoder();
const codecs = {
    [CODEC.RAW]: buffer => buffer,
    [CODEC.JSON]: buffer => JSON.parse(textDecoder.decode(buffer)),
    [CODEC.V8]: buffer => v8DeserializeDB(buffer),
    [CODEC.GZIP_RAW]: buffer => gunzip(buffer),
    [CODEC.GZIP_JSON]: async buffer => JSON.parse(textDecoder.decode(await gunzip(buffer))),
    [CODEC.GZIP_V8]: async buffer => v8DeserializeDB(await gunzip(buffer))
};

self.onmessage = async ({ data }) => {
    if (data.cmd !== 'load')
        return;

    try {
        const started = performance.now();
        const response = await fetch('/api/data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ offset: data.offset }),
            cache: 'no-store'
        });

        if (!response.ok)
            throw new Error(await response.text());

        const reader = response.body.getReader();
        const parser = createParser();
        let loaded = 0;

        for (;;) {
            const { value, done } = await reader.read();

            if (done)
                break;

            loaded += value.length;
            self.postMessage({ type: 'progress', loaded });
            await parser.write(value);
        }

        if (!parser.done)
            throw new Error('Incomplete SeamFlux response');

        const finished = performance.now();

        self.postMessage({
            type: 'done',
            columns: parser.columns,
            rows: parser.rows,
            receiveMs: finished - started
        });
    } catch (error) {
        self.postMessage({ type: 'error', message: error.message });
    }
};

function createParser() {
    const state = {
        header: new Uint8Array(HEADER_SIZE),
        headerSize: 0,
        frame: null,
        columns: [],
        rows: 0,
        done: false,

        async write(chunk) {
            let offset = 0;

            while (offset < chunk.length) {
                if (!this.frame) {
                    const n = Math.min(HEADER_SIZE - this.headerSize, chunk.length - offset);
                    this.header.set(chunk.subarray(offset, offset + n), this.headerSize);
                    this.headerSize += n;
                    offset += n;

                    if (this.headerSize < HEADER_SIZE)
                        continue;

                    const view = new DataView(this.header.buffer, this.header.byteOffset, HEADER_SIZE);
                    const length = view.getUint32(0);

                    if (length < HEADER_SIZE)
                        throw new Error(`Invalid SeamFlux frame length: ${length}`);

                    this.frame = {
                        id: view.getUint32(4),
                        kind: view.getUint8(8),
                        codec: view.getUint8(9),
                        payload: new Uint8Array(length - HEADER_SIZE),
                        size: 0
                    };
                    this.headerSize = 0;

                    if (!this.frame.payload.length) {
                        await this.finishFrame();
                        continue;
                    }
                }

                const frame = this.frame;
                const n = Math.min(frame.payload.length - frame.size, chunk.length - offset);

                frame.payload.set(chunk.subarray(offset, offset + n), frame.size);
                frame.size += n;
                offset += n;

                if (frame.size === frame.payload.length)
                    await this.finishFrame();
            }
        },

        async finishFrame() {
            const frame = this.frame;
            this.frame = null;

            if (frame.kind === KIND.DONE) {
                this.done = true;
                return;
            }

            const decode = codecs[frame.codec];

            if (!decode)
                throw new Error(`Unsupported codec: ${frame.codec}`);

            const data = await decode(frame.payload);

            if (frame.kind === KIND.META) {
                this.columns = data;
                return;
            }

            if (frame.kind === KIND.RESPONSE) {
                this.rows += data?.length ?? 0;
                return;
            }

            if (frame.kind === KIND.ERROR)
                throw new Error(data.message);
        }
    };

    return state;
}

async function gunzip(buffer) {
    const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream('gzip'));
    return new Uint8Array(await new Response(stream).arrayBuffer());
}
