const TAG = {
    VERSION: 0xff,
    PADDING: 0x00,
    VERIFY_OBJECT_COUNT: 0x3f,       // ?
    HOLE: 0x2d,                      // -
    UNDEFINED: 0x5f,                 // _
    NULL: 0x30,                      // 0
    TRUE: 0x54,                      // T
    FALSE: 0x46,                     // F
    INT32: 0x49,                     // I
    UINT32: 0x55,                    // U
    DOUBLE: 0x4e,                    // N
    BIGINT: 0x5a,                    // Z
    UTF8_STRING: 0x53,               // S
    ONE_BYTE_STRING: 0x22,           // "
    TWO_BYTE_STRING: 0x63,           // c
    OBJECT_REFERENCE: 0x5e,          // ^
    BEGIN_OBJECT: 0x6f,              // o
    END_OBJECT: 0x7b,                // {
    BEGIN_SPARSE_ARRAY: 0x61,        // a
    END_SPARSE_ARRAY: 0x40,          // @
    BEGIN_DENSE_ARRAY: 0x41,         // A
    END_DENSE_ARRAY: 0x24,           // $
    ARRAY_BUFFER: 0x42,              // B
    IMMUTABLE_ARRAY_BUFFER: 0x43,    // C
    HOST_OBJECT: 0x5c                // \
};

const textDecoder = new TextDecoder('utf-8', { fatal: true });

function toUint8Array(input) {
    if (input instanceof ArrayBuffer)
        return new Uint8Array(input);

    if (ArrayBuffer.isView(input))
        return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);

    throw new TypeError('Expected ArrayBuffer or ArrayBufferView');
}

function latin1(bytes) {
    if (!bytes.length)
        return '';

    const CHUNK = 0x8000;
    let result = '';

    for (let i = 0; i < bytes.length; i += CHUNK)
        result += String.fromCharCode(...bytes.subarray(i, i + CHUNK));

    return result;
}

function utf16le(bytes) {
    if (bytes.length & 1)
        throw new Error('Invalid V8 two-byte string length');

    const CHARS = 0x4000;
    let result = '';

    for (let offset = 0; offset < bytes.length;) {
        const count = Math.min(CHARS, (bytes.length - offset) >> 1);
        const chars = new Array(count);

        for (let i = 0; i < count; i++, offset += 2)
            chars[i] = bytes[offset] | (bytes[offset + 1] << 8);

        result += String.fromCharCode(...chars);
    }

    return result;
}

function defineProperty(object, key, value) {
    Object.defineProperty(object, String(key), {
        value,
        enumerable: true,
        writable: true,
        configurable: true
    });
}


const VALUE_READERS = {
    [TAG.VERIFY_OBJECT_COUNT]: decoder => {
        decoder.readVarint();
        return decoder.readValue();
    },
    [TAG.UNDEFINED]: () => undefined,
    [TAG.NULL]: () => null,
    [TAG.TRUE]: () => true,
    [TAG.FALSE]: () => false,
    [TAG.INT32]: decoder => decoder.readZigZag32(),
    [TAG.UINT32]: decoder => decoder.readVarint(),
    [TAG.DOUBLE]: decoder => decoder.readDouble(),
    [TAG.BIGINT]: decoder => decoder.readBigInt(),
    [TAG.UTF8_STRING]: decoder => decoder.readUtf8String(),
    [TAG.ONE_BYTE_STRING]: decoder => decoder.readOneByteString(),
    [TAG.TWO_BYTE_STRING]: decoder => decoder.readTwoByteString(),
    [TAG.OBJECT_REFERENCE]: decoder => decoder.readObjectReference(),
    [TAG.BEGIN_OBJECT]: decoder => decoder.readObject(),
    [TAG.BEGIN_DENSE_ARRAY]: decoder => decoder.readDenseArray(),
    [TAG.BEGIN_SPARSE_ARRAY]: decoder => decoder.readSparseArray(),
    [TAG.ARRAY_BUFFER]: decoder => decoder.readArrayBuffer(),
    [TAG.IMMUTABLE_ARRAY_BUFFER]: decoder => decoder.readArrayBuffer(),
    [TAG.HOST_OBJECT]: decoder => decoder.readHostObject()
};

const HOST_OBJECT_READERS = {
    0: (decoder, bytes) => decoder.makeTypedArray(Int8Array, bytes),
    1: (decoder, bytes) => decoder.makeTypedArray(Uint8Array, bytes),
    2: (decoder, bytes) => decoder.makeTypedArray(Uint8ClampedArray, bytes),
    3: (decoder, bytes) => decoder.makeTypedArray(Int16Array, bytes),
    4: (decoder, bytes) => decoder.makeTypedArray(Uint16Array, bytes),
    5: (decoder, bytes) => decoder.makeTypedArray(Int32Array, bytes),
    6: (decoder, bytes) => decoder.makeTypedArray(Uint32Array, bytes),
    7: (decoder, bytes) => decoder.makeTypedArray(Float32Array, bytes),
    8: (decoder, bytes) => decoder.makeTypedArray(Float64Array, bytes),
    9: (decoder, bytes) => new DataView(bytes.slice().buffer),
    10: (decoder, bytes) =>
        new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    11: (decoder, bytes) => decoder.makeTypedArray(BigInt64Array, bytes),
    12: (decoder, bytes) => decoder.makeTypedArray(BigUint64Array, bytes),
    13: (decoder, bytes) => {
        if (typeof globalThis.Float16Array !== 'function')
            throw decoder.error('Float16Array is not supported by this browser');

        return decoder.makeTypedArray(globalThis.Float16Array, bytes);
    }
};

export class V8DBDecoder {
    constructor(input) {
        this.bytes = toUint8Array(input);
        this.view = new DataView(
            this.bytes.buffer,
            this.bytes.byteOffset,
            this.bytes.byteLength
        );

        this.pos = 0;
        this.version = 0;
        this.refs = [];
    }

    decode() {
        this.readHeader();
        const result = this.readValue();

        while (this.pos < this.bytes.length && this.bytes[this.pos] === TAG.PADDING)
            this.pos++;

        if (this.pos !== this.bytes.length)
            throw this.error('Trailing data');

        return result;
    }

    readHeader() {
        if (this.readByte() !== TAG.VERSION)
            throw this.error('Missing V8 serialization header');

        this.version = this.readVarint();

        // Node 18+ / V8 formats relevant to current Node versions.
        // The subset decoded below is stable across these versions.
        if (this.version < 13 || this.version > 16)
            throw this.error(`Unsupported V8 serialization version ${this.version}`);
    }

    readValue() {
        let value = this.readValueInternal();
        return value;
    }

    readValueInternal() {
        const tag = this.readTag();
        const reader = VALUE_READERS[tag];

        if (!reader)
            throw this.error(
                `Unsupported V8 tag 0x${tag.toString(16).padStart(2, '0')} (${String.fromCharCode(tag)})`
            );

        return reader(this);
    }

    readObject() {
        const object = {};
        this.refs.push(object);

        const count = this.readProperties(object, TAG.END_OBJECT);
        const expected = this.readVarint();

        if (count !== expected)
            throw this.error(`Object property count mismatch: ${count} != ${expected}`);

        return object;
    }

    readDenseArray() {
        const length = this.readVarint();
        const array = new Array(length);
        this.refs.push(array);

        for (let i = 0; i < length; i++) {
            if (this.peekTag() === TAG.HOLE) {
                this.readTag();
                continue;
            }

            array[i] = this.readValue();
        }

        const count = this.readProperties(array, TAG.END_DENSE_ARRAY);
        const expectedCount = this.readVarint();
        const expectedLength = this.readVarint();

        if (count !== expectedCount)
            throw this.error(`Array property count mismatch: ${count} != ${expectedCount}`);

        if (length !== expectedLength)
            throw this.error(`Array length mismatch: ${length} != ${expectedLength}`);

        return array;
    }

    readSparseArray() {
        const length = this.readVarint();
        const array = new Array(length);
        this.refs.push(array);

        const count = this.readProperties(array, TAG.END_SPARSE_ARRAY);
        const expectedCount = this.readVarint();
        const expectedLength = this.readVarint();

        if (count !== expectedCount)
            throw this.error(`Sparse array property count mismatch: ${count} != ${expectedCount}`);

        if (length !== expectedLength)
            throw this.error(`Sparse array length mismatch: ${length} != ${expectedLength}`);

        return array;
    }

    readProperties(object, endTag) {
        let count = 0;

        while (this.peekTag() !== endTag) {
            const key = this.readValue();

            if (typeof key !== 'string' && typeof key !== 'number')
                throw this.error(`Invalid object key type: ${typeof key}`);

            const value = this.readValue();
            defineProperty(object, key, value);
            count++;
        }

        this.readTag();
        return count;
    }

    readObjectReference() {
        const id = this.readVarint();

        if (id >= this.refs.length)
            throw this.error(`Invalid object reference ${id}`);

        return this.refs[id];
    }

    readBigInt() {
        const bitfield = this.readVarint();
        const negative = (bitfield & 1) !== 0;
        const byteLength = Math.floor(bitfield / 2);
        const bytes = this.readBytes(byteLength);

        if (!byteLength) {
            if (negative)
                throw this.error('Invalid negative zero BigInt');
            return 0n;
        }

        let value = 0n;

        // V8 stores serialized BigInt magnitude least-significant byte first.
        for (let i = bytes.length - 1; i >= 0; i--)
            value = (value << 8n) | BigInt(bytes[i]);

        return negative ? -value : value;
    }

    readUtf8String() {
        const length = this.readVarint();
        return textDecoder.decode(this.readBytes(length));
    }

    readOneByteString() {
        const length = this.readVarint();
        return latin1(this.readBytes(length));
    }

    readTwoByteString() {
        const length = this.readVarint();
        return utf16le(this.readBytes(length));
    }

    readArrayBuffer() {
        const length = this.readVarint();
        const bytes = this.readBytes(length);

        // Plain ArrayBuffer is uncommon for DB drivers; make it independent
        // from the serialized response buffer to preserve ArrayBuffer semantics.
        const buffer = bytes.slice().buffer;
        this.refs.push(buffer);
        return buffer;
    }

    readHostObject() {
        const typeIndex = this.readVarint();
        const byteLength = this.readVarint();
        const bytes = this.readBytes(byteLength);
        const reader = HOST_OBJECT_READERS[typeIndex];

        if (!reader)
            throw this.error(`Unsupported Node host-object type ${typeIndex}`);

        const value = reader(this, bytes);
        this.refs.push(value);
        return value;
    }

    makeTypedArray(Type, bytes) {
        const size = Type.BYTES_PER_ELEMENT || 1;

        if (bytes.byteLength % size)
            throw this.error(`Invalid ${Type.name} byte length ${bytes.byteLength}`);

        // Direct view when naturally aligned; otherwise copy once to aligned storage.
        if ((bytes.byteOffset % size) === 0)
            return new Type(bytes.buffer, bytes.byteOffset, bytes.byteLength / size);

        const copy = bytes.slice();
        return new Type(copy.buffer, copy.byteOffset, copy.byteLength / size);
    }

    readDouble() {
        this.ensure(8);
        const value = this.view.getFloat64(this.pos, true);
        this.pos += 8;
        return value;
    }

    readZigZag32() {
        const value = this.readVarint();
        return (value & 1) ? -((value + 1) / 2) : value / 2;
    }

    readVarint() {
        let value = 0;
        let factor = 1;

        for (let i = 0; i < 8; i++) {
            const byte = this.readByte();
            value += (byte & 0x7f) * factor;

            if ((byte & 0x80) === 0) {
                if (!Number.isSafeInteger(value))
                    throw this.error('Varint exceeds Number.MAX_SAFE_INTEGER');
                return value;
            }

            factor *= 128;
        }

        throw this.error('Invalid varint');
    }

    peekTag() {
        let pos = this.pos;

        while (pos < this.bytes.length && this.bytes[pos] === TAG.PADDING)
            pos++;

        if (pos >= this.bytes.length)
            throw this.error('Unexpected end of data');

        return this.bytes[pos];
    }

    readTag() {
        while (this.pos < this.bytes.length && this.bytes[this.pos] === TAG.PADDING)
            this.pos++;

        return this.readByte();
    }

    readByte() {
        this.ensure(1);
        return this.bytes[this.pos++];
    }

    readBytes(length) {
        if (!Number.isSafeInteger(length) || length < 0)
            throw this.error(`Invalid byte length ${length}`);

        this.ensure(length);

        const result = new Uint8Array(
            this.bytes.buffer,
            this.bytes.byteOffset + this.pos,
            length
        );

        this.pos += length;
        return result;
    }

    ensure(length) {
        if (this.pos + length > this.bytes.length)
            throw this.error('Unexpected end of data');
    }

    error(message) {
        return new Error(`${message} at byte ${this.pos}`);
    }
}

export function v8DeserializeDB(input) {
    return new V8DBDecoder(input).decode();
}
