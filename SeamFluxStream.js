const { Duplex } = require('node:stream');
const { SeamFluxCore } = require('./SeamFluxCore');

const CORE_EVENTS = ['request', 'message', 'frame', 'decodeError', 'protocolError', 'encodeError'];

class SeamFluxStream extends Duplex {
    constructor(server = false) {
        super();

        this.core = new SeamFluxCore(server);
        this._out = [];
        this._blocked = false;

        for (const event of CORE_EVENTS)
            this.core.on(event, (...args) => this.emit(event, ...args));

        this.core.on('output', data => {
            this._out.push(data);
            this._flushOutput();
        });
    }

    request(data = null) {
        return this.core.request(data);
    }

    send(data = null) {
        this.core.send(data);
        return this;
    }

    resetConnection(error = new Error('Connection closed')) {
        this._out.length = 0;
        this._blocked = false;

        while (this.read() !== null) {
        }

        this.core.resetConnection(error);
        return this;
    }

    _flushOutput() {
        while (!this._blocked && this._out.length) {
            const data = this._out.shift();
            if (!this.push(data))
                this._blocked = true;
        }
    }

    _read() {
        this._blocked = false;
        this._flushOutput();
    }

    _write(chunk, encoding, callback) {
        this.core.input(chunk);
        callback();
    }
}

module.exports = { SeamFluxStream };
