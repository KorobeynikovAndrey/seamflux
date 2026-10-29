const net = require('node:net');
const { SeamFluxStream } = require('./SeamFluxStream');

const FIRST_DELAY = 1000;
const MAX_DELAY = 30000;

function attach(socket, protocol) {
    protocol.socket = socket;
    socket.pipe(protocol, { end: false });
    protocol.pipe(socket, { end: false });
}

function detach(socket, protocol) {
    socket.unpipe(protocol);
    protocol.unpipe(socket);

    if (protocol.socket === socket)
        protocol.socket = null;
}

class SocketClient extends SeamFluxStream {
    constructor(options = {}) {
        super(false);

        this.options = options;
        this.socket = null;
        this.timer = null;
        this.delay = FIRST_DELAY;
        this.stopped = true;

        this.on('protocolError', () => this.socket?.destroy());
    }

    connect(options) {
        if (options)
            this.options = options;

        this.stopped = false;
        this._connect();
        return this;
    }

    _connect() {
        if (this.stopped)
            return;

        const socket = net.connect(this.options);
        this.socket = socket;
        attach(socket, this);

        socket.once('connect', () => {
            clearTimeout(this.timer);
            this.timer = null;
            this.delay = FIRST_DELAY;
            this.emit('connect');
        });

        socket.on('error', error => this.emit('socketError', error));
        socket.once('close', hadError => {
            detach(socket, this);

            if (this.socket === socket)
                this.socket = null;

            this.resetConnection();
            this.emit('socketClose', hadError);
            this._reconnect();
        });
    }

    _reconnect() {
        if (this.stopped || this.timer)
            return;

        this.timer = setTimeout(() => {
            this.timer = null;
            this._connect();
            this.delay = Math.min(this.delay * 2, MAX_DELAY);
        }, this.delay);
    }

    close() {
        this.stopped = true;
        clearTimeout(this.timer);
        this.timer = null;
        this.socket?.destroy();
        return this;
    }
}

class SocketServer extends net.Server {
    constructor(options, listener) {
        if (typeof options === 'function') {
            listener = options;
            options = {};
        }

        super(options, socket => {
            const protocol = new SeamFluxStream(true);
            attach(socket, protocol);

            protocol.on('protocolError', () => socket.destroy());
            socket.on('error', error => {
                protocol.emit('socketError', error);
                socket.destroy();
            });
            socket.once('close', () => {
                detach(socket, protocol);
                protocol.resetConnection();
                protocol.emit('socketClose');
            });

            listener?.(protocol);
        });
    }
}

module.exports = {
    SocketClient,
    SocketServer,
    MAX_DELAY
};
