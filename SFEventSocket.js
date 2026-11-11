const { SocketClient, SocketServer } = require('./SeamFluxSocket');
const { SeamFluxEvents } = require('./SeamFluxEvents');

class SFEventClient extends SeamFluxEvents {
    constructor(options = {}) {
        super(new SocketClient(options));
    }

    connect(options) {
        this.sf.connect(options);
        return this;
    }

    close() {
        this.sf.close();
        return this;
    }
}

class SFEventServer extends SocketServer {
    constructor(options, listener) {
        if (typeof options === 'function') {
            listener = options;
            options = {};
        }

        super(options, sf => listener?.(new SeamFluxEvents(sf)));
    }
}

module.exports = { SFEventClient, SFEventServer };
