const { SocketClient, SocketServer } = require('./SeamFluxSocket');
const { SeamFluxEvents } = require('./SeamFluxEvents');

class SFEventClient extends SeamFluxEvents {
    constructor(options = {}) {
        const { meta, ...socketOptions } = options;
        const sf = new SocketClient(socketOptions);
        super(sf);

        this.meta = meta ?? null;
        sf.core.sendConnectionMeta(this.meta);
        sf.on('socketClose', () => {
            if (!sf.stopped)
                sf.core.sendConnectionMeta(this.meta);
        });
    }

    updateMeta(data) {
        this.meta = data;
        this.sf.core.sendConnectionMeta(data);
        return this;
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

        super(options, sf => {
            const conn = new SeamFluxEvents(sf);
            let initialized = false;

            sf.core.on('connectionMeta', data => {
                conn.meta = data;
                if (!initialized) {
                    initialized = true;
                    listener?.(conn);
                }
            });
        });
    }
}

module.exports = { SFEventClient, SFEventServer };
