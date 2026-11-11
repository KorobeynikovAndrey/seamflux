const { EventEmitter } = require('node:events');

class SeamFluxEvents {
    constructor(sf) {
        this.sf = sf;
        this.events = new EventEmitter();

        sf.on('message', message => {
            if (message && typeof message.event === 'string')
                this.events.emit(message.event, message.data);
        });
    }

    on(event, listener) {
        this.events.on(event, listener);
        return this;
    }

    off(event, listener) {
        this.events.off(event, listener);
        return this;
    }

    emit(event, data) {
        this.sf.send({ event, data });
        return this;
    }
}

module.exports = { SeamFluxEvents };
