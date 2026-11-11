const { EventEmitter } = require('node:events');

class SeamFluxEvents extends EventEmitter {
    constructor(sf) {
        super();
        this.sf = sf;

        sf.on('message', message => {
            if (message && typeof message.event === 'string')
                super.emit(message.event, message.data);
        });

        sf.on('request', (request, message) => {
            if (!message || typeof message.event !== 'string')
                return;

            let replied = false;
            request.once('cancel', () => replied = true);

            if (!super.emit(message.event, message.data, data => {
                if (replied) return;
                replied = true;
                request.response(data);
                request.done();
            })) request.done();
        });
    }

    emit(event, data, callback) {
        if (event === 'newListener' || event === 'removeListener')
            return super.emit(event, data, callback);

        if (typeof callback === 'function') {
            const request = this.sf.request({ event, data });
            request.once('response', callback);
        } else {
            this.sf.send({ event, data });
        }
        return this;
    }
}

module.exports = { SeamFluxEvents };
