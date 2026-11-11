const { EventEmitter } = require('node:events');

class SeamFluxEvents {
    constructor(sf) {
        this.sf = sf;
        this._handlers = new EventEmitter();

        sf.on('request', (request, message) => this._handle(request, message));
    }

    on(event, handler) {
        this._handlers.on(event, handler);
        return this;
    }

    off(event, handler) {
        this._handlers.off(event, handler);
        return this;
    }

    once(event, handler) {
        this._handlers.once(event, handler);
        return this;
    }

    emit(event, data) {
        const request = this.sf.request({ event, data });
        const result = new Promise((resolve, reject) => {
            let response;
            let received = false;

            request.on('response', data => {
                if (!received) {
                    response = data;
                    received = true;
                }
            });
            request.once('fault', reject);
            request.once('done', () => resolve(response));
        });

        result.catch(() => {}); // Allow emit() without await.
        return result;
    }

    _handle(request, message) {
        if (!message || typeof message.event !== 'string') {
            request.done();
            return;
        }

        const handlers = this._handlers.rawListeners(message.event);
        if (!handlers.length) {
            request.done();
            return;
        }

        let pending = handlers.length;
        let finished = false;
        let failure;

        request.once('cancel', () => finished = true);

        for (const handler of handlers) {
            Promise.resolve().then(() => handler.call(this._handlers, message.data)).then(data => {
                if (finished) return;
                if (data !== undefined) {
                    finished = true;
                    request.response(data);
                    request.done();
                } else if (!--pending) {
                    finished = true;
                    if (failure) request.fault(failure);
                    else request.done();
                }
            }, error => {
                if (finished) return;
                failure ??= error;
                if (!--pending) {
                    finished = true;
                    request.fault(failure);
                }
            });
        }
    }
}

module.exports = { SeamFluxEvents };
