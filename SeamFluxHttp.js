const { KIND } = require('./SeamFluxStream');

function SeamFluxHttp(connection) {
    const responses = new Map();

    function writeFrame(state, frame) {
        if (!state.started) {
            state.started = true;
            state.res.status(200);
            state.res.set({
                'Content-Type': 'application/octet-stream',
                'Cache-Control': 'no-store'
            });
        }

        state.res.write(frame.head);

        if (!frame.length) {
            frame.stream.resume();
            return;
        }

        state.stream = frame.stream;

        frame.stream.once('end', () => {
            state.stream = null;
        });

        frame.stream.pipe(state.res, { end: false });
    }

    const handlers = {
        [KIND.META](frame, state) {
            writeFrame(state, frame);
        },

        [KIND.RESPONSE](frame, state) {
            writeFrame(state, frame);
        },

        [KIND.ERROR](frame, state) {
            writeFrame(state, frame);
        },

        [KIND.DONE](frame, state) {
            writeFrame(state, frame);
            responses.delete(frame.request);
            state.res.end();
        }
    };

    connection.on('frame', frame => {
        const state = responses.get(frame.request);
        const handler = handlers[frame.kind];

        if (!state || !handler) {
            frame.stream.resume();
            return;
        }

        handler(frame, state);
    });

    connection.on('socketClose', () => {
        for (const state of responses.values()) {
            if (state.stream) {
                state.stream.unpipe(state.res);
                state.stream.resume();
            }

            if (!state.res.headersSent) {
                state.res.status(502).type('text').end('Data server connection closed');
            } else {
                state.res.destroy(new Error('Data server connection closed'));
            }
        }

        responses.clear();
    });

    return data => (req, res) => {
        if (connection.socket?.readyState !== 'open') {
            res.status(503).type('text').send('Data server is not connected');
            return;
        }

        const request = typeof data === 'function' ? data(req) : data;
        const operation = connection.request(request);
        const state = { res, stream: null, started: false };

        responses.set(operation, state);

        res.once('close', () => {
            if (!responses.delete(operation))
                return;

            operation.cancel();

            if (state.stream) {
                state.stream.unpipe(res);
                state.stream.resume();
            }
        });
    };
}

module.exports = SeamFluxHttp;
