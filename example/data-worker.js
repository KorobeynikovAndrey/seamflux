const fs = require('fs/promises');
const { parentPort, workerData } = require('worker_threads');
const { SeamFluxStream, CODEC } = require('..');

parentPort.on('message', ({ socket }) => {
    const connection = new SeamFluxStream(true);

    connection.socket = socket;
    connection.setCodec(CODEC.GZIP_V8);

    socket.pipe(connection, { end: false });
    connection.pipe(socket, { end: false });

    connection.on('protocolError', () => socket.destroy());

    socket.on('error', error => {
        connection.emit('socketError', error);
        socket.destroy();
    });

    socket.once('close', () => {
        socket.unpipe(connection);
        connection.unpipe(socket);
        connection.resetConnection();
        connection.destroy();
    });

    connection.on('request', async (request, data) => {
        const controller = new AbortController();

        request.on('cancel', () => controller.abort());

        try {
            const text = await fs.readFile(workerData.file, {
                encoding: 'utf8',
                signal: controller.signal
            });

            if (controller.signal.aborted)
                return;

            const file = JSON.parse(text);

            if (controller.signal.aborted)
                return;

            request.meta(file.columns);
            request.response(file.rows.slice(data.offset));
            request.done();
        } catch (error) {
            if (error.name !== 'AbortError')
                request.fault(error);
        }
    });
});
