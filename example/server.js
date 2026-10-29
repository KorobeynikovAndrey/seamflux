const { SocketServer } = require('..');

const server = new SocketServer(seam => {
    seam.on('request', (request, data) => {
        request.response({ echo: data });
        request.done();
    });

    seam.on('message', data => {
        console.log('message:', data);
    });
});

server.listen(5000, () => console.log('listening on 5000'));
