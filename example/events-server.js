const { SocketServer, SeamFluxEvents } = require('..');

const server = new SocketServer(sf => {
    const events = new SeamFluxEvents(sf);

    events.on('vehicle:update', data => console.log('update:', data));
    events.on('getStatus', (data, reply) => {
        reply({ id: data.id, status: 'ok' });
    });
});

server.listen(5000, () => console.log('listening on 5000'));
