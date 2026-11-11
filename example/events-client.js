const { SocketClient, SeamFluxEvents } = require('..');

const sf = new SocketClient({ host: '127.0.0.1', port: 5000 });
const events = new SeamFluxEvents(sf);

sf.on('connect', () => {
    events.emit('vehicle:update', { id: 123, speed: 45 });
    events.emit('getStatus', { id: 123 }, data => {
        console.log('response:', data);
        sf.close();
    });
});

sf.connect();
