const { SFEventClient } = require('..');

const client = new SFEventClient({ host: '127.0.0.1', port: 5000, meta: { clientId: 42 } });

(async () => {
    client.connect();
    client.emit('vehicle:update', { id: 123, speed: 45 });
    console.log('response:', await client.emit('getStatus', { id: 123 }));
    client.close();
})().catch(error => {
    console.error(error);
    client.close();
});
