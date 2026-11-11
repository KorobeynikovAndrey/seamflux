const { SFEventServer } = require('..');

const server = new SFEventServer(conn => {
    console.log('connection meta:', conn.meta);
    conn.on('vehicle:update', data => console.log('update:', data));
    conn.on('getStatus', async data => {
        return { id: data.id, status: 'ok' };
    });
});

server.listen(5000, () => console.log('listening on 5000'));
