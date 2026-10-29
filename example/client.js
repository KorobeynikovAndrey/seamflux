const { SocketClient } = require('..');

const client = new SocketClient({ host: '127.0.0.1', port: 5000 });

client.on('connect', () => {
    const request = client.request({ value: 123 });

    request.on('response', data => console.log('response:', data));
    request.on('done', () => client.close());
});

client.connect();
