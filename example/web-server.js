const express = require('express');
const path = require('path');
const { SocketClient, SeamFluxHttp } = require('..');

const HTTP_PORT = Number(process.argv[2]) || 8080;
const DATA_PORT = Number(process.argv[3]) || 5001;
const DATA_HOST = process.argv[4] || '127.0.0.1';
const PUBLIC = path.join(__dirname, 'public');

const app = express();
const dataServer = new SocketClient({ host: DATA_HOST, port: DATA_PORT });
const http = SeamFluxHttp(dataServer);

dataServer.on('connect', () => {
    console.log(`connected to data-server ${DATA_HOST}:${DATA_PORT}`);
});

dataServer.on('socketError', error => {
    console.log(`data-server: ${error.code || error.message}`);
});

dataServer.connect();

app.post('/api/data', express.json(), http(req => req.body));
app.use(express.static(PUBLIC));

app.listen(HTTP_PORT, () => {
    console.log(`web-server: http://127.0.0.1:${HTTP_PORT}`);
});
