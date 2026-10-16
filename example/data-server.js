const net = require('net');
const path = require('path');
const { Worker } = require('worker_threads');

const PORT = Number(process.argv[2]) || 5001;
const FILE = process.argv[3] || 'data.json';

const worker = new Worker(path.join(__dirname, 'data-worker.js'), {
    workerData: { file: FILE }
});

worker.on('error', console.error);

net.createServer(socket => {
    worker.postMessage({ socket }, [socket]);
}).listen(PORT, () => {
    console.log(`data-server: 127.0.0.1:${PORT}`);
    console.log(`file: ${FILE}`);
});
