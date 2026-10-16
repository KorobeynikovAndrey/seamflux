const fs = require('fs');

const MB = 1024 * 1024;
const TARGET_MB = Number(process.argv[2]) || 400;
const FILE = process.argv[3] || 'data.json';

const columns = [
    'id', 'object_id', 'time', 'x', 'y',
    'value', 'active', 'name', 'text'
];

const target = TARGET_MB * MB;
const out = fs.createWriteStream(FILE);
const text = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'.repeat(96);

function row(i) {
    return JSON.stringify([
        i,
        1000000000 + (i % 100000),
        1700000000000 + i * 1000,
        123456.789 + (i % 1000),
        987654.321 + (i % 1000),
        (i % 10000) / 10,
        (i & 1) === 0,
        `object-${i % 10000}`,
        text
    ]);
}

function write(data) {
    return new Promise(resolve => {
        if (out.write(data))
            resolve();
        else
            out.once('drain', resolve);
    });
}

(async () => {
    const head = `{"columns":${JSON.stringify(columns)},"rows":[`;
    let bytes = Buffer.byteLength(head);
    let i = 0;

    await write(head);

    while (bytes < target - 2) {
        const data = (i ? ',' : '') + row(i++);

        await write(data);
        bytes += Buffer.byteLength(data);

        if ((i % 10000) === 0)
            process.stdout.write(`\r${(bytes / MB).toFixed(1)} MB`);
    }

    await write(']}');

    out.end(() => {
        console.log(`\n${FILE}: ${(bytes / MB).toFixed(2)} MB, rows: ${i}`);
    });
})().catch(console.error);
