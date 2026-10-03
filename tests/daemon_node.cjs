'use strict';

const assert = require('node:assert/strict');
const readline = require('node:readline');
const config = JSON.parse(process.argv[2]);
const NodeQueued = require(config.sdk);

function emit(message) {
    process.stdout.write(JSON.stringify(message) + '\n');
}

async function bounded(promise) {
    let timer;
    try {
        return await Promise.race([promise, new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('Node smoke operation timed out')), 5000);
        })]);
    } finally {
        clearTimeout(timer);
    }
}

async function run() {
    const client = new NodeQueued('latzero://node-peer', config.pool, {
        host: '127.0.0.1', port: config.port, autoConnect: false, timeout: 4000
    });
    const errors = [];
    client.on('error', error => {
        assert.ok(errors.length < 16, 'Node smoke error budget exceeded');
        errors.push(error.message);
    });
    let lines;
    let shutdownId;
    try {
        await bounded(client.connect());
        client.on('echo', data => ({ owner: 'node-peer', kind: 'app', value: data.value }));
        const registration = await bounded(client.process.register(
            data => ({ owner: 'node-peer', kind: 'process', value: data.value }),
            'echo', { minWorkers: 1, maxWorkers: 1 }
        ));
        assert.equal(registration.type, 'ack');
        assert.equal(registration.payload.process_id, 'node-peer:echo');
        emit({ ok: true, ready: true, client_id: client.clientId, process_id: registration.payload.process_id });
        lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
        for await (const line of lines) {
            const command = JSON.parse(line);
            if (command.operation === 'shutdown') {
                shutdownId = command.id;
                break;
            }
            let result;
            if (command.operation === 'call') {
                const options = { timeout: 4000, responseTo: command.response_to ?? null };
                result = await bounded(command.kind === 'app'
                    ? client.callEvent('echo', { ...options, targetClientId: command.target, data: { value: command.value } })
                    : client.process.call(`${command.target}:echo`, { value: command.value }, options));
            } else if (command.operation === 'set') {
                await bounded(client.set(command.key, command.value));
                result = { set: true };
            } else if (command.operation === 'get') {
                result = { exists: await bounded(client.exists(command.key)), value: await bounded(client.get(command.key, 'missing')) };
            } else {
                throw new Error(`Unknown Node smoke command: ${command.operation}`);
            }
            assert.deepEqual(errors, []);
            emit({ ok: true, id: command.id, result });
        }
    } finally {
        if (lines) lines.close();
        const socket = client.socket;
        const closed = socket && !socket.closed
            ? new Promise(resolve => socket.once('close', resolve)) : Promise.resolve();
        client.disconnect();
        await bounded(closed);
    }
    assert.deepEqual(errors, []);
    if (shutdownId !== undefined) emit({ ok: true, id: shutdownId, result: { stopped: true } });
}

run().catch(error => {
    emit({ ok: false, error: error.stack || String(error) });
    process.exitCode = 1;
});
