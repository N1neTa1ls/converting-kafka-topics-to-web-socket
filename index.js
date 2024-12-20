import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'kafkaToFrontWS',
    brokers: ['172.16.60.65:9094'],
})
const consumer = kafka.consumer({ groupId: 'frontendGroup' })
const app = express();
const server = createServer(app);
const io = new Server(server);

const __dirname = dirname(fileURLToPath(import.meta.url));

app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
    console.log('Client connected');

    // Обработка отключения клиента
    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});

consumer.connect().then(() => {
    consumer.subscribe({topic: 'device-control.commandresultmessage.1', fromBeginning: false}).then(() => {
        consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                io.emit('kafka', {
                    value: message.value,
                    timestamp: message.timestamp,
                    topic: topic,
                });
            },
        })
    })
})

// Функция для получения сообщений из Kafka
// const run = async () => {
//     await consumer.connect();
//     await consumer.subscribe({topic: 'device-control.commandresultmessage.1', fromBeginning: false})
//     await consumer.run({
//         eachMessage: async ({topic, partition, message}) => {
//             const msg = message.value.toString();
//             console.log('Received message', msg);
//
//             // Broadcast the message to all connected clients
//             io.emit('message', msg);
//         },
//     });
// }

server.listen(3000, () => {
    console.log('server running at http://localhost:3000');
    // run().catch(console.error);
});

