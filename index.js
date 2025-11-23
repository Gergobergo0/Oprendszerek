// index.js
const express = require("express");
const path = require("path");
const http = require("http");
const { WebSocketServer } = require("ws");
const { Kafka } = require("kafkajs");

// --------- ENV VARS ----------
const PORT = process.env.PORT || 8080;
const KAFKA_BROKER = process.env.KAFKA_BROKER; // e.g. "kafka:9092"
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "db.changes";

if (!KAFKA_BROKER) {
  console.error("Missing KAFKA_BROKER env var");
  process.exit(1);
}

// --------- EXPRESS + HTTP SERVER ----------
const app = express();
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);

// --------- WEBSOCKET SERVER ----------
const wss = new WebSocketServer({ server, path: "/ws" });

function broadcast(json) {
  const data = JSON.stringify(json);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(data);
    }
  });
}

wss.on("connection", (socket) => {
  console.log("Client connected");
  socket.on("close", () => console.log("Client disconnected"));
});

// --------- KAFKA CONSUMER (NO AUTH) ----------
const kafka = new Kafka({
  clientId: "stream-demo",
  brokers: [KAFKA_BROKER]  // no ssl/sasl here
});

const consumer = kafka.consumer({ groupId: "stream-demo-group" });

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log(`Subscribed to topic: ${KAFKA_TOPIC}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const valueStr = message.value?.toString() || "{}";
        let payload;

        try {
          payload = JSON.parse(valueStr);
        } catch {
          payload = { raw: valueStr };
        }

        const event = {
          topic,
          partition,
          offset: message.offset,
          timestamp: message.timestamp,
          key: message.key ? message.key.toString() : null,
          value: payload
        };

        broadcast(event);
      } catch (err) {
        console.error("Error processing message:", err);
      }
    }
  });
}

// --------- START SERVER + KAFKA ----------
server.listen(PORT, () => {
  console.log(`HTTP/WebSocket server listening on port ${PORT}`);
  startKafka().catch((err) => {
    console.error("Kafka error:", err);
    process.exit(1);
  });
});
