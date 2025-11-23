// index.js
const express = require("express");
const path = require("path");
const http = require("http");
const { WebSocketServer } = require("ws");
const { Kafka } = require("kafkajs");

// --------- ENV VARS ----------
const PORT = process.env.PORT || 8080;
let broker = process.env.KAFKA_BROKER; // e.g. "kafka:29092"
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "KUTYAKAJA";

if (!broker) {
  console.error("Missing KAFKA_BROKER env var");
  process.exit(1);
}

// strip things like PLAINTEXT:// if present
broker = broker.replace(/^[A-Z_]+:\/\//i, "");

console.log("Using broker:", broker, "topic:", KAFKA_TOPIC);

// --------- EXPRESS + HTTP SERVER ----------
const app = express();
app.use(express.json()); // for JSON POST body
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

// --------- KAFKA SETUP ----------
const kafka = new Kafka({
  clientId: "stream-demo",
  brokers: [broker],
});

const consumer = kafka.consumer({ groupId: "stream-demo-group" });
const producer = kafka.producer();

// consume from Kafka and push to WS clients
async function startKafka() {
  await producer.connect();
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
          value: payload,
        };

        // send to all connected browsers
        broadcast(event);
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
}

// --------- HTTP ENDPOINT TO PRODUCE MESSAGES ----------
// Students hit this via the frontend button
app.post("/produce", async (req, res) => {
  try {
    const { text, user } = req.body || {};
    if (!text || text.trim() === "") {
      return res.status(400).json({ error: "text is required" });
    }

    const payload = {
      type: "CHAT_MESSAGE",
      text: text.trim(),
      user: user || "student",
      createdAt: new Date().toISOString(),
    };

    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [{ value: JSON.stringify(payload) }],
    });

    return res.json({ status: "ok" });
  } catch (err) {
    console.error("Error producing message:", err);
    return res.status(500).json({ error: "failed to produce message" });
  }
});

// --------- START SERVER + KAFKA ----------
server.listen(PORT, () => {
  console.log(`HTTP/WebSocket server listening on port ${PORT}`);
  startKafka().catch((err) => {
    console.error("Kafka error:", err);
    process.exit(1);
  });
});
