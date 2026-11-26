import "dotenv/config";
import { Kafka } from "kafkajs";
import { db } from "../src/prisma/db";

const TOPIC_NAME = "zap-events";

const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
let running = true;

async function main() {
  await producer.connect();

  while (running) {
    const pendingRows = await db.orm.public.ZapRunOutbox
      .orderBy((r) => r.id.asc())
      .limit(10)
      .all();

    if (pendingRows.length > 0) {
      // Send first. Delete only after Kafka acknowledges.
      await producer.send({
        topic: TOPIC_NAME,
        messages: pendingRows.map((r) => ({
          value: JSON.stringify({ zapRunId: r.zapRunId, stage: 0 }),
        })),
      });

      const ids = pendingRows.map((r) => r.id);
      await db.orm.public.ZapRunOutbox
        .where((r) => r.id.in(ids))
        .deleteAndCount();
    }

    await new Promise((r) => setTimeout(r, 3000));
  }
}

async function shutdown() {
  running = false;
  await producer.disconnect();
  await db.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

main().catch(async (err) => {
  console.error(err);
  await shutdown();
});