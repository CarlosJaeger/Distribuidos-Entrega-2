import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { getLLMResponse } from './llm.js';

const DB_HOST = "postgres";
const DB_USER = "user";
const DB_PASS = "1234";
const DB_NAME = "yahoo_dataset";
const DB_PORT = 5432;
const BOOTSTRAP_SERVERS = "kafka:9092";
// --- Kafka setup ---
const kafka = new Kafka({
  clientId: 'llm-app',
  brokers: BOOTSTRAP_SERVERS.split(','),
  logLevel: logLevel.INFO
});

const workerConsumer = kafka.consumer({ groupId: 'llm-worker' });
const workerProducer = kafka.producer();
const persisterConsumer = kafka.consumer({ groupId: 'answers-persister' });
const retriesConsumer = kafka.consumer({ groupId: "retry-persister" });
// --- DB setup ---
const { Pool } = pg;
const pool = new Pool({
  host: DB_HOST,
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASS,
  port: DB_PORT ? parseInt(DB_PORT) : 5432
});

async function upsertAnswer(question_id, answer_text) {
  await pool.query(
    `INSERT INTO llm_answers (question_id, answer_llm)
     VALUES ($1, $2)
     ON CONFLICT (question_id) DO UPDATE SET answer_llm = EXCLUDED.answer_llm`,
    [question_id, answer_text]
  );
}
async function saveLLMMetrics(question_id, llm_latency_ms, llm_retries) {
  await pool.query(
    `INSERT INTO llm_call_metrics (question_id, llm_latency_ms, llm_retries)
     VALUES ($1, $2, $3)
     ON CONFLICT (question_id)
     DO UPDATE SET
       llm_latency_ms = EXCLUDED.llm_latency_ms,
       llm_retries    = EXCLUDED.llm_retries`,
    [question_id, llm_latency_ms, llm_retries]
  );
}
// --- Worker logic ---
async function startWorker() {
  await workerProducer.connect();
  await workerConsumer.connect();
  await workerConsumer.subscribe({ topic: 'questions.pending', fromBeginning: true });

  console.log('🟢 Worker escuchando questions.pending → answers.validated');
  console.log(DB_HOST)

await workerConsumer.run({
  eachMessage: async ({ message, partition }) => {
    try {
      const payload = JSON.parse(message.value?.toString('utf8') || '{}');
      const { question_id, question_text, trace_id, retry_count } = payload;

      if (!question_id || !question_text) {
        console.warn('⚠️ [Worker] Mensaje inválido:', payload);
        return;
      }
      const t0 = Date.now();
      const { text: answer_llm, retries: llm_retries } = await getLLMResponse(question_text);
      const llm_latency_ms = Date.now() - t0;

      // Guarda métricas en la base
      await saveLLMMetrics(question_id, llm_latency_ms, llm_retries);
      const out = {
        question_id,
        answer_llm,
        ts: Date.now(),
        trace_id: trace_id || null,
        retry_count
      };
      await workerProducer.send({
        topic: 'answers.validated', 
        messages: [{ value: JSON.stringify(out) }]
      });

      console.log(`✔️ [Worker] ${question_id} (p${partition}) llm_latency=${llm_latency_ms}ms retries=${llm_retries}`);
    } catch (err) {
      console.error('❌ [Worker] Error:', err.message);
    }
  }
});
}

// --- Persister logic ---
async function startPersister() {
  await persisterConsumer.connect();
  await persisterConsumer.subscribe({ topic: 'answers.success', fromBeginning: false });

  console.log('🟢 Persister escuchando answers.success → DB');

  await persisterConsumer.run({
    eachMessage: async ({ message, partition }) => {
      try {
        const payload = JSON.parse(message.value?.toString('utf8') || '{}');
        const { question_id, answer_llm } = payload;

        if (!question_id || typeof answer_llm !== 'string') {
          console.warn('⚠️ [Persister] Mensaje inválido:', payload);
          return;
        }

        await upsertAnswer(question_id, answer_llm);
        console.log(`💾 [Persister] guardado ${question_id} (p${partition})`);
      } catch (err) {
        console.error('❌ [Persister] Error:', err.message);
      }
    }
  });
}

// --- Retry Persister logic ---
async function startRetryPersister() {
  await retriesConsumer.connect();
  await retriesConsumer.subscribe({ topic: 'answers.retry', fromBeginning: false });

  console.log('🟢 RetryPersister escuchando answers.retry → llm_scores');

  await retriesConsumer.run({
    eachMessage: async ({ message, partition }) => {
      try {
        const payload = JSON.parse(message.value?.toString('utf8') || '{}');
        const { question_id, retry_count, quality_score } = payload;

        if (!question_id || typeof retry_count !== 'number' || typeof quality_score !== 'number') {
          console.warn('⚠️ [RetryPersister] Mensaje inválido:', payload);
          return;
        }

        await pool.query(
          `INSERT INTO llm_scores (question_id, retry_count, quality_score)
           VALUES ($1, $2, $3)`,
          [question_id, retry_count, quality_score]
        );

        console.log(`💾 [RetryPersister] guardado ${question_id} retry=${retry_count} score=${quality_score}`);
      } catch (err) {
        console.error('❌ [RetryPersister] Error:', err.message);
      }
    }
  });
}

// --- Run both in parallel ---
async function run() {
  await Promise.all([
    startWorker(),
    startPersister(),
    startRetryPersister()
  ]);
}

async function shutdown() {
  console.log('⛔ Apagando app...');
  await workerConsumer.disconnect().catch(() => {});
  await workerProducer.disconnect().catch(() => {});
  await persisterConsumer.disconnect().catch(() => {});
  await retriesConsumer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
