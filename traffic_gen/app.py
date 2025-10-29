import os, asyncio, orjson, time, uuid, random
import psycopg
from aiokafka import AIOKafkaProducer
from psycopg_pool import ConnectionPool
BOOTSTRAP = os.environ["BOOTSTRAP_SERVERS"]
PENDING_TOPIC = os.environ.get("PENDING_TOPIC", "questions.pending")

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
#BD
pool = ConnectionPool(f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASS}")

# Función para obtener una pregunta random desde la tabla yahoo_data
def get_random_question():
    """Devuelve una pregunta random de yahoo_data."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, question_title, question_content, best_answer
                FROM yahoo_data
                ORDER BY RANDOM()
                LIMIT 1;
            """)
            row = cur.fetchone()
    if row:
        qid = str(row[0])
        qtext = (row[1] or "") + " " + (row[2] or "")
        qbest = row[3] or ""
        return qid, qtext.strip(), qbest.strip()
    return None, None, None

def question_in_llm_answers(qid: str) -> bool:
    """Devuelve True si la pregunta ya está en llm_answers."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM llm_answers WHERE question_id = %s LIMIT 1;", (qid,))
            return cur.fetchone() is not None
        
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: orjson.dumps(v)
    )
    await producer.start()

    print("⏳ Esperando a que Kafka estabilice las particiones...")
    await asyncio.sleep(5)
    await producer.client.force_metadata_update()

    try:
        while True:
            qid, qtext, qbest = get_random_question()
            if qid and qtext:
                if question_in_llm_answers(qid):
                    print(f"✔️ Pregunta {qid} ya está en llm_answers → no se envía")
                else:
                    payload = {
                        "question_id": qid,
                        "question_text": qtext,
                        "answer_ref": qbest,
                        "attempt": 0,
                        "retry_count": 0,
                        "trace_id": str(uuid.uuid4()),
                        "ts": time.time()
                    }
                    try:
                        print(f"🚀 Enviando pregunta {qid} → Kafka")
                        await producer.send_and_wait(PENDING_TOPIC, payload)
                    except Exception as e:
                        print(f"⚠️ Error enviando {qid}: {e}, reintentando en 2s")
                        await asyncio.sleep(2)
                        continue
            await asyncio.sleep(2)
    finally:
        await producer.stop()
        pool.close()  # cerrar pool al terminar


if __name__ == "__main__":
    asyncio.run(main())
