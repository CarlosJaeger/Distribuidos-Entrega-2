import json
import re
from pyflink.datastream import StreamExecutionEnvironment
# Import compatible con distintas versiones
try:
    from pyflink.datastream import WatermarkStrategy
except ImportError:
    from pyflink.common import WatermarkStrategy

from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types

# ---------- Config ----------
BOOTSTRAP = "kafka:9092"
TOPIC_PENDING   = "questions.pending"
TOPIC_SUCCESS   = "answers.validated"
TOPIC_SCORED    = "answers.success"
TOPIC_REJECTED  = "answers.rejected.lowquality"

QUALITY_THRESHOLD = 0.5
MAX_RETRIES = 3


def tokenize(text: str):
    """Convierte un texto en lista de tokens alfanuméricos en minúsculas"""
    return re.findall(r"\w+", text.lower())

def rouge_l(ref: str, llm: str) -> float:
    """Calcula un score ROUGE-L aproximado basado en Longest Common Subsequence"""
    a = tokenize(ref)
    b = tokenize(llm)
    m, n = len(a), len(b)
    if m == 0 or n == 0:
        return 0.0
    dp = [[0]*(n+1) for _ in range(m+1)]
    for i in range(1, m+1):
        for j in range(1, n+1):
            if a[i-1] == b[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])
    lcs = dp[m][n]
    # normalizamos contra promedio de longitudes de ref y llm
    return lcs / ((m + n) / 2)

def process_success(value: str):
    n = json.loads(value)
    qid = n.get("question_id")
    ref = n.get("answer_ref", "")
    llm = n.get("answer_llm", "")
    retries = n.get("retry_count", 0)

    score = rouge_l(ref, llm)
    print(score)
    # Caso 1: respuesta aceptable → scored
    if score >= QUALITY_THRESHOLD:
        yield ("scored", json.dumps({
            "question_id": qid, "answer_ref": ref, "answer_llm": llm,
            "quality_score": score, "retry_count": retries,
        }))

    # Caso 2: mala calidad pero aún puede reintentarse
    elif retries < MAX_RETRIES:
        requeue = {
            "question_id": qid,
            "question_text": n.get("question_text", ""),
            "answer_ref": ref,
            "retry_count": retries + 1,
        }
        yield ("pending", json.dumps(requeue))
        yield ("retry", json.dumps(requeue))

    # Caso 3: mala calidad y ya sin retries disponibles → rejected
    else:
        yield ("rejected", json.dumps({
            "question_id": qid, "reason": "low_quality",
            "score": score, "retry_count": retries,
        }))

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ---------- Source ----------
    success_src = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC_SUCCESS)
        .set_group_id("flink-quality")
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    wm = WatermarkStrategy.no_watermarks()
    success_stream = env.from_source(success_src, wm, "answers.validated")

    # ---------- Sinks ----------
    scored_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_SCORED)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()
    )

    pending_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_PENDING)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()
    )

    rejected_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_REJECTED)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()
    )

    retry_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("answers.retry")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()
    )

    # ---------- Procesamiento ----------
    def flatmap_fn(value):
        for topic, msg in process_success(value):
            yield topic, msg   # FIX: no usar str(msg)

    processed = success_stream.flat_map(
        flatmap_fn,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    processed.filter(lambda t: t[0] == "scored") \
        .map(lambda t: t[1], output_type=Types.STRING()) \
        .sink_to(scored_sink)

    processed.filter(lambda t: t[0] == "pending") \
        .map(lambda t: t[1], output_type=Types.STRING()) \
        .sink_to(pending_sink)

    processed.filter(lambda t: t[0] == "rejected") \
        .map(lambda t: t[1], output_type=Types.STRING()) \
        .sink_to(rejected_sink)

    processed.filter(lambda t: t[0] == "retry") \
        .map(lambda t: t[1], output_type=Types.STRING()) \
        .sink_to(retry_sink)

    env.execute("LLM Quality & Retry Job (PyFlink)")


if __name__ == "__main__":
    main()
