FROM apache/flink:1.18.1-scala_2.12-java11

USER root
# Python + alias 'python'
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir "apache-flink==1.18.1"

# ---- Conector Kafka (DataStream) ----
ARG FLINK_KAFKA_CONNECTOR_VER=3.1.0-1.18
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_KAFKA_CONNECTOR_VER}/flink-connector-kafka-${FLINK_KAFKA_CONNECTOR_VER}.jar" \
    -o /opt/flink/lib/flink-connector-kafka-${FLINK_KAFKA_CONNECTOR_VER}.jar

# ---- Dependencias obligatorias: kafka-clients + compresores ----
ARG KAFKA_CLIENTS_VER=3.6.0
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VER}/kafka-clients-${KAFKA_CLIENTS_VER}.jar" \
    -o /opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VER}.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" \
    -o /opt/flink/lib/lz4-java-1.8.0.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" \
    -o /opt/flink/lib/snappy-java-1.1.10.5.jar

# ---- Tu job ----
WORKDIR /app
COPY score.py /app/score.py

# Cluster + job (PyFlink): ya tienes python, forzamos pyexec
CMD ["/bin/bash", "-c", "/opt/flink/bin/start-cluster.sh && sleep 5 && /opt/flink/bin/flink run -py /app/score.py -pyexec python3 && tail -f /opt/flink/log/*"]
