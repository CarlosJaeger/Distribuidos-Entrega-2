CREATE TABLE IF NOT EXISTS yahoo_data (
    id SERIAL PRIMARY KEY,
    class_index INT NOT NULL,
    question_title TEXT,
    question_content TEXT,
    best_answer TEXT
);

CREATE TABLE IF NOT EXISTS llm_answers (
    question_id TEXT PRIMARY KEY,
    question_text TEXT NOT NULL,
    llm_answer TEXT,
    score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS llm_scores (
  question_id TEXT,
  retry_count INT,
  quality_score FLOAT,
  created_t TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS llm_call_metrics (
  question_id     TEXT PRIMARY KEY,     
  llm_latency_ms  BIGINT NOT NULL,
  llm_retries     INT    NOT NULL,
  created_at      TIMESTAMP DEFAULT NOW()
);