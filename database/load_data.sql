COPY yahoo_data(class_index, question_title, question_content, best_answer)
FROM '/docker-entrypoint-initdb.d/test.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

