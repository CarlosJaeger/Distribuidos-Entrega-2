FROM python:3.11-slim
WORKDIR /app

# ahora sí se puede copiar porque requirements.txt está en el contexto
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install psycopg[binary]

COPY traffic_gen/app.py .

CMD ["python", "app.py"]
