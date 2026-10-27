# Imagen de la aplicación RapidGo V4: productor de eventos y trabajo de Spark.
# Python 3.12 + Java 17 (requisito de Spark 4) + dependencias de requirements.txt.
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ producer/
COPY spark/ spark/
COPY samples/ samples/
COPY scripts/ scripts/
