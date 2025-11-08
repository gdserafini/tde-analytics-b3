FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    python3-dev \
    openjdk-21-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY kaggle.json ./.kaggle/kaggle.json

RUN chmod 600 /app/.kaggle/kaggle.json

ENV PYTHONPATH=/app