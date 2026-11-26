FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    -r fetch/requirements.txt \
    -r load/requirements.txt \
    -r dbt/requirements.txt