FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    -r data/fetch/requirements.txt \
    -r data/load/requirements.txt \
    -r data/dbt/requirements.txt

ENV BINANCE_BASE_URL=https://data-api.binance.vision

CMD ["sh", "run.sh"]    