FROM python:3.12-slim

WORKDIR /opt/tap/elastic_indexer

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY index_scored_events.py .
COPY query_observability.py .

CMD ["python", "/opt/tap/elastic_indexer/index_scored_events.py"]
