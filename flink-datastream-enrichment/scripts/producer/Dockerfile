FROM python:3.9-slim-buster
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY .. .
CMD ["python", "producer.py", "--bootstrap_servers", "kluster-kafka-bootstrap:9092", "--topic", "orders", "--num_messages", "400"]
