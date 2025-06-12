# Dockerfile.pos_simulator

FROM python:3.9-slim

WORKDIR /app

COPY pos_producer.py .

RUN pip install kafka-python faker

CMD ["python", "pos_producer.py"]
