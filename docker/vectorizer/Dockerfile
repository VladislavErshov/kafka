FROM python:3.9-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng fastembed

COPY vectorizer_service.py .

ARG MODEL_NAME=BAAI/bge-small-en-v1.5
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='${MODEL_NAME}')"

CMD ["python", "-u", "vectorizer_service.py"]
