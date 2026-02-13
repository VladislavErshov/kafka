FROM python:3.9-slim

WORKDIR /app
RUN pip install --no-cache-dir requests kafka-python-ng

COPY loader_service.py .

CMD ["python", "-u", "loader_service.py"]
