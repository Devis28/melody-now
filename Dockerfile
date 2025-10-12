# Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# uprav podľa toho, kde máš FastAPI aplikáciu:
# ak používaš api.py s "app = FastAPI()", nechaj api:app
ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    APP_MODULE=app:app

USER appuser
CMD ["sh", "-c", "uvicorn $APP_MODULE --host 0.0.0.0 --port ${PORT}"]
