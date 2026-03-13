FROM python:3.11-slim

WORKDIR /app

# Install dependencies as a separate layer so rebuilds are fast
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create the data directory for the ledger log (used when LOG_FILE=/data/trades.log)
RUN mkdir -p /data

# Default entrypoint — overridden per-service in docker-compose.yml
CMD ["python", "feed-handler.py"]
