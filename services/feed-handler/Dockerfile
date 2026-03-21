FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/feed-handler/market_data.py ./market_data.py

CMD ["python", "market_data.py"]
