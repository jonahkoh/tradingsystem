FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/sentiment-oracle/sentiment_service.py ./sentiment_service.py

CMD ["python", "sentiment_service.py"]
