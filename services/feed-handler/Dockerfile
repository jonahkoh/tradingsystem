FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/feed-handler/feed-handler.py ./feed-handler.py

CMD ["python", "feed-handler.py"]
