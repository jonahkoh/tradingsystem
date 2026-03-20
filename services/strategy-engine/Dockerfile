FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/strategy-engine/strategy-engine.py ./strategy-engine.py

CMD ["python", "strategy-engine.py"]
