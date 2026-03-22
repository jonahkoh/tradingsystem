FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/paxg-strategy/paxg_strategy_engine.py ./paxg_strategy_engine.py

CMD ["python", "paxg_strategy_engine.py"]
