FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/fet-strategy/fet_strategy_engine.py ./fet_strategy_engine.py

CMD ["python", "fet_strategy_engine.py"]
