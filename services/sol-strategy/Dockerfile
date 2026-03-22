FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/sol-strategy/sol_strategy_engine.py ./sol_strategy_engine.py

CMD ["python", "sol_strategy_engine.py"]
