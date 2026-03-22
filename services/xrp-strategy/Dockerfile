FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/xrp-strategy/xrp_strategy_engine.py ./xrp_strategy_engine.py

CMD ["python", "xrp_strategy_engine.py"]
