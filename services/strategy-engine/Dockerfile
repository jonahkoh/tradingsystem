FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/strategy-engine/btc_eth_strategy_engine.py ./btc_eth_strategy_engine.py
COPY services/strategy-engine/sol_strategy_engine.py ./sol_strategy_engine.py
COPY services/strategy-engine/xrp_strategy_engine.py ./xrp_strategy_engine.py
COPY services/strategy-engine/fet_strategy_engine.py ./fet_strategy_engine.py
COPY services/strategy-engine/sol_ma_test_strategy.py ./sol_ma_test_strategy.py

CMD ["sh", "-c", "python ${STRATEGY_FILE:-btc_eth_strategy_engine.py}"]
