FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/execution-manager/execution_service.py ./execution_service.py
COPY services/execution-manager/binance_adapter.py ./binance_adapter.py

CMD ["python", "execution_service.py"]
