FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ledger/ledger_service.py ./ledger_service.py

RUN mkdir -p /data

CMD ["python", "ledger_service.py"]
