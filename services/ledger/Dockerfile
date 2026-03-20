FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ledger/ledger.py ./ledger.py
COPY services/ledger/db.py ./db.py

RUN mkdir -p /data

CMD ["python", "ledger.py"]
