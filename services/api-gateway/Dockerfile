FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/api-gateway/api_gateway.py ./api_gateway.py

RUN mkdir -p /data

CMD ["uvicorn", "api_gateway:app", "--host", "0.0.0.0", "--port", "8000"]
