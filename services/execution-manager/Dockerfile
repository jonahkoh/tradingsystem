FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/execution-manager/execution.py ./execution.py

CMD ["python", "execution.py"]
