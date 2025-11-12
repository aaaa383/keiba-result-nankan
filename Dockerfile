FROM python:3.11-slim

# 速度・サイズを意識した最低限の依存
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt

COPY . .

# Cloud Run で使われるポート
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
