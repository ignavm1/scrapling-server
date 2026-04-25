FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y wget curl gnupg ca-certificates libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 fonts-liberation xdg-utils && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN scrapling install
COPY scrapling_server.py .
EXPOSE 8765
CMD ["python", "scrapling_server.py"]
