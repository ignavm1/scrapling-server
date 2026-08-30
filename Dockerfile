FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scrapling_server.py .
# El paquete nuevo: sin esta linea la imagen construye y el contenedor muere
# al primer import.
COPY venara_discovery/ ./venara_discovery/
# Ejecutar como usuario no-root (defensa en profundidad).
RUN useradd -r -m -u 10001 appuser
USER appuser
EXPOSE 8765
CMD ["python", "scrapling_server.py"]
