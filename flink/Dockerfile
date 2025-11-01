FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY flink_processor.py .

# Crear directorio para datos persistentes
RUN mkdir -p /app/data

# Exponer puerto
EXPOSE 8005

# Comando de inicio
CMD ["python", "flink_processor.py"]