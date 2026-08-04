FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY webapp/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY webapp/ .
COPY config/ /app/config/

# Create data directory for database
RUN mkdir -p /data

# Create a non-root user
RUN useradd -m -u 1000 webapp && chown -R webapp:webapp /app
USER webapp

EXPOSE 5000

CMD ["python", "app.py"]
