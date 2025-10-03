# syntax=docker/dockerfile:1

# Base image with Python 3.11
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install tini for proper signal handling (recommended for long-running services)
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

# App directory
WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src ./src
COPY config ./config
COPY README.md ./

# Default entrypoint/cmd; config path can be overridden via args
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "src/meshgram.py", "-c", "config/config.yaml"]
