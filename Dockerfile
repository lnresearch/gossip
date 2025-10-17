# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy source code first
COPY . .

# Install dependencies including the package itself
RUN uv pip install --system -e .

# Create directories for volumes
RUN mkdir -p /data/annex /data/db

# Set environment variables with defaults
ENV RABBITMQ_URL="amqp://guest:guest@rabbitmq:5672/"
ENV UPSTREAM_RABBITMQ_URL="amqp://guest:guest@upstream:5672/"
ENV UPSTREAM_QUEUE_NAME="router.gossip"
ENV DATABASE_PATH="/data/db/gossip.db"
ENV ARCHIVE_DIRECTORY="/data/annex"
ENV ARCHIVE_ROTATION="daily"
ENV WEB_HOST="0.0.0.0"
ENV WEB_PORT="8000"
ENV LOG_LEVEL="INFO"

# Expose web port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the main application
CMD ["python", "-m", "lnr.main"]