FROM python:3.14.2-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ghcr_cleanup.py ./

# Create non-root user
RUN useradd -m -u 1000 cleanup && \
    chown -R cleanup:cleanup /app && \
    pip install --no-cache-dir -r requirements.txt

USER cleanup

# Expose metrics port
EXPOSE 8000

# Run the application
CMD ["python", "-u", "ghcr_cleanup.py"]