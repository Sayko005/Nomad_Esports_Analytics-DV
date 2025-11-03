FROM python:3.12-slim
WORKDIR /app
# Add CA certificates for HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir requests prometheus_client
COPY custom_exporter.py .
ENV PORT=8000 LOOP_SECONDS=20 CITY_LAT=51.1694 CITY_LON=71.4491 GITHUB_REPO=torvalds/linux
CMD ["python", "custom_exporter.py"]
