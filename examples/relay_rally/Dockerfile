# Relay Rally in a container, for Cloud Run or any host with WebSockets: ohra from this checkout.
# The game lives in one actor in memory, so run exactly one instance. From the repository root:
#   docker build --platform linux/amd64 -t IMAGE -f examples/relay_rally/Dockerfile .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
COPY examples/fastapi_relay_rally.py ./examples/
COPY examples/relay_rally ./examples/relay_rally
RUN pip install --no-cache-dir '.[web]'
# behind a proxy that ends TLS, --proxy-headers makes /join hand out https, not http
CMD exec uvicorn examples.fastapi_relay_rally:app --host 0.0.0.0 --port ${PORT:-8080} \
    --proxy-headers --forwarded-allow-ips '*'
