# Relay Rally in a container, for Cloud Run or any host with WebSockets: ohra from this checkout.
# The game lives in one actor in memory, so run exactly one instance. From the repository root:
#   docker build --platform linux/amd64 -t IMAGE -f examples/relay_rally/Dockerfile .

# the pages: type-checked against clock.ts and built by Vite; clock.js built from clock.ts
FROM node:24-slim AS pages
WORKDIR /build
COPY src/ohra/ext/clock.ts ./src/ohra/ext/
RUN npx --yes esbuild@0.28.2 src/ohra/ext/clock.ts --minify --format=esm --outfile=src/ohra/ext/clock.js
COPY examples/relay_rally ./examples/relay_rally
RUN cd examples/relay_rally && npm ci && npm run build

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
COPY --from=pages /build/src/ohra/ext/clock.js ./src/ohra/ext/
COPY examples/fastapi_relay_rally.py ./examples/
COPY --from=pages /build/examples/relay_rally/dist ./examples/relay_rally/dist
RUN pip install --no-cache-dir '.[web]'
# behind a proxy that ends TLS, --proxy-headers makes /join hand out https, not http
CMD exec uvicorn examples.fastapi_relay_rally:app --host 0.0.0.0 --port ${PORT:-8080} \
    --proxy-headers --forwarded-allow-ips '*'
