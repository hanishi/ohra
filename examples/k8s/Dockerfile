# The Kubernetes examples image: pekko-py from this checkout; runs examples/k8s/node.py, and carts.yaml runs carts.py.
# Build from the repository root:  docker build -t YOU/pekko-py-cluster:dev -f examples/k8s/Dockerfile .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml LICENSE NOTICE ./
COPY src ./src
COPY examples ./examples
RUN pip install --no-cache-dir '.[web]'   # FastAPI and uvicorn for carts.py
EXPOSE 2551 7626 8000
CMD ["python", "examples/k8s/node.py"]
