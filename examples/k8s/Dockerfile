# The Kubernetes example image: pekko-py installed from this checkout, running examples/k8s/node.py.
# Build from the repository root:  docker build -t YOU/pekko-py-cluster:dev -f examples/k8s/Dockerfile .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml LICENSE NOTICE ./
COPY src ./src
COPY examples ./examples
RUN pip install --no-cache-dir .
EXPOSE 2551 7626
CMD ["python", "examples/k8s/node.py"]
