# Arclap Timelapse Cleaner — CUDA-enabled container.
#
# Build:
#   docker build -t arclap-cleaner .
#
# Run with GPU + persistent state:
#   docker run --gpus all -p 8000:8000 \
#     -v $(pwd)/_data:/app/_data \
#     -v $(pwd)/_outputs:/app/_outputs \
#     -v $(pwd)/_uploads:/app/_uploads \
#     -v $(pwd)/_models:/app/_models \
#     arclap-cleaner
#
# CPU-only build (no GPU):
#   docker build --build-arg BASE_IMAGE=python:3.12-slim --build-arg TORCH_INDEX=cpu -t arclap-cleaner-cpu .

ARG BASE_IMAGE=nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04
ARG TORCH_INDEX=cu124

FROM ${BASE_IMAGE} AS base

ARG TORCH_INDEX

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.12 python3.12-venv python3-pip \
        ffmpeg \
        libgl1 libglib2.0-0 \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.12 /usr/local/bin/python \
    && ln -sf /usr/bin/python3.12 /usr/local/bin/python3

WORKDIR /app

# Install PyTorch first (gives Docker layer caching the right boundaries)
RUN if [ "${TORCH_INDEX}" = "cpu" ]; then \
        pip install --break-system-packages torch torchvision; \
    else \
        pip install --break-system-packages torch torchvision \
            --index-url https://download.pytorch.org/whl/${TORCH_INDEX}; \
    fi

# Then app dependencies
COPY requirements.txt /app/
RUN pip install --break-system-packages -r requirements.txt

# Then the app source itself
COPY . /app

# Pre-download default YOLO weight at build time so first run is offline-capable
RUN python -c "from ultralytics import YOLO; YOLO('yolov8x-seg.pt')" || true

EXPOSE 8000

# Healthcheck against the /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://127.0.0.1:8000/health || exit 1

# Bind to all interfaces inside the container
CMD ["python", "-c", \
     "import uvicorn; uvicorn.run('app:app', host='0.0.0.0', port=8000, log_level='info')"]
