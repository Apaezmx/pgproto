FROM postgres:18

# Install build essentials and protobuf-c
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    postgresql-server-dev-18 \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory for builds
WORKDIR /workspace

# Verify tools exist
RUN pg_config --version && protoc-c --version
