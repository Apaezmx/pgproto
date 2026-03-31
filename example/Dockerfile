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

# Set working directory
WORKDIR /workspace

# Copy source code and build it inside the image (runs as root!)
# This ensures it installs to /usr/lib/postgresql/ without permission issues.
COPY . /workspace

RUN make clean && make && make install

# Clean up workspace to avoid overriding volume mounts if mounted over,
# although volumes would override it anyway. We remove it to keep the image clean.
RUN rm -rf /workspace/*

# The standard entrypoint from postgres:18 will take over
