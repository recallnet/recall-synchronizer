# Build stage
FROM rust:latest AS builder

WORKDIR /app

# Copy everything
COPY . .

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -u 1000 -m recall

# Copy binary from builder
COPY --from=builder /app/target/release/recall-synchronizer /usr/local/bin/recall-synchronizer

# Create directories for runtime files
RUN mkdir -p /data /app && chown -R recall:recall /data /app

# Switch to non-root user
USER recall

# Set working directory
WORKDIR /data

# Default configuration
ENV RUST_LOG=info

# Expose metrics port if needed in future
# EXPOSE 9090

# Run the synchronizer
ENTRYPOINT ["recall-synchronizer"]
CMD ["start", "--interval", "60"]