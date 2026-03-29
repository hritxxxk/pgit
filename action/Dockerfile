FROM rust:1.75-slim-bookworm

LABEL "com.github.actions.name"="PGit - Statistical Data Drift Detection"
LABEL "com.github.actions.description"="Check for statistical drift in your data pipelines"
LABEL "com.github.actions.icon"="activity"
LABEL "com.github.actions.color"="blue"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pgit
RUN cargo install pgit

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
