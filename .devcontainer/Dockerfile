# exarrow-rs Development Container

FROM debian:bookworm-slim

# Install development tools, networking, and firewall dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials for Rust
    build-essential \
    pkg-config \
    libssl-dev \
    # Version control and tools
    git \
    gh \
    # Shell and utilities
    bash \
    curl \
    wget \
    ca-certificates \
    unzip \
    less \
    procps \
    sudo \
    fzf \
    man-db \
    nano \
    vim \
    jq \
    # Firewall and networking
    iptables \
    ipset \
    iproute2 \
    dnsutils \
    aggregate \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set timezone (after apt-get for better caching)
ARG TZ
ENV TZ="${TZ:-UTC}"

# Create non-root user for development
ARG USERNAME=coder
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME \
    && echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Set DEVCONTAINER environment variable
ENV DEVCONTAINER=true

# Create workspace and config directories
RUN mkdir -p /workspace /home/$USERNAME/.claude /home/$USERNAME/.cargo \
    && chown -R $USERNAME:$USERNAME /workspace /home/$USERNAME/.claude /home/$USERNAME/.cargo

WORKDIR /workspace

# Switch to non-root user for remaining installations
USER $USERNAME
WORKDIR /home/$USERNAME

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . $HOME/.cargo/env \
    && rustup component add rustfmt clippy rust-analyzer

# Set up Rust environment
ENV PATH="/home/$USERNAME/.cargo/bin:$PATH"
ENV CARGO_HOME="/home/$USERNAME/.cargo"
ENV RUSTUP_HOME="/home/$USERNAME/.rustup"

# Install Node.js (required for Claude Code CLI and OpenSpec)
USER root
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up npm global directory for non-root user
RUN mkdir -p /usr/local/share/npm-global \
    && chown -R $USERNAME:$USERNAME /usr/local/share/npm-global

USER $USERNAME

# Configure npm to use user-writable global prefix
ENV NPM_CONFIG_PREFIX=/usr/local/share/npm-global
ENV PATH=$PATH:/usr/local/share/npm-global/bin

# Install Claude Code CLI (pinned version for caching, updated via updateContentCommand)
ARG CLAUDE_CODE_VERSION=2.0.54
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}
RUN mkdir -p /home/$USERNAME/.claude

# Install OpenSpec CLI (pinned version for caching)
RUN npm install -g @fission-ai/openspec@0.16.0

# Set shell and editor preferences
ENV SHELL=/bin/bash
ENV EDITOR=vim
ENV VISUAL=vim

# Copy and set up firewall script (last step)
USER root
COPY init-firewall.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init-firewall.sh \
    && echo "$USERNAME ALL=(root) NOPASSWD: /usr/local/bin/init-firewall.sh" > /etc/sudoers.d/$USERNAME-firewall \
    && chmod 0440 /etc/sudoers.d/$USERNAME-firewall

USER $USERNAME
WORKDIR /workspace
