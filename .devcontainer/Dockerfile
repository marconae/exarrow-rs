# exarrow-rs Development Container

FROM debian:bookworm-slim

# Install development tools, networking, and firewall dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials for Rust
    build-essential \
    pkg-config \
    libssl-dev \
    # Shell and utilities
    bash \
    curl \
    wget \
    ca-certificates \
    unzip \
    less \
    procps \
    sudo \
    man-db \
    nano \
    vim \
    jq \
    tree \
    # Firewall and networking
    iptables \
    ipset \
    iproute2 \
    dnsutils \
    aggregate \
    # Network debugging tools
    netcat-openbsd \
    iputils-ping \
    traceroute \
    tcpdump \
    nmap \
    # SSH server
    openssh-server \
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
    && echo "$USERNAME:$USERNAME" | chpasswd

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

# Install Context7 MCP server (npm package)
RUN npm install -g @upstash/context7-mcp

# Install uv (Python package manager) for Serena MCP server
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/$USERNAME/.local/bin:$PATH"

# Pre-install Serena MCP server dependencies (cached for faster startup)
RUN uv tool install git+https://github.com/oraios/serena

# Set shell and editor preferences
ENV SHELL=/bin/bash
ENV EDITOR=vim
ENV VISUAL=vim

# Add PATH to bashrc and profile for SSH sessions (ENV vars don't persist in SSH)
# .profile is sourced for login shells, .bashrc for interactive non-login shells
# Also fix cargo env sourcing to be conditional (avoids errors when file doesn't exist)
RUN echo 'export PATH="/home/coder/.local/bin:/home/coder/.cargo/bin:/usr/local/share/npm-global/bin:$PATH"' >> /home/$USERNAME/.bashrc \
    && echo 'export CARGO_HOME="/home/coder/.cargo"' >> /home/$USERNAME/.bashrc \
    && echo 'export RUSTUP_HOME="/home/coder/.rustup"' >> /home/$USERNAME/.bashrc \
    && echo 'export NPM_CONFIG_PREFIX=/usr/local/share/npm-global' >> /home/$USERNAME/.bashrc \
    && echo 'export PATH="/home/coder/.local/bin:/home/coder/.cargo/bin:/usr/local/share/npm-global/bin:$PATH"' >> /home/$USERNAME/.profile \
    && echo 'export NPM_CONFIG_PREFIX=/usr/local/share/npm-global' >> /home/$USERNAME/.profile \
    && sed -i 's|^\. "\$HOME/.cargo/env"$|[ -f "\$HOME/.cargo/env" ] \&\& . "\$HOME/.cargo/env"|g' /home/$USERNAME/.profile /home/$USERNAME/.bashrc

# Configure and start SSH server
USER root
RUN mkdir -p /var/run/sshd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

EXPOSE 22

WORKDIR /workspace

# Start SSHD and keep container running
CMD ["/usr/sbin/sshd", "-D"]
