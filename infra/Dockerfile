#
# Build Stage
#
FROM --platform=linux/amd64 rustlang/rust:nightly-slim@sha256:1489939da2c8e707b56129b25417cb1f52f441d37f748f7e4d0550a56475a6ba AS build

# Install dependencies
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    # For Rust builds, OpenSSL, pkg-config
    openssl \
    libssl-dev \
    pkg-config \
    # General build tools for native dependencies
    build-essential \
    libclang-dev \
    diffutils \
    gcc \
    m4 \
    make \
    # Utilities
    wget \
    tar \
    git \
    curl \
    unzip \
    openssh-client \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Go
ENV GO_VERSION=1.22.5
RUN wget -q https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_VERSION}.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

# Install sp1 toolchain
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN curl -L https://sp1.succinct.xyz | bash && ~/.sp1/bin/sp1up

# Install protoc
ENV PROTOC_ZIP=protoc-29.4-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v29.4/$PROTOC_ZIP && \
    unzip -o $PROTOC_ZIP -d /usr/local bin/protoc && \
    unzip -o $PROTOC_ZIP -d /usr/local 'include/*' && \
    rm -f $PROTOC_ZIP

# Application source code
WORKDIR /app
COPY . .

# Environment variables for the build
ARG BUILD_PROFILE=release
ENV BUILD_PROFILE=$BUILD_PROFILE

ARG RUSTFLAGS=""
ENV RUSTFLAGS="$RUSTFLAGS"

ARG FEATURES=""
ENV FEATURES=$FEATURES

ARG VERGEN_GIT_SHA
ENV VERGEN_GIT_SHA=$VERGEN_GIT_SHA

# Build the application
RUN \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    echo "Building with profile: $BUILD_PROFILE | features: $FEATURES | rustflags: $RUSTFLAGS" && \
    cargo build --profile $BUILD_PROFILE --features "$FEATURES" \
    --bin sp1-cluster-api \
    --bin sp1-cluster-bidder \
    --bin sp1-cluster-cli \
    --bin sp1-cluster-coordinator \
    --bin sp1-cluster-fulfiller \
    --bin sp1-cluster-node && \
    cp target/$BUILD_PROFILE/sp1-cluster-api /api && \
    cp target/$BUILD_PROFILE/sp1-cluster-bidder /bidder && \
    cp target/$BUILD_PROFILE/sp1-cluster-cli /cli && \
    cp target/$BUILD_PROFILE/sp1-cluster-coordinator /coordinator && \
    cp target/$BUILD_PROFILE/sp1-cluster-fulfiller /fulfiller && \
    cp target/$BUILD_PROFILE/sp1-cluster-node /node

#
# Runtime Stage
#
FROM --platform=linux/amd64 debian:bookworm-slim@sha256:364d3f277f79b11fafee2f44e8198054486583d3392e2472eb656d5c780156f5 AS runtime

# Install only essential runtime dependencies
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create a non-root user and group for better security
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/false --create-home appuser

# Copy the built application binaries from the build stage
COPY --from=build /api /api
COPY --from=build /bidder /bidder
COPY --from=build /cli /cli
COPY --from=build /coordinator /coordinator
COPY --from=build /fulfiller /fulfiller
COPY --from=build /node /node

# Set ownership to the non-root user and ensure the binary is executable
RUN chown appuser:appgroup /api && \
    chown appuser:appgroup /bidder && \
    chown appuser:appgroup /cli && \
    chown appuser:appgroup /coordinator && \
    chown appuser:appgroup /fulfiller && \
    chown appuser:appgroup /node && \
    chmod +x /api && \
    chmod +x /bidder && \
    chmod +x /cli && \
    chmod +x /coordinator && \
    chmod +x /fulfiller && \
    chmod +x /node

# Set the user to the non-root user
USER appuser

# Set the entrypoint for the container
CMD ["/api"]
