#!/bin/bash
# This script is used to build docker images. Set GITHUB_USER and GITHUB_TOKEN environment variables before running this script.
# Usage: ./build.sh [base|node-gpu|alloy]
# Defaults to all of them

set -e

# Check required environment variables
if [ -z "$GITHUB_USER" ]; then
    echo "Error: GITHUB_USER environment variable is not set"
    echo "Please set it with: export GITHUB_USER=<your-github-username>"
    exit 1
fi

# Configuration
SHORT_SHA=$(git rev-parse --short HEAD)
TAG="${GITHUB_USER}-${SHORT_SHA}"

# Function to build a single image
build_image() {
    local IMAGE_TYPE=$1
    local DOCKERFILE=""
    local TAG_PREFIX=""

    case "$IMAGE_TYPE" in
        base)
            DOCKERFILE="./infra/Dockerfile"
            TAG_PREFIX="base"
            ;;
        node-gpu)
            DOCKERFILE="./infra/Dockerfile.node_gpu"
            TAG_PREFIX="node-gpu"
            ;;
        alloy)
            DOCKERFILE="./infra/Dockerfile.alloy"
            TAG_PREFIX="alloy"
            ;;
        *)
            echo "Unknown image type: $IMAGE_TYPE"
            return 1
            ;;
    esac

    LOCAL_TAG="sp1-cluster-wip:${TAG_PREFIX}-${TAG}"

    echo "============================================"
    echo "Building $IMAGE_TYPE image..."
    echo "Dockerfile: $DOCKERFILE"
    echo "Tag: $LOCAL_TAG"
    echo "Git SHA: $SHORT_SHA"
    echo "============================================"
    echo ""

    # Build the image
    echo "Building Docker image..."
    docker buildx build \
        --platform linux/amd64 \
        --file "$DOCKERFILE" \
        --build-arg GITHUB_TOKEN="${GITHUB_TOKEN:-}" \
        --build-arg VERGEN_GIT_SHA="$SHORT_SHA" \
        --tag "$LOCAL_TAG" \
        --load \
        .

    echo ""
    echo "Successfully built: $LOCAL_TAG"
    echo ""
}

# Determine which images to build (default to base and node-gpu)
if [ $# -eq 0 ]; then
    # No arguments provided, build both base and node-gpu
    build_image "base"
    build_image "node-gpu"
    echo "============================================"
    echo "All images built successfully!"
    echo "============================================"
else
    # Build specific image types provided as arguments
    for IMAGE_TYPE in "$@"; do
        build_image "$IMAGE_TYPE"
    done
fi
