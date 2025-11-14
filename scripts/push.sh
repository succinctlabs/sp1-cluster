#!/bin/bash
# This script pushes the image built with build.sh to a registry.
# Usage: ./push.sh [--registry ghcr|ecr]
# Defaults to ghcr

set -e

# Configuration
GHCR_REPOSITORY="ghcr.io/succinctlabs/sp1-cluster-wip"
ECR_REPOSITORY="421253708207.dkr.ecr.us-east-1.amazonaws.com/sp1-cluster"
ECR_REGION="us-east-1"
SHORT_SHA=$(git rev-parse --short HEAD)
TAG="${GITHUB_USER}-${SHORT_SHA}"

# Default to GHCR
REGISTRY="ghcr"

# Parse registry flag
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            REGISTRY="$2"
            if [[ "$REGISTRY" != "ghcr" && "$REGISTRY" != "ecr" ]]; then
                echo "Error: --registry must be either 'ghcr' or 'ecr'"
                exit 1
            fi
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

# Check required environment variables
if [ -z "$GITHUB_USER" ]; then
    echo "Error: GITHUB_USER environment variable is not set"
    echo "Please set it with: export GITHUB_USER=<your-github-username>"
    exit 1
fi

# Check required credentials based on registry
if [ "$REGISTRY" = "ghcr" ]; then
    if [ -z "$GITHUB_TOKEN" ]; then
        echo "Error: GITHUB_TOKEN environment variable is not set"
        echo "Please set it with: export GITHUB_TOKEN=<your-token>"
        exit 1
    fi
elif [ "$REGISTRY" = "ecr" ]; then
    if ! command -v aws &> /dev/null; then
        echo "Error: AWS CLI is not installed"
        echo "Please install it: https://aws.amazon.com/cli/"
        exit 1
    fi
fi

# Function to push a single image
push_image() {
    local IMAGE_TYPE=$1
    local TAG_PREFIX=""

    case "$IMAGE_TYPE" in
        base)
            TAG_PREFIX="base"
            ;;
        node-gpu)
            TAG_PREFIX="node-gpu"
            ;;
        alloy)
            TAG_PREFIX="alloy"
            ;;
        *)
            echo "Unknown image type: $IMAGE_TYPE"
            return 1
            ;;
    esac

    LOCAL_TAG="sp1-cluster-wip:${TAG_PREFIX}-${TAG}"

    # Set remote repository based on registry
    if [ "$REGISTRY" = "ecr" ]; then
        REMOTE_TAG="${ECR_REPOSITORY}:${TAG_PREFIX}-${TAG}"
    else
        REMOTE_TAG="${GHCR_REPOSITORY}:${TAG_PREFIX}-${TAG}"
    fi

    echo "============================================"
    echo "Pushing $IMAGE_TYPE image to $REGISTRY..."
    echo "Local tag: $LOCAL_TAG"
    echo "Remote tag: $REMOTE_TAG"
    echo "============================================"
    echo ""

    # Tag for remote
    docker tag "$LOCAL_TAG" "$REMOTE_TAG"

    # Push the image
    echo "Pushing Docker image..."
    docker push "$REMOTE_TAG"

    echo ""
    echo "Successfully pushed: $REMOTE_TAG"
    echo ""
}

# Login to registry
if [ "$REGISTRY" = "ecr" ]; then
    echo "Logging into Amazon ECR..."
    aws ecr get-login-password --region "$ECR_REGION" | docker login --username AWS --password-stdin "${ECR_REPOSITORY%%/*}"
    echo ""
else
    echo "Logging into GHCR..."
    echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_USER" --password-stdin
    echo ""
fi

# Determine which images to push (default to base and node-gpu)
if [ $# -eq 0 ]; then
    # No arguments provided, push both base and node-gpu
    push_image "base"
    push_image "node-gpu"
    echo "============================================"
    echo "All images pushed successfully!"
    echo "============================================"
else
    # Push specific image types provided as arguments
    for IMAGE_TYPE in "$@"; do
        push_image "$IMAGE_TYPE"
    done
fi
