#!/bin/bash

# Set image name and tag, allowing override via environment variables
IMAGE_NAME="${IMAGE_NAME:-serverless-demo}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# Build the Lithops runtime Docker image using the provided Dockerfile
lithops runtime build -f Dockerfile "$FULL_IMAGE"

# Deploy the Lithops runtime
lithops runtime deploy "$FULL_IMAGE"