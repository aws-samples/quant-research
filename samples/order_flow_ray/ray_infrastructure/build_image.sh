#!/bin/bash
# Build and push custom Anyscale image

set -e

# Configuration
IMAGE_NAME="ray_anyscale_custom"
TAG="latest"
REGISTRY="614393260192.dkr.ecr.us-east-1.amazonaws.com"
AWS_PROFILE="blitvinfdp"

echo "Building custom Anyscale image..."

# Copy source code to build context
echo "Copying source code..."
cp -r ../src .

# Authenticate Docker to ECR
echo "Authenticating to ECR..."
aws ecr get-login-password --region us-east-1 --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${REGISTRY}

# Build the image for AMD64 platform (Anyscale clusters)
echo "Building Docker image for AMD64 platform..."
docker build --platform linux/amd64 -t ${IMAGE_NAME}:${TAG} .

# Tag for registry
docker tag ${IMAGE_NAME}:${TAG} ${REGISTRY}/${IMAGE_NAME}:${TAG}

# Push to registry
echo "Pushing to ECR..."
docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}

echo "Image built and pushed: ${REGISTRY}/${IMAGE_NAME}:${TAG}"
echo "Use this image in your Anyscale workspace configuration"

# Cleanup
echo "Cleaning up..."
rm -rf src