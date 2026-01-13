# Ray Infrastructure Setup

This directory contains Docker and Anyscale configuration for the order flow pipeline.

## Files

- **`Dockerfile`**: Custom Anyscale image with order flow dependencies
- **`requirements.txt`**: Python dependencies for the pipeline
- **`build_image.sh`**: Script to build and push Docker image
- **`anyscale_config.yaml`**: Anyscale workspace configuration
- **`deploy.md`**: Deployment instructions

## Quick Setup

1. **Build custom image:**
   ```bash
   cd ray_infrastructure
   chmod +x build_image.sh
   ./build_image.sh
   ```

2. **Create Anyscale workspace:**
   ```bash
   anyscale workspace_v2 create --config anyscale_config.yaml
   ```

3. **Connect and develop:**
   - Use VS Code extension or Ray client
   - All dependencies pre-installed
   - Ready for order flow processing

## Image Contents

- **Base**: `anyscale/ray:2.9.0-py310`
- **Dependencies**: polars, boto3, ray[data], ML libraries
- **Code**: `/home/ray/src/` (your pipeline code)
- **Environment**: PYTHONPATH configured

## Compute Configuration

- **Head node**: 8CPU-32GB
- **CPU workers**: 16CPU-64GB (0-10 nodes, spot)
- **Memory workers**: 32CPU-128GB (0-5 nodes, spot)
- **Auto-scaling**: Based on workload demand