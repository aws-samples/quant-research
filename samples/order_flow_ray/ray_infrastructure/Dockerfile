# Custom Anyscale Docker Image for Order Flow Pipeline

FROM anyscale/ray:2.9.0-py310

# Set working directory
WORKDIR /home/ray

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Switch back to ray user
USER ray

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create src directory for application code
RUN mkdir -p /home/ray/order_flow_ray/src

# Debug: List current directory contents
RUN ls -la

# Copy application code (copy ../src contents to ray_infrastructure/src first)
COPY src/ /home/ray/order_flow_ray/src/

# Set Python path
ENV PYTHONPATH=/home/ray/order_flow_ray/src:${PYTHONPATH}

# Default command
CMD ["python", "-c", "import ray; ray.init(); print('Ray initialized successfully')"]