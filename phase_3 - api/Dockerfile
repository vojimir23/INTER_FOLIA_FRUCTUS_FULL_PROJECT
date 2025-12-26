# main.py folder Dockerfile
FROM python:3.10-slim

# Install system tools
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code
COPY . .

# CRITICAL: Create the empty folder where we will mount the volume later
RUN mkdir -p /app/images

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]