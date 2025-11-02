# Use official image for python 3.14
FROM python:3.14-slim

# Set working directory
WORKDIR /app

# Copy python requirements
COPY requirements.txt .

# Instal requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY app/ .

# Run main
CMD ["python", "main.py"]