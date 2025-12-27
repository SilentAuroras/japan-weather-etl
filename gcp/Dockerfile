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

# Expose 8080 for cloud run
EXPOSE 8080

# Run main
CMD ["python", "main.py"]