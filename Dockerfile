# Dockerfile
FROM apache/airflow:2.5.0-python3.8

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
