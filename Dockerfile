# Dockerfile
FROM apache/airflow:2.5.0-python3.8

# Install additional Python packages
RUN pip install pymysql pandas requests sqlalchemy
