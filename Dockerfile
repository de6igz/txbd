FROM apache/airflow:3.1.3

RUN pip install --no-cache-dir minio pymongo
