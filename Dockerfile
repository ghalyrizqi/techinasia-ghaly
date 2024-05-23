FROM apache/airflow:latest
RUN pip install apache-airflow-providers-docker pandas sqlalchemy psycopg2 requests beautifulsoup4
# RUN pip install cryptographic

