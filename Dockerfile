FROM quay.io/astronomer/astro-runtime:12.4.0
COPY include/gcp-service-account.json /usr/local/airflow/gcp-service-account.json