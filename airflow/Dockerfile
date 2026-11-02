# Imagen de Airflow para RapidGo V4: Airflow 3.3.2 (Python 3.12) + pyarrow/pandas para leer el Data Lake.
# Se instalan con el archivo de restricciones oficial de Airflow para no romper sus dependencias.
FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyarrow pandas tzdata \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
