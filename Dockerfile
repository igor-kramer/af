FROM apache/airflow:2.5.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim
RUN apt-get install -y freetds-dev
USER airflow
RUN pip install apache-airflow-providers-microsoft-mssql \
                apache-airflow-providers-papermill \
                apache-airflow-providers-postgres \
                airflow-clickhouse-plugin[pandas]



