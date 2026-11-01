FROM apache/airflow:2.10.3-python3.11

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root dags /opt/airflow/dags