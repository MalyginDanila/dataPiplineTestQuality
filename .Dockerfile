FROM apache/airflow:2.9.0-python3.11

USER root
RUN mkdir -p /opt/airflow/data
RUN chown -R airflow:airflow /opt/airflow

USER airflow 

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY dags /opt/airflow/dags
COPY src /opt/airflow/src
COPY tests /opt/airflow/tests

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"