FROM apache/airflow:2.4.1

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER root
COPY src src
RUN chmod +x src

USER $AIRFLOW_UID