FROM apache/airflow:latest
USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt