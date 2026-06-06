FROM apache/airflow:2.9.3

USER root

RUN apt update && \
    apt install -y openjdk-17-jdk && \
    apt clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install \
pyspark==3.5.1 \
kafka-python \
dbt-core==1.7.19 \
dbt-postgres==1.7.19