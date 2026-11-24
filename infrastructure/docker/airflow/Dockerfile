FROM apache/airflow:2.10.5

USER root

# Install Java 17 for Apache Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install project runtime dependencies
COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow[statsd]==2.10.5" \
    -r /requirements-airflow.txt