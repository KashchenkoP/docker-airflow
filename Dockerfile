# VERSION 1.10.9
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-buster
LABEL maintainer="Puckel_"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.2' \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags
COPY sql ${AIRFLOW_HOME}/sql

# Install Hadoop and Hive
# It is done in one step to share variables.
ENV HADOOP_HOME="/opt/hadoop-cdh" HIVE_HOME="/opt/hive"


RUN HADOOP_DISTRO="cdh" \
    && HADOOP_MAJOR="5" \
    && HADOOP_DISTRO_VERSION="5.11.0" \
    && HADOOP_VERSION="2.6.0" \
    && HADOOP_URL="https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/"\
    && HADOOP_DOWNLOAD_URL="${HADOOP_URL}hadoop-${HADOOP_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz" \
    && HADOOP_TMP_FILE="/tmp/hadoop.tar.gz" \
    && mkdir -pv "${HADOOP_HOME}" \
    && curl --fail --location "${HADOOP_DOWNLOAD_URL}" --output "${HADOOP_TMP_FILE}" \
    && tar xzf "${HADOOP_TMP_FILE}" --absolute-names --strip-components 1 -C "${HADOOP_HOME}" \
    && rm "${HADOOP_TMP_FILE}" \
    && echo "Installing Hive" \
    && HIVE_VERSION="1.1.0" \
    && HIVE_URL="${HADOOP_URL}hive-${HIVE_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz" \
    && HIVE_VERSION="1.1.0" \
    && HIVE_TMP_FILE="/tmp/hive.tar.gz" \
    && mkdir -pv "${HIVE_HOME}" \
    && mkdir -pv "/user/hive/warehouse" \
    && chmod -R 777 "${HIVE_HOME}" \
    && chmod -R 777 "/user/" \
    && curl --fail --location  "${HIVE_URL}" --output "${HIVE_TMP_FILE}" \
    && tar xzf "${HIVE_TMP_FILE}" --strip-components 1 -C "${HIVE_HOME}" \
    && rm "${HIVE_TMP_FILE}"

ENV PATH "${PATH}:/opt/hive/bin"


# Install Java SDK
RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

RUN apt-get install unzip -y && \
    apt-get autoremove -y

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME


RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]