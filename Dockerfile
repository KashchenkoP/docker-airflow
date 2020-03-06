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

#
ARG HADOOP_DISTRO="cdh"
ARG HADOOP_MAJOR="5"
ARG HADOOP_DISTRO_VERSION="5.11.0"
ARG HADOOP_VERSION="2.6.0"
ARG HADOOP_URL="https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/"
ARG HADOOP_DOWNLOAD_URL="${HADOOP_URL}hadoop-${HADOOP_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz"
ARG HADOOP_TMP_FILE="/tmp/hadoop.tar.gz"

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Install Hadoop and Hive
# It is done in one step to share variables.
ENV HADOOP_HOME="/opt/hadoop-cdh" HIVE_HOME="/opt/hive"

RUN set -ex \
RUN buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
RUN apt-get update -yqq
RUN apt-get upgrade -yqq
RUN apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales  \
RUN mkdir -pv "${HADOOP_HOME}"
RUN curl --fail --location "${HADOOP_DOWNLOAD_URL}" --output "${HADOOP_TMP_FILE}"
RUN tar xzf "${HADOOP_TMP_FILE}" --absolute-names --strip-components 1 -C "${HADOOP_HOME}"
RUN rm "${HADOOP_TMP_FILE}"
RUN echo "Installing Hive"
RUN HIVE_VERSION="1.1.0"
RUN HIVE_URL="${HADOOP_URL}hive-${HIVE_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz"
RUN HIVE_VERSION="1.1.0"
RUN HIVE_TMP_FILE="/tmp/hive.tar.gz"
RUN mkdir -pv "${HIVE_HOME}"
RUN mkdir -pv "/user/hive/warehouse"
RUN chmod -R 777 "${HIVE_HOME}"
RUN chmod -R 777 "/user/"
RUN curl --fail --location  "${HIVE_URL}" --output "${HIVE_TMP_FILE}"
RUN tar xzf "${HIVE_TMP_FILE}" --strip-components 1 -C "${HIVE_HOME}"
RUN cd & rm "${HIVE_TMP_FILE}"

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_S --log-level WARNING

RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen
RUN locale-gen
RUN update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
RUN useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow
RUN pip install -U pip setuptools wheel
RUN pip install pytz
RUN pip install pyOpenSSL
RUN pip install ndg-httpsclient
RUN pip install pyasn1
RUN pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION}
RUN pip install 'redis==3.2'
RUN if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi
RUN apt-get purge --auto-remove -yqq $buildDeps
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf \
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
COPY jars ${AIRFLOW_HOME}/jars

ENV PATH="${PATH}:/opt/hive/bin"

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
