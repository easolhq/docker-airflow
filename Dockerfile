FROM python:3.6.1
MAINTAINER schnie <greg@astronomer.io>

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.8.1
ENV AIRFLOW_HOME /airflow_home

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ADD requirements.txt .

# Install system dependencies.
RUN set -ex \
    && buildDeps=' \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        git \
    ' \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF \
    && echo "deb http://repos.mesosphere.com/ubuntu vivid main" > /etc/apt/sources.list.d/mesosphere.list \
    && echo "deb http://http.debian.net/debian jessie-backports main" > /etc/apt/sources.list.d/backports.list \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        apt-utils \
        curl \
        netcat \
        locales \
        dnsutils \
    && apt-get install -yqq -t jessie-backports python-requests cython libpq-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && pip install -r requirements.txt \
    && apt-get remove --purge -yqq $buildDeps libpq-dev \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Set python path so airflow can find pip installed packages.
ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}

# Add scripts.
ADD script script

# Set airflow home.
ADD airflow_home ${AIRFLOW_HOME}/
ADD astronomer astronomer
RUN cd astronomer && pip install -e .

EXPOSE 8080 5555 8793
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/script/entrypoint.sh"]
