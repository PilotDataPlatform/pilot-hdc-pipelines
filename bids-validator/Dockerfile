FROM python:3.9-buster
USER root

ENV PYTHONDONTWRITEBYTECODE=true \
    PYTHONIOENCODING=UTF-8 \
    POETRY_VERSION=1.3.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

ENV PATH="${POETRY_HOME}/bin:${PATH}"

# RUN apt-get update -y && apt-get install -y rsync python3-pip python3-dev build-essential \
#     default-jre-headless locales nodejs npm && rm -rf /var/lib/apt/lists/*
RUN apt-get update -y
SHELL ["/bin/bash", "--login", "-c"]

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash
RUN nvm install v18.12.1
RUN npm install -g npm@9.2.0
RUN npm install -g bids-validator@1.9.9
RUN curl -sSL https://install.python-poetry.org | python3 -

SHELL ["/bin/sh", "-c"]
RUN apt-get clean && apt-get update && apt-get install -y locales
COPY ./ /dataset-validate
WORKDIR /dataset-validate
RUN chmod 555 -R /dataset-validate
COPY poetry.lock pyproject.toml ./
RUN poetry install --no-root --no-interaction
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV PATH=/root/.nvm/versions/node/v18.12.1/bin:$PATH
RUN ls
