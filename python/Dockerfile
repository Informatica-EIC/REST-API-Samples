# docker image setup to run python scripts with dependencies installed via requirements.txt
FROM python:3.8.1-slim-buster as base
FROM base as builder
RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt

# RUN pip install --install-option="--prefix=/install" -r /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

