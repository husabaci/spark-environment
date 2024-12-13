FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jre-headless procps

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

ENV PYTHONBUFFERED 1

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app