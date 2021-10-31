FROM python:3.8

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt

RUN  pip install  -r /tmp/requirements.txt

WORKDIR /s3_cli

COPY . .

RUN pip install -e .
