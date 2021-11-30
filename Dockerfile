FROM python:3.8-alpine3.14

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ./main.py /main.py
COPY ./requirements.txt /requirements.txt

RUN apk add --no-cache --virtual .build-deps postgresql-libs zlib-dev jpeg-dev gcc musl-dev postgresql-dev \
    && pip install --upgrade pip \
    && pip install -r requirements.txt
