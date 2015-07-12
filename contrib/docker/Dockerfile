FROM ubuntu:14.04
MAINTAINER firmament-dev@camsas.org

RUN apt-get -y update
RUN apt-get -y install git build-essential make python subversion wget clang
RUN git clone https://github.com/ms705/firmament /firmament
ENV NONINTERACTIVE 1
RUN mkdir /var/log/firmament && cd /firmament && make all
COPY firmament-default.conf /firmament/default.conf
RUN apt-get clean
