FROM ubuntu
MAINTAINER ms705

RUN apt-get update -y
RUN apt-get install -y git make python
RUN git clone https://github.com/ms705/firmament /firmament
RUN cd /firmament && make all
