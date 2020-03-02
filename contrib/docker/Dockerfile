FROM ubuntu:16.04
MAINTAINER firmament-dev@camsas.org

RUN apt-get -y update
RUN apt-get --no-install-recommends -y install git-core build-essential make && git clone https://github.com/camsas/firmament /firmament
RUN bash -c 'source /firmament/include/pkglist.Ubuntu-16.04 && apt-get --no-install-recommends -y install ${BASE_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${BOOST_PKGS} ${MISC_PKGS} ${HDFS_PKGS} ${PION_PKGS}'
RUN mkdir /var/log/firmament && cd /firmament && make && cd build && make -j12
COPY firmament-default.conf /firmament/default.conf
RUN apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base
