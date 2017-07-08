FROM ubuntu:14.04
MAINTAINER firmament-dev@camsas.org

RUN apt-get -y update
RUN apt-get -y install git-core build-essential make && git clone https://github.com/camsas/firmament /firmament
RUN bash -c 'source /firmament/include/pkglist.Ubuntu-14.04 && sudo apt-get -y install ${BASE_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${BOOST_PKGS} ${MISC_PKGS} ${HDFS_PKGS} ${PION_PKGS}'
RUN mkdir /var/log/firmament && cd /firmament && make && cd build && make -j12
COPY firmament-default.conf /firmament/default.conf
RUN apt-get clean
