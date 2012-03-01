#!/bin/bash
source include/bash_header.sh

# Valid targets: unix, scc
TARGET="unix"

mkdir -p ext
cd ext
EXT_DIR=${PWD}

# If we are running on a Debian-based system, a couple of dependencies
# are packaged, so we prompt the user to allow us to install them.
# Currently, we support Ubuntu and Debian.
UBUNTU_PKGS="wget clang libgoogle-perftools0 libgoogle-perftools-dev libboost-math-dev libboost-system-dev libboost-thread-dev libprotobuf-dev protobuf-compiler subversion"
DEBIAN_PKGS="wget clang libgoogle-perftools0 libgoogle-perftools-dev libboost-math-dev libboost-system-dev libboost-thread-dev libprotobuf-dev protobuf-compiler subversion"

GFLAGS_VER="1.7"
GLOG_VER="HEAD"
PROTOBUF_VER="2.4.1"
BOOST_VER="1.48.0"

#################################

function get_arch() {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "i386"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "amd64"
  else
    echo "unknown"
  fi
}

function get_archx {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "x86"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "x86_64"
  else
    echo "unknown"
  fi
}

##################################

function check_os_release_compatibility() {
  print_subhdr "OS COMPATIBILITY CHECK ($1 $2)"
  if [[ $1 == "Ubuntu" ]]; then
    if [[ $2 == "10.04" || $2 == "11.04" ]]; then
      echo_failure
      echo "There are known issues running Firmament with your version " \
        "of Ubuntu ($2). See README for details and possible workarounds."
      ask_continue
    else
      echo -n "$1 $2 is compatible."
      echo_success
      echo
    fi
  elif [[ $1 == "Debian" ]]; then
    echo -n "$1 $2 is compatible."
    echo_success
    echo
    echo "WARNING: Running Firmament on Debian is currently not well tested." \
      "YMMV!"
    ask_continue
  else
    echo_failure
    echo "Unsupported OS! Proceed at your own risk..."
    ask_continue
  fi
}

##################################

function check_dpkg_packages() {
  print_subhdr "$1 PACKAGE CHECK"
  if [[ $1 == "Ubuntu" ]]; then
    OS_PKGS=${UBUNTU_PKGS}
  else
    OS_PKGS=${DEBIAN_PKGS}
  fi
  for i in ${OS_PKGS}; do
    PKG_RES=$(dpkg-query -W -f='${Status}\n' ${i} | grep -E "^install" 2>/dev/null)
    if [[ $PKG_RES == "" ]]; then
#    if [ $(echo $PKG_RES | grep "No package") ]; then
      MISSING_PKGS="${MISSING_PKGS} ${i}"
    fi
  done

  if [[ $MISSING_PKGS != "" ]]; then
    echo -n "The following packages are required to run ${PROJECT}, "
    echo "but are not currently installed: "
    echo ${MISSING_PKGS}
    echo
    echo "Please install them using the following commmand: "
    echo "$ sudo apt-get install${MISSING_PKGS}"
    echo
    exit 1
  else
    echo -n "All required Ubuntu packages are installed."
    echo_success
    echo
    touch .$1-ok
  fi
}

##################################

function get_dep_svn {
  REPO=$2
  NAME=$1
  if [[ ${REPO} == "googlecode" ]]; then
    REPO="http://${NAME}.googlecode.com/svn/trunk/"
  fi

  if [ -d ${NAME}-svn ]
  then
    svn up ${NAME}-svn/
  else
    mkdir -p ${NAME}-svn
    svn co ${REPO} ${NAME}-svn/
  fi
}

##################################

function get_dep_deb {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
#    sudo dpkg -i ${FILE}
    touch -r ${FILE} ${NAME}-timestamp
  fi
}

##################################

function get_dep_arch {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  tar -xzf ${FILE}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
    touch -r ${FILE} ${NAME}-timestamp
  fi
}


###############################################################################

print_hdr "FETCHING & INSTALLING EXTERNAL DEPENDENCIES"

OS_ID=$(lsb_release -i -s)
OS_RELEASE=$(lsb_release -r -s)
ARCH_UNAME=$(uname -m)
ARCH=$(get_arch "${ARCH_UNAME}")
ARCHX=$(get_archx "${ARCH_UNAME}")

SCC_CC_SCRIPT="/opt/compilerSetupFiles/crosscompile.sh"

# N.B.: We explicitly exclude the SCC here since we need to build on the MCPC
# in the case of the SCC. The MCPC runs 64-bit Ubuntu, but the SCC cores run
# some custom stripped-down thing that does not have packages.
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  echo "Detected $OS_ID..."
  check_os_release_compatibility ${OS_ID} ${OS_RELEASE}
  if [ -f "${OS_ID}-ok" ]; then
    echo -n "${OS_ID} package check previously ran successfully; skipping. "
    echo "Delete .${OS_ID}-ok file if you want to re-run it."
  else
    check_dpkg_packages ${OS_ID}
  fi
elif [[ ${TARGET} == "scc" ]]; then
  echo "Building for the SCC. Note that you MUST build on the MCPC, and "
  echo "that ${SCC_CC_SCRIPT} MUST exist and be accessible."
  ask_continue
else
  echo "Operating systems other than Ubuntu (>=10.04) and Debian are not"
  echo "currently supported for automatic configuration."
  exit 0
fi

## Google Log macros
print_subhdr "GOOGLE GLOG LIBRARY"
GLOG_BUILD_DIR=${EXT_DIR}/google-glog-build
mkdir -p ${GLOG_BUILD_DIR}
get_dep_svn "google-glog" "googlecode"
cd google-glog-svn
echo -n "Building google-glog library..."
RES=$(./configure --prefix=${GLOG_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
print_succ_or_fail $RES
cd ${EXT_DIR}

## Google Gflags command line flag library
print_subhdr "GOOGLE GFLAGS LIBRARY"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  PKG_RES1=$(dpkg-query -l | grep "libgflags0" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "libgflags-dev" 2>/dev/null)
  if [[ $PKG_RES1 != "" && $PKG_RES2 != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  else
    get_dep_deb "google-gflags" "http://google-gflags.googlecode.com/files/libgflags0_${GFLAGS_VER}-1_${ARCH}.deb"
    get_dep_deb "google-gflags" "http://google-gflags.googlecode.com/files/libgflags-dev_${GFLAGS_VER}-1_${ARCH}.deb"
    echo -n "libgflags not installed."
    echo_failure
    echo "Please install libgflags0_${GFLAGS_VER}_${ARCH}.deb "
    echo "and libgflags-dev_${GFLAGS_VER}_${ARCH}.deb from the ext/ directiory:"
    echo
    echo "$ sudo dpkg -i libgflags0_${GFLAGS_VER}_${ARCH}.deb"
    echo "$ sudo dpkg -i libgflags-dev_${GFLAGS_VER}_${ARCH}.deb"
 fi
else
  # non-deb OS -- need to get tarball and extract, config, make & install
  echo "Downloading and extracting release tarball for Google gflags library..."
  GFLAGS_BUILD_DIR=${EXT_DIR}/google-gflags-build
  mkdir -p ${GFLAGS_BUILD_DIR}
  get_dep_arch "google-gflags" "http://google-gflags.googlecode.com/files/gflags-${GFLAGS_VER}.tar.gz"
  cd gflags-${GFLAGS_VER}
  echo -n "Building google-gflags library..."
  RES=$(./configure --prefix=${GFLAGS_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
  print_succ_or_fail $RES
  cd ${EXT_DIR}
fi

## Google unit testing library
print_subhdr "GOOGLE TEST LIBRARY FOR C++"
get_dep_svn "googletest" "googlecode"
cd googletest-svn/make
echo -n "Building googletest library..."
#RES=$(make all --quiet 2>/dev/null)
RES=$(make all)
print_succ_or_fail $RES
cd ${EXT_DIR}

## Boost
print_subhdr "BOOST C++ LIBRARIES"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  PKG_RES=$(dpkg-query -l | grep "libboost-dev" 2>/dev/null)
  if [[ ${PKG_RES} != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  fi
else
  BOOST_VER_US=$(echo ${BOOST_VER} | sed 's/\./_/g')
  # Get Boost release archive
  echo "Downloading and extracting Boost ${BOOST_VER}..."
  get_dep_arch "boost" "http://downloads.sourceforge.net/project/boost/boost/${BOOST_VER}/boost_${BOOST_VER_US}.tar.gz"
  mkdir -p ${EXT_DIR}/boost-build
  BOOST_EXTRACT_DIR=${EXT_DIR}/boost_${BOOST_VER_US}
  cd ${BOOST_EXTRACT_DIR}
  echo "Building..."
  BOOST_BUILD_DIR=${EXT_DIR}/boost-build
  ${BOOST_EXTRACT_DIR}/bootstrap.sh --prefix=${BOOST_BUILD_DIR}
  echo "Installing... (This may take a long time!)"
  echo
  ${BOOST_EXTRACT_DIR}/b2 -d0 install
  echo_success
  echo
fi

## Protocol buffers
print_subhdr "GOOGLE PROTOCOL BUFFERS"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  PKG_RES1=$(dpkg-query -l | grep "libprotobuf" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "protobuf-compiler" 2>/dev/null)
  if [[ ${PKG_RES1} != "" && ${PKG_RES2} != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  fi
else
  # Get protobufs release archive
  echo "Downloading and extracting Google protocol buffers ${PROTOBUF_VER}..."
  get_dep_arch "protobuf" "http://protobuf.googlecode.com/files/protobuf-${PROTOBUF_VER}.tar.gz"
  mkdir -p ${EXT_DIR}/protobuf-build
  PROTOBUF_EXTRACT_DIR=${EXT_DIR}/protobuf-${PROTOBUF_VER}
  cd ${PROTOBUF_EXTRACT_DIR}
  PROTOBUF_BUILD_DIR=${EXT_DIR}/protobuf-build
  echo -n "Building..."
  RES=$(./configure --prefix=${PROTOBUF_BUILD_DIR} && make --quiet && make --quiet check && make --quiet install 2>/dev/null)
  print_succ_or_fail $RES
  cd ${EXT_DIR}
fi
