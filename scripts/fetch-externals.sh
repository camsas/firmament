#!/bin/bash
if [[ ! -f include/bash_header.sh ]]; then
  echo "Please run this script from the Firmament root directory."
  exit
else
  source include/bash_header.sh
fi

# Valid targets: unix, scc, ia64
TARGET="unix"

mkdir -p ext
cd ext
EXT_DIR=${PWD}

# Headline versions of different libraries. These are supported; others might
# be pulled in by specific distributions or OS versions.
GFLAGS_VER="2.0"
GLOG_VER="HEAD"
HWLOC_VER="1.9"
PROTOBUF_VER="2.4.1"
BOOST_VER="1.55"
CS2_VER="4.6"
PION_VER="5.0.5"

OS_ID=$(lsb_release -i -s)
OS_RELEASE=$(lsb_release -r -s)
ARCH_UNAME=$(uname -m)
ARCH=$(get_arch "${ARCH_UNAME}")
ARCHX=$(get_archx "${ARCH_UNAME}")

# Setting compiler variables globally for the ext packages.
export CC=clang
export CXX=clang++
export CXXFLAGS="$CXXFLAGS -std=c++11"

# If we are running on a Debian-based system, a couple of dependencies
# are packaged, so we prompt the user to allow us to install them.
# Currently, we support Ubuntu and Debian.
if [[ -f ../include/pkglist.${OS_ID}-${OS_RELEASE} ]]; then
  source ../include/pkglist.${OS_ID}-${OS_RELEASE}
elif [[ -f ../include/pkglist.${OS_ID}-generic ]]; then
  source ../include/pkglist.${OS_ID}-generic
else
  source ../include/pkglist.generic
fi

UBUNTU_x86_PKGS="${BASE_PKGS} ${CLANG_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${PERFTOOLS_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS}"
DEBIAN_x86_PKGS="${BASE_PKGS} ${CLANG_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${PERFTOOLS_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS}"
DEBIAN_ia64_PKGS="${BASE_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS}"

#################################

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
    if [[ $2 == "x86" || $2 == "x86_64" ]]; then
      OS_PKGS=${UBUNTU_x86_PKGS}
    else
      echo "No package list available for OS $1 on $2!"
      echo "Sorry, you're on your own now..."
    fi
  elif [[ $1 == "Debian" ]]; then
    if [[ $2 == "x86" || $2 == "x86_64" ]]; then
      OS_PKGS=${DEBIAN_x86_PKGS}
    elif [[ $2 == "ia64" ]]; then
      OS_PKGS=${DEBIAN_ia64_PKGS}
    else
      echo "No package list available for OS $1 on $2!"
      echo "Sorry, you're on your own now..."
    fi
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
    echo -n "All required packages are installed."
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

function get_dep_git {
  REPO=$2
  NAME=$1

  if [ -d ${NAME}-git ]
  then
    cd ${NAME}-git
    git fetch
    cd ..
  else
    mkdir -p ${NAME}-git
    git clone ${REPO} ${NAME}-git/
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

##################################

function get_dep_wget {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
    touch -r ${FILE} ${NAME}-timestamp
  fi
}


###############################################################################

print_hdr "FETCHING & INSTALLING EXTERNAL DEPENDENCIES"

SCC_CC_SCRIPT="/opt/compilerSetupFiles/crosscompile.sh"

# N.B.: We explicitly exclude the SCC here since we need to build on the MCPC
# in the case of the SCC. The MCPC runs 64-bit Ubuntu, but the SCC cores run
# some custom stripped-down thing that does not have packages.
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  echo "Detected ${OS_ID} on ${ARCHX}..."
  check_os_release_compatibility ${OS_ID} ${OS_RELEASE}
  if [ -f "${OS_ID}-ok" ]; then
    echo -n "${OS_ID} package check previously ran successfully; skipping. "
    echo "Delete .${OS_ID}-ok file if you want to re-run it."
  else
    echo "Checking if necessary packages are installed..."
    check_dpkg_packages ${OS_ID} ${ARCHX}
  fi
elif [[ ${TARGET} == "scc" ]]; then
  echo "Building for the SCC. Note that you MUST build on the MCPC, and "
  echo "that ${SCC_CC_SCRIPT} MUST exist and be accessible."
  ask_continue
  source ${SCC_CC_SCRIPT}
else
  echo "Operating systems other than Ubuntu (>=10.04) and Debian are not"
  echo "currently supported for automatic configuration."
  ask_continue
fi

# Google Gflags command line flag library
print_subhdr "GOOGLE GFLAGS LIBRARY"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) && ${ARCHX} != "ia64" ]];
then
  PKG_RES1=$(dpkg-query -l | grep "libgflags0" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "libgflags-dev" 2>/dev/null)
  if [[ $PKG_RES1 != "" && $PKG_RES2 != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  else
    get_dep_deb "google-gflags" "http://gflags.googlecode.com/files/libgflags0_${GFLAGS_VER}-1_${ARCH}.deb"
    get_dep_deb "google-gflags" "http://gflags.googlecode.com/files/libgflags-dev_${GFLAGS_VER}-1_${ARCH}.deb"
    echo -n "libgflags not installed."
    echo_failure
    echo "Please install libgflags0_${GFLAGS_VER}_${ARCH}.deb "
    echo "and libgflags-dev_${GFLAGS_VER}_${ARCH}.deb from the ${EXT_DIR}/ directiory:"
    echo
    echo "$ cd ${EXT_DIR}"
    echo "$ sudo dpkg -i libgflags0_${GFLAGS_VER}-1_${ARCH}.deb"
    echo "$ sudo dpkg -i libgflags-dev_${GFLAGS_VER}-1_${ARCH}.deb"
    exit 1
 fi
else
  # non-deb OS -- need to get tarball and extract, config, make & install
  echo "Downloading and extracting release tarball for Google gflags library..."
#  GFLAGS_BUILD_DIR=${EXT_DIR}/google-gflags-build
#  mkdir -p ${GFLAGS_BUILD_DIR}
  get_dep_arch "google-gflags" "http://gflags.googlecode.com/files/gflags-${GFLAGS_VER}.tar.gz"
  cd gflags-${GFLAGS_VER}
  echo -n "Building google-gflags library..."
#  RES=$(./configure && make --quiet && make --quiet install 2>/dev/null)
  RES=$(./configure && make --quiet 2>/dev/null)
  print_succ_or_fail $RES
  if [[ ${TARGET} != "scc" ]]; then
    echo "google-gflags library (v${GFLAGS_VER}) was built in ${GFLAGS_DIR}. "
    echo "Please run the following commands to install it: "
    echo
    echo "$ cd ${EXT_DIR}/${GFLAGS_DIR}"
    echo "$ sudo make install"
    echo
    echo "... and then re-run."
    exit 1
  fi
  cd ${EXT_DIR}
fi

## Google Log macros
## N.B.: This must go *after* gflags, since glog will notice that gflags is
## installed, and produce extra options (default flags like --logtostderr).
print_subhdr "GOOGLE GLOG LIBRARY"
GLOG_DIR=google-glog-svn
GLOG_INSTALL_FILE="/usr/lib/pkgconfig/libglog.pc"
#GLOG_BUILD_DIR=${EXT_DIR}/google-glog-build
#mkdir -p ${GLOG_BUILD_DIR}
if [[ ${TARGET} == "scc" || ! -f ${GLOG_INSTALL_FILE} ]]; then
  get_dep_svn "google-glog" "googlecode"
  cd ${GLOG_DIR}
  echo -n "Building google-glog library..."
  if [[ ${TARGET} == "scc" ]]; then
    RES=$(./configure --prefix=${GLOG_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
  else
    RES=$(./configure --prefix=/usr && make --quiet 2>/dev/null)
  fi
  print_succ_or_fail $RES
  if [[ ${TARGET} != "scc" ]]; then
    echo "google-glog library (v${GLOG_VER}) was built in ${GLOG_DIR}. "
    echo "Please run the following commands to install it: "
    echo
    echo "$ cd ${EXT_DIR}/${GLOG_DIR}"
    echo "$ sudo make install"
    echo
    echo "... and then re-run."
    exit 1
  fi
else
  echo -n "Already installed!"
  print_succ_or_fail 0
fi
cd ${EXT_DIR}

## Google unit testing library
print_subhdr "GOOGLE TEST LIBRARY FOR C++"
get_dep_svn "googletest" "googlecode"
cd googletest-svn/make
if [[ ${OS_ID} == 'Ubuntu' && ( ${OS_RELEASE} == '11.10' || ${OS_RELEASE} == '12.04' ) ]]; then
  echo "Applying Ubuntu 11.10/12.04-specific patch to googletest library..."
  patch -p0 -s -N -r - < ${EXT_DIR}/../scripts/fix-gtest-ubuntu.diff
fi
echo -n "Building googletest library..."
#RES=$(make all --quiet 2>/dev/null)
RES=$(make all)
print_succ_or_fail ${RES}
cd ${EXT_DIR}

## Thread-safe STL containers
print_subhdr "THREAD-SAFE STL CONTAINERS"
get_dep_svn "thread-safe-stl-containers" "googlecode"
cd thread-safe-stl-containers-svn
RES=$(ls *.h)
print_succ_or_fail ${RES}
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

## hwloc library
if [[ ${TARGET} == "scc" || ( ${OS_ID} != "Ubuntu" || ${OS_ID} != "Debian" ) ]];
then
  print_subhdr "HWLOC"
  # Get hwloc archive
  echo "Downloading and extracting hwloc v${HWLOC_VER}..."
  get_dep_arch "hwloc" "http://www.open-mpi.org/software/hwloc/v${HWLOC_VER}/downloads/hwloc-${HWLOC_VER}.tar.gz"
  HWLOC_EXTRACT_DIR=${EXT_DIR}/hwloc-${HWLOC_VER}
  HWLOC_BUILD_DIR=${EXT_DIR}/hwloc-build
  mkdir -p ${HWLOC_BUILD_DIR}
  cd ${HWLOC_EXTRACT_DIR}
  echo -n "Building..."
  RES=$(./configure --prefix=${HWLOC_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
  print_succ_or_fail $RES
  cd ${EXT_DIR}
fi


## pb2json library (converts protobufs to JSON)
print_subhdr "PB2JSON LIBRARY"
get_dep_git "pb2json" "https://github.com/ms705/pb2json"
cd pb2json-git/
echo -n "Building pb2json library..."
RES=$(make)
print_succ_or_fail ${RES}
cd ${EXT_DIR}


## PION library (for integrated web server)
print_subhdr "PION HTTP SERVER LIBRARY"
PION_BUILD_DIR=${EXT_DIR}/pion-build
PION_INSTALL_FILE="${PION_BUILD_DIR}/lib/pkgconfig/pion.pc"
if [[ ! -f ${PION_INSTALL_FILE} ]]; then
  PION_DIR=pion-git
  mkdir -p ${PION_BUILD_DIR}
  get_dep_git "pion" "https://github.com/splunk/pion"
  cd ${PION_DIR}/
  git checkout -q ${PION_VER}
  echo -n "Generating build infrastructure..."
  RES1=$(./autogen.sh)
  print_succ_or_fail ${RES1}
  echo -n "Configuring pion library..."
  RES2=$(./configure --disable-tests --prefix=${PION_BUILD_DIR})
  print_succ_or_fail ${RES2}
  echo -n "Building pion library..."
  RES3=$(make)
  print_succ_or_fail ${RES3}
  echo -n "Installing pion library..."
  RES4=$(make install)
  print_succ_or_fail ${RES4}
fi
cd ${EXT_DIR}


## cpplint.py (linter script)
print_subhdr "CPPLINT HELPER SCRIPT"
get_dep_wget "cpplint" "http://google-styleguide.googlecode.com/svn/trunk/cpplint/cpplint.py"


## CS2 solver code for min-flow scheduler
print_subhdr "CS2 MIN COST FLOW SOLVER"
get_dep_wget "cs2" "http://igsystems.com/cs2/cs2-${CS2_VER}.tar"
tar -xf cs2-${CS2_VER}.tar
cd cs2-${CS2_VER}
if [[ ! -f cs2.exe ]]; then
  echo -n "Building..."
  # add patched makefile
  cp ${EXT_DIR}/../scripts/cs2-custom-makefile makefile
  RES=$(make 2>/dev/null)
else
  RES=0
fi
print_succ_or_fail ${RES}
cd ${EXT_DIR}


## Cake (for building libDIOS)
print_subhdr "CAKE BUILD TOOL"
get_dep_git "cake" "https://github.com/Zomojo/Cake.git"
cd cake-git
RES=$(ls cake)
print_succ_or_fail ${RES}
cd ${EXT_DIR}


## libDIOS checkout or ask user to request tarball
print_hdr "CHECKING OUT LIBDIOS CODE"
echo "OPTIONAL: Do you want to check out the libDIOS code base into the 'ext' directory?"
echo "Note that this requires access to the CL NFS server for now (as the libDIOS repo is "
echo "hosted there). Please contact malte.schwarzkopf@cl.cam.ac.uk if you do not have access."
echo -n "Do you want to check out the libDIOS code? [yN] "
CONT=$(ask_continue_graceful)
if [[ ${CONT} == "0" ]]; then
  get_dep_git "libdios" "slogin.cl.cam.ac.uk:/usr/groups/netos/libdios/"
  cd libdios-git
  export PATH=${PATH}:${EXT_DIR}/cake-git/
  RES=$(./build.sh --quiet)
  print_succ_or_fail ${RES}
  if [[ ! ${RES} ]]; then
    echo "Failed to build libDIOS. Please see above for errors or warnings."
    echo "N.B.: Please note that this script treats warnings as errors; so binaries may have nevertheless been built."
  fi
  cd ${EXT_DIR}
else
  echo_skipped
  echo "Skipping."
fi


touch ${EXT_DIR}/.ext-ok
