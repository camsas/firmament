#!/bin/bash
source include/bash_header.sh

mkdir -p ext
cd ext

# If we are running on a Debian-based system, a couple of dependencies 
# are packaged, so we prompt the user to allow us to install them.
# Currently, we support Ubuntu and Debian.
UBUNTU_PKGS="clang libgoogle-perftools0 libgoogle-perftools-dev"
DEBIAN_PKGS="clang libgoogle-perftools0 libgoogle-perftools-dev"

GFLAGS_VER="1.6-1"

#################################

function get_arch() {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "i386"
  elif [[ $1 == "amd64" ]]; then
    echo "amd64"
  else
    echo "unknown"
  fi
}

function get_archx {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "x86"
  elif [[ $1 == "amd64" ]]; then
    echo "x86_64"
  else
    echo "unknown"
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
    PKG_RES=$(dpkg-query -W -f='${Package}\n' ${i} 2>/dev/null)
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

###############################################################################

print_hdr "FETCHING & INSTALLING EXTERNAL DEPENDENCIES"

OS_ID=$(lsb_release -i -s) 
ARCH_UNAME=$(uname -m)
ARCH=$(get_arch "${ARCH_UNAME}")
ARCHX=$(get_archx "${ARCH_UNAME}")

if [[ ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ]]; then
  echo "Detected $OS_ID..."
  if [ -f "${OS_ID}-ok" ]; then
    echo -n "${OS_ID} package check previously ran successfully; skipping. "
    echo "Delete .${OS_ID}-ok file if you want to re-run it."
  else
    check_dpkg_packages ${OS_ID}
  fi
fi

## Google Log macros
print_subhdr "GOOGLE GLOG LIBRARY"
get_dep_svn "google-glog" "googlecode"
cd google-glog-svn
echo -n "Building google-glog library..."
RES=$(./configure && make --quiet 2>/dev/null)
print_succ_or_fail $RES
cd ..

## Google Gflags command line flag library
print_subhdr "GOOGLE GFLAGS LIBRARY"
if [[ ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ]]; then
  PKG_RES1=$(dpkg-query -l | grep "libgflags0" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "libgflags-dev" 2>/dev/null)
  if [[ $PKG_RES1 != "" && $PKG_RES2 != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  else
    get_dep_deb "google-gflags" "http://google-gflags.googlecode.com/files/libgflags0_${GFLAGS_VER}_${ARCH}.deb"
    get_dep_deb "google-gflags" "http://google-gflags.googlecode.com/files/libgflags-dev_${GFLAGS_VER}_${ARCH}.deb"
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
  echo "Non-Debian OS support missing from fetch-externals.sh for: google-gflags"
  echo_failure
  exit 1
fi
cd ..

# Mahout
#wget -N <URL> 
#if [ url-timestamp -ot <arch> ]
#then
#    tar -x -j -v --file=mahout-0.3.tar.bz2 mahout-0.3/mahout-core-0.3.jar mahout-0.3/mahout-math-0.3.jar mahout-0.3/mahout-collections-0.3.jar mahout-0.3/lib/commons-logging-1.1.1.jar mahout-0.3/lib/slf4j-api-1.5.8.jar mahout-0.3/lib/slf4j-jcl-1.5.8.jar mahout-0.3/lib/uncommons-maths-1.2.jar mahout-0.3/lib/gson-1.3.jar mahout-0.3/lib/hadoop-core-0.20.2.jar
#    touch -r mahout-0.3.tar.bz2 mahout-timestamp
#fi

# RCCE
#if [ -d rcce ]
#then
#	svn up rcce/
#else
#	mkdir -p rcce
#	svn co http://marcbug.scc-dc.com/svn/repository/trunk/rcce/ rcce/
#fi

# iRCCE
#wget -N http://communities.intel.com/servlet/JiveServlet/download/110482-19045/iRCCE.tar.zip
#if [ ircce-timestamp -ot iRCCE.tar.zip ]
#then
#	unzip iRCCE.tar.zip
#	tar -xf iRCCE.tar
#	touch -r iRCCE.tar.zip icce-timestamp
#fi

# GSON
#curl -v -z gson-timestamp -o google-gson-1.7.1-release.zip -R http://google-gson.googlecode.com/files/google-gson-1.7.1-release.zip
#if [ gson-timestamp -ot google-gson-1.7.1-release.zip ]
#then
#    unzip google-gson-1.7.1-release.zip google-gson-1.7.1/gson-1.7.1.jar
#    touch -r google-gson-1.7.1-release.zip gson-timestamp
#fi

