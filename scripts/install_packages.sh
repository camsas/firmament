#!/bin/bash
if [[ ! -f include/bash_header.sh ]]; then
    echo "Please run this script from the Firmament root directory."
    exit
else
    source include/bash_header.sh
fi

# Valid targets: unix, ia64
TARGET="unix"

OS_ID=$(lsb_release -i -s)
OS_RELEASE=$(lsb_release -r -s)
ARCH_UNAME=$(uname -m)
ARCH=$(get_arch "${ARCH_UNAME}")
ARCHX=$(get_archx "${ARCH_UNAME}")

print_subhdr ${OS_ID}-${OS_RELEASE}

# If we are running on a Debian-based system, a couple of dependencies
# are packaged, so we prompt the user to allow us to install them.
# Currently, we support Ubuntu and Debian.
if [[ -f include/pkglist.${OS_ID}-${OS_RELEASE} ]]; then
    source include/pkglist.${OS_ID}-${OS_RELEASE}
elif [[ -f include/pkglist.${OS_ID}-generic ]]; then
    source include/pkglist.${OS_ID}-generic
else
    source include/pkglist.generic
fi

UBUNTU_x86_PKGS="${BASE_PKGS} ${CLANG_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${PERFTOOLS_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS} ${HDFS_PKGS}"
DEBIAN_x86_PKGS="${BASE_PKGS} ${CLANG_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${PERFTOOLS_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS} ${HDFS_PKGS}"
DEBIAN_ia64_PKGS="${BASE_PKGS} ${COMPILER_PKGS} ${GOOGLE_PKGS} ${BOOST_PKGS} ${PION_PKGS} ${MISC_PKGS} ${HDFS_PKGS}"

# Super-user? Should I run sudo commands non-interactively?
USER=$(whoami)
if [[ ${USER} == "root" ]]; then
    NONINTERACTIVE=1
else
    NONINTERACTIVE=${NONINTERACTIVE:-0}
fi
if [[ ${NONINTERACTIVE} -eq 1 ]]; then
    echo "Running as root or with NONINTERACTIVE=1, so will attempt to sort things out non-interactively."
fi


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
        if [[ ${NONINTERACTIVE} -eq 1 ]]; then
                sudo apt-get -y install ${MISSING_PKGS}
                else
                echo
                    echo "Please install them using the following commmand: "
                        echo "$ sudo apt-get install ${MISSING_PKGS}"
                            echo
                                exit 1
                                fi
    else
        echo -n "All required packages are installed."
        echo_success
        echo
    fi
}

###############################################################################

print_hdr "FETCHING & INSTALLING EXTERNAL DEPENDENCIES"

# On older Ubuntu versions, we must add the boost-latest PPA for boost-1.55
if [[ ${TRAVIS} != "true" && ${OS_ID} == 'Ubuntu' && ( ${OS_RELEASE} == '12.04' || ${OS_RELEASE} == '13.10' ) ]]; then
    echo "Adding boost-latest PPA..."
    if [[ ${NONINTERACTIVE} -eq 1 ]]; then
        echo "Installing..."
        sudo add-apt-repository -y ppa:boost-latest/ppa
        sudo apt-get -y update
    else
        echo_failure
        echo "Please add the \"boost-latest\" PPA to your apt respositories:"
        echo
        echo "$ sudo add-apt-repository ppa:boost-latest/ppa"
        exit 1
    fi

fi

# N.B.: We explicitly exclude the SCC here since we need to build on the MCPC
# in the case of the SCC. The MCPC runs 64-bit Ubuntu, but the SCC cores run
# some custom stripped-down thing that does not have packages.
if [[ ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ]];
then
    echo "Detected ${OS_ID} on ${ARCHX}..."
    check_os_release_compatibility ${OS_ID} ${OS_RELEASE}
    check_dpkg_packages ${OS_ID} ${ARCHX}
else
    echo "Operating systems other than Ubuntu (>=10.04) and Debian are not"
    echo "currently supported for automatic configuration."
    ask_continue
fi
