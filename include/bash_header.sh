#!/bin/bash

PROJECT="Firmament"

export PROJECT

function print_hdr {
  echo "+---------------------------------------------------------------------"
  echo "| $1 "
  echo "+---------------------------------------------------------------------"
}

function print_subhdr {
  echo "--> $1"
}

# Column number to place the status message
RES_COL=60
# Command to move out to the configured column number
MOVE_TO_COL="echo -en \\033[${RES_COL}G"
# Command to set the color to SUCCESS (Green)
SETCOLOR_SUCCESS="echo -en \\033[1;32m"
# Command to set the color to FAILED (Red)
SETCOLOR_FAILURE="echo -en \\033[1;31m"
# Command to set the color back to normal
SETCOLOR_NORMAL="echo -en \\033[0;39m"

# Function to print the SUCCESS status
echo_success() {
  $MOVE_TO_COL
  echo -n "["
  $SETCOLOR_SUCCESS
  echo -n $"  OK  "
  $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 0
}

# Function to print the FAILED status message
echo_failure() {
  $MOVE_TO_COL
  echo -n "["
  $SETCOLOR_FAILURE
  echo -n $"FAILED"
  $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 1
}

# Function to print the SKIPPED status message
echo_skipped() {
  $MOVE_TO_COL
  echo -n "["
  $SETCOLOR_NORMAL
  echo -n $"SKIPPED"
  $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 1
}

print_succ_or_fail() {
  if [[ $1 ]]; then
    echo_success
  else
    echo_failure
  fi
  echo
}

# Function to ask the user whether he/she would like to continue.
# Valid responses are "Y", "y", "N" and "n".
function ask_continue() {
  response=""
  while [[ $response != "n" && $response != "N" \
    && $response != "y" && $response != "Y" ]]
  do
    echo -n  "Do you want to continue? [yN] "
    read response
    if [[ $response == "" ]]; then
      break
    fi
  done
  # if we have seen an "n", "N" or a blank response (defaults to N),
  # we exit here
  if [[ $response == "n" || $response == "N" || $response == "" ]]
  then
    exit 1
  fi
}

# Function to ask the user whether he/she would like to continue.
# Valid responses are "Y", "y", "N" and "n".
# Unlike the previous variant, this returns an indication, as opposed
# to existing hard.
function ask_continue_graceful() {
  response=""
  while [[ $response != "n" && $response != "N" \
    && $response != "y" && $response != "Y" ]]
  do
    #echo -n  "Do you want to continue? [yN] "
    read response
    if [[ $response == "" ]]; then
      break
    fi
  done
  # if we have seen an "n", "N" or a blank response (defaults to N),
  # we exit here
  if [[ $response == "n" || $response == "N" || $response == "" ]]
  then
    echo 1
  elif [[ $response == "y" || $response == "Y" ]]
  then
    echo 0
  fi
}

##################################
# Functions to extract machine architecture

function get_arch() {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "i386"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "amd64"
  elif [[ $1 == "ia64" || $1 == "IA-64" ]]; then
    echo "ia64"
  else
    echo "unknown"
  fi
}

function get_archx {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "x86"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "x86_64"
  elif [[ $1 == "ia64" || $1 == "IA-64" ]]; then
    echo "ia64"
  else
    echo "unknown"
  fi
}

##################################


