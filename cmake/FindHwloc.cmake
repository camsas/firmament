# FindHwloc module from https://github.com/Eyescale/CMake
# BSD-licensed
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# - Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of Eyescale Software GmbH nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# Copyright (c) 2012 Marwan Abdellah <marwan.abdellah@epfl.ch>
#                    Daniel Nachbaur <daniel.nachbaur@epfl.ch>
#               2013 Stefan.Eilemann@epfl.ch
#               2016 Malte Schwarzkopf <malte@csail.mit.edu>

# Use pkg-config to fetch the contents of the .pc file
# After that, use the directories refer to the libraries and
# also the headers

if(NOT PKGCONFIG_FOUND)
  find_package(PkgConfig QUIET)
endif()

if(HWLOC_ROOT)
  set(ENV{PKG_CONFIG_PATH} "${HWLOC_ROOT}/lib/pkgconfig")
else()
  foreach(PREFIX ${CMAKE_PREFIX_PATH})
    set(PKG_CONFIG_PATH "${PKG_CONFIG_PATH}:${PREFIX}/lib/pkgconfig")
  endforeach()
  set(ENV{PKG_CONFIG_PATH} "${PKG_CONFIG_PATH}:$ENV{PKG_CONFIG_PATH}")
endif()

if(hwloc_FIND_REQUIRED)
  set(_hwloc_OPTS "REQUIRED")
endif()
if(hwloc_FIND_QUIETLY)
  set(_hwloc_OPTS "QUIET")
endif()
if(hwloc_FIND_REQUIRED AND hwloc_FIND_QUIETLY)
  set(_hwloc_OPTS "REQUIRED QUIET")
endif()
if(NOT hwloc_FIND_QUIETLY)
  set(_hwloc_output 1)
endif()

if(hwloc_FIND_VERSION)
  if(hwloc_FIND_VERSION_EXACT)
    pkg_check_modules(hwloc ${_hwloc_OPTS} hwloc=${hwloc_FIND_VERSION})
  else()
    pkg_check_modules(hwloc ${_hwloc_OPTS} hwloc>=${hwloc_FIND_VERSION})
  endif()
else()
  pkg_check_modules(hwloc ${_hwloc_OPTS} hwloc)
endif()

if(hwloc_FOUND)
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(hwloc DEFAULT_MSG hwloc_LIBRARIES)

  if(NOT ${hwloc_VERSION} VERSION_LESS 1.7.0)
    set(hwloc_GL_FOUND 1)
  endif()
else()
  message(FATAL_ERROR "Failed to find hwloc!")
endif()
