# FindLibjansson by Jon Szymaniak
#
# MIT-licensed, see
# https://github.com/jynik/OOKiedokie/blob/master/cmake/Modules/FindLibjansson.cmake

if(NOT LIBJANSSON_FOUND)
  pkg_check_modules (LIBJANSSON_PKG libjansson)
  find_path(LIBJANSSON_INCLUDE_DIR NAMES jansson.h
    PATHS
    ${LIBJANSSON_PKG_INCLUDE_DIRS}
    /usr/include
    /usr/local/include
  )

  find_library(LIBJANSSON_LIBRARIES NAMES jansson 
    PATHS
    ${LIBJANSSON_PKG_LIBRARY_DIRS}
    /usr/lib
    /usr/local/lib
  )

if(LIBJANSSON_INCLUDE_DIR AND LIBJANSSON_LIBRARIES)
  set(LIBJANSSON_FOUND TRUE CACHE INTERNAL "libjansson found")
  message(STATUS "Found libjansson: ${LIBJANSSON_INCLUDE_DIR}, ${LIBJANSSON_LIBRARIES}")
else(LIBJANSSON_INCLUDE_DIR AND LIBJANSSON_LIBRARIES)
  set(LIBJANSSON_FOUND FALSE CACHE INTERNAL "libjansson found")
  message(STATUS "libjansson not found.")
endif(LIBJANSSON_INCLUDE_DIR AND LIBJANSSON_LIBRARIES)

mark_as_advanced(LIBJANSSON_INCLUDE_DIR LIBJANSSON_LIBRARIES)

endif(NOT LIBJANSSON_FOUND)
