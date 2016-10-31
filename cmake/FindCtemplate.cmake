# FindCtemplate by Olaf van der Spek
#
# BSD-licensed, see https://github.com/olafvdspek/ctemplate/issues/91
#
# - Try to find the ctemplate
# Once done this will define
#
#  CTEMPLATE_FOUND - system has ctemplate
#  CTEMPLATE_INCLUDE_DIR - the ctemplate include directory
#  CTEMPLATE_LIBRARIES - Link this to use ctemplate
#  CTEMPLATE_COMPILER - CTemplate compiler executable
#
#  CTEMPLATE_VARNAMES - Macro that wraps templates int varnames.h
#     Usage: CTEMPLATE_VARNAMES(header1.h header2.h ${CT_FILES})
#

# Copyright (c) 2009, Thomas Richard, <thomas.richard@proan.be>
# Copyright (c) 2012, Siemens <pascal.bach@siemems.com>
#
# Redistribution and use is allowed according to the terms of the BSD license.
# For details see the accompanying COPYING-CMAKE-SCRIPTS file.

if (CTEMPLATE_INCLUDE_DIR AND CTEMPLATE_LIBRARIES)
  # in cache already
  set(CTEMPLATE_FOUND TRUE)

else (CTEMPLATE_INCLUDE_DIR AND CTEMPLATE_LIBRARIES)
  find_path(CTEMPLATE_INCLUDE_DIR ctemplate/template.h)
  find_library(CTEMPLATE_LIBRARIES NAMES ctemplate)
  find_program(CTEMPLATE_COMPILER make_tpl_varnames_h)

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(Ctemplate DEFAULT_MSG CTEMPLATE_INCLUDE_DIR CTEMPLATE_LIBRARIES )

endif (CTEMPLATE_INCLUDE_DIR AND CTEMPLATE_LIBRARIES)


macro (CTEMPLATE_VARNAMES outfiles)

  foreach (infile ${ARGN})
    get_filename_component(infile ${infile} ABSOLUTE)
    CTEMPLATE_MAKE_OUTPUT_file(${infile} varnames.h outfile)
    add_custom_command(OUTPUT ${outfile}
                       COMMAND ${CTEMPLATE_COMPILER} -q -f ${outfile} ${infile} 
                       DEPENDS ${infile} VERBATIM)
    set(${outfiles} ${${outfiles}} ${outfile})
  endforeach(infile)

endmacro (CTEMPLATE_VARNAMES outfiles)

# macro used to create the names of output files preserving relative dirs
macro (CTEMPLATE_MAKE_OUTPUT_file infile ext outfile )
  string(LENGTH ${CMAKE_CURRENT_BINARY_DIR} _binlength)
  string(LENGTH ${infile} _infileLength)
  set(_checkinfile ${CMAKE_CURRENT_SOURCE_DIR})
  if(_infileLength GREATER _binlength)
    string(SUBSTRING "${infile}" 0 ${_binlength} _checkinfile)
    if(_checkinfile STREQUAL "${CMAKE_CURRENT_BINARY_DIR}")
      file(RELATIVE_PATH rel ${CMAKE_CURRENT_BINARY_DIR} ${infile})
    else(_checkinfile STREQUAL "${CMAKE_CURRENT_BINARY_DIR}")
      file(RELATIVE_PATH rel ${CMAKE_CURRENT_SOURCE_DIR} ${infile})
    endif(_checkinfile STREQUAL "${CMAKE_CURRENT_BINARY_DIR}")
  else(_infileLength GREATER _binlength)
    file(RELATIVE_PATH rel ${CMAKE_CURRENT_SOURCE_DIR} ${infile})
  endif(_infileLength GREATER _binlength)
  if(WIN32 AND rel MATCHES "^[a-zA-Z]:") # absolute path 
    string(REGEX REPLACE "^([a-zA-Z]):(.*)$" "\\1_\\2" rel "${rel}")
  endif(WIN32 AND rel MATCHES "^[a-zA-Z]:") 
  set(_outfile "${CMAKE_CURRENT_BINARY_DIR}/${rel}")
  string(REPLACE ".." "__" _outfile ${_outfile})
  get_filename_component(outpath ${_outfile} PATH)
  get_filename_component(_outfile ${_outfile} NAME)
  file(MAKE_DIRECTORY ${outpath})
  set(${outfile} ${outpath}/${_outfile}.${ext})
endmacro (CTEMPLATE_MAKE_OUTPUT_file)
