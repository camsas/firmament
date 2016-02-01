file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/misc)

set(MISC_SRC
  misc/trace_generator.cc
  misc/pb_utils.cc
  misc/wall_time.cc
  misc/string_utils.cc
  misc/utils.cc
  )

set(MISC_TESTS
  misc/envelope_test.cc
  misc/utils_test.cc
)
