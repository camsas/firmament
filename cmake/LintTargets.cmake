configure_file(${Firmament_ROOT_DIR}/cmake/CppLint.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/lint_wrapper.cmake" @ONLY)
add_custom_target(lint
  COMMAND "${CMAKE_COMMAND}" -DLINT_VERBOSE=False
    -P "${CMAKE_CURRENT_BINARY_DIR}/lint_wrapper.cmake"
  COMMENT "Linting...")
add_dependencies(lint cpplint)

add_custom_target(lint-verb
  COMMAND "${CMAKE_COMMAND}" -DLINT_VERBOSE=True
    -P "${CMAKE_CURRENT_BINARY_DIR}/lint_wrapper.cmake"
  COMMENT "Linting (verbose)...")
add_dependencies(lint-verb cpplint)

