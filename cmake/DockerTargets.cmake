add_custom_target(docker-image
  COMMAND ${Firmament_ROOT_DIR}/contrib/docker-build.sh
  COMMENT "Building Docker image...")
