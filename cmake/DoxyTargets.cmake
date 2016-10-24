add_custom_target(doxy-doc
  COMMAND ${Firmament_ROOT_DIR}/contrib/generate-documentation.sh ${Firmament_ROOT_DIR}
  COMMENT "Generating Doxy documentation...")
