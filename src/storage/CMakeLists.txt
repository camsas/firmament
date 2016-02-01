file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/storage)

set(STORAGE_SRC
  storage/simple_object_store.cc
  storage/stub_object_store.cc
  )

set(STORAGE_TESTS
  storage/stub_object_store_test.cc
)
