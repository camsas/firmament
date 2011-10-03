// TODO:header

#include <iostream>

#include "engine/worker.h"

namespace firmament {

Worker::Worker() {
  LOG(INFO) << "Hello from Worker constructor!";
  std::cout << "constructor" << std::endl;
}

int64_t Worker::Test() {
  std::cout << "test" << std::endl;
  return 1;
}

}  // namespace firmament
