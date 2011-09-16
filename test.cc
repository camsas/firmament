#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

using namespace std;

DEFINE_bool(hello_gflags, false, "Say hello from GFlags.");
//DEFINE_bool(logtostderr, false, "Send log messages to stderr.");

int main(int argc, char* argv[]) {
   google::ParseCommandLineFlags(&argc, &argv, true);
   google::InitGoogleLogging(argv[0]);

   std::cout << "hello clang!" << std::endl;
   //printf("hello clang!\n");
   LOG(INFO) << "hello glog!";
   if (FLAGS_hello_gflags || FLAGS_logtostderr) 
      printf("hello gflags!\n");
   return 0;
}
