//#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

//using namespace std;

DEFINE_bool(hello_gflags, false, "Say hello from GFlags.");

int main(int argc, char* argv[]) {
   google::InitGoogleLogging(argv[0]);
   google::ParseCommandLineFlags(&argc, &argv, true);

   //std::cout << "hello clang!" << std::endl;
   printf("hello clang!\n");
   LOG(INFO) << "hello glog!";
   if (FLAGS_hello_gflags) 
      printf("hello gflags!\n");
   void *foo = malloc(1024);
   return 0;
}
