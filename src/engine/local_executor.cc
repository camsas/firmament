// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Local executor class.

#include "engine/local_executor.h"

#include "base/common.h"
#include "base/types.h"

extern "C" {
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
}

namespace firmament {
namespace executor {

LocalExecutor::LocalExecutor(ResourceID_t resource_id)
    : local_resource_id_(resource_id) {
}

bool LocalExecutor::RunTask(shared_ptr<TaskDescriptor> td) {
  // XXX(malte): hack
  bool res = (RunProcessSync(td->binary()) == 0);
  VLOG(1) << "res is " << res;
  return res;
}

int32_t LocalExecutor::RunProcessSync(const string& cmdline) {
  pid_t pid;
  int pipe_to[2];    // pipe to feed input data to task
  int pipe_from[2];  // pipe to receive output data from task
  if ( pipe(pipe_to) != 0 ) {
    LOG(ERROR) << "Failed to create pipe to task.";
  }
  if ( pipe(pipe_from) != 0 ) {
    LOG(ERROR) << "Failed to create pipe from task.";
  }
  pid = fork();
  switch (pid) {
    case -1:
      // Error
      LOG(ERROR) << "Failed to fork child process.";
      break;
    case 0:
      // Child
      // set up pipes
      dup2(pipe_to[0], STDIN_FILENO);
      dup2(pipe_from[1], STDOUT_FILENO);
      // close unnecessary pipe descriptors
      close(pipe_to[0]);
      close(pipe_to[1]);
      close(pipe_from[0]);
      close(pipe_from[1]);
      // Run the task binary
      execlp(cmdline.c_str(), "\0", NULL);
      // execl only returns if there was an error
      LOG(ERROR) << "execlp failed for task command " << cmdline << "!";
      VLOG(1) << "failing at execlp";
      return -1;
    default:
      // Parent
      VLOG(1) << "Task process with PID " << pid << " created.";
      // close unused pipe ends
      close(pipe_to[0]);
      close(pipe_from[1]);
      // TODO
      close(pipe_to[1]);
      ReadFromPipe(pipe_from[0]);
      // wait for task to terminate
      int status;
      while (!WIFEXITED(status)) {
        waitpid(pid, &status, NULL);
      }
      VLOG(1) << "Task process with PID " << pid << " exited with status "
              << status << ".";
      return status;
  }
  VLOG(1) << "failing at dropout";
  return -1;
}

void LocalExecutor::WriteToPipe(int fd) {
  FILE *stream;
  // Open the pipe
  if ( (stream = fdopen(fd, "w")) == NULL ) {
    LOG(ERROR) << "Failed to open pipe for writing. FD: " << fd;
  }
  // Write the data to the pipe
  // fputs()
  // Finally, close the pipe
  fclose(stream);
}

void LocalExecutor::ReadFromPipe(int fd) {
  FILE *stream;
  int ch;
  if ( (stream =fdopen(fd, "r")) == NULL) {
    LOG(ERROR) << "Failed to open pipe for reading. FD " << fd;
  }
  while ( (ch = getc(stream)) != EOF ) {
    // XXX(malte): temp hack
    putc(ch, stdout);
  }
  fflush(stdout);
  fclose(stream);
}

}  // namespace executor
}  // namespace firmament
