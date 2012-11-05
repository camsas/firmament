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

using common::pb_to_vector;

LocalExecutor::LocalExecutor(ResourceID_t resource_id)
    : local_resource_id_(resource_id) {
}

bool LocalExecutor::RunTask(shared_ptr<TaskDescriptor> td) {
  // Set environment variables
  // TODO(malte): this really shouldn't be happening here
  setenv("TASK_ID", to_string(td->uid()).c_str(), 1);
  setenv("FLAGS_coordinator_uri", "tcp://localhost:9999", 1);
  setenv("FLAGS_resource_id", to_string(local_resource_id_).c_str(), 1);
  // TODO(malte): hack
  vector<string> args = pb_to_vector(td->args());
  args.push_back("--tryfromenv=coordinator_uri,resource_id");
  // TODO(malte): This is somewhat hackish
  bool res = (RunProcessSync(td->binary(), args) == 0);
  return res;
}

int32_t LocalExecutor::RunProcessSync(const string& cmdline,
                                      vector<string> args) {
  pid_t pid;
  int pipe_to[2];    // pipe to feed input data to task
  int pipe_from[2];  // pipe to receive output data from task
  int status;
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
    case 0: {
      // Child
      // set up pipes
      dup2(pipe_to[0], STDIN_FILENO);
      dup2(pipe_from[1], STDOUT_FILENO);
      // close unnecessary pipe descriptors
      close(pipe_to[0]);
      close(pipe_to[1]);
      close(pipe_from[0]);
      close(pipe_from[1]);
      // Convert args from string to char*
      vector<char*> argv;
      argv.reserve(args.size() + 2);
      // argv[0] is always the command name
      argv.push_back((char*)(cmdline.c_str()));  // NOLINT
      for (uint32_t i = 0; i < args.size(); ++i) {
        // N.B.: This casts away the const qualifier on the c_str() result.
        // This is joyfully unsafe, of course.
        argv.push_back((char*)(args[i].c_str()));  // NOLINT
      }
      // The last argument to execvp is always NULL.
      argv.push_back(NULL);
      // Run the task binary
      execvp(cmdline.c_str(), &argv[0]);
      // execl only returns if there was an error
      PLOG(ERROR) << "execvp failed for task command " << cmdline << "!";
      _exit(1);
    }
    default:
      // Parent
      VLOG(1) << "Task process with PID " << pid << " created.";
      // close unused pipe ends
      close(pipe_to[0]);
      close(pipe_from[1]);
      // TODO(malte): fix the pipe stuff to work properly
      close(pipe_to[1]);
      ReadFromPipe(pipe_from[0]);
      // wait for task to terminate
      while (!WIFEXITED(status)) {
        waitpid(pid, &status, 0);
      }
      VLOG(1) << "Task process with PID " << pid << " exited with status "
              << WEXITSTATUS(status);
      return status;
  }
  return -1;
}

void LocalExecutor::WriteToPipe(int fd) {
  FILE *stream;
  // Open the pipe
  if ((stream = fdopen(fd, "w")) == NULL) {
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
  if ((stream =fdopen(fd, "r")) == NULL) {
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
