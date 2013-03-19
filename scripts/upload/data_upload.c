#include "data_upload.h"

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <camio/camio.h>
#include <camio/iostreams/camio_iostream_tcp.h>

#include "messages/base_message.pb-c.h"
#include "base/reference_desc.pb-c.h"

static camio_iostream_t* tcp_endpoint = NULL;
static camio_ostream_t* out      = NULL;
static camio_perf_t* perf_mon    = NULL;

struct camio_cat_options_t {
    char* remote;
    char* output;
    char* format;
    char* path;
    uint8_t verbose;
} options ;

static inline void decapsulate_envelope(uint8_t** buff, uint64_t* len){
  *buff += sizeof(uint64_t); //Skip over envelope
  *len -= sizeof(uint64_t);
}

static int64_t delimit(uint8_t* buff, uint64_t size){
  if(size < sizeof(uint64_t)){
    wprintf("size=%lu\n", size  );
    wprintf("delimiter fail\n");
    return -1;
  }

  const uint64_t* envelope_size = (uint64_t*)buff;

  if(size >= *envelope_size) {
    return *envelope_size + sizeof(uint64_t);
  }

  wprintf("size=%lu envelope size=%lu\n", size , *envelope_size );
  wprintf("delimiter fail\n");
  return -1;
}

static void send_coord_base_msg(Firmament__BaseMessage* base_msg) {
  uint8_t* buff = NULL;
  uint64_t len = 0;

  len = firmament__base_message__get_packed_size(base_msg);
  buff = malloc(len);
  if (!buff){
    printf("Could not create buffer to encode message\n");
    exit(1);
  }
  firmament__base_message__pack(base_msg,buff);

  tcp_endpoint->assign_write(tcp_endpoint, (uint8_t*)&len, sizeof(len));
  tcp_endpoint->end_write(tcp_endpoint, sizeof(len));

  tcp_endpoint->assign_write(tcp_endpoint, buff, len);
  tcp_endpoint->end_write(tcp_endpoint, len);

  //I hate to see a malloc/free on the critical path
  free(buff);
}

static int generate_guid_name(dios_name_t* dios_name_out, char str_name_out[65]){
  int rand_fd = open("/dev/urandom", O_RDONLY);
  if(rand_fd < 0){
    printf("Warning, could not open /dev/urandom for generating GUID\n");
    return -1;
  }

  uint64_t bytes_read = 0;
  while(bytes_read < DIOS_NAME_BYTES){
    bytes_read += read(rand_fd,dios_name_out + bytes_read, DIOS_NAME_BYTES - bytes_read);
  }

  int i = 0;
  for(; i < DIOS_NAME_QWORDS; i++){
    sprintf(&str_name_out[0] + 16*i, "%0lX", dios_name_out->value[i] );
    //printf("\n--> %0lX", dios_name_out->value[i] );
  }

  return 0;
}

void configure_options(int argc, char** argv) {
  camio_options_short_description("data_upload");
  camio_options_add(CAMIO_OPTION_OPTIONAL, 'r', "remote",    "Coordinator connection point URI: e.g. tcp:127.0.0.1:9998", CAMIO_STRING, &options.remote, "tcp:127.0.0.1:9998");
  camio_options_add(CAMIO_OPTION_OPTIONAL, 'o', "output",    "Output descriptions in camio format. eg log:/file.txt", CAMIO_STRING, &options.output, "std-log");
  camio_options_add(CAMIO_OPTION_OPTIONAL, 'f', "format",    "Format to return the DIOS name in. One of: hex, ascii, bin.", CAMIO_STRING, &options.format, "hex");
  camio_options_add(CAMIO_OPTION_OPTIONAL, 'p', "path",      "Path of data object to register.", CAMIO_STRING, &options.path, "");
  camio_options_add(CAMIO_OPTION_FLAG, 'v', "verbose",   "Produce verbose output.", CAMIO_BOOL, &options.verbose, 0);
  camio_options_long_description("Firmament/DIOS upload utility.");
  camio_options_parse(argc, argv);
}

int main(int argc, char** argv){
  signal(SIGTERM, term);
  signal(SIGINT, term);

  configure_options(argc, argv);
  perf_mon = camio_perf_init("blob:debug.perf", 256 * 1024);

  if (options.format != "hex" && options.format != "ascii" &&
      options.format != "bin") {
    printf("ERROR: invalid output format for name specified; must be one of "
           "'ascii', 'bin', 'hex'!\n");
    exit(1);
  }

  tcp_endpoint = camio_iostream_delimiter_new( camio_iostream_new(options.remote, NULL, NULL, NULL), delimit, NULL);
  //out = camio_ostream_new(options.output, NULL, NULL, perf_mon);
  printf("Writing name to %s (%p)\n", options.output, out);

  // First generate a random name for this object
  dios_name_t name;
  char str_name[65];
  generate_guid_name(&name, &str_name);
  printf("Name: %s\n", str_name);

  // Manufacture an object creation message
  Firmament__BaseMessage bm = FIRMAMENT__BASE_MESSAGE__INIT;
  Firmament__CreateRequest create_req = FIRMAMENT__CREATE_REQUEST__INIT;
  Firmament__ReferenceDescriptor rd = FIRMAMENT__REFERENCE_DESCRIPTOR__INIT;
  bm.create_request = &create_req;
  bm.create_request->reference = &rd;
  rd.type = FIRMAMENT__REFERENCE_DESCRIPTOR__REFERENCE_TYPE__CONCRETE;
  rd.location = options.path;

  // Set up reference descriptor
  rd.id.len = sizeof(dios_name_t);
  rd.id.data = &name;

  // Pack and send!
  send_coord_base_msg(&bm);

  // Check if we receive an ACK
  uint8_t* resp_buff;
  uint64_t len = tcp_endpoint->start_read(tcp_endpoint, &resp_buff);
  decapsulate_envelope(&resp_buff, &len);

  Firmament__BaseMessage* base_msg = NULL;
  base_msg = firmament__base_message__unpack(NULL, len, resp_buff);

  if (!base_msg->create_response) {
    printf("ERROR: received a response message that did not contained a "
           "CreateResponse field!\n");
    exit(1);
  } else {
    if (base_msg->create_response->success) {
      printf("Data object uploaded SUCCESSFULLY!\n");
    } else {
      printf("Upload of data object FAILED, try again!\n");
      exit(1);
    }
  }

  // Write the name to the requested output stream
  // TODO(malte)

  // Clean up and quit
  term(0);
}

void term(int signum) {
    if(tcp_endpoint) { tcp_endpoint->close(tcp_endpoint);  }
    if(out)        { out->close(out);              }
    exit(0);
}
