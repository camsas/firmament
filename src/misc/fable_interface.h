// TODO: header
// Interface definition strub for libfable, as per RESoLVE paper.

#ifndef FIRMAMENT_MISC_FABLE_INTERFACE_H_
#define FIRMAMENT_MISC_FABLE_INTERFACE_H_

struct xio_context {
}

struct xio_buffer {
}

struct xio_handle {
  // Actual I/O memory described by this handle
  void *mem_ptr;
  uint64_t mem_size;

  // Epoch number for reconfiguration purposes
  uint64_t epoch;

  // FD for polling on reconfiguration events
  FILE *fd;
}

xio_context xio_register_name(char *name);
void xio_connect(xio_context *ctx, char *remote_uri);
void xio_listen(xio_context *ctx);
void xio_accept();

xio_buffer* xio_getreadbuf();
xio_buffer* xio_getwritebuf();

void xio_commmit(xio_buffer *buf);
void xio_release(xio_buffer *buf);


#endif  // FIRMAMENT_MISC_FABLE_INTERFACE_H_
