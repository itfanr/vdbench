
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#define _LARGEFILE64_SOURCE

#include <vdbjni.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/ioctl.h>
#include <sys/diskio.h>
#include <sys/param.h>

extern struct Shared_memory *shared_mem;

static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";


extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
  int rc;
  jlong   filesize;
  struct stat64 xstat;
  capacity_type cap;

  rc = fstat64((int)fhandle, &xstat);
  if ( rc < 0 )
  {
    PTOD2("file_size(), fstat64 %s failed: %s\n", fname, strerror(errno));
    /* abort(999); */
	abort();
  }
  if ( S_ISREG(xstat.st_mode) )
  {
    filesize = (jlong)xstat.st_size;
  }
  else if ( S_ISCHR(xstat.st_mode) )
  {
    /* ioctl returns number of blocks of size DEV_BSIZE  */
    if ( ioctl((int)fhandle,DIOC_CAPACITY,&cap) == 0 )
    {
      filesize = (jlong)cap.lba * DEV_BSIZE;
    }
    else
    {
      filesize=0;
    }
  }
  return filesize;
}


extern jlong file_open(JNIEnv *env, const char *filename, int open_flags, int flag)
{
  int fd;
  int rc;
  int access_type;


  /* Determine access: */
  if (flag & 0x01 == 1)
    access_type = ( O_RDWR | O_CREAT );
  else
    access_type = ( O_RDONLY );

  /* Add flags requested by caller: */
  access_type |= open_flags;

  /* Open the file: */
  fd = open64(filename, (access_type), 0666);
  if ( fd == -1 )
  {
    PTOD1("error: %d", errno);
    PTOD1("file_open(), open %s failed", filename);
    return -1;
  }

  return fd;
}


/*                                                                            */
/* Close workload disk (or file)                                              */
/*                                                                            */
extern jlong file_close(JNIEnv *env, jlong fhandle)
{
  return (jlong) close((int) fhandle);
}


/*                                                                            */
/* Read workload disk:                                                        */
/*                                                                            */
extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  /* Set fixed values at start and end of buffer: */
  prepare_read_buffer(env, buffer, length);

  int rc = pread64((int) fhandle, (void*) (int) buffer,
                   (size_t) (int) length, (off64_t) seek);

  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed read. Setting to 799");
      return 799;
    }
    return errno;
  }

  else if (rc != length)
  {
    PTOD2("Invalid byte count. Expecting %lld, but read only %d bytes.", length, rc);
    PTOD("Returning ENOENT");
    return ENOENT;
  }

  /* Make sure read was REALLY OK: */
  return check_read_buffer(env, buffer, length);
}


/*                                                                            */
/* Write workload disk                                                        */
/*                                                                            */
extern jlong file_write(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int rc = pwrite64((int) fhandle, (void*) (int) buffer,
                    (size_t) length, (off64_t) seek);

  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed read. Setting to 799");
      return 799;
    }
    return errno;
  }

  else if (rc != length)
  {
    PTOD2("Invalid byte count. Expecting %lld, but wrote only %d bytes.", length, rc);
    PTOD("Returning ENOENT");
    return ENOENT;
  }

  return 0;
}


/*                                                                            */
/* Allocate memory for i/o buffers                                            */
/*                                                                            */
extern jlong alloc_buffer(JNIEnv *env, int bufsize)
{
  void *buffer = (void*) malloc(bufsize);

  return (jlong) buffer;
}


/*                                                                            */
/* Free the i/o buffer                                                        */
/*                                                                            */
extern void free_buffer(int bufsize, jlong buffer)
{
  free(buffer);
}

/*                                                                            */
/* Simple time of day: does not have to be an accurate current tod,           */
/* as long as it can be used for tod delta calculations.                      */
/*                                                                            */
extern jlong get_simple_tod(void)
{
  jlong tod;
  struct timeval tv;

  /* Portable: */
  gettimeofday(&tv, NULL);
  tod = ((jlong)tv.tv_sec * 1000000) + (unsigned) tv.tv_usec;

  return tod ;
}
