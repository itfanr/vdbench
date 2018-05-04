

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */


//# define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64


#include "vdbjni.h"
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <features.h>

#include <stdint.h>
#include <string.h>

extern struct Shared_memory *shared_mem;
char ptod_txt[256]; /* workarea for PTOD displays   */


static char c[] =
"Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";



/*                                                                            */
/* I have not been able to figure out yet how to get a size for raw files!    */
/*                                                                            */
extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
  int rc;

  jlong   filesize;
  struct stat xstat;

  //printf("I have just entered file_size.\n");

  /* Can not get vtoc, so try fstat: */
  rc = fstat(fhandle, &xstat);
  if (rc < 0)
  {
    PTOD1("file_size(), fstat %s failed", fname);
    PTOD1("error: %d", errno);
    ABORT("file_size(), fstat %s failed", fname);
  }

  //printf("fsize: %lld\n", xstat.st_size  );
  filesize = xstat.st_size;

  //printf("size: %lld  \n", filesize);

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
  int rc = close((int) fhandle);
  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed close. Setting to 799");
      return 799;
    }
    return errno;
  }

  return 0;
}


/*                                                                            */
/* Read workload disk:                                                        */
/*                                                                            */
extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  /* Set fixed values at start and end of buffer: */
  prepare_read_buffer(env, buffer, length);

  int rc = pread64((int) fhandle, (void*) buffer, (size_t) (int) length, (off64_t) seek);

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
    PTOD1("Invalid byte count. Expecting %lld", length);
    PTOD1("but read only %d bytes.", rc);;
    return 798;
  }

  /* Make sure read was REALLY OK: */
  return check_read_buffer(env, buffer, length);
}


/*                                                                            */
/* Write workload disk                                                        */
/*                                                                            */
extern jlong file_write(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int rc = pwrite64((int) fhandle, (void*) buffer, (size_t) length, (off64_t) seek);

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
    PTOD1("Invalid byte count. Expecting %lld", length);
    PTOD1("but wrote only %d bytes.", rc);;
    return 798;
  }

  return 0;
}


/*                                                                            */
/* Allocate memory for i/o buffers                                            */
/*                                                                            */
extern jlong alloc_buffer(JNIEnv *env, int bufsize)
{
  void *buffer = (void*) valloc(bufsize);

  return(jlong) buffer;
}


/*                                                                            */
/* Free the i/o buffer                                                        */
/*                                                                            */
extern void free_buffer(int bufsize, jlong buffer)
{
  //printf("I have just entered free_buffer.\n");
  free((void*) buffer);
}

/**
 * Simple time of day: does not have to be an accurate current tod, as long as
 * it can be used for tod delta calculations.
 */
extern jlong get_simple_tod(void)
{
  static int monotonic_checked = 0;
  static int monotonic_works   = 0;

  struct timeval   tv;
  struct timespec  time;
  jlong  tod;
  long   check_rc;
  size_t nsecs;

  #ifndef PPC

  /* Check to see if we can use this call: */
  /* From: kishore.kumar.pusukuri@oracle.com */
  if (!monotonic_checked)
  {
    monotonic_checked = 1;
    check_rc = clock_gettime(CLOCK_MONOTONIC, &time);
    if (check_rc == 0)
    {
      monotonic_works = 1;
      //printf("clock_gettime(CLOCK_MONOTONIC) works. \n");
    }
    else
      printf("clock_gettime(CLOCK_MONOTONIC) does not work. Using gettimeofday() instead: %d %s\n", errno, strerror(errno));
  }

  if (monotonic_works)
  {
    /* Not sure it works everywhere: */
    clock_gettime(CLOCK_MONOTONIC, &time);
    size_t nsecs = (time.tv_sec * 1e9 + time.tv_nsec);
    return nsecs / 1000;
  }
  else
  {
    /* Portable: */
    gettimeofday(&tv, NULL);
    tod = ((jlong)tv.tv_sec * 1000000) + (unsigned) tv.tv_usec;
    return tod ;
  }

  #else

  /* Portable: */
  gettimeofday(&tv, NULL);
  tod = ((jlong)tv.tv_sec * 1000000) + (unsigned) tv.tv_usec;
  return tod ;

  #endif
}


/*
 * Frees up memory mapped file region of supplied size. The
 * file descriptor "fd" indicates which memory mapped file.
 * If successful, returns 0. Otherwise returns -1 if "size"
 * is zero, or -1 times the number of times msync() failed.
 *
 * This has been taken from filebench fileset.c
 *
 * This code works around a bug in NFS code where the call to
 * directio(DIRECTIO_ON) still continues using blocks that
 * are already in cache.
 */
JNIEXPORT jint JNICALL Java_Vdb_Native_eraseFileSystemCache(JNIEnv *env,
                                                            jclass  this,
                                                            jlong   fhandle,
                                                            jlong   size)
{
  jlong left;
  int ret = 0;
#define	MMAP_SIZE	(1024UL * 1024UL * 1024UL)
#define	MIN(x, y) ((x) < (y) ? (x) : (y))

  for (left = size; left > 0; left -= MMAP_SIZE)
  {
    off64_t thismapsize;
    caddr_t addr;

    thismapsize = MIN(MMAP_SIZE, left);
    addr = mmap64(0, thismapsize, PROT_READ|PROT_WRITE,
                  MAP_SHARED, fhandle, size - left);
    ret += msync(addr, thismapsize, MS_INVALIDATE);
    (void) munmap(addr, thismapsize);
  }
  return(ret);
}


/**
 * Experiment creating sparse files:
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_truncateFile(JNIEnv *env,
                                                     jclass  this,
                                                     jlong   handle,
                                                     jlong   filesize)
{
  jlong rc = ftruncate((int) handle, (off_t) filesize);
  if (rc)
  {
    int error = errno;
    PTOD1("ftruncate error. Handle: %lld", handle);
    PTOD1("ftruncate error. Size:   %lld", filesize);
    PTOD1("ftruncate error. rc:     %d", rc);
    PTOD1("ftruncate error. errno:  %d"  , error);
    return error;
  }
  return 0;
}

