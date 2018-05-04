

/*
 * Copyright (c) 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


/*
 * Author: Henk Vandenbergh.
 */


//# define _LARGEFILE_SOURCE
# define _LARGEFILE64_SOURCE
# define _FILE_OFFSET_BITS 64


#include <jni.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <features.h>
#include "vdbjni.h"

#ifdef LINUX
#include <stdint.h>
#endif

extern struct Shared_memory *shared_mem;
char ptod_txt[256]; /* workarea for PTOD displays

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
    sprintf(ptod_txt, "file_size(), fstat %s failed: %s\n", fname, strerror(errno));
    PTOD(ptod_txt);
    ABORT(ptod_txt, "xx");
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
    sprintf(ptod_txt, "file_open(), open for '%s' failed: %s", filename, strerror(errno));
    PTOD(ptod_txt);
    return -1;
  }

  return fd;
}


/*                                                                            */
/* Close workload disk (or file)                                              */
/*                                                                            */
extern jlong file_close(JNIEnv *env, jlong fhandle)
{
  //printf("I have just entered file_close.\n");
  return (jlong) close((int) fhandle);
}


/*                                                                            */
/* Read workload disk:                                                        */
/*                                                                            */
extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
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
    sprintf(ptod_txt, "Invalid byte count. Expecting %lld, but read only %d bytes. ",
            length, rc);
    PTOD(ptod_txt);
    PTOD("Returning ENOENT");
    return ENOENT;
  }

  return 0;
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
    sprintf(ptod_txt, "Invalid byte count. Expecting %lld, but wrote only %d bytes. ",
            length, rc);
    PTOD(ptod_txt);
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
  void *buffer;

  buffer = (void*) valloc(bufsize);
  if ( buffer == NULL )
  {
    sprintf(ptod_txt, "get_buffer() for %d bytes failed\n", bufsize);
    PTOD(ptod_txt);
    abort();
  }



  return (jlong) buffer;
}


/*                                                                            */
/* Free the i/o buffer                                                        */
/*                                                                            */
extern void free_buffer(int bufsize, jlong buffer)
{
  //printf("I have just entered free_buffer.\n");
  free((void*) buffer);
}

/*                                                                            */
/* Simple time of day: does not have to be an accurate current tod,           */
/* as long as it can be used for tod delta calculations.                      */
/*                                                                            */
extern jlong get_simple_tod(void)
{
  jlong tod;
  struct timeval tv;

  //printf("I have just entered get_simple_tod.\n");

  /* Portable: */
  gettimeofday(&tv, NULL);
  tod = ((jlong)tv.tv_sec * 1000000) + (unsigned) tv.tv_usec;

  return tod ;
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
