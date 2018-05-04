

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


#define _LARGEFILE64_SOURCE

#include <jni.h>
#include "vdbjni.h"
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/ioctl.h>
#include <sys/diskio.h>
#include <sys/param.h>




extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
  int rc;
  jlong   filesize;
  struct stat64 xstat;
  capacity_type cap;
  char txt[255];

  rc = fstat64((int)fhandle, &xstat);
  if ( rc < 0 )
  {
    sprintf(txt, "file_size(), fstat64 %s failed: %s\n", fname, strerror(errno));
    PTOD(txt);
    abort(999);
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


extern jlong file_open(JNIEnv *env, const char *filename, int flush, int write)
{
  int fd;
  int access_type;
  char txt[255];

  /* Determine access: */
  if ( write )
    access_type = (O_RDWR | O_CREAT);
  else
    access_type = (O_RDONLY);

  /* Open file, specify flush if specifically requested: */
  if ( !flush )
    fd = open64(filename, ( access_type ), 0666);
  else
    fd = open64(filename, ( access_type | O_DSYNC ), 0666);
  if ( fd == -1 )
  {
    sprintf(txt, "file_open(), open %s failed: %s\n", filename, strerror(errno));
    PTOD(txt);
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
  char txt[255];

  buffer = (void*) malloc(bufsize);
  if ( buffer == NULL )
  {
    sprintf(txt, "get_buffer() for %d bytes failed\n", bufsize);
    PTOD(txt);
    abort(999);
  }



  return (jlong) (int) buffer;
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
