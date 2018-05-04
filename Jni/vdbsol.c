

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


#include <jni.h>
#include <fcntl.h>
#include <sys/unistd.h>
#include <wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vtoc.h>
#include <sys/mman.h>
#include <sys/resource.h>  /* PRIO_PROCESS  */
#include <sys/systeminfo.h>
#include "vdbjni.h"


extern int shmid = -1;

extern struct Shared_memory *shared_mem;


extern jlong get_vtoc(JNIEnv *env, jlong fhandle, const char* fname);

/**
 * Determine size of the raw disk or of the file system file.
 * Code has been moved to separate source file since dlopen() can
 * not handle 64 i/o
 */
extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
  return get_vtoc(env, fhandle, fname);
}



extern int set_max_open_files(JNIEnv *env)
{
  struct rlimit rlim;
  int rc;
  rlim_t newmax;
  rlim_t my_files = 0;

  /* Get the original content: */
  rc = getrlimit(RLIMIT_NOFILE, &rlim);
  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed getrlimit. Setting to 799");
      return 799;
    }
    return errno;
  }

  my_files = rlim.rlim_cur;
  for ( newmax = rlim.rlim_max; newmax > my_files; newmax -= 256 )
  {
    rlim.rlim_cur = newmax;
    if ( setrlimit(RLIMIT_NOFILE, &rlim) != 0 )
      continue;
    my_files = rlim.rlim_cur;
    break;
  }

  return my_files;
}




/**
 * Open workload disk (or file)
 * flag:
 * - 0 - read
 * - 1 - write
 */
extern jlong file_open(JNIEnv *env, const char *filename, int open_flags, int flag)
{
  int use_directio;


  int fd;
  int rc;
  int access_type;


  /* Read or write? */
  if (flag & 0x01 == 1)
    access_type = ( O_RDWR | O_CREAT | O_LARGEFILE);
  else
    access_type = ( O_RDONLY | O_LARGEFILE);

  /* Add flags requested by caller: */
  access_type |= open_flags;

  /* Open the file: */
  fd = open64(filename, (access_type), 0666);
  if ( fd == -1 )
  {
    sprintf(ptod_txt, "file_open(), open for '%s' failed: flags: %08x %s", filename,
            open_flags, strerror(errno));
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




/**
 * Read workload disk:
 */
extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int rc = pread64((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);
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
  int rc = pwrite64((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);

  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed read. Setting to 799");
      return 799;
    }

    PTODS("handle: %lld", fhandle);
    PTODS("length: %lld", length);
    PTODS("seek:   %lld", seek);

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
    sprintf(ptod_txt, "alloc_buffer() for %d bytes failed: %d  %s\n", bufsize, errno, strerror(errno));
    PTOD(ptod_txt);
  }

  return(jlong) buffer;
}


/*                                                                            */
/* Free the i/o buffer                                                        */
/*                                                                            */
extern void free_buffer(int bufsize, jlong buffer)
{

  free((void*) buffer);
}

/*                                                                            */
/* Simple time of day: does not have to be an accurate current tod,           */
/* as long as it can be used for tod delta calculations.                      */
/*                                                                            */
extern jlong get_simple_tod(void)
{

  jlong tod = gethrtime();
  tod = tod / 1000;

  return tod ;
}


/**
  * Read the volume label
  */
extern char* get_label(char* fname)
{
  int    rc;
  static struct vtoc vtoc;
  jlong  filesize, size;
  struct stat xstat;
  int    slice, i;
  int    fd;

  fd = open(fname, ( O_RDONLY), 0666);
  if ( fd < 0 )
  {
    //printf("open error: %s", strerror(errno));
    return "";
  }

  /* If we can get the vtoc then we know we have a raw disk: */
  if ( (slice = read_vtoc(fd, &vtoc)) >= 0 )
  {
    close(fd);
    return vtoc.v_asciilabel;
  }
  close(fd);
  return "";
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
    if (ret != 0)
    {
      PTODS("Java_Vdb_Native_eraseFileSystemCache fhandle: %lld", fhandle);
      PTODS("left: %lld", left);
      PTODS("ret: %d", ret);
      PTODS("errno: %d", errno);
    }
  }
  return(ret);
}



/**
 * Call directio() function.
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_directio(JNIEnv *env,
                                                 jclass  this,
                                                 jlong   handle,
                                                 jlong   on_flag)

{
  int rc;
  if (on_flag)
    rc = directio((int) handle, DIRECTIO_ON);
  else
    rc = directio((int) handle, DIRECTIO_OFF);
  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed directio. Setting to 799");
      return 799;
    }
    return errno;
  }

  return 0;
}

