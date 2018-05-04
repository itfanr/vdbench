

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


#define _LARGE_FILES

#include <jni.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/ioctl.h>
#include <sys/devinfo.h>
#include "vdbjni.h"

extern jlong
file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
	int rc;
	jlong   filesize;
	struct stat xstat;

	struct devinfo dinfo;

	rc = fstat((int)fhandle, &xstat);
	if (rc < 0)
	{
		printf("file_size(), fstat %s failed: %s\n",
				fname, strerror(errno));
		abort();
	}
	if ( S_ISREG(xstat.st_mode))
	{
		filesize = (jlong)xstat.st_size;
	}
	else if ( S_ISCHR(xstat.st_mode))
	{
		/*                                                           */
		/* ioctl will succeed for many device types other than disk  */
		/*     scsi and ide disks respond as type DD_SCDISK          */
		/*     logical volumes respond as type DD_DISK               */
		/*     invalid types will fall through the switch.           */
		/*                                                           */
		if (ioctl((int)fhandle,IOCINFO,&dinfo) != 0)
		{
			printf("file_size(), ioctl %s failed: %s\n", fname, strerror(errno));
			abort();
		}
		switch (dinfo.devtype)
		{
			case DD_SCDISK:
				filesize = (uint64_t)dinfo.un.scdk.blksize * dinfo.un.scdk.numblks;
				break;
			case DD_DISK:
				filesize = (uint64_t)dinfo.un.dk.bytpsec * dinfo.un.dk.numblks;
				break;
			default:
				printf("file_size, iocinfo %s device is not raw storage.\n",fname);
				abort();
		}
	}
	else  /* not a regular file or character device */
	{
		filesize = 0;
	}

	return filesize;
}


/*                                                           */
/* This needs to be changed to handle 64 bit seek addresses! */
/* Also need to find out how to force 'flush'                */
/*                                                           */
extern jlong
file_open(JNIEnv *env, const char *filename, int flush, int write)
{
  int fd;
  int access_type;

  /* Determine access: */
  if (write)
    access_type = (O_RDWR | O_CREAT);
  else
    access_type = (O_RDONLY);

  /* Open file, specify flush if specifically requested: */
  if (!flush)
    fd = open(filename, ( access_type ), 0666);
  else
    fd = open(filename, ( access_type | O_DSYNC ), 0666);

  if (fd == -1)
  {
    printf("file_open(), open (%s) failed: %s\n", filename, strerror(errno));
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
  int rc = pread((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);

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
  int rc = pwrite((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);

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

  buffer = (void*) malloc(bufsize);
  if (buffer == NULL)
  {
    printf("get_buffer() for %d bytes failed\n", bufsize);
    abort();
  }



  return(jlong) buffer;
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


