

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */


#define _LARGE_FILES

#include "vdbjni.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/ioctl.h>
#include <sys/devinfo.h>


static char c[] =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

extern struct Shared_memory *shared_mem;

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
				filesize = (jlong)(dinfo.un.scdk.blksize) * (jlong) ((__ulong32_t)dinfo.un.scdk.numblks);
				//filesize = (jlonguint64_t)dinfo.un.scdk.blksize * dinfo.un.scdk.numblks;
				break;
			case DD_DISK:
				filesize = (jlong)(uint64_t)dinfo.un.dk.bytpsec * (jlong) ((__ulong32_t)dinfo.un.dk.numblks);
				//filesize = (uint64_t)dinfo.un.dk.bytpsec * dinfo.un.dk.numblks;
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
  fd = open(filename, (access_type), 0666);
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
    PTOD2("Invalid byte count. Expecting %lld, but read only %d bytes. ", length, rc);
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
    PTOD2("Invalid byte count. Expecting %lld, but wrote only %d bytes. ", length, rc);
    return 798;
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

  /* Portable: */
  gettimeofday(&tv, NULL);
  tod = ((jlong)tv.tv_sec * 1000000) + (unsigned) tv.tv_usec;

  return tod ;
}


