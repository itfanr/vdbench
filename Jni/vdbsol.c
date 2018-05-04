

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */


#include "vdbjni.h"
#include <fcntl.h>
#include <sys/unistd.h>
#include <wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vtoc.h>
#include <sys/mman.h>
#include <sys/resource.h>  /* PRIO_PROCESS  */
#include <sys/systeminfo.h>


static char c[] =
"Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


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
    PTOD3("file_open(), open for '%s' failed: flags: %08x %s",
          filename, open_flags, strerror(errno));
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
  int rc;
  //if (seek & 0x2)
  //  return 0;

  /* Set fixed values at start and end of buffer: */
  prepare_read_buffer(env, buffer, length);

  rc = pread64((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);
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
  int rc;
  //if (seek & 0x2)
  //  return 0;

  rc = pwrite64((int) fhandle, (void*) buffer, (size_t) length, (off_t) seek);

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
  if ( buffer == NULL )
  {
    PTOD3("alloc_buffer() for %d bytes failed: %d  %s\n", bufsize, errno, strerror(errno));
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
  return gethrtime() / 1000;
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

    if (addr == MAP_FAILED)
    {
      PTOD1("mmap64 failed: handle=%lld ", fhandle);
      PTOD1("mmap64 failed: addr=%08p ", addr);
      PTOD1("mmap64 failed: errno=%d ", errno);
      return -1;
    }

    ret += msync(addr, thismapsize, MS_INVALIDATE);
    (void) munmap(addr, thismapsize);
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

JNIEXPORT jlong JNICALL Java_Vdb_Native_getSolarisPids(JNIEnv *env,
                                                       jclass  this)
{
  jlong ret;
  ret  = ((jlong) getpid()) << 32;
  ret |= pthread_self();
  return ret;
}

static sema_t wait_sema;
static JNIEnv *global_env;

void handle_sig(int x)
{
  int rc;
  //JNIEnv *env = global_env;
  //PTOD("hhh");
  //printf("here in handle_sig: %12lld\n", tod);

  if ((rc = sema_post(&wait_sema)) != 0)
  {
    printf("sema_post failed: %d\n", rc);
    abort();
  }
}

/**
 * Native sleep.
 * This codes depends on there being only ONE caller, WT_task().
 */
JNIEXPORT void JNICALL Java_Vdb_Native_nativeSleep(JNIEnv *env,
                                                   jclass  this,
                                                   jlong   wakeup)
{
  static int              first = 1;
  static timer_t          t_id;
  static itimerspec_t     time_struct;
  static struct sigevent  sig_struct;
  static struct sigaction act_struct;
  static jlong            NANO = 1000000000;
  int                     rc   = 999;

  if (first)
  {
    first = 0;

    bzero(&sig_struct, sizeof (struct sigevent));
    bzero(&act_struct, sizeof (struct sigaction));
    bzero(&wait_sema,  sizeof (sema_t));

    /* Create timer */
    sig_struct.sigev_notify          = SIGEV_SIGNAL;
    sig_struct.sigev_signo           = SIGUSR1;
    sig_struct.sigev_value.sival_int = 0;
    if ((rc = timer_create(CLOCK_HIGHRES, &sig_struct, &t_id)) != 0)
    {
      ABORT("Timer creation failed: %d", rc);
      //return;
    }

    /* Set interrupt handler for SIGUSR1: */
    act_struct.sa_handler = handle_sig;
    if (sigaction(SIGUSR1, &act_struct, NULL) != 0)
      ABORT("Could not set up signal handler: %d", rc);

    sema_init(&wait_sema, 0, USYNC_THREAD, NULL);
  }

  /* Fill in new time values: */
  time_struct.it_value.tv_sec     = wakeup / NANO;
  time_struct.it_value.tv_nsec    = wakeup % NANO;
  time_struct.it_interval.tv_sec  = 0;
  time_struct.it_interval.tv_nsec = 0;

  /* Arm timer */
  if ((rc = timer_settime(t_id, TIMER_ABSTIME, &time_struct, NULL)) != 0)
    ABORT("Setting timer failed: %d", rc);

  /* Wait for semaphore from the interrupted thread. If we get */
  /* an interrupt ourselves, that is fine also.                */
  rc = sema_wait(&wait_sema);
  if (rc != 0 && rc != EINTR)
    ABORT("sema wait failed %d", rc);
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

