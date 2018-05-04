

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */


#include "vdbjni.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
  #include <unistd.h>
#endif

#ifdef _WIN32
  #include <sys/timeb.h>
  #define uintptr_t __int64
#endif

#ifdef LINUX
//#include <sys/timeb.h>
  #define uintptr_t jlong
#endif

static char c[] =
"Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


extern struct Shared_memory *shared_mem;
static int negative_response_count = 0;



/**
 * Java call to copy an 'int' array to a native buffer
 */
JNIEXPORT void JNICALL Java_Vdb_Native_array_1to_1buffer(JNIEnv   *env,
                                                         jclass    this,
                                                         jintArray array,
                                                         jlong     buffer,
                                                         jint      bytes)
{
  int   i;
  jint  len;
  void *body;

  len  = (*env)->GetArrayLength(env, array);
  if (bytes > len * 4)
  {
    PTOD1("array length in bytes: %d", len * 4);
    PTOD1("bytes requested:       %d", bytes);
    ABORT("array_to_buffer(); invalid sizes", "");
  }
  body = (*env)->GetIntArrayElements(env, array, 0);

  memcpy((void*) buffer, body, bytes);

  (*env)->ReleaseIntArrayElements(env, array, body, JNI_ABORT);

  return;
}



/**
 * Java call to copy native memory to an 'int' array
 */
JNIEXPORT void JNICALL Java_Vdb_Native_buffer_1to_1array(JNIEnv *env,
                                                         jclass this,
                                                         jintArray array,
                                                         jlong  buffer,
                                                         jint   bytes)
{
  (*env)->SetIntArrayRegion(env, array, 0, (int) (bytes / 4), (jint*) (uintptr_t) buffer);
}



/**
 * Java call to obtain timestamp
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_get_1simple_1tod(JNIEnv *env,
                                                         jclass this)
{
  return GET_SIMPLE_TOD();
}


/**
 * Java call to allocate native memory
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_allocbuf(JNIEnv *env,
                                                 jclass this,
                                                 jint   bytes)
{
  jlong addr = alloc_buffer(env, bytes);

  return addr;
}


/**
 * Java call to free native memory
 */
JNIEXPORT void JNICALL Java_Vdb_Native_freebuf(JNIEnv *env,
                                               jclass this,
                                               jint  bytes,
                                               jlong buffer)
{
  free_buffer(bytes, buffer);
}


#ifndef _WIN32
/**
 * Synchronous a file before closing.
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_fsync(JNIEnv *env,
                                              jclass  this,
                                              jlong   handle)
{
  int rc = fsync(handle);
  if (rc == -1)
  {
    if (errno == 0)
    {
      PTOD("Errno is zero after a failed fsync. Setting to 799");
      return 799;
    }
    return errno;
  }

  return 0;
}
#endif


/**
 * Place response time in the proper histogram bucket.
 */
static void updateHistogram(JNIEnv *env, struct Histogram *hist, jlong response)
{
  int i;

  if (hist->buckets == 0)
  {
    ABORT("No histogram buckets found.", "");
  }

  /* First check to see if the last bucket used is the proper one. */
  if (response >= hist->bucket[hist->last].min &&
      response  < hist->bucket[hist->last].max)
  {
    hist->bucket[hist->last].count++;
    return;
  }

  //PTOD1("response:      %lld", response);
  //PTOD1("hist->buckets: %lld", hist->buckets);
  for (i = 0; i < hist->buckets; i++)
  {
    //PTOD1("min: %lld", hist->bucket[ i ].min);
    //PTOD1("max: %lld", hist->bucket[ i ].max);
    if (response >= hist->bucket[ i ].min &&
        response  < hist->bucket[ i ].max)
    {
      hist->bucket[ i ].count++;
      hist->last = i;
      return;
    }
  }

  return;

  // Problem 'solved' by the caller of this function.
  //#ifdef _WIN32
  //  PTOD1("Unable to find bucket. Are we having a Harry Belafonte moment here? %I64d", response);
  //#else
  //  PTOD1("Unable to find bucket. Are we having a Harry Belafonte moment here? %lld", response);
  //#endif
  //
  //ABORT("Unable to find bucket. Are we having a Harry Belafonte moment here? ", "");
}


/**
 * Initialize shared memory.
 * Clear it all to zero, and store a timestamp that is used as
 * true 1/1/1970 offset in microseconds that we can use to add
 * the results of GET_SIMPLE_TOD() to so that we have an efficient
 * true tod stamp. (for DV)
 */
extern void init_shared_mem(JNIEnv *env, jlong pid)
{
  jlong  usecs;
  jlong  simple;

  /* Clear it all out: */
  memset(shared_mem, 0, sizeof(struct Shared_memory));

  /* Process id is used by DV: */
  shared_mem->pid = pid;

#ifdef _WIN32
  {
    struct _timeb tod;
    _ftime( &tod );

    /* Translate this timestamp into usecs: */
    usecs = ((jlong) tod.time * 1000000) + tod.millitm;
  }

#else
  {
    struct timeval tod;
    /* Obtain current tod in microseconds, offset to 1/1/1970: */
    if ( gettimeofday(&tod, NULL) != 0 )
      ABORT("init_shared_mem", strerror(errno));

    /* Translate this timestamp into usecs: */
    usecs = ((jlong) tod.tv_sec * 1000000) + tod.tv_usec;
  }

#endif

  /* Subtract from that the simple tod (then we have to subtract it only once */
  /* when we store the 'write' timestamp in the sector): */
  simple = GET_SIMPLE_TOD();
  usecs -= simple;

  shared_mem->base_hrtime = usecs;

  /* Lock to avoid some problems around sprintf() not being */
  /* thread safe on some systems, e.g. linux                */
  MUTEX_INIT(shared_mem->printf_lock, "printf_lock");
  MUTEX_INIT(shared_mem->hash_lock, "hash_lock");
}


/**
 * These statistics will be picked up every interval.
 *
 * Note: The response time calculations are done outside of the Mutex locks.
 * The objective is to measure i/o response times and therefore should not
 * include delays caused by lock contention.
 *
 * Note: instead of having one set of data per WG_entry, have one per IO_task.
 * We then do not need to lock, which on one Windows 'ignore locks' test
 * increased throughput a lot. This was when bypassing i/o though.
 *
 */
extern void update_workload_stats(JNIEnv *env,
                                  struct Workload *wkl,
                                  jint   read_flag,
                                  jint   xfersize,
                                  jlong  tod1,
                                  jlong  rc)
{
  /* Update statistics: */
  jlong tod      = GET_SIMPLE_TOD();
  jlong response = tod - tod1;
  jlong square   = response * response;
  jlong delta, rcnt;

  if (response < 0)
  {
    if (negative_response_count == 0)
    {
      PTOD("Negative response time. Usually caused by out of sync cpu timers.");
      PTOD("Will report a maximum of 100 times after which Vdbench will continue.");
    }
    if (negative_response_count++ < 100)
    {
#ifdef _WIN32
      PTOD1("Response time (microseconds): %I64d", response);
#else
      PTOD1("Response time (microseconds): %lld", response);
#endif
    }
    response = 0;
  }

  MUTEX_LOCK(wkl->stat_lock);

  if (read_flag )
  {
    if (wkl->r_max < response) wkl->r_max = response;
    wkl->r_resptime  += response;
    wkl->r_resptime2 += square;
    wkl->r_bytes += xfersize;
    wkl->reads++;
    if (rc != 0 ) wkl->r_errors++;
    updateHistogram(env, &wkl->read_hist, response);
  }
  else
  {
    if (wkl->w_max < response) wkl->w_max = response;
    wkl->w_resptime  += response;
    wkl->w_resptime2 += square;
    wkl->w_bytes += xfersize;
    wkl->writes++;
    if (rc != 0 ) wkl->w_errors++;
    updateHistogram(env, &wkl->write_hist, response);
  }

  /* kstat_runq_exit */
  delta             = tod - wkl->rlastupdate;
  wkl->rlastupdate  = tod;
  rcnt              = wkl->q_depth--;
  wkl->rlentime    += delta * rcnt;
  wkl->rtime       += delta;

  MUTEX_UNLOCK(wkl->stat_lock);
}


/**
 * Function to setup statistics calculations.
 * May be candidate for inlining in START_WORKLOAD_STATS()?
 */
extern jlong start_workload_stats(JNIEnv          *env,
                                  struct Workload *wkl)
{
  jlong delta, rcnt;
  jlong tod = GET_SIMPLE_TOD();

  /* kstat_runq_enter */
  MUTEX_LOCK(wkl->stat_lock);
  delta            = tod - wkl->rlastupdate;
  wkl->rlastupdate = tod;
  rcnt             = wkl->q_depth++;
  if (rcnt != 0)
  {
    wkl->rlentime += delta * rcnt;
    wkl->rtime    += delta;
  }
  MUTEX_UNLOCK(wkl->stat_lock);

  return tod;
}

/**
 * Function to close off a reporting interval.
 */
extern void close_workload_interval(JNIEnv          *env,
                                    struct Workload *wkl)
{
  jlong delta, rcnt;
  jlong tod = GET_SIMPLE_TOD();

  /* Mutexes are already locked upon entry. */
  delta            = tod - wkl->rlastupdate;
  wkl->rlastupdate = tod;
  rcnt             = wkl->q_depth;
  if (rcnt != 0)
  {
    wkl->rlentime += delta * rcnt;
    wkl->rtime    += delta;
  }
}


/**
 * Java call to allocate memory for JNI control of workloads.
 * Either allocate true shared memory when JNI will be doing the workload,
 * or allocate non-shared memory when Java does the workload.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_alloc_1jni_1shared_1memory(JNIEnv *env,
                                                                  jclass  this,
                                                                  jlong   pid)
{

#ifdef SOLARIS

  if (shared_mem == NULL )
  {
    shared_mem = (struct Shared_memory*) valloc(sizeof(struct Shared_memory));
    if (shared_mem == NULL )
      ABORT("valloc for shared memory failed", strerror(errno));
    init_shared_mem(env, pid);
  }

#else

  if (shared_mem == NULL )
  {
    shared_mem = (struct Shared_memory*) malloc(sizeof(struct Shared_memory));
    if (shared_mem == NULL )
      ABORT("malloc for shared memory failed", strerror(errno));
    init_shared_mem(env, pid);
  }

#endif

  /* Erase all workload data from previous run: */
  shared_mem->max_workload = 0;
  memset(shared_mem->workload, 0, sizeof(struct Workload) * SHARED_WORKLOADS);
}



/**
 * Java context to pass on workload information
 */
JNIEXPORT void JNICALL Java_Vdb_Native_setup_1jni_1context(JNIEnv     *env,
                                                           jclass     this,
                                                           jint       jni_index,
                                                           jstring    sdname,
                                                           jlongArray read_hist,
                                                           jlongArray write_hist)
{
  struct Workload *wkl;

  jlong *read_hist_a ;
  jlong *write_hist_a;
  jint   r_len;
  jint   w_len;

  read_hist_a  = (*env)->GetLongArrayElements(env, read_hist,  NULL);
  write_hist_a = (*env)->GetLongArrayElements(env, write_hist, NULL);

  r_len  = (*env)->GetArrayLength(env, read_hist);
  w_len  = (*env)->GetArrayLength(env, write_hist);

  //PTOD1("jni_index: %d", jni_index);
  //PTOD1("r_len: %d", r_len);
  //PTOD1("w_len: %d", w_len);


  if (shared_mem == NULL )
  {
    PTOD("JNI shared memory not yet initialized");
    abort();
  }

  if (jni_index >= SHARED_WORKLOADS )
  {
    PTOD2("Vdbench: too many workloads requested: %d; only %d allowed",
          jni_index, SHARED_WORKLOADS);
    abort();
  }

  // It doesn't appear that max_workload has a function at all.
  if (jni_index > shared_mem->max_workload)
    shared_mem->max_workload = jni_index;

  wkl = &shared_mem->workload[jni_index];

  /* These two strings are never freed. I can live with that: */
  //wkl->lun    = (char*) (*env)->GetStringUTFChars(env, lun, 0);
  wkl->sdname = (char*) (*env)->GetStringUTFChars(env, sdname, 0);

  MUTEX_INIT(wkl->stat_lock, "stat_lock");

  /* Copy the histogram array for this Workload: */
  wkl->read_hist.jni_bytes  = r_len * 8;
  wkl->write_hist.jni_bytes = w_len * 8;
  wkl->read_hist.buckets    = r_len / 3;
  wkl->write_hist.buckets   = w_len / 3;

  wkl->read_hist.last  = 0;
  wkl->write_hist.last = 0;

  memcpy(&wkl->read_hist.bucket[0],  &read_hist_a[0],  wkl->read_hist.jni_bytes );
  memcpy(&wkl->write_hist.bucket[0], &write_hist_a[0], wkl->write_hist.jni_bytes);


  (*env)->ReleaseLongArrayElements(env, read_hist,  read_hist_a,  JNI_ABORT);
  (*env)->ReleaseLongArrayElements(env, write_hist, write_hist_a, JNI_ABORT);

  return;
}

/**
 * Java call to get statistics for ONE workload.
 * Also used to signal end-of-run to JNI
 */
JNIEXPORT jstring JNICALL Java_Vdb_Native_get_1one_1set_1statistics(JNIEnv     *env,
                                                                    jclass     this,
                                                                    jint       jni_index,
                                                                    jlongArray read_hist,
                                                                    jlongArray write_hist)
{

  struct Workload *wkl = (struct Workload*) &shared_mem->workload[jni_index];
  jlong *read_hist_a   = (*env)->GetLongArrayElements(env, read_hist,  NULL);
  jlong *write_hist_a  = (*env)->GetLongArrayElements(env, write_hist, NULL);

  /* The 'labels' can be used to verify that JNI and Java are in sync. */
  /* Not used at this time.                                            */
#ifdef _WIN32
  char format[] = "reads  %I64d r_resptime %I64d r_resptime2 %I64d r_max %I64d r_bytes %I64d r_errors %I64d "\
                  "writes %I64d w_resptime %I64d w_resptime2 %I64d w_max %I64d w_bytes %I64d w_errors %I64d "\
                  "rtime  %I64d rlentime   %I64d ";
#else
  char format[] = "reads  %lld  r_resptime %lld  r_resptime2 %lld  r_max %lld  r_bytes %lld  r_errors %lld  "\
                  "writes %lld  w_resptime %lld  w_resptime2 %lld  w_max %lld  w_bytes %lld  w_errors %lld  "\
                  "rtime  %lld  rlentime   %lld  ";
#endif

  char long_line[4096];


  /* Store the data (lock it to make sure that we are not in the middle */
  /* of an update and the caller sees an average of 511 bytes...        */
  MUTEX_LOCK(wkl->stat_lock);
  close_workload_interval(env, wkl);

  sprintf(long_line, format,
          wkl->reads,
          wkl->r_resptime,
          wkl->r_resptime2,
          wkl->r_max,
          wkl->r_bytes,
          wkl->r_errors,
          wkl->writes,
          wkl->w_resptime,
          wkl->w_resptime2,
          wkl->w_max,
          wkl->w_bytes,
          wkl->w_errors,
          wkl->rtime,
          wkl->rlentime);

  wkl->r_max = 0;
  wkl->w_max = 0;
  //PTOD(long_line);

  /* Copy the accumulated histogram data: */
  memcpy(&read_hist_a[0],  &wkl->read_hist.bucket[0],  wkl->read_hist.jni_bytes );
  memcpy(&write_hist_a[0], &wkl->write_hist.bucket[0], wkl->write_hist.jni_bytes);

  /* Release (and copy new content) of Histogram arrays back to java: */
  (*env)->ReleaseLongArrayElements(env, read_hist,  read_hist_a,  0);
  (*env)->ReleaseLongArrayElements(env, write_hist, write_hist_a, 0);

  MUTEX_UNLOCK(wkl->stat_lock);

  return(*env)->NewStringUTF(env, long_line);
}



JNIEXPORT jstring JNICALL Java_Vdb_Native_getErrorText(JNIEnv *env,
                                                       jclass this,
                                                       jint   msg_no)
{
  char *buffer = strerror(msg_no);
  if (buffer == 0)
    return 0;

  return(*env)->NewStringUTF(env, buffer);
}



#ifndef WIN32
JNIEXPORT jlong JNICALL Java_Vdb_Native_getTickCount(JNIEnv *env,
                                                     jclass  this)
{
  jlong ticks;

  ticks = sysconf(_SC_CLK_TCK);
  return ticks;
}
#endif

/**
 * Store a fixed value in the read buffer for later checks to make sure that a
 * read really was done.
 */
extern void prepare_read_buffer(JNIEnv *env,
                                jlong   buffer,
                                jlong   length)
{
  *((unsigned int *) (buffer+0))        = 0xdeadbbbb;
  *((unsigned int *) (buffer+4))        = (unsigned int) buffer;
  *((unsigned int *) (buffer+length-4)) = 0xdeadeeee;
  *((unsigned int *) (buffer+length-8)) = (unsigned int) buffer;
}


extern int check_read_buffer(JNIEnv *env,
                             jlong   buffer,
                             jlong   length)
{
  int failure = 0;

  // debugging
  //*((unsigned int *) (buffer+0))        = 0xdeadbbbb;
  //*((unsigned int *) (buffer+4))        = (unsigned int) buffer;
  //*((unsigned int *) (buffer+length-8)) = (unsigned int) buffer;
  //*((unsigned int *) (buffer+length-4)) = 0xdeadeeee;


  if (*((unsigned int *) (buffer+0)) == 0xdeadbbbb &&
      *((unsigned int *) (buffer+4)) == (unsigned int) buffer)
    failure = 1;

  else if (*((unsigned int *) (buffer+length-4)) == 0xdeadeeee &&
           *((unsigned int *) (buffer+length-8)) == (unsigned int) buffer)
    failure = 2;

  else
    return 0;

  PTOD("Pre-formatted read buffer contents still found after successful read. Returning error 797");
  PTOD2("Failure %d, buffer: %08p", failure, buffer);
  PTOD2("Front: %08x %08x ", *((unsigned int *) (buffer+0)),        *((unsigned int*) (buffer+4)));
  PTOD2("End:   %08x %08x ", *((unsigned int *) (buffer+length-8)), *((unsigned int*) (buffer+length-4)));

  return 797;
}
