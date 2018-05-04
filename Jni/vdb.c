

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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vdbjni.h"

#ifndef _WINDOWS
#include <unistd.h>
#endif

#ifdef _WINDOWS
  #include <sys/timeb.h>
  #define uintptr_t __int64
#endif

#ifdef LINUX
//#include <sys/timeb.h>
  #define uintptr_t jlong
#endif


extern struct Shared_memory *shared_mem;




/**
 * Java call to copy an 'int' array to a native buffer
 */
JNIEXPORT void JNICALL Java_Vdb_Native_array_1to_1buffer(JNIEnv   *env,
                                                         jclass    this,
                                                         jintArray array,
                                                         jlong     buffer)
{

  int   i, *buf;
  jsize len;
  jint *body;

  len  = (*env)->GetArrayLength(env, array);
  body = (*env)->GetIntArrayElements(env, array, 0);


  for ( buf = (int*) (uintptr_t) buffer, i = 0; i < len; i++ )
    buf[i] = body[i];

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
                                                         jlong  bufsize)
{
  (*env)->SetIntArrayRegion(env, array, 0, (int) bufsize / 4, (jint*) (uintptr_t) buffer);
}




/**
 * Java call to obtain timestamp
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_get_1simple_1tod(JNIEnv *env,
                                                         jclass this)
{
  return get_simple_tod();
}


/**
 * Java call to allocate native memory
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_allocbuf(JNIEnv *env,
                                                 jclass this,
                                                 jlong bufsize)
{
  jlong addr;
  int size = (int) bufsize;

  addr = alloc_buffer(env, size);

  return addr;
}


/**
 * Java call to free native memory
 */
JNIEXPORT void JNICALL Java_Vdb_Native_freebuf(JNIEnv *env,
                                               jclass this,
                                               jlong bufsize,
                                               jlong buffer)
{
  free_buffer((int) bufsize, buffer);
}


#ifndef _WINDOWS
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
 * Initialize shared memory.
 * Clear it all to zero, and store a timestamp that is used as
 * true 1/1/1970 offset in microseconds that we can use to add
 * the results of get_simple_tod() to so that we have an efficient
 * true tod stamp. (for DV)
 */
extern void init_shared_mem()
{
  jlong  usecs;
  jlong  simple;

  /* Clear it all out: */
  memset(shared_mem, 0, sizeof(struct Shared_memory));


#ifdef _WINDOWS
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
  simple = get_simple_tod();
  usecs -= simple;

  shared_mem->base_hrtime = usecs;


}








