

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


#ifdef SOLARIS
  #include <sys/shm.h>
  #include <sys/mutex.h>
  #include <synch.h>
  #define MUTEX   mutex_t
  #define PROCESS pid_t
  #define MUTEX_INIT(lock, name)  mutex_init(&lock, USYNC_PROCESS, NULL);
  #define MUTEX_LOCK(lock) mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) mutex_unlock(&lock);


#elif LINUX
  #include <pthread.h>
//#include <signal.h>     // added 12/23/03
  #define _SIGNAL_H
  #include <bits/signum.h>
  #define MUTEX   pthread_mutex_t
  #define PROCESS pid_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#elif _AIX
  #include <pthread.h>
  #define _SIGNAL_H
  #define MUTEX   pthread_mutex_t
  #define PROCESS pid_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#elif WIN32
  #include <windows.h>
  #include <signal.h>
  #define WINERROR(x) { printf("%s %d \n", x, GetLastError()); abort(); }
  #define MUTEX   HANDLE
  #define PROCESS HANDLE
  #define MUTEX_INIT(lock, name) lock = CreateMutex(NULL, FALSE, #name); if (lock == NULL) WINERROR("CreateMutex failed");
  #define MUTEX_LOCK(lock) WaitForSingleObject(lock, INFINITE);
  #define MUTEX_UNLOCK(lock) ReleaseMutex(lock);


#elif HP
  #include <sys/shm.h>
//#include <sys/mutex.h>
  #include <pthread.h>
  #include <synch.h>
  #define MUTEX   pthread_mutex_t
  #define PROCESS pid_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#endif


#define uint  unsigned int
#define uchar unsigned char

char ptod_txt[256]; /* workarea for PTOD displays */

struct Seek_range
{
  jlong  next_lba;
  jlong  seek_low;
  jlong  seek_width;
  jlong  seek_high;
};


struct Workload                   /* One per workload                         */
{
  char  *lun;                     /* lun name                                 */
  char  *sdname;
  MUTEX   seek_lock;              /* Lock for seq seek address calculations:  */
  MUTEX   stat_lock;              /* Lock for statistics                      */

  struct Seek_range hits;         /* Seek info for cache hits                 */
  struct Seek_range miss;         /* seek infor for cache misses              */

  jdouble rdpct;                  /* Data for JNI controlled run              */
  jdouble rhpct;
  jdouble whpct;
  jdouble seekpct;
  jlong   fhandle;
  jint    threads_requested;      /* Threads\process requested by user        */
  PROCESS *pids;
  jint    xfersize;

  int    seq_done;               /* Sequential workload is done, skip new ios */

  jlong  reads;                  /* Statistics per workload */
  jlong  writes;
  jlong  resptime;
  jlong  resptime2;
  jlong  respmax;
  jlong  read_errors;
  jlong  write_errors;
  jlong  rbytes;
  jlong  wbytes;

};

#define SHARED_WORKLOADS 2048
#define MAX_PATTERNS (126+1)
struct Shared_memory
{
  int   go_ahead;
  int   workload_done;
  int   jni_controlled_run;
  jlong base_hrtime;
  char *patterns[MAX_PATTERNS];
  char  repeatable[MAX_PATTERNS];    /* Identifies that a pattern is 512 byte */
                                     /* repeatable (cpu efficiency)           */
  char  slib[256];                /* shared library name                      */
  struct Workload workload[SHARED_WORKLOADS];
};


#define ABORT(a,b)               \
{                                \
   printf("%s %s\n", a, b);      \
   if (shared_mem != 0) shared_mem->workload_done = 1; \
   if (shared_mem != 0) shared_mem->go_ahead      = 1; \
   abort();                      \
}

#define PP8(a) printf(" ++ %-20s: %lld\n", #a, (jlong) a);
#define PP4(a) printf(" ++ %-20s: %d\n",   #a, (jint)  a);


#define LOAD_LONG_ID(a, b)                                     \
  cls = (*env)->FindClass(env, #b);                            \
  if ( ( (a) = (*env)->GetFieldID(env, cls, #a, "J")) == NULL) \
    ABORT("Unable to load field ID of", #a);

#define CHECK(a)                                  \
  if ((*env)->ExceptionCheck(env))                \
  {                                               \
    printf("ExceptionCheck error %d \n", a);      \
    (*env)->ExceptionDescribe(env);               \
    (*env)->FatalError(env, "ExceptionCheck\n");  \
    return;                                       \
  }

#define PTOD(string)                                  \
{                                                     \
  jclass clx = (*env)->FindClass(env, "Vdb/common");      \
  jmethodID ptod = (*env)->GetStaticMethodID(env, clx,\
                   "ptod", "(Ljava/lang/String;)V");  \
  jstring jstr = (*env)->NewStringUTF(env, string);   \
  (*env)->CallStaticVoidMethod(env, clx, ptod, jstr); \
}

#define PTODS(string, x)                              \
{                                                     \
  jstring jstr;                                       \
  jclass clx = (*env)->FindClass(env, "Vdb/common");  \
  jmethodID ptod = (*env)->GetStaticMethodID(env, clx,\
                   "ptod", "(Ljava/lang/String;)V");  \
  sprintf(ptod_txt, string, x);                       \
  jstr = (*env)->NewStringUTF(env, ptod_txt);         \
  (*env)->CallStaticVoidMethod(env, clx, ptod, jstr); \
}

#define PLOG(string)                                  \
{                                                     \
  jclass clx = (*env)->FindClass(env, "Vdb/common");      \
  jmethodID plog = (*env)->GetStaticMethodID(env, clx,\
                   "plog", "(Ljava/lang/String;)V");  \
  jstring jstr = (*env)->NewStringUTF(env, string);   \
  (*env)->CallStaticVoidMethod(env, clx, plog, jstr); \
}


/* This describes the layout of each sector written:                       */
/* No long's used to avoid byte reversal on x86                            */
struct Sector
{
  uint lba_1;               /* logical byte address */
  uint lba_2;
  uint time_1;              /* Timestamp */
  uint time_2;
  uint bytes;               /* DV Key + Timestamp checksum  + 2 spares */
  char name[8];             /* SD or FSD name */
  uint spare;
  uint data[120];
#define DATA_OFFSET        (&data)
#define DATA_LENGTH        (480)
};


/* Common platform prototypes: */

extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname);
extern jlong file_close(JNIEnv *env, jlong fhandle);
extern jlong file_open(JNIEnv *env, const char *filename, int open_flags, int write);
extern jlong file_read(JNIEnv *env, jlong, jlong, jlong, jlong);
extern jlong file_write(JNIEnv *env, jlong, jlong, jlong, jlong);
extern jlong alloc_buffer(JNIEnv *env, int bufsize);
extern void  free_buffer(int bufsize, jlong buffer);
extern jlong get_simple_tod(void);
extern void  sleep_usecs(jlong usecs);
extern void  init_shared_mem();

extern void fill_buffer(JNIEnv *env,
                        jlong  buffer,
                        jlong  lba,
                        uint   key,
                        int    xfersize,
                        char   *name_in);

extern int validate_whole_buffer(JNIEnv *env,
                                 jlong  handle,
                                 jlong  buffer,
                                 jlong  file_start_lba,
                                 jlong  file_lba,
                                 jint   key,
                                 jint   xfersize,
                                 char*  name_in);

extern jlong get_vtoc(JNIEnv *env, jlong fhandle, const char* fname);

extern int set_max_open_files(JNIEnv *env);

#define PP(a) printf(" ++ %-20s: %lld\n", #a, (jlong) a);


void report_bad_block(JNIEnv *env, jlong read_flag, jlong fhandle,
                      jlong   lba, jlong xfersize,  jlong error);

