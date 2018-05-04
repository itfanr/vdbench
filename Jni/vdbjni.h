

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */

#include <jni.h>
#include "Vdb_Native.h"
#include <fcntl.h>

#ifdef SOLARIS
  #include <sys/shm.h>
  #include <sys/mutex.h>
  #include <synch.h>
  #define MUTEX   mutex_t
  #define MUTEX_INIT(lock, name)  mutex_init(&lock, USYNC_PROCESS, NULL);
  #define MUTEX_LOCK(lock) mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) mutex_unlock(&lock);
//#define MUTEX_LOCK(lock) ;
//#define MUTEX_UNLOCK(lock) ;


#elif LINUX
  #include <pthread.h>
  #define MUTEX   pthread_mutex_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);

#elif __APPLE__
  #include <pthread.h>
  #include <sys/types.h>
  #define MUTEX   pthread_mutex_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#elif AIX
  #include <pthread.h>
  #define MUTEX   pthread_mutex_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#elif WIN32old
  #include <windows.h>
  #define WINERROR(x) { printf("%s %d \n", x, GetLastError()); abort(); }

  #define MUTEX   HANDLE
  #define MUTEX_INIT(lock, name) lock = CreateMutex(NULL, FALSE, #name); if (lock == NULL) WINERROR("CreateMutex failed");
  #define MUTEX_LOCK(lock) WaitForSingleObject(lock, INFINITE);
  #define MUTEX_UNLOCK(lock) ReleaseMutex(lock);

#elif WIN32
  #include <windows.h>
  #define WINERROR(x) { printf("%s %d \n", x, GetLastError()); abort(); }

//http://preshing.com/20111124/always-use-a-lightweight-mutex/
  #define MUTEX   CRITICAL_SECTION
  #define MUTEX_INIT(lock, name) InitializeCriticalSection(&lock);
  #define MUTEX_LOCK(lock) EnterCriticalSection(&lock);
  #define MUTEX_UNLOCK(lock) LeaveCriticalSection(&lock);



#elif HP
  #include <sys/shm.h>
//#include <sys/mutex.h>
  #include <pthread.h>
  #include <synch.h>
  #define MUTEX   pthread_mutex_t
  #define MUTEX_INIT(lock, name)  pthread_mutex_init(&lock, NULL);
  #define MUTEX_LOCK(lock) pthread_mutex_lock(&lock);
  #define MUTEX_UNLOCK(lock) pthread_mutex_unlock(&lock);


#endif


#define uint   unsigned int
#define uchar  unsigned char
#define ushort unsigned short

char ptod_txt[256]; /* workarea for PTOD displays */


/* These flags come from Validate.java. */
static jint FLAG_VALIDATE        = 0x0001;
static jint FLAG_DEDUP           = 0x0002;
static jint FLAG_COMPRESSION     = 0x0004;
static jint FLAG_SPARE1          = 0x0008;
static jint FLAG_VALIDATE_NORMAL = 0x0010;
static jint FLAG_VALIDATE_DEDUP  = 0x0020;  // unused in JNI
static jint FLAG_VALIDATE_COMP   = 0x0040;
static jint FLAG_USE_PATTERN_BUF = 0x0080;
static jint FLAG_NORMAL_READ     = 0x0100;
static jint FLAG_PRE_READ        = 0x0200;
static jint FLAG_READ_IMMEDIATE  = 0x0400;
//static jint WRITING_FROM_PATTERN = 0x0800;  // Only exists in JNI


/* These first four also reside in Dedup.java */
#ifndef WIN32
static jlong UNIQUE_KEY_MASK         = 0x7F00000000000000LL;
static jlong UNIQUE_MASK             = 0x00F0000000000000LL;
static jlong UNIQUE_BLOCK_MASK       = 0x0080000000000000LL;
static jlong UNIQUE_BLOCK_ACROSS_YES = 0x00C0000000000000LL;
static jlong UNIQUE_BLOCK_ACROSS_NO  = 0x00E0000000000000LL;
static jlong DEDUP_NO_DEDUP          = 0x00F0000000000000LL;
static jlong DEDUPSET_TYPE_MASK      = 0x0000FFFF00000000LL;
static jlong DEDUPSET_NUMBER_MASK    = 0x00000000FFFFFFFFLL;
#else
static jlong UNIQUE_KEY_MASK         = 0x7F00000000000000l;
static jlong UNIQUE_MASK             = 0x00F0000000000000l;
static jlong UNIQUE_BLOCK_MASK       = 0x0080000000000000l;
static jlong UNIQUE_BLOCK_ACROSS_YES = 0x00C0000000000000l;
static jlong UNIQUE_BLOCK_ACROSS_NO  = 0x00E0000000000000l;
static jlong DEDUP_NO_DEDUP          = 0x00F0000000000000l;
static jlong DEDUPSET_TYPE_MASK      = 0x0000FFFF00000000l;
static jlong DEDUPSET_NUMBER_MASK    = 0x00000000FFFFFFFFl;
#endif



/* If I decide to have the buckets sorted on use frequency, all I need to do */
/* in JNI is copy the old Bucket.count contents to the new ordered position. */
#define MAX_BUCKETS 64
struct Bucket
{
  jlong  min;
  jlong  max;
  jlong  count;
};
struct Histogram
{
  int    jni_bytes;
  jlong  buckets;
  jlong  last;
  struct Bucket bucket[MAX_BUCKETS];
};

struct Workload                   /* One per workload                         */
{
  char*  sdname;
  MUTEX  stat_lock;               /* Lock for statistics                      */

  jlong  reads;                  /* Statistics per workload                   */
  jlong  r_resptime;
  jlong  r_resptime2;
  jlong  r_max;
  jlong  r_bytes;
  jlong  r_errors;

  jlong  writes;
  jlong  w_resptime;
  jlong  w_resptime2;
  jlong  w_max;
  jlong  w_bytes;
  jlong  w_errors;

  jlong  rlastupdate;             /* For kstat_runq_enter and kstat_runq_exit */
  jlong  rlentime;
  jlong  rtime;
  jlong  q_depth;

  struct Histogram read_hist;
  struct Histogram write_hist;
};

/* Make sure you synchronize this with JniIndex.java!!! */
#define SHARED_WORKLOADS 10240    /* At 10240 elements that is 34,734,152 bytes */


/* Though this is called Shared_memory, it no longer resides in shared memory. */
/* The shared memory need disapeared with the removal of vdblite.              */
struct Shared_memory
{
  jlong base_hrtime;              /* Discovered a .5 second delta here on 07/24/15. Not sure why. No longer using it */
  jlong pid;                      /* Used for the Data Validation header, but not compared */
  char *pattern;
  int   pattern_length;
  MUTEX printf_lock;              /* Lock to avoid (s)printf threading issue  */
                                  /* Linux especially.                        */
  MUTEX hash_lock;

  int max_workload;
  struct Workload workload[SHARED_WORKLOADS];
};


void snap(JNIEnv *env, char *text, void* start, int length);


void prepare_read_buffer(JNIEnv *env,
                         jlong   buffer,
                         jlong   length);

int check_read_buffer(JNIEnv *env,
                      jlong   buffer,
                      jlong   length);




#define ABORT(a,b)                                            \
{                                                             \
   MUTEX_LOCK(shared_mem->printf_lock);                       \
   sprintf(ptod_txt, "\n\nVdbench JNI abort: %s %s\n", a, b); \
   MUTEX_UNLOCK(shared_mem->printf_lock);                     \
   PTOD(ptod_txt);                                            \
   abort();                                                   \
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
  jclass clx = (*env)->FindClass(env, "Vdb/common");  \
  jmethodID ptod = (*env)->GetStaticMethodID(env, clx,\
                   "ptod", "(Ljava/lang/String;)V");  \
  jstring jstr = (*env)->NewStringUTF(env, string);   \
  (*env)->CallStaticVoidMethod(env, clx, ptod, jstr); \
}

#define PTOD1(string, x)                              \
{                                                     \
  char tmptxt[256];                                   \
  MUTEX_LOCK(shared_mem->printf_lock);                \
  sprintf(tmptxt, string, x);                         \
  MUTEX_UNLOCK(shared_mem->printf_lock);              \
  PTOD(tmptxt);                                       \
}
// Sleep(100);           \

#define PTOD2(string, x, y)                           \
{                                                     \
  char tmptxt[256];                                   \
  MUTEX_LOCK(shared_mem->printf_lock);                \
  sprintf(tmptxt, string, x, y);                      \
  MUTEX_UNLOCK(shared_mem->printf_lock);              \
  PTOD(tmptxt);                                       \
}

#define PTOD3(string, x, y, z)                        \
{                                                     \
  char tmptxt[256];                                   \
  MUTEX_LOCK(shared_mem->printf_lock);                \
  sprintf(tmptxt, string, x, y, z);                   \
  MUTEX_UNLOCK(shared_mem->printf_lock);              \
  PTOD(tmptxt);                                       \
}

#define PTOD4(string, x, y, z, a)                     \
{                                                     \
  char tmptxt[256];                                   \
  MUTEX_LOCK(shared_mem->printf_lock);                \
  sprintf(tmptxt, string, x, y, z, a);                \
  MUTEX_UNLOCK(shared_mem->printf_lock);              \
  PTOD(tmptxt);                                       \
}

#define PLOG(string)                                  \
{                                                     \
  jclass clx = (*env)->FindClass(env, "Vdb/common");  \
  jmethodID plog = (*env)->GetStaticMethodID(env, clx,\
                   "plog", "(Ljava/lang/String;)V");  \
  jstring jstr = (*env)->NewStringUTF(env, string);   \
  (*env)->CallStaticVoidMethod(env, clx, plog, jstr); \
}



/* This describes the layout of each sector written: */
/* No longs used to avoid byte reversal on x86       */
struct Sector
{
  uint   lba1;             /* logical byte address */
  uint   lba2;
  uint   time1;            /* Timestamp */
  uint   time2;
  uint   bytes;            /* DV Key + Timestamp checksum  + dedup_type + 1 spare */
  char   name[8];          /* SD or FSD name */
  uint   pid;              /* PID used when writing. */
                           /* Can not be checked yet since journal recovery */
                           /* can of course use a different PID. TBD.       */
  uint   data[120];

#define DATA_OFFSET        (&data)
#define DATA_LENGTH        (480)
};



/* This describes one single i/o request, with all the info received from java */
struct Request
{
  struct Sector *sector;
  jlong  fhandle;             /* file handle                                           */
  jint   data_flag;           /* What type of pattern to generate, dedup, compression  */
  jlong  caller_buffer;       /* Data passed by caller                                 */
  jlong  buffer;              /* Current buffer address for data manipulation          */
  jlong  file_lba;            /* The lba to read+write against within file or lun      */
  jlong  file_start_lba;      /* The logical lba of the start of the file or 0 for lun */
  jlong  compression;         /* An offset into the compression data pattern buffer    */
  jlong  dedup_set;           /* Dedupset to be used for pattern generation            */
  jlong  offset_in_key_block; /* Where were we when we hit a DV error                  */
  jlong  write_time_ms;       /* tod at write to store in DV prefix                    */
  jlong  sector_lba;          /* The lba to use for data pattern generation            */
  jint   sectors;             /* How many 512-bnyte sectors in key block               */
  jint   key;                 /* Key to WRITE or key to COMPARE with when reading      */
  jint   key_blksize;         /* Key block size                                        */
  jint   data_length;         /* Data block size                                       */
  jlong  pattern_lba;         /* What lba is data pattern based on                     */
  jint   pattern_length;      /* The length of this pattern (can be > data_length)     */
  jint   jni_index;           /* Index into statistics gathering                       */
  char*  dv_text;             /* Used to place in the sector header, e.g. SD name      */
};


#define SECTOR_START() \
    req->sector              = (struct Sector*) req->buffer;  \
    req->sectors             = req->key_blksize >> 9;          \
    req->offset_in_key_block = 0;                             \
    req->sector_lba          = req->pattern_lba;

#define SECTOR_PLUS() \
    req->sector               = (struct Sector*) ((char*) req->sector + 512); \
    req->offset_in_key_block += 512; \
    req->sector_lba          += 512;


/* Measured on a 4600e 9/22/10: 129 nanoseconds with or without '/ 1000' */
#ifdef SOLARIS
  #define GET_SIMPLE_TOD() (gethrtime() / 1000)
#else
  #define GET_SIMPLE_TOD() (get_simple_tod())
#endif

#define UPDATE_WORKLOAD_STATS(env, wkl, read_flag, xfersize, tod1, rc) \
        if (wkl)                                                  \
          update_workload_stats(env, wkl, read_flag, xfersize, tod1, rc);

#define START_WORKLOAD_STATS(env, wkl)    \
        (wkl) ? start_workload_stats(env, wkl) : 0;


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
extern void  init_shared_mem(JNIEnv *env, jlong pid);

extern void fill_key_block(JNIEnv *env, struct Request *req);

extern int validate_whole_buffer(JNIEnv *env, struct Request *req);

extern jlong get_vtoc(JNIEnv *env, jlong fhandle, const char* fname);

extern int set_max_open_files(JNIEnv *env);

#define PP(a) printf(" ++ %-20s: %lld\n", #a, (jlong) a);


extern jlong start_workload_stats(JNIEnv *env,
                                  struct Workload *wkl);

extern void update_workload_stats(JNIEnv *env,
                                  struct Workload *wkl,
                                  jint   read_flag,
                                  jint   xfersize,
                                  jlong  tod1,
                                  jlong  rc);

extern void close_workload_interval(JNIEnv          *env,
                                    struct Workload *wkl);

extern void report_io_error(JNIEnv *env,
                            jlong  read_flag,
                            jlong  fhandle,
                            jlong  lba,
                            jint   xfersize,
                            jlong  error,
                            jlong  buffer);

extern void generate_lfsr_data(JNIEnv       *env,
                               uint*        buffer,
                               jint         bytes,
                               jlong        lba,
                               uint         key,
                               const char*  name);
extern void prevent_dedup(JNIEnv *env,
                          jlong   fhandle,
                          jlong   file_lba,
                          jlong   buffer,
                          int     xfersize);

void printRequest(JNIEnv *env, struct Request *req);

