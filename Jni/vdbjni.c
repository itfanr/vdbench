

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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#ifndef _WINDOWS
  #include <unistd.h>
  #ifndef LINUX
  #include <stropts.h>
#endif
#endif
#include <time.h>
#include "vdbjni.h"


/* Array offsets to pick up data from Java.                     */
/* This MUST stay in sync with the same offsets in IO_task.java */
#define CMD_READ_FLAG  0
#define CMD_RAND       1
#define CMD_HIT        2
#define CMD_LBA        3
#define CMD_XFERSIZE   4
#define DV_KEY         5
#define CMD_FHANDLE    6
#define CMD_BUFFER     7
#define WG_NUM         8
#define CMD_FAST_LONGS 9

/* Definiton of shmid to be used by vdbsol.c */
int shmid;

struct Shared_memory *shared_mem = NULL;

static int negative_response_count = 0;


/**
 * Java call to get a lun's size in bytes
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_getsize(JNIEnv *env,
                                                jclass this,
                                                jlong  fhandle,
                                                jstring filename)
{

  const    char *fname;
  jlong    size;

  fname = (*env)->GetStringUTFChars(env, filename, 0);
  size = file_size(env, (jlong) fhandle, fname);
  (*env)->ReleaseStringUTFChars(env, filename, fname);

  return size;

}


/**
 * Java call to close a lun
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_closefile(JNIEnv *env,
                                                  jclass this,
                                                  jlong  fhandle)
{
  return file_close(env, fhandle);
}


/**
 * Java call to open a lun
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_openfile(JNIEnv *env,
                                                 jobject this,
                                                 jstring filename,
                                                 jint    flush,
                                                 jint    write)
{
  const char *fname;
  jlong fhandle;

#ifdef SOLARIS
  {
    static unsigned long max_files = 0;
    if (max_files == 0)
      max_files = set_max_open_files(env);
  }
#endif

  fname = (*env)->GetStringUTFChars(env, filename, 0);
  fhandle = file_open(env, fname, flush, write);

  (*env)->ReleaseStringUTFChars(env, filename, fname);

  return fhandle;
}


/**
 * Java call to read data; only used for Journal
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_read(JNIEnv *env,
                                             jobject this,
                                             jlong   fhandle,
                                             jlong   seek,
                                             jlong   length,
                                             jlong   buffer)
{
  jlong rc;

  if ( (rc = file_read(env, fhandle, seek, length, buffer)) != 0 )
    return rc;

  return rc;
}


/**
 * Java call to write data. Is only used for Journal and SCSI reset!
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_write(JNIEnv *env,
                                              jobject this,
                                              jlong   fhandle,
                                              jlong   seek,
                                              jlong   length,
                                              jlong   buffer)
{
  jlong rc;

#ifdef SOLARIS
#include <sys/scsi/impl/uscsi.h>

  if ( buffer < 0 )
  {
    static struct uscsi_cmd cmd;

    if ( buffer == -1 )
      cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET;
    else
      cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET_ALL;

    if ( ioctl((int) fhandle, USCSICMD, &cmd) < 0 )
    {
      fprintf( stderr, "Reset error: %s\n", strerror(errno) );
      return -1;
    }
    return 0;
  }
#endif

  if ( (rc = file_write(env, fhandle, seek, length, buffer)) != 0 )
    return rc;

  return rc;
}




static void lower_seq_eofs(JNIEnv *env)
{
  jclass clx;
  jmethodID lower;

  CHECK(1);
  clx = (*env)->FindClass(env, "Vdb/WG_entry");
  CHECK(2);
  lower = (*env)->GetStaticMethodID(env, clx, "sequentials_lower", "()V");
  CHECK(3);

  (*env)->CallStaticVoidMethod(env, clx, lower);
}


/**
 * Java call to issue one or more i/o's
 */
JNIEXPORT void JNICALL Java_Vdb_Native_multi_1io(JNIEnv *env,
                                                 jclass this,
                                                 jlongArray cmd_fast_java,
                                                 jint   burst)
{

  jsize              i;
  int                offset, xfersize, key;
  int                wg_num, doseek, hit, read_flag;
  jlong              rc, buffer, fhandle, lba;
  jlong             *cmd_fast;
  jboolean           iscopy;
  struct Workload   *wkl;
  struct Seek_range *seek_range;
  jlong              tod1, elapsed, square;

  /* Load the array of longs: */
  cmd_fast = (*env)->GetLongArrayElements(env, cmd_fast_java, &iscopy);

  for ( i = 0; i < burst; i++ )
  {
    /* Get all of the data that we always need: */
    offset    = CMD_FAST_LONGS * (int) i;
    buffer    = cmd_fast[ offset + CMD_BUFFER    ];
    fhandle   = cmd_fast[ offset + CMD_FHANDLE   ];
    lba       = cmd_fast[ offset + CMD_LBA       ];
    wg_num    = (int) cmd_fast[ offset + WG_NUM        ];
    doseek    = (int) cmd_fast[ offset + CMD_RAND      ];
    xfersize  = (int) cmd_fast[ offset + CMD_XFERSIZE  ];
    hit       = (int) cmd_fast[ offset + CMD_HIT       ];
    key       = (int) cmd_fast[ offset + DV_KEY        ];
    read_flag = (int) cmd_fast[ offset + CMD_READ_FLAG ];


    /* Get the context info and synchronize: */
    wkl       = &shared_mem->workload[wg_num];
    seek_range = &wkl->miss;
    if ( hit )
      seek_range = &wkl->hits;


    /* Calculate next seek address and roll over if needed: */
    MUTEX_LOCK(wkl->seek_lock);

    /* Already sequential eof for this workload? */
    if ( wkl->seq_done )
    {
      MUTEX_UNLOCK(wkl->seek_lock);
      continue;
    }


    /* Get CURRENT seq lba and calculate lba for the NEXT i/o: */
    if ( doseek != 1 )
      lba = seek_range->next_lba;
    seek_range->next_lba = lba + xfersize;
    if ( seek_range->next_lba >= seek_range->seek_high )
    {
      if ( doseek == 2 )
      {
        wkl->seq_done = 1;
        lower_seq_eofs(env);
        //MUTEX_UNLOCK(wkl->seek_lock);
      }
      seek_range->next_lba = seek_range->seek_low;
    }
    MUTEX_UNLOCK(wkl->seek_lock);


    tod1 = get_simple_tod();

    /* Allow debugging by forcing an io error: */
    if (lba & 1 != 0)
      rc = 60002;
    else
    {
      if ( read_flag )
      {
        *(uint*) buffer = 0xffffffff;  // seed buffer in case read just fails!
        rc = file_read(env, fhandle, lba, xfersize, buffer);
      }
      else
      {
        // Check removed because of undocumented '-erase' parameter
        //if ( lba == 0 )
        //{
        //  PTOD("Trying to write to lba 0 ");
        //  ABORT("Trying to write to lba 0", wkl->lun);
        //}

        /* Move the requested key value: */
        if ( key != 0 )
          fill_buffer(env, buffer, lba, key, xfersize, wkl->sdname);

        rc = file_write(env, fhandle, lba, xfersize, buffer);
      }
    }


    /* Update statistics: */
    elapsed = get_simple_tod() - tod1;
    square  = elapsed * elapsed;
    MUTEX_LOCK(wkl->stat_lock);

    if ( read_flag )
      wkl->rbytes += xfersize;
    else
      wkl->wbytes += xfersize;

    wkl->resptime  += elapsed;
    wkl->resptime2 += square;
    if ( wkl->respmax < elapsed ) wkl->respmax = elapsed;
    if ( read_flag )
    {
      wkl->reads++;
      if ( rc != 0 )
        wkl->read_errors++;
    }
    else
    {
      wkl->writes++;
      if ( rc != 0 )
        wkl->write_errors++;
    }
    MUTEX_UNLOCK(wkl->stat_lock);

    if ( rc != 0 )
    {
      MUTEX_LOCK(wkl->stat_lock);
      /*
  void report_bad_block(JNIEnv *env, jlong read_flag, jlong fhandle,
                        jlong   lba, jlong xfersize,  jlong error)*/
      report_bad_block(env, read_flag, fhandle, lba, (jlong) xfersize, rc);
      MUTEX_UNLOCK(wkl->stat_lock);
    }
    else
    {
      if ( key != 0 && read_flag)
      {
        /*
extern int validate_whole_buffer(JNIEnv *env,
                                 jlong  handle,
                                 jlong  buffer,
                                 jlong  file_start_lba,
                                 jlong  file_lba,
                                 jint   key,
                                 jint   xfersize,
                                 char*  name_in)
        */
        if (validate_whole_buffer(env, fhandle, buffer, 0, lba, key, xfersize, wkl->sdname) > 0)
          report_bad_block(env, read_flag, fhandle, lba, (jlong) xfersize, 60003);
      }
    }

    if (elapsed < 0)
    {
      if (negative_response_count == 0)
      {
        PTOD("Negative response time. Usually caused by out of sync cpu timers.");
        PTOD("Will reported a maximum of 100 times after which Vdbench will continue.");
      }
      if (negative_response_count++ < 100)
      {

#ifdef _WINDOWS
        sprintf(ptod_txt, "Response time (microseconds): %I64d", elapsed);
#else
        sprintf(ptod_txt, "Response time (microseconds): %lldd", elapsed);
#endif
        PTOD(ptod_txt);
      }
    }
  }


  (*env)->ReleaseLongArrayElements(env, cmd_fast_java, cmd_fast, JNI_ABORT);

}



/**
 * Signal routine for shared memory cleanup
 */
void jni_cleanup(int signal)
{
#ifdef SOLARIS

  /* note: do not code things like 'SIGBUS'. Windows won't like it: */
  if ( signal == 10 )
    printf("jni_cleanup: received signal 10 (SIGBUS);  bus error \n");
  else if ( signal == 2 )
    printf("jni_cleanup: received signal  2 (SIGINT);  interrupt \n");
  else if ( signal == 6 )
    printf("jni_cleanup: received signal  6 (SIGABRT); abort \n");
  else if ( signal == 18 )        /* 18 (SIGCLD);  child status change */
    return;
  else if ( signal == 4 )        /* 18 (SIGCLD);  child status change */
  {
    printf("signal 4, ignored \n");
    return;
  }
  else if ( signal == 16 )       /* 16 (SIGUSR1);  user defined signal 1 */
  {
    printf("jni_cleanup: received signal 16 (SIGUSR1) \n");
    printf("jni_cleanup: ignoring signal until reason for signal is better understood \n", signal);
    return;
  }

  else
    printf("jni_cleanup: received signal %d \n", signal);

  if ( shared_mem != 0 )
  {
    shared_mem->go_ahead = 1;
    shared_mem->workload_done = 1;
    if (shmid != -1)
      shmctl(shmid, IPC_RMID, 0);
  }

  exit(0);
#endif
}


/**
 * Java call to allocate memory for JNI control of workloads.
 * Either allocate true shared memory when JNI will be doing the workload,
 * or allocate non-shared memory when Java does the workload.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_alloc_1jni_1shared_1memory(JNIEnv *env,
                                                                  jclass  this,
                                                                  jboolean jni_in_control,
                                                                  jstring shared_lib)
{

#ifdef SOLARIS

  if ( jni_in_control )
  {
    int i;
    for ( i = 0; i < 20; ++i )
    {
      /* Removed signal 4. Sonboleh had a situation where it showed up without reason! */
      if (i != 16 &&  // SIGUSR1
          i !=  4 &&  // SIGILL
          i != 11 &&  // SIGSEGV
          i != 18 &&  // SIGCLD
          i != 17 )   // SIGUSR2
        signal(i, jni_cleanup);
    }

    if ( shared_mem == NULL )
    {
      if ( (shmid = shmget(IPC_PRIVATE, sizeof(struct Shared_memory), 0777 | IPC_CREAT)) == -1 )
      {
        sprintf(ptod_txt, "shmget failed: length %d", sizeof(struct Shared_memory));
        PTOD(ptod_txt);
        ABORT("shmget failed", strerror(errno));
      }

      if ( (int) (shared_mem = (struct Shared_memory*) shmat(shmid, 0, 0)) == -1 )
      {
        PTOD("If you are running Solaris 2.8, you have to increase /etc/system "
             "set shmsys:shminfo_shmseg=6 (default) to at least the amount "
             "of runs you are doing. Run 'java vdbench -s' to count the runs.\n");
        ABORT("shmat failed", strerror(errno));
      }
      init_shared_mem();
    }
  }

  else
  {
    if ( shared_mem == NULL )
    {
      shared_mem = (struct Shared_memory*) valloc(sizeof(struct Shared_memory));
      if ( shared_mem == NULL )
        ABORT("valloc for shared memory failed", strerror(errno));
      init_shared_mem();
    }
  }

#else
  if ( shared_mem == NULL )
  {
    shared_mem = (struct Shared_memory*) malloc(sizeof(struct Shared_memory));
    if ( shared_mem == NULL )
      ABORT("malloc for shared memory failed", strerror(errno));
    init_shared_mem();
  }
#endif

  /* Erase all workload data from previous run: */
  memset(shared_mem->workload, 0, sizeof(struct Workload) * SHARED_WORKLOADS);

  /* Store the shared library name, used only for vdblite: */
  strcpy(shared_mem->slib, (*env)->GetStringUTFChars(env, shared_lib, 0));

}


/**
 * Free the vdblite shared memory, if there.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_free_1jni_1shared_1memory(JNIEnv *env,
                                                                 jclass  this)
{
#ifdef SOLARIS
  if (shmid != -1)
    shmctl(shmid, IPC_RMID, 0);
#endif
}


/**
 * Java context to pass on workload information
 */
JNIEXPORT void JNICALL Java_Vdb_Native_setup_1jni_1context(JNIEnv  *env,
                                                           jclass  this,
                                                           jint    xfersize,
                                                           jdouble seekpct,
                                                           jdouble readpct,
                                                           jdouble rhpct,
                                                           jdouble whpct,
                                                           jlong   hseek_low,
                                                           jlong   hseek_high,
                                                           jlong   hseek_width,
                                                           jlong   mseek_low,
                                                           jlong   mseek_high,
                                                           jlong   mseek_width,
                                                           jlong   fhandle,
                                                           jint    threads_requested,
                                                           jint    workload_no,
                                                           jstring lun,
                                                           jstring sdname)
{
  struct Workload *wkl;

  if ( shared_mem == NULL )
  {
    PTOD("JNI shared memory not yet initialized");
    abort();
  }

  if ( workload_no >= SHARED_WORKLOADS )
  {
    sprintf(ptod_txt, "vdblite: too many workloads/threads; requested: %d; only %d allowed",
            workload_no, SHARED_WORKLOADS);
    PTOD(ptod_txt);
    abort();
  }

  if ( threads_requested == 0 )
  {
    sprintf(ptod_txt, "JNI received zero thread count\n");
    PTOD(ptod_txt);
    abort();
  }


  wkl = &shared_mem->workload[workload_no];
  wkl->lun    = (char*) (*env)->GetStringUTFChars(env, lun, 0);
  wkl->sdname = (char*) (*env)->GetStringUTFChars(env, sdname, 0);
  wkl->xfersize          = xfersize;
  wkl->seekpct           = seekpct;
  wkl->rdpct             = readpct;
  wkl->rhpct             = rhpct;
  wkl->whpct             = whpct;

  wkl->hits.seek_low     = hseek_low;
  wkl->hits.seek_high    = hseek_high;
  wkl->hits.seek_width   = hseek_width;
  wkl->hits.next_lba     = hseek_low;

  wkl->miss.seek_low     = mseek_low;
  wkl->miss.seek_high    = mseek_high;
  wkl->miss.seek_width   = mseek_width;
  wkl->miss.next_lba     = mseek_low;

  wkl->fhandle           = fhandle;
  wkl->threads_requested = threads_requested;
  MUTEX_INIT(wkl->seek_lock, "seek_lock");
  MUTEX_INIT(wkl->stat_lock, "stat_lock");

  return;
}


/**
 * Java call to get statistics for ONE workload.
 * Also used to signal end-of-run to JNI
 */
JNIEXPORT void JNICALL Java_Vdb_Native_get_1one_1set_1statistics(JNIEnv   *env,
                                                                 jclass   this,
                                                                 jobject  stats,
                                                                 jint     workload_no,
                                                                 jboolean workload_done)
{
  jclass  cls;
  struct Workload *wkl = (struct Workload*) &shared_mem->workload[workload_no];

  jfieldID  reads;
  jfieldID  writes;
  jfieldID  resptime;
  jfieldID  resptime2;
  jfieldID  respmax;
  jfieldID  read_errors;
  jfieldID  write_errors;
  jfieldID  rbytes;
  jfieldID  wbytes;


  /* If we try to send 'done' message and we have no shared memory, that's OK */
  if ( workload_done && shared_mem == 0 )
    return;


  CHECK(3);
  LOAD_LONG_ID( reads        , Vdb/WG_stats );
  //printf("++ wkl: %08p %d %lld\n", wkl, workload_no, wkl->reads);
  CHECK(4);
  LOAD_LONG_ID( writes       , Vdb/WG_stats );
  LOAD_LONG_ID( resptime     , Vdb/WG_stats );
  LOAD_LONG_ID( resptime2    , Vdb/WG_stats );
  LOAD_LONG_ID( respmax      , Vdb/WG_stats );
  LOAD_LONG_ID( read_errors  , Vdb/WG_stats );
  LOAD_LONG_ID( write_errors , Vdb/WG_stats );
  LOAD_LONG_ID( rbytes       , Vdb/WG_stats );
  LOAD_LONG_ID( wbytes       , Vdb/WG_stats );
  CHECK(5);

  /* Save run status: */
  shared_mem->workload_done = workload_done ? 1 : 0;


  /* Store the data (lock it to make sure that we are not in the middle */
  /* of an update and the caller sees an average of 511 bytes...        */
  MUTEX_LOCK(wkl->stat_lock);
  (*env)->SetLongField(env, stats, reads       , wkl->reads       );
  (*env)->SetLongField(env, stats, writes      , wkl->writes      );
  (*env)->SetLongField(env, stats, resptime    , wkl->resptime    );
  (*env)->SetLongField(env, stats, resptime2   , wkl->resptime2   );
  (*env)->SetLongField(env, stats, respmax     , wkl->respmax     );
  (*env)->SetLongField(env, stats, read_errors , wkl->read_errors );
  (*env)->SetLongField(env, stats, write_errors, wkl->write_errors);
  (*env)->SetLongField(env, stats, rbytes      , wkl->rbytes      );
  (*env)->SetLongField(env, stats, wbytes      , wkl->wbytes      );
  wkl->respmax = 0;
  MUTEX_UNLOCK(wkl->stat_lock);
}



