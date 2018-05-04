

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */


#include "vdbjni.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <kstath.h>
#include <kstat.h>
#include <errno.h>


static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";


kstat_ctl_t *global_kstat_kc;

JNIEXPORT jlong JNICALL Java_Vdb_Native_closeKstatGlobal(JNIEnv *env,
                                                         jclass this)
{
  int rc = kstat_close(global_kstat_kc);
  global_kstat_kc = 0;
  return (jlong) rc;
}

JNIEXPORT jlong JNICALL Java_Vdb_Native_openKstatGlobal(JNIEnv *env,
                                                        jclass this)
{
  global_kstat_kc = kstat_open();
  return (jlong) global_kstat_kc;
}

/**
 * Java call to obtain Solaris kstat pointer for device statistics
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_getKstatPointer(JNIEnv *env,
                                                        jclass  this,
                                                        jstring in_stance)
{
  const char      *instance;
  kstat_t         *kstat_ptr;
  struct Dev_name *dev;
  jlong            ks_ptr;

  instance = (*env)->GetStringUTFChars(env, in_stance, 0);

  ks_ptr =  (jlong) get_kstat_t(env, instance);

  (*env)->ReleaseStringUTFChars(env, in_stance, instance);

  return ks_ptr;
}



JNIEXPORT jlong JNICALL Java_Vdb_Native_getKstatData(JNIEnv *env,
                                                     jclass this,
                                                     jobject  ks,
                                                     jlong    pointer)
{
  struct kstat *kstatp = (struct kstat*) pointer;
  struct kstat_io kio;
  jclass          cls;
  static int      first_time = 1;
  static jfieldID nread       ;
  static jfieldID nwritten    ;
  static jfieldID reads       ;
  static jfieldID writes      ;
  static jfieldID wlentime    ;
  static jfieldID rtime       ;
  static jfieldID rlentime    ;
  static jfieldID totalio     ;

#define LOAD_ID(a)                                  \
  (a) = (*env)->GetFieldID(env, cls, #a, "J");      \
  if ((a) == NULL)                                  \
  {                                                 \
    printf("Unable to load field ID of %s \n", #a); \
    abort();                                        \
  }

  /* Preload field IDs: */
  if ( first_time )
  {
    first_time = 0;

    /* Get object class: */
    cls = (*env)->GetObjectClass(env, ks);

    /* Load addresses: */
    LOAD_ID(nread);
    LOAD_ID(nwritten);
    LOAD_ID(reads);
    LOAD_ID(writes);
    LOAD_ID(wlentime);
    LOAD_ID(rtime);
    LOAD_ID(rlentime);
    LOAD_ID(totalio);
  }

  if ( kstat_read(global_kstat_kc, kstatp, &kio) == -1 )
  {
    sprintf(ptod_txt, "Java_Vdb_Native_getKstatData: errno: %d", errno);
    PTOD(ptod_txt);
    return -1;
  }


  (*env)->SetLongField(env, ks, nread       , (jlong) kio.nread       );
  (*env)->SetLongField(env, ks, nwritten    , (jlong) kio.nwritten    );
  (*env)->SetLongField(env, ks, reads       , (jlong) kio.reads       );
  (*env)->SetLongField(env, ks, writes      , (jlong) kio.writes      );
  (*env)->SetLongField(env, ks, wlentime    , (jlong) kio.wlentime    );
  (*env)->SetLongField(env, ks, rtime       , (jlong) kio.rtime       );
  (*env)->SetLongField(env, ks, rlentime    , (jlong) kio.rlentime    );

  (*env)->SetLongField(env, ks, totalio     , (jlong) kio.writes + kio.reads);

  return 0;

}

JNIEXPORT jlong JNICALL Java_Vdb_Native_getCpuData(JNIEnv *env,
                                                   jclass this,
                                                   jobject ks)
{
  static int      first_time = 1;
  struct Cpu      cpu;
  jclass          cls;
  static jfieldID cpu_count;
  static jfieldID cpu_total;
  static jfieldID cpu_idle;
  static jfieldID cpu_user;
  static jfieldID cpu_kernel;
  static jfieldID cpu_wait;
  static jfieldID cpu_hertz;


  /* Preload field IDs: */
  if ( first_time )
  {

    /* Get object class: */
    cls = (*env)->GetObjectClass(env, ks);

    /* Load addresses: */
    LOAD_ID(cpu_count);
    LOAD_ID(cpu_total);
    LOAD_ID(cpu_idle);
    LOAD_ID(cpu_user);
    LOAD_ID(cpu_kernel);
    LOAD_ID(cpu_wait);
    LOAD_ID(cpu_hertz);

    first_time = 0;
  }

  get_cpu_times(&cpu);

  (*env)->SetLongField(env, ks, cpu_count      , cpu.cpu_count      );
  (*env)->SetLongField(env, ks, cpu_total      , cpu.cpu_total      );
  (*env)->SetLongField(env, ks, cpu_idle       , cpu.cpu_idle       );
  (*env)->SetLongField(env, ks, cpu_user       , cpu.cpu_user       );
  (*env)->SetLongField(env, ks, cpu_kernel     , cpu.cpu_kernel     );
  (*env)->SetLongField(env, ks, cpu_wait       , cpu.cpu_wait       );
  (*env)->SetLongField(env, ks, cpu_hertz      , cpu.usecs_per_tick );

  return 0;

}



//JNIEXPORT jstring JNICALL Java_Vdb_Native_libdev_1info(JNIEnv *env,
//                                                   jclass this)
//{
//  extern char* libdev_info();
//  return (*env)->NewStringUTF(env, libdev_info());
//}




extern kstat_t* get_kstat_t(JNIEnv *env, const char *instance)
{

  kstat_t     *ksp;



  /* Start kstat scan: */
  if ( global_kstat_kc == NULL )
  {
    PTOD("NULL global_kstat_kc");
    abort();                      \
  }

  /* Scan through each kstat: */
  for ( ksp = global_kstat_kc->kc_chain; ksp != NULL; ksp = ksp->ks_next )
  {
    /* Use only IO stats: */
    if ( ksp->ks_type == KSTAT_TYPE_IO )
    {

      /* If this was the correct device, return pointer to kstat_t: */
      //PTOD1("ks_name: %s", ksp->ks_name);
      if ( strcmp(instance, ksp->ks_name) == 0 )
      {
        //PTOD1("======================================ks_name: %s", ksp->ks_name);
        //kstat_close(global_kstat_kc);
        return ksp;
      }
    }
  }

  /* We could not find the instance name: */
  return 0;
}
