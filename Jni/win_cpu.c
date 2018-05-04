

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
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <windows.h>
#include <winioctl.h>
#include "vdbjni.h"

#include <pdhmsg.h>
#include <pdh.h>
#pragma comment(lib, "pdh")



#define WINMSG(x)                                                \
{                                                                \
    printf("==========> %s %d \n", x, GetLastError());           \
}



struct Query
{
  HQUERY               hquery;
  HCOUNTER             hcounter[100];
  PDH_FMT_COUNTERVALUE rdata[100];
  PDH_FMT_COUNTERVALUE ordata[100];
  int                  counters;
  jlong                prev_tod;
  jlong                last_tod;
  jlong                delta_tod;
  double               tod_adjust;
  char                 disk[256];
};


#define LOAD_ID(a)                                  \
  (a) = (*env)->GetFieldID(env, cls, #a, "J");      \
  if ((a) == NULL)                                  \
  {                                                 \
    sprintf(ptod_txt, "Unable to load field ID of %s \n", #a); \
    PTOD(ptod_txt);                                 \
    abort();                                        \
  }




static int get_query(JNIEnv *env, struct Query *qp, int ignerror)
{
  PDH_STATUS   rc;
  int i;

  /* Collect data: */
  qp->prev_tod   = qp->last_tod;
  qp->last_tod   = get_simple_tod();
  qp->delta_tod  = qp->last_tod - qp->prev_tod;
  qp->tod_adjust = (double) qp->delta_tod / 1000000.0;
  rc = PdhCollectQueryData(qp->hquery);
  if (rc != ERROR_SUCCESS )
  {
    sprintf(ptod_txt, "PCQD failed %08x\n", rc );
    PTOD(ptod_txt);
    return -1;
  }


  /* Extract the raw performance counter value for each counter: */
  fflush(stdout);
  for ( i = 0; i < qp->counters; i++ )
  {
    if ( (rc = PdhGetFormattedCounterValue(qp->hcounter[i], PDH_FMT_DOUBLE,
                                           NULL, &qp->rdata[i])) != ERROR_SUCCESS )
    {
      if ( rc == PDH_CALC_NEGATIVE_VALUE && ignerror)
        continue;
      else
      {
        sprintf(ptod_txt, "PGFCV failed %08x (%d,%d)\n", rc, i, qp->counters );
        PTOD(ptod_txt);
        continue;
      }
    }
  }

  return 0;
}




static struct Query* add_query(JNIEnv *env, PDH_COUNTER_PATH_ELEMENTS cpe[], int cnt)
{

  char szFullPath[MAX_PATH];
  DWORD cbPathSize;
  int   i;
  PDH_STATUS   rc ;  //PDH_INVALID_ARGUMENT
  struct Query *qp = malloc(sizeof(struct Query));


  /* Setup query: */
  qp->counters = cnt;
  for ( i = 0; i < qp->counters; i++ )
    qp->ordata[i].doubleValue = 0;
  if ( (rc  = PdhOpenQuery(NULL, 0, &qp->hquery)) != ERROR_SUCCESS )
  {
    sprintf(ptod_txt, "POQ failed %08x\n", rc );
    PTOD(ptod_txt);
    return NULL;
  }


  for ( i = 0; i < cnt; i++ )
  {
    cbPathSize = sizeof(szFullPath);

    if ( (rc  = PdhMakeCounterPath(&cpe[i], szFullPath, &cbPathSize, 0)) != ERROR_SUCCESS )
    {
      sprintf(ptod_txt, "MCP failed %08x\n", rc );
      PTOD(ptod_txt);
      return NULL;
    }

    if ( (rc  = PdhAddCounter(qp->hquery, szFullPath, 0, &qp->hcounter[i]))
         != ERROR_SUCCESS )
    {
      sprintf(ptod_txt, "PAC failed %08x\n", rc );
      PTOD(ptod_txt);
      return NULL;
    }
  }

  /* Do a firsttime query to create a baseline for the next query: */
  if (get_query(env, qp, 1) < 0)
    return NULL;


  return qp;
}




JNIEXPORT jlong JNICALL Java_Vdb_Native_getCpuData(JNIEnv *env,
                                                   jclass  this,
                                                   jobject ks)
{
  static int      first_time = 1;
  jclass          cls;
  static jfieldID cpu_count;
  static jfieldID cpu_total;
  static jfieldID cpu_idle;
  static jfieldID cpu_user;
  static jfieldID cpu_kernel;
  static jfieldID cpu_wait;
  static jfieldID cpu_hertz;
  static jlong    hertz = 0;

  static struct Query *qp;

  double total_time;
  double user_time;
  double kernel_time;

#define PCOUNTERS 4

  PDH_COUNTER_PATH_ELEMENTS cpe[PCOUNTERS] =
  {
    { NULL, "Processor", "_Total", NULL, -1, "% Processor Time"},
    { NULL, "Processor", "_Total", NULL, -1, "% DPC Time"},
    { NULL, "Processor", "_Total", NULL, -1, "% Interrupt Time"},
    { NULL, "Processor", "_Total", NULL, -1, "% User Time"},
  };



  /* Preload field IDs: */
  if ( first_time )
  {
    BOOL          rc;
    LARGE_INTEGER hz;

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

    qp = add_query(env, &cpe[0], PCOUNTERS);
    if (qp == NULL)
      return -1;

    rc = QueryPerformanceFrequency(&hz);
    if ( rc == 0 )
    {
      printf("QueryPerformanceFrequency() failed\n");
      abort();
    }
    hertz = hz.QuadPart;

    first_time = 0;
  }

  /* Get counters: */
  if (get_query(env, qp, 0) < 0)
    return -1;

  total_time   = qp->rdata[0].doubleValue;
  user_time    = qp->rdata[3].doubleValue;
  kernel_time  = total_time - user_time;


  /* Store counters: */
  (*env)->SetLongField(env, ks, cpu_hertz , hertz);
  (*env)->SetLongField(env, ks, cpu_total , qp->delta_tod);
  (*env)->SetLongField(env, ks, cpu_user  , (jlong) (user_time * qp->delta_tod / 100));
  (*env)->SetLongField(env, ks, cpu_kernel, (jlong) (kernel_time * qp->delta_tod / 100));

  return 0;

}


