
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include "vdbjni.h"
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pdhmsg.h>
#include <pdh.h>
#include "Vdb_WindowsPDH.h"
#pragma comment(lib, "pdh")


extern struct Shared_memory *shared_mem;

static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";


/* This structure has enough space for 100 different queries: */
struct Query
{
  HQUERY               hquery;
  HCOUNTER             hcounter[100];
  PDH_FMT_COUNTERVALUE new_data[100];
  int                  counters;
  char                 *field[100];
};

static int debug = 0;


/**
 * Translate an english field name to a filed index so that we
 * can run on any language PC.
 */
char* translateField(JNIEnv *env, char* field_name)
{
#define TOTALBYTES    20480
#define BYTEINCREMENT 10240
  static int first_time = 1;
  static DWORD buffer_length = TOTALBYTES;
  static DWORD returned_length;
  static DWORD rc;
  int i;
  char number[255];
  char field[255];
  char translated_field[255];
  static  char *buffer;

  if (debug) printf("translateField 1 \n");

  if (first_time)
  {
    /* Read all the field names and indexes: */
    PPERF_DATA_BLOCK perfdata = (PPERF_DATA_BLOCK) malloc( buffer_length );
    buffer          = (char*) perfdata;
    returned_length = buffer_length;

    rc = RegQueryValueEx( HKEY_PERFORMANCE_DATA,
                          TEXT("Counter 009"),
                          NULL,
                          NULL,
                          (LPBYTE) perfdata,
                          &returned_length );

    if (debug) printf("translateField 2 %d \n", rc);

    /* If we don't have enough memory, reallocate and try again: */
    while ( rc == ERROR_MORE_DATA )
    {
      buffer_length  += BYTEINCREMENT;
      perfdata        = (PPERF_DATA_BLOCK) realloc( perfdata, buffer_length );
      returned_length = buffer_length;
      buffer          = (char*) perfdata;

      if (debug) printf("translateField 3 \n");
      rc = RegQueryValueEx( HKEY_PERFORMANCE_DATA,
                            TEXT("Counter 009"),
                            NULL,
                            NULL,
                            (LPBYTE) perfdata,
                            &returned_length );
    }
    if ( rc != ERROR_SUCCESS )
    {
      PTOD1("RegQueryValueEx failed %08x\n", rc );
      abort();
    }

    first_time = 0;

    if (debug)
    {
      /* Print all names: */
      for (i = 0; i < returned_length; i++)
      {
        strcpy(number, buffer+i);
        strcpy(field, buffer+1+i+strlen(number));
        {
          int field_no = atoi(number);
          int new_field_length = sizeof(translated_field);
          rc = PdhLookupPerfNameByIndex(NULL, field_no, translated_field, &new_field_length);
          if (debug) printf("translated_field: %d %s \n", field_no, translated_field);
        }

        i = i + strlen(number) + strlen(field) + 1;
      }
    }
  }

  if (debug) printf("translateField  4\n");
  /* Look for the requested field name: */
  for (i = 0; i < returned_length; i++)
  {
    strcpy(number, buffer+i);
    strcpy(field, buffer+1+i+strlen(number));
    //if (debug) printf("search: %s %s \n", number, field);

    if (strcmp(field_name, field) == 0)
    {
      char *ret_name;
      int field_no = atoi(number);
      int new_field_length = sizeof(translated_field);
      rc = PdhLookupPerfNameByIndex(NULL, field_no, translated_field, &new_field_length);
      //if (debug) printf("rc: %x \n", rc);
      //if (debug) printf("translated_field: %s \n", translated_field);

      ret_name = malloc(strlen(translated_field) + 1);
      strcpy(ret_name, translated_field);
      return ret_name;

    }

    i = i + strlen(number) + strlen(field) + 1;
  }

  PTOD1("Unable to translate PDH field name %s\n", field_name);
  return NULL;
}


JNIEXPORT jlong JNICALL Java_Vdb_WindowsPDH_createQuery(JNIEnv      *env,
                                                        jclass       this,
                                                        jobjectArray array)
{
  int i, rc;
  jint length      = (*env)->GetArrayLength(env, array);
  struct Query *qp = malloc(sizeof(struct Query));

  /* Initialize new PDH query: */
  if ( (rc = PdhOpenQuery(NULL, 0, &qp->hquery)) != ERROR_SUCCESS )
  {
    PTOD1("POQ failed %08x\n", rc );
    return -1;
  }
  qp->counters = length;

  /* Loop through PDH full names and add to query: */
  for (i = 0; i < length; i++)
  {
    jstring string = (jstring) (*env)->GetObjectArrayElement(env, array, i);
    qp->field[i] = (char*) (*env)->GetStringUTFChars(env, string, NULL);

    if (debug) printf("utf: %s \n", qp->field[i]);

    if ( (rc = PdhAddCounter(qp->hquery, qp->field[i], 0, &qp->hcounter[i])) != ERROR_SUCCESS )
    {
      PTOD2("PAC failed %08x %s \n", rc, qp->field[i] );
      return -1;
    }
  }

  return(jlong) qp;
}


JNIEXPORT jstring JNICALL Java_Vdb_WindowsPDH_getQueryData(JNIEnv *env,
                                                           jclass  this,
                                                           jlong   pointer)
{
  int i, rc;
  char tmp[80];
  struct Query *qp = (struct Query*) pointer;
  char *buffer     = malloc(8192);
  jstring data;

  if ( (rc = PdhCollectQueryData(qp->hquery)) != ERROR_SUCCESS )
  {
    PTOD1("PCQD failed %08x\n", rc );
    return NULL;
  }

  /* Loop through all counters and return numeric values as a long string: */
  buffer[0] = 0;
  for ( i = 0; i < qp->counters; i++ )
  {
    if ( (rc = PdhGetFormattedCounterValue(qp->hcounter[i], PDH_FMT_DOUBLE,
                                           NULL, &qp->new_data[i])) != ERROR_SUCCESS )
    {
      PTOD4("PGFCV failed %s %08x (%d,%d)\n", qp->field[i], rc, i, qp->counters );

      if ( rc != PDH_CALC_NEGATIVE_VALUE)
        return NULL;

      qp->new_data[i].doubleValue = 0;
    }
//    snap("data", &qp->new_data[i].doubleValue, 8);

    sprintf(tmp, "%I64d ", (jlong) qp->new_data[i].doubleValue);
    strcat(buffer, tmp);
    //printf("new_data: %I64d %s \n",  (jlong) qp->new_data[i].doubleValue, qp->field[i]);
    //printf("new_data: %.20g %s \n",  qp->new_data[i].doubleValue, qp->field[i]);
  }

  data = (*env)->NewStringUTF(env, buffer);
  free(buffer);
  return data;
}


JNIEXPORT jstring JNICALL Java_Vdb_WindowsPDH_translateFieldName(JNIEnv *env,
                                                                 jclass  this,
                                                                 jstring object)
{
  char *field      = (char*) (*env)->GetStringUTFChars(env, object, 0);
  char *translated = translateField(env, field);

  if (strlen(field) == 0)
  {
    debug = 1;
    (*env)->ReleaseStringUTFChars(env, object, field);
    return NULL;
  }

  (*env)->ReleaseStringUTFChars(env, object, field);
  return(*env)->NewStringUTF(env, translated);
}


JNIEXPORT jstring JNICALL Java_Vdb_WindowsPDH_expandCounterPath(JNIEnv *env,
                                                                jclass  this,
                                                                jstring object)
{
  char *buffer;
  char *bufptr;
  char *outptr;
  DWORD buffer_length   = TOTALBYTES;
  DWORD returned_length = buffer_length;
  DWORD rc;
  jstring ret;

  char *path = (char*) (*env)->GetStringUTFChars(env, object, 0);


  /* Allocate buffer: */
  buffer = (char*) malloc( buffer_length );

  /* Read fields: */
  rc = PdhExpandCounterPath(path, buffer, &buffer_length);

  if (debug) printf("expand 2 %d %s\n", rc, path);

  /* If we don't have enough memory, reallocate and try again: */
  while ( rc == ERROR_MORE_DATA )
  {
    buffer_length  += BYTEINCREMENT;
    buffer          = (char*) realloc( buffer, buffer_length );
    returned_length = buffer_length;

    if (debug) printf("expand 3 %s\n", path);

    rc = PdhExpandCounterPath(path, buffer, &buffer_length);
  }
  if ( rc != ERROR_SUCCESS )
  {
    PTOD2("expand failed %08x %s\n", rc, path );
    return NULL;
  }

  if (debug) snap(env, "buffer", buffer, returned_length);

  /* Copy all null terminated strings: */
  outptr = malloc(buffer_length);
  outptr[0] = 0;
  bufptr = buffer;
  while (*bufptr)
  {
    strcat(outptr, bufptr);
    strcat(outptr, "$");
    bufptr += strlen(bufptr) + 1;
  }

  if (debug) printf("outptr: %s \n", outptr);


  ret = (*env)->NewStringUTF(env, outptr);
  free(buffer);
  free(outptr);

  return ret;
}




/**
 */
void main(int argc, char **argv)
{
  int rc;
  int i,j;
  struct Query *qp = malloc(sizeof(struct Query));
  //char field[] = "\\Processor(_Total)\\% Processor Time";
  char field[] = "\\Redirector()\\Bytes Received/sec";


  printf("Hello World \n");

  qp->counters = 1;

  if ( (rc = PdhOpenQuery(NULL, 0, &qp->hquery)) != ERROR_SUCCESS )
  {
    printf("POQ failed %08x\n", rc );
    return;
  }

  if ( (rc = PdhAddCounter(qp->hquery, field, 0, &qp->hcounter[0])) != ERROR_SUCCESS )
  {
    printf("PAC failed %08x %s \n", rc, field );
    return;
  }

  if ( (rc = PdhCollectQueryData(qp->hquery)) != ERROR_SUCCESS )
  {
    printf("PCQD failed %08x\n", rc );
    return;
  }

  for (j = 0; j < 10; j++)
  {
    Sleep(2000);

    if ( (rc = PdhCollectQueryData(qp->hquery)) != ERROR_SUCCESS )
    {
      printf("PCQD failed %08x\n", rc );
      return;
    }

    for ( i = 0; i < qp->counters; i++ )
    {
      if ( (rc = PdhGetFormattedCounterValue(qp->hcounter[i], PDH_FMT_DOUBLE,
                                             NULL, &qp->new_data[i])) != ERROR_SUCCESS )
      {
        printf("rc: %d \n", rc);
        printf("here\n");
        if ( rc == PDH_CALC_NEGATIVE_VALUE)
          continue;
        else
        {
          printf("PGFCV failed %s %08x (%d,%d)\n", qp->field, rc, i, qp->counters );
        }
      }

      //printf("rc: %d \n", rc);
      printf("new_data: %.20g \n",  qp->new_data[i].doubleValue);
      printf("new_data: %I64d \n",  (jlong) qp->new_data[i].doubleValue);
    }
  }

}


