
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include "vdbjni.h"
#include <time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/timeb.h>
#include <string.h>
#include <kstat.h>
#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <inttypes.h>
#include <Utils_NamedKstat.h>


static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";



char *extract_named_data(kstat_ctl_t *kc, kstat_t *named_kstat)
{
  int            i;
  kstat_named_t *named_list;
  int            names_in_list = 0;
  static char    output_buffer[8192];
  char           error_message[256];

  if (kstat_read(kc, named_kstat, NULL) == -1)
  {
    strcpy(error_message, "JNI failure: kstat_read(): ");
    strcat(error_message, strerror(errno));
    return error_message;
  }

  if ((named_list = kstat_data_lookup(named_kstat, "null")) == NULL)
  {
    strcpy(error_message, "JNI failure: kstat_data_lookup(): ");
    strcat(error_message, strerror(errno));
    return error_message;
  }

  names_in_list = named_kstat->ks_ndata - (named_list - KSTAT_NAMED_PTR(named_kstat));

  /* First, create a String with all labels: */
  output_buffer[0] = 0;
  for (i = 0; i < names_in_list; i ++)
  {
    char tmp[256];
    if (strcmp(named_list[i].name, "null") == 0)
      continue;
    sprintf(tmp, "%s ", named_list[i].name);
    strcat (output_buffer, tmp);
  }

  /* Separate the String with '*': */
  strcat(output_buffer, "* ");

  /* Now add all the counters: */
  for (i = 0; i < names_in_list; i ++)
  {
    char tmp[256];
    if (strcmp(named_list[i].name, "null") == 0)
      continue;
    sprintf(tmp, "%lld ", named_list[i].value.ui64);
    strcat (output_buffer, tmp);
  }

  return output_buffer;
}



JNIEXPORT jlong JNICALL Java_Utils_NamedKstat_kstat_1open(JNIEnv *env,
                                                       jclass  this)
{
  return(jlong) kstat_open();
}


JNIEXPORT jlong JNICALL Java_Utils_NamedKstat_kstat_1close(JNIEnv *env,
                                                        jclass  this,
                                                        jlong   kc)
{
  return(jlong) kstat_close((kstat_ctl_t *) kc);
}


JNIEXPORT jstring JNICALL Java_Utils_NamedKstat_kstat_1lookup_1stuff(JNIEnv *env,
                                                                  jclass this,
                                                                  jlong  kc_in,
                                                                  jstring module_in,
                                                                  jstring name_in)
{
  char *module = (char*) (*env)->GetStringUTFChars(env, module_in, 0);
  char *name   = (char*) (*env)->GetStringUTFChars(env, name_in,   0);
  kstat_ctl_t *kc = (kstat_ctl_t*) kc_in;
  kstat_t *named_kstat;
  char    *results_char;
  char    error_message[256];


  /* First make sure that 'kc' is still valid: */
  if (kstat_chain_update(kc) < 0)
  {
    (*env)->ReleaseStringUTFChars(env, module_in, module);
    (*env)->ReleaseStringUTFChars(env, name_in,   name);
    strcpy(error_message, "JNI failure: kstat_chain_update(): ");
    strcat(error_message, strerror(errno));
    return(*env)->NewStringUTF(env, error_message);
  }

  /* Retrieve all the kstat data we need: */
  named_kstat = kstat_lookup(kc, module, 0, name);

  (*env)->ReleaseStringUTFChars(env, module_in, module);
  (*env)->ReleaseStringUTFChars(env, name_in,   name);

  /* If there is an error, return: */
  if (named_kstat == NULL)
  {
    strcpy(error_message, "JNI failure: kstat_lookup(): ");
    strcat(error_message, strerror(errno));
    return(*env)->NewStringUTF(env, error_message);
  }

  /* Translate the data into a string label num label num,... */
  results_char = extract_named_data(kc, named_kstat);

  /* Return a Java string: */
  return(*env)->NewStringUTF(env, results_char);

}
