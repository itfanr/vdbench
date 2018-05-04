

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
  #ifndef LINUX
    #ifndef __APPLE__
      #include <stropts.h>
    #endif
  #endif
#endif
#include <time.h>

static char c[] =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


struct Shared_memory *shared_mem = NULL;




#ifdef SOLARIS
  #include <sys/scsi/impl/uscsi.h>

jlong scsi_reset(jlong fhandle, jlong buffer)
{
  static struct uscsi_cmd cmd;

  if (buffer == -1 )
    cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET;
  else
    cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET_ALL;

  if (ioctl((int) fhandle, USCSICMD, &cmd) < 0 )
  {
    fprintf( stderr, "Reset error: %s\n", strerror(errno) );
    return -1;
  }
  return 0;
}
#endif



void printRequest(JNIEnv *env, struct Request *req)
{
#ifdef _WIN32
  PTOD1("fhandle            : %I64x ", req->fhandle             );
  PTOD1("data_flag          : %04x  ", req->data_flag           );
  PTOD1("buffer             : %I64x ", req->buffer              );
  PTOD1("pattern_lba        : %I64x ", req->pattern_lba         );
  PTOD1("file_start_lba     : %I64x ", req->file_start_lba      );
  PTOD1("sector_lba         : %I64x ", req->sector_lba          );
  PTOD1("compression        : %I64x ", req->compression         );
  PTOD1("dedup_set          : %I64x ", req->dedup_set           );
  PTOD1("offset_in_key_block: %I64x ", req->offset_in_key_block );
  PTOD1("sectors            : %x    ", req->sectors             );
  PTOD1("key                : %x    ", req->key                 );
  PTOD1("data_blksize       : %d    ", req->data_length         );
  PTOD1("jni_index          : %x    ", req->jni_index           );
  //PTOD1("dv_text        : %s   ", req->dv_text        );
#else
  PTOD1("fhandle            : %llx ", req->fhandle             );
  PTOD1("data_flag          : %04x ", req->data_flag           );
  PTOD1("buffer             : %llx ", req->buffer              );
  PTOD1("pattern_lba        : %llx ", req->pattern_lba         );
  PTOD1("file_start_lba     : %llx ", req->file_start_lba      );
  PTOD1("sector_lba         : %llx ", req->sector_lba          );
  PTOD1("compression        : %llx ", req->compression         );
  PTOD1("dedup_set          : %llx ", req->dedup_set           );
  PTOD1("offset_in_key_block: %llx ", req->offset_in_key_block );
  PTOD1("sectors            : %x   ", req->sectors             );
  PTOD1("key                : %x   ", req->key                 );
  PTOD1("data_blksize       : %d   ", req->data_length         );
  PTOD1("jni_index          : %x   ", req->jni_index           );
  //PTOD1("dv_text        : %s   ", req->dv_text        );
#endif
}



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
 * Java call to read data.
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_read(JNIEnv *env,
                                             jobject this,
                                             jlong   fhandle,
                                             jlong   seek,
                                             jlong   length,
                                             jlong   buffer,
                                             jint    jni_index)
{
  struct Workload *wkl = (jni_index < 0) ? 0 : &shared_mem->workload[jni_index];
  jlong tod1, rc;

  /* Seed buffer in case read just fails! */
  tod1 = START_WORKLOAD_STATS(env, wkl);
  if ((rc = file_read(env, fhandle, seek, length, buffer)) != 0 )
    report_io_error(env, 1, fhandle, seek, length, rc, buffer);
  UPDATE_WORKLOAD_STATS(env, wkl, 1, (jint) length, tod1, rc);

  return rc;
}


/**
 * Java call to write data.
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_write(JNIEnv *env,
                                              jobject this,
                                              jlong   fhandle,
                                              jlong   seek,
                                              jlong   length,
                                              jlong   buffer,
                                              jint    jni_index)
{
  struct Workload *wkl = (jni_index < 0) ? 0 : &shared_mem->workload[jni_index];
  jlong tod1, rc;

#ifdef SOLARIS
  if (buffer < 0 )
    return scsi_reset(fhandle, buffer);
#endif

  tod1 = START_WORKLOAD_STATS(env, wkl);
  if ((rc = file_write(env, fhandle, seek, length, buffer)) != 0 )
    report_io_error(env, 0, fhandle, seek, length, rc, buffer);
  UPDATE_WORKLOAD_STATS(env, wkl, 0, (jint) length, tod1, rc);

  return rc;
}


/**
 * Blocks written this way will have each 4k of the buffer overlaid with the
 * last 32bits of the current TOD in microseconds, and with the lba.
 * This then prevents the data from accidentally (or purposely) benefitting from
 * possible undocumented Dedup.
 * First 8 bytes in each 4k:
 * bits  0-23: relative 4k piece , xor'ed with the file handle
 * bits 24-63: last 32bits of get_simple_tod()
 *
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_noDedupWrite(JNIEnv *env,
                                                     jobject this,
                                                     jlong   fhandle,
                                                     jlong   seek,
                                                     jlong   length,
                                                     jlong   buffer,
                                                     jint    jni_index)
{
  int i;
  long rc = 0;
  struct Workload *wkl = (jni_index < 0) ? 0 : &shared_mem->workload[jni_index];
  jlong tod1;

  prevent_dedup(env, fhandle, seek, buffer, (jint) length);

  tod1 = START_WORKLOAD_STATS(env, wkl);
  if ((rc = file_write(env, fhandle, seek, length, buffer)) != 0)
    report_io_error(env, 0, fhandle, seek, length, rc, buffer);
  UPDATE_WORKLOAD_STATS(env, wkl, 0, (jint) length, tod1, rc);

  return rc;
}


