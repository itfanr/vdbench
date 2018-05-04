

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
#include "Vdb_Native.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vdbjni.h"



extern struct Shared_memory *shared_mem;
char ptod_txt[256]; /* workarea for PTOD displays */



const unsigned int vd_polynomial_coefficients[ 128 ] =
{
  0x8AD8A4E8, 0x8B026ED8, 0x8B386301, 0x8BB55E2B, 0x8BB586D1, 0x8BFEC841,
  0x8C61EFE0, 0x8C9D4855, 0x8CEBE0DD, 0x8D058F13, 0x8DACD66D, 0x8E3CD7F9,
  0x8EAB9F5D, 0x8EC08385, 0x8EFDD7B0, 0x8F2B5568, 0x8F42C6DA, 0x8FB3E662,
  0x915E5A0F, 0x917CCCC9, 0x9260E006, 0x92BDA6E9, 0x92FDE296, 0x934CD0E4,
  0x934D6A55, 0x934DD265, 0x935466AE, 0x93E371DF, 0x9466D0D8, 0x94CB06B4,
  0x957B71ED, 0x95F882A7, 0x9635D44E, 0x963D360E, 0x970AB3F3, 0x97390A40,
  0x9752F0E2, 0x98313201, 0x98332229, 0x98AF1149, 0x99238C6A, 0x996A24BE,
  0x99D574CB, 0x9AF79122, 0x9B7ED838, 0x9BDA4C13, 0x9D53683F, 0x9D5FF2F9,
  0x9DF65A38, 0x9E26DFA2, 0x9E7EAF6B, 0x9EBE160C, 0x9F75CA73, 0x9F868146,
  0xA079BA6C, 0xA0B3BD68, 0xA1831C3D, 0xA1866FDB, 0xA1AB06F5, 0xA1B23118,
  0xA2283C26, 0xA3778316, 0xA3C0D8DB, 0xA410F83D, 0xA43DD058, 0xA442C7CC,
  0xA4720575, 0xA526DAC3, 0xA5E7E8BB, 0xA688D850, 0xA6D641E5, 0xA719AC4E,
  0xA7A3B1F1, 0xA84A1B21, 0xA92881CC, 0xA92DF947, 0xA95729FC, 0xAA9D6EB7,
  0xABB6C8C4, 0xABBB6D87, 0xAC098E27, 0xAC63DD72, 0xAC87D325, 0xACB80AB4,
  0xACF6AC8B, 0xAD412A87, 0xAD5450E8, 0xAED0E2D1, 0xB0768827, 0xB20E4FC0,
  0xB2626739, 0xB26F10C4, 0xB2B2D433, 0xB2CDFD03, 0xB383A02E, 0xB3BBD553,
  0xB3CF081E, 0xB3F20861, 0xB3F45D54, 0xB4D08B5E, 0xB4FCF7F4, 0xB539E491,
  0xB5F3EAAC, 0xB5F8D6F7, 0xB61CFE8F, 0xB6BAB103, 0xB73E25DD, 0xB7736D5A,
  0xB7D95002, 0xB8BF6B3C, 0xB95E33D2, 0xBA6511DC, 0xBA6C6141, 0xBB0D522B,
  0xBB190FBC, 0xBB553C48, 0xBBF10F34, 0xBBF29A64, 0xBC77028B, 0xBD7A6D3C,
  0xBF88A0C1, 0xBF962F30, 0xC01D184B, 0xC0FA97A7, 0xC10432E6, 0xC144D566,
  0xC14E6718, 0xC1A0B464
};



/**
 * Generate 480 byte 'Linear Feedback Shift Register' data pattern in 'buffer',
 * using 'name, 'lba' and 'key' as seed values.
 */
extern void generate_lfsr_data(JNIEnv       *env,
                               uint*        buffer,
                               jlong        lba,
                               uint         key,
                               const char*  name)
{
  uint *data_ptr;
  uint  seed;
  int   loop_count = 480 / sizeof( uint );
  uint  poly_bits;
  uint  data_value;

  /* The 'name' must be 8 bytes: */
  if (strlen(name) != 8)
  {
    PTODS("generate_lfsr_data(): String passed must be 8 bytes long: >>>%s<<<", name);
    PTODS("generate_lfsr_data(): String length: %d", strlen(name));
    ABORT("generate_lfsr_data(): String passed must be 8 bytes long: ", name);
  }

  /* Add the SD or FSD name to the seed: */
  seed  = *(uint*) &name[0] ^ *(uint*) &name[4];

  /* Use the key to select the generator polynomial so each write to an LBA  */
  /* gets a different data pattern.                                          */
  poly_bits = vd_polynomial_coefficients[ key ];

  /* Seed the sequence with the LBA so each sector contains unique data. */
  data_value = ((lba ^ seed) >> 9) * poly_bits;

  /* Set the buffer address: */
  data_ptr = (uint*) buffer;

  do
  {
    /* This generates the next value in a Linear Feedback Shift Register Sequence. */
    data_value = (data_value >> 1) ^ ( poly_bits & -(data_value & 1U) );
    *data_ptr++ = data_value;

  } while ( --loop_count != 0 );
}


/**
 * Store data pattern
 */
JNIEXPORT void JNICALL Java_Vdb_Native_store_1pattern(JNIEnv   *env,
                                                      jclass    this,
                                                      jintArray jarray,
                                                      jint      key)
{

  int   i, *buf;
  jsize len;
  jint *body;

  if ( key >= MAX_PATTERNS )
  {
    sprintf(ptod_txt, "Java_Vdb_Native_store_1pattern(): invalid key value: %d\n", key);
    PTOD(ptod_txt);
    ABORT("Invalid key", "");
  }


  /* Determine length and address of array: */
  len  = (*env)->GetArrayLength(env, jarray);
  body = (*env)->GetIntArrayElements(env, jarray, 0);

  /* Free old memory if needed: */
  if ( shared_mem->patterns[key] != 0 )
  {
    free(shared_mem->patterns[key]);
  }

  /* Allocate new memory for pattern: */
#ifdef SOLARIS
  shared_mem->patterns[key] = valloc(len * 4);
#else
  shared_mem->patterns[key] = malloc(len * 4);
#endif
  if ( shared_mem->patterns[key] == NULL )
  {
    sprintf(ptod_txt, "valloc/malloc for data pattern failed: %d %s\n", len, strerror(errno));
    PTOD(ptod_txt);
    ABORT("valloc/malloc for data pattern failed", strerror(errno));
  }

  /* Copy the data pattern from Java memory to C memory: */
  for ( buf = (int*) shared_mem->patterns[key], i = 0; i < len; i++ )
    buf[i] = body[i];

  (*env)->ReleaseIntArrayElements(env, jarray, body, JNI_ABORT);

  /* See if the patterns cleanly repeats itself every 512 bytes: */
  shared_mem->repeatable[key] = 1;
  //printf("len: %d\n", len);
  for ( i = 0; i < len * 4 / 512; i++ )
  {
    if ( memcmp(buf , &buf[ i * (512 / 4) ], 512) != 0 )
    {
      shared_mem->repeatable[key] = 0;
      break;
    }
  }

  return;
}


#define CHECKSUM(cs, ts)                        \
{                                               \
  jlong tmp = ts;                               \
  cs = 0;                                       \
  while (tmp != 0)                              \
  {                                             \
    cs += (uchar) tmp;                          \
    tmp = tmp >> 8;                             \
  }                                             \
}

// This line from above removed. Caused Windows DV timestamp compare problems.
// Don't understand exactly why. Probably has something to do
// with byte reversal.
//    tmp = tmp & 0x00ffffff;

/**
 * Fill the data buffer with proper data
 */
extern void fill_buffer(JNIEnv *env,
                        jlong  buffer,
                        jlong  lba,
                        uint   key,
                        int    xfersize,
                        char   *name_in)
{
  int secs = xfersize >> 9;
  int i;
  struct Sector *sector  = (struct Sector*) buffer;
  jlong time_written     = shared_mem->base_hrtime + get_simple_tod();
  uchar checksum;

  /* The 'name' must be 8 bytes: */
  if (strlen(name_in) != 8)
  {
    PTODS("fill_buffer(): String passed must be 8 bytes long: >>>%s<<<", name_in);
    PTODS("fill_buffer(): String length: %d", strlen(name_in));
    ABORT("fill_buffer(): String passed must be 8 bytes long: ", name_in);
  }

  /* We have byte 0 left in the timestamp. That will last us until */
  /* the year 45 * (2002 - 1970) = 3442. Remember Y2k?             */
  CHECKSUM(checksum, time_written);

  /* Fill in data in the whole sector: */
  for ( i = 0; i < secs; i++ )
  {
    sector->lba_1  = (uint) (lba >> 32);
    sector->lba_2  = (uint) lba;
    sector->bytes  = key << 24;
    sector->bytes |= checksum << 16;
    sector->spare  = 0;
    memcpy(sector->name, name_in, 8);
    sector->time_1 = (uint) (time_written >> 32);
    sector->time_2 = (uint) time_written;

    /* The rest of the data contents is generated using LFSR: */
    generate_lfsr_data(env, sector->data, lba, key, name_in);

    /* Increment pointers: */
    sector = (struct Sector*) ((char*) sector + 512);
    lba   += 512;
  }
}




/**
 * We have a bad sector. The reporting of this is done in Java.
 */
static void report_bad_sector(JNIEnv *env,
                              struct Sector *sector,
                              struct Sector *pattern,
                              jlong  file_start_lba,
                              jlong  file_lba,
                              jlong  offset_in_block,
                              jlong  handle,
                              uint   key,
                              uint   xfersize,
                              char  *name_in,
                              int    error_flag)
{

  jclass clx;
  jmethodID report;
  jintArray sector_array, pattern_array;
  jstring sd_left, sd_right;
  char txt[256];
  jlong tod;
  uchar checksum = (sector->bytes >> 16);
  tod  = (jlong) sector->time_1 << 32;
  tod += sector->time_2;
  /*
  static synchronized void reportBadSector(int[]  sector_array,  // 512 bytes
                                           int[]  pattern_array, // 512 bytes
                                           long   handle,
                                           long   file_start_lba,
                                           long   file_lba,
                                           long   offset_in_block,
                                           long   timestamp,
                                           int    error_flag,
                                           int    key,
                                           int    xfersize,
                                           int    checksum,
                                           String name_left,
                                           String name_right)
  */

  CHECK(1);
  clx = (*env)->FindClass(env, "Vdb/Bad_sector");
  CHECK(2);
  report = (*env)->GetStaticMethodID(env, clx, "reportBadSector",
                                     "([I[IJJJJJIIIILjava/lang/String;Ljava/lang/String;)V");
  CHECK(3);

  sector_array = (*env)->NewIntArray(env, 128);
  (*env)->SetIntArrayRegion(env, sector_array, 0, 128, (jint*) sector);
  pattern_array = (*env)->NewIntArray(env, 128);
  (*env)->SetIntArrayRegion(env, pattern_array, 0, 128, (jint*) pattern);

  memset(txt, 0, sizeof(txt));
  memcpy(txt, sector->name, 8);

  sd_left  = (*env)->NewStringUTF(env, name_in);
  sd_right = (*env)->NewStringUTF(env, txt);

  (*env)->CallStaticVoidMethod(env, clx, report,
                               sector_array,
                               pattern_array,
                               handle,
                               file_start_lba,
                               file_lba,
                               offset_in_block,
                               tod,
                               error_flag,
                               key,
                               xfersize,
                               checksum,
                               sd_left,
                               sd_right);
}


/**
 * - handle:          file handle to be passed to Bad_sector()
 * - sector:          address of 512-byte sector to compare
 * - file_start_lba:  the offset of this block within this FSD (zero for SD)
 * - file_lba:        the offset within the file or lun
 * - offset_in_block: the offset in the data block for this sector
 * - key:             Data Validation key
 * - xfersize:        User's xfersize used for this read.
 * - name_in:         SD or FSD name (8-bytes, trailing blanks)
 */
extern int validate_sector(JNIEnv *env,
                           jlong  handle,
                           struct Sector *sector,
                           jlong  file_start_lba,
                           jlong  file_lba,
                           jlong  offset_in_block,
                           jint   key,
                           jint   xfersize,
                           char*  name_in)
{
  int i;
  int error_flag = 0;

  /* pattern_lba: the lba to be used for data pattern generation: */
  jlong pattern_lba = file_start_lba + file_lba + offset_in_block;

  uint* data_ptr = (uint*) &sector->data;
  uint  error_accum;     /* LFSR field */
  uint  expected_value;  /* LFSR field */
  uint  actual_value;    /* LFSR field */
  uint  poly_bits  = vd_polynomial_coefficients[ key ];
  int   loop_count = 480 / sizeof( uint );

  /* Add the SD or FSD name to the seed: */
  uint seed  = (*(uint*) &name_in[0] ^ *(uint*) &name_in[4]);

  jlong ts = ((jlong) sector->time_1 << 32) + sector->time_2;
  uchar ts_check;
  CHECKSUM(ts_check, ts);


  if ( key != (sector->bytes >> 24) )
    error_flag |= 0x01;
  if ( ts_check != (uchar) (sector->bytes >> 16) )
    error_flag |= 0x02;
  if ( sector->lba_1 != (unsigned int) (pattern_lba >> 32) )
    error_flag |= 0x04;
  if ( sector->lba_2 != (unsigned int) pattern_lba )
    error_flag |= 0x04;
  if ( memcmp(sector->name, name_in, 8 ) != 0 )
    error_flag |= 0x08;

  /* A request to validate using key 127 means that this is a forced */
  /* Data Validation error using 'force_error_after'.                */
  /* We therefore don't know the real key so we pick up the key from */
  /* the buffer to use for LFSR: */
  if (key == 127)
    poly_bits = vd_polynomial_coefficients[ (sector->bytes >> 24) ];

  // debugging:
  // debugging poly_bits = 0;

  /* The 480 remaining bytes have been filled using LFSR.                      */
  /* Use that again for comparison:                                            */
  /* The following value will contain 0 only if every bit of data was correct. */
  error_accum = 0;

  /* Seed the sequence with the LBA so each sector contains unique data. */
  expected_value = ((pattern_lba ^ seed) >> 9) * poly_bits;
  do
  {
    /* Start read early so that it overlaps with the generating the expected value. */
    actual_value = *data_ptr++;

    /* This generates the next value in a Linear Feedback Shift Register Sequence. */
    expected_value = (expected_value >> 1) ^ ( poly_bits & -(expected_value & 1U) );

    /* OR a non-zero value into error_accum if the data doesn't match. */
    error_accum |= actual_value ^ expected_value;

  } while ( --loop_count != 0 );

  /* Do we have an error? */
  if (error_accum != 0)
    error_flag |= 0x10;

  //PTODS("error_flag: %02x", error_flag);

  /* Only with errors will be create a buffer with a required LFSR pattern */
  /* so that we can pass it to java (java could create it!)                */
  if ( error_flag != 0 )
  {
    struct Sector* pattern = malloc(512);
    if (key == 127)
      generate_lfsr_data(env, pattern->data, pattern_lba, (sector->bytes >> 24), (const char*) name_in);
    else
      generate_lfsr_data(env, pattern->data, pattern_lba, key, name_in);

    report_bad_sector(env, sector, pattern, file_start_lba, file_lba, offset_in_block,
                      handle, key, xfersize, name_in, error_flag);

    free(pattern);
  }

  return error_flag;
}


/**
 * Validate a complete buffer.
 */
extern int validate_whole_buffer(JNIEnv *env,
                                 jlong  handle,
                                 jlong  buffer,
                                 jlong  file_start_lba,
                                 jlong  file_lba,
                                 jint   key,
                                 jint   xfersize,
                                 char*  name_in)
{
  int secs = xfersize >> 9;
  int i;
  int errors_in_data_block = 0;

  /* Compare data in each sector: */
  for ( i = 0; i < secs; i++ )
  {
    int offset_in_buffer = i * 512;
    struct Sector *sector = (struct Sector*) (buffer + offset_in_buffer);

    int rc = validate_sector(env, handle, sector, file_start_lba, file_lba,
                             offset_in_buffer, key, xfersize, name_in);
    if (rc != 0)
      errors_in_data_block ++;
  }

  return errors_in_data_block;
}



/**
 * Typically an AMD problem: out of sync High Resolution timers.
 */
static void report_bad_clocks(JNIEnv *env, jlong elapsed)
{
  static int negative_response_count = 0;

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

/**
 * File system Data Validation.
 *
 * Read the whole block, and then compare the data for each key
 * block.
 *
 * file_start_lba: the offset of this block within this FSD
 * file_lba:       the offset of this block within the file
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_readAndValidate(JNIEnv    *env,
                                                        jclass    this,
                                                        jlong     handle,
                                                        jlong     file_start_lba,
                                                        jlong     file_lba,
                                                        jint      xfersize,
                                                        jlong     buffer,
                                                        jint      key_count,
                                                        jintArray keys,
                                                        jstring   fsd_name)
{
  jlong rc;
  int i, j, key_block_size;

  const char *fsd = (*env)->GetStringUTFChars(env, fsd_name, 0);
  jsize len       = (*env)->GetArrayLength(env, keys);
  jint* key_array = (*env)->GetIntArrayElements(env, keys, 0);
  int offset_in_block = 0;
  int errors_found    = 0;

  /* Calculate size of a key block: */
  key_block_size = xfersize / key_count;

  /* Read the whole block: */
  rc = file_read(env, handle, file_lba, xfersize, buffer);
  if (rc == 0)
  {
    /* Validate each key block: */
    for (i = 0; i < key_count; i++)
    {
      /* Only those with a non-zero key: */
      if (key_array[i] > 0)
      {
        /* Loop through each sector within key+block_size: */
        int sectors = key_block_size / 512;
        for (j = 0; j < sectors; j++)
        {
          struct Sector *sector = (struct Sector*) (buffer + offset_in_block);
          /*
          PTODS("sector: %p", sector);
          PTODS("file_start_lba: %08llx", file_start_lba);
          PTODS("file_lba: %08llx", file_lba);
          PTODS("offset_in_block: %d", offset_in_block);
          PTODS("key_array[i]: %d", key_array[i]);*/

          if (validate_sector(env, handle, sector, file_start_lba, file_lba,
                              offset_in_block, key_array[i],
                              xfersize, (char*) fsd) > 0)
          {
            errors_found++;
          }
          offset_in_block += 512;
        }
      }

      /* WE don't need to validate, so increase by key blocksize: */
      else
        offset_in_block += key_block_size;

    }

    if (errors_found > 0)
      report_bad_block(env, 1, handle,
                       file_start_lba + file_lba, xfersize, 60003);
  }

  (*env)->ReleaseStringUTFChars(env, fsd_name, fsd);
  (*env)->ReleaseIntArrayElements(env, keys, key_array, JNI_ABORT);

  return rc;
}


/**
 * File system Data Validation
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_fillAndWrite(JNIEnv    *env,
                                                     jclass    this,
                                                     jlong     handle,
                                                     jlong     file_start_lba,
                                                     jlong     file_lba,
                                                     jint      xfersize,
                                                     jlong     buffer,
                                                     jint      key_count,
                                                     jintArray keys,
                                                     jstring   fsd_name)
{
  jlong rc, pattern_lba;
  int i, key_block_size;

  const char *fsd = (*env)->GetStringUTFChars(env, fsd_name, 0);
  jsize len       = (*env)->GetArrayLength(env, keys);
  jint* key_array = (*env)->GetIntArrayElements(env, keys, 0);

  /* Calculate size of a key block: */
  key_block_size = xfersize / key_count;

  pattern_lba = file_start_lba + file_lba;
  for (i = 0; i < key_count; i++)
  {
    /* Fill a key block: */
    fill_buffer(env, buffer + (i * key_block_size),
                pattern_lba + (i * key_block_size),
                key_array[i], key_block_size, (char*) fsd);
  }

  (*env)->ReleaseStringUTFChars(env, fsd_name, fsd);
  (*env)->ReleaseIntArrayElements(env, keys, key_array, JNI_ABORT);

  //snap("buffer", (void*) buffer, 4096);

  return file_write(env, handle, file_lba, xfersize, buffer);
}


/**
 * Call to Java to report i/o error or Data Validation error.
 */
void report_bad_block(JNIEnv *env, jlong read_flag, jlong fhandle,
                      jlong   lba, jlong xfersize,  jlong error)
{
  jclass clx;
  jmethodID report;

  CHECK(1);
  clx = (*env)->FindClass(env, "Vdb/IO_task");
  CHECK(2);
  report = (*env)->GetStaticMethodID(env, clx, "io_error_report", "(JJJJJ)V");
  CHECK(3);

  (*env)->CallStaticVoidMethod(env, clx, report, read_flag,
                               fhandle, lba, xfersize, error);
}



/**
 * Fills a 512-byte buffer with 480 bytes worth of LFSR data.
 * The first 32-bytes cotain zeros.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_fillLFSR(JNIEnv    *env,
                                                jclass    this,
                                                jintArray array,
                                                jlong     lba,
                                                jint      key,
                                                jstring   name_in)
{
  struct Sector sector;
  int*  prefix = (int*) &sector;
  const char *name   = (*env)->GetStringUTFChars(env, name_in, 0);
  int i;

  /* Clear the 32-byte header: */
  for (i = 0; i < 8; i++)
    prefix[i] = 0;

  generate_lfsr_data(env, sector.data, lba, key, name);

  (*env)->SetIntArrayRegion(env, array, 0, 128, (jint*) &sector);

  (*env)->ReleaseStringUTFChars(env, name_in, name);
}

