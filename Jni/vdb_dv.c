

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


static char c[] =
"Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


extern struct Shared_memory *shared_mem;


#define CHECKSUM(cs, ts)                        \
{                                               \
  jlong tmp = ts & 0x7fffffffffffffff;          \
  cs = 0;                                       \
  while (tmp != 0)                              \
  {                                             \
    cs += (uchar) tmp;                          \
    tmp = tmp >> 8;                             \
  }                                             \
}

static int debug = 0;

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


/* These must match the bits in Bad_sector.java */
static int BAD_KEY      = 0x0001;
static int BAD_CHECKSUM = 0x0002;
static int BAD_LBA      = 0x0004;
static int BAD_NAME     = 0x0008;
static int BAD_DATA     = 0x0010;
static int BAD_COMP     = 0x0020;
static int BAD_DEDUPSET = 0x0040;
static int BAD_ZERO     = 0x0080;
static int BAD_PID      = 0x0100;

/**
 * Generate 480 byte 'Linear Feedback Shift Register' data pattern in 'buffer',
 * using 'name, 'lba' and 'key' as seed values.
 */
extern void generate_lfsr_data(JNIEnv       *env,
                               uint*        buffer,
                               jint         bytes,
                               jlong        lba,
                               uint         key,
                               const char*  name)
{
  uint *data_ptr;
  uint  seed;
  int   loop_count = bytes / sizeof( uint );
  uint  poly_bits;
  uint  data_value;

  if (debug) PTOD("start generate lfsr_data");

  /* The 'name' must be 8 bytes: */
  // We should be able to remove this after a decent amount of paranoia!
  if (strlen(name) != 8)
  {
    PTOD1("generate lfsr_data(): String passed must be 8 bytes long: >>>%s<<<", name);
    PTOD1("generate lfsr_data(): String length: %d", strlen(name));
    ABORT("generate lfsr_data(): String passed must be 8 bytes long: ", name);
  }

  //PTOD1("key: %d", key);

  /* Add the SD or FSD name to the seed: */
  seed  = *(uint*) &name[0] ^ *(uint*) &name[4];

  /* Use the key to select the generator polynomial so each write to an LBA  */
  /* gets a different data pattern.                                          */
  poly_bits = vd_polynomial_coefficients[ key ];

  /* Seed the sequence with the LBA so each sector contains unique data. */
  data_value = ((lba ^ seed) >> 9) * poly_bits;

  /* A zero data_value will generate all zeros. Use something else: */
  if (data_value == 0)
    data_value = (uint) lba;

  /* Set the buffer address: */
  data_ptr = (uint*) buffer;

  do
  {
    //if (debug) PTOD1("loop: %8d", loop_count);
    /* This generates the next value in a Linear Feedback Shift Register Sequence. */
    data_value = (data_value >> 1) ^ ( poly_bits & -(data_value & 1U) );
    *data_ptr++ = data_value;

  } while ( --loop_count != 0 );

  //snap(env, "lfsr", (void*) buffer, 512);

  if (debug) PTOD("end generate lfsr_data");
}


/**
 * Store data pattern
 */
JNIEXPORT void JNICALL Java_Vdb_Native_store_1pattern(JNIEnv   *env,
                                                      jclass    this,
                                                      jintArray jarray)
{

  int   i, *buf;
  jsize len;
  jint *body;

  /* Determine length and address of array: */
  len  = (*env)->GetArrayLength(env, jarray);
  body = (*env)->GetIntArrayElements(env, jarray, 0);

  /* Free old memory if needed: */
  if ( shared_mem->pattern != 0 )
  {
    free(shared_mem->pattern);
  }

  /* Allocate new memory for pattern: */
#ifdef SOLARIS
  shared_mem->pattern = valloc(len * 4);
#else
  shared_mem->pattern = malloc(len * 4);
#endif
  if ( shared_mem->pattern == NULL )
  {
    PTOD2("valloc/malloc for data pattern failed: %d %s\n", len, strerror(errno));
    ABORT("valloc/malloc for data pattern failed", strerror(errno));
  }

  /* Copy the data pattern from Java memory to C memory: */
  for ( buf = (int*) shared_mem->pattern, i = 0; i < len; i++ )
    buf[i] = body[i];

  (*env)->ReleaseIntArrayElements(env, jarray, body, JNI_ABORT);

  /**
   * Save the length of this pattern: The pattern buffer in reality is
   * TWICE longer than needed.
   * This is so that writes directly from the END of the pattern buffer
   * don't have to worry about having th wrap back to the BEGIN of the
   * pattern buffer.
   * Dirty trick, but it works.
   */
  shared_mem->pattern_length  = len * 4;
  //PTOD1("pattern_length: %d", shared_mem->pattern_length);
  shared_mem->pattern_length /= 2;

  return;
}


/**
 * Initialize buffer with a sector header and an LFSR data pattern for Data
 * Validation.
 * The LFSR pattern optionally is skipped if the buffer is already filled with
 * the compression pattern.
 */
static void fill_dv_pattern(JNIEnv *env, struct Request *req, int use_lfsr)
{
  int    i;
  uchar  checksum;
  int    sectors      = req->key_blksize >> 9;

  if (debug) PTOD("start fill dv_pattern");


  /* We have byte 0 left in the timestamp. That will last us until */
  /* the year 45 * (2002 - 1970) = 3442. Remember Y2k?             */
  CHECKSUM(checksum, req->write_time_ms);

  /* Fill in data in each sector: */
  SECTOR_START();
  //PTOD2("lba: %08llx %08lld", req->sector_lba, req->sector_lba );
  for (i = 0; i < sectors; i++ )
  {
    //PTOD2("    lba: %08llx %08lld", req->sector_lba, req->sector_lba );
    req->sector->lba1 = req->sector_lba >> 32;
    req->sector->lba2 = (uint) req->sector_lba;

    //PTOD("debugxxx1");
    /* Store the rest of the standard DV header, unless we want */
    /* blocks across SDs to be duplicates from each other:      */
    if ( (req->dedup_set & UNIQUE_MASK) != UNIQUE_BLOCK_ACROSS_NO)
    {
      //PTOD("debug2");
      req->sector->bytes  = req->key << 24;
      req->sector->bytes |= checksum << 16;
      req->sector->time1  = (uint) (req->write_time_ms >> 32);
      req->sector->time2  = (uint) req->write_time_ms;
      req->sector->pid    = (uint) shared_mem->pid;
      //PTOD("debugxxxx3");
      memcpy(req->sector->name, req->dv_text, 8);

      /* Unique Dedup blocks get their own dedup type number added: */
      if (req->data_flag & FLAG_DEDUP && req->dedup_set & UNIQUE_BLOCK_MASK)
      {
        int dedup_type = (req->dedup_set & DEDUPSET_TYPE_MASK) >> 32;
        req->sector->bytes |= (dedup_type << 8);
      }

      //PTOD("debug4");
      //PTOD2("type: %d %016I64X", dedup_type, req->dedup_set);
    }

    // else
    // {
    //   req->sector->bytes  = 0;
    //   req->sector->time1  = 0;
    //   req->sector->time2  = 0;
    //   req->sector->pid    = 0;
    //   //PTOD("debug3");
    //   memset(req->sector->name, 0, 8);
    // }

    /* The rest of the data contents is generated using LFSR, */
    /* unless we already put the dedup data pattern there:    */
    if (use_lfsr)
      generate_lfsr_data(env, req->sector->data, 480, req->sector_lba, req->key, req->dv_text);

    /* Increment pointers: */
    SECTOR_PLUS();
  }

  if (debug) PTOD("End fill dv_pattern");
}


/**
 * Initialize buffer with the compression/dedup data pattern.
 *
 * The data pattern is initially filled with the compression data pattern
 * requested, with a default of compratio=1
 * The compression data pattern is always at least 1mb long.
 *
 * The 'compression' input variable contains the offset within the
 * data/compression pattern where the copy starts. If the offset and the
 * xfersize together run over the end of the pattern, the remainder of the data
 * is copied from the beginning of the pattern.
 * This is done to allow continued use of the same data/compression pattern, but
 * without the risk of the data contents of every block being the same. If the
 * content is always the same we risk that the loss of a few bits or bytes (or a
 * complete cache line) will not be recognized during Data Validation.
 *
 * Though Data Validation normally uses an LFSR data pattern, the need of having
 * a certain compressible pattern required me to come up with a data pattern
 * that is as unique as possible while still honoring the compressibility.
 *
 * The 'compression' offset is calculated in Java in the Dedup.java code.
 *
 * To prevent cpu cache pollution we use only ONE input data pattern, but copy
 * it as often as we want. This may even be cheaper than LFSR?????
 *
 */
static void fill_compression_pattern(JNIEnv *env, struct Request *req)
{
  jlong pattern    = (jlong) shared_mem->pattern;
  int   pat_length = shared_mem->pattern_length;
  void *buf        = (void*) req->buffer;
  int   offset;
  int   debugc = 0;
  if (pattern == 0)
    ABORT("fill compression_pattern(): No valid data pattern: %d ", (int) 0);

  /* To prevent cpu cache pollution we use only ONE input data pattern, */
  /* but copy it to the target buffer using offsets equal to the input  */
  /* compression offset given.                                          */

  /* Calculate offset in pattern to use */
  offset = req->compression;
  if (debugc) PTOD1("debugc req:        %08p", req);
  if (debugc) PTOD1("debugc offset:     %d", offset);
  if (debugc) PTOD1("debugc pat_length: %d", pat_length);
  if (debugc) PTOD1("debugc req->key_blksize: %d", req->key_blksize);

  /* If the length of this buffer does not go across the end */
  /* of the pattern just copy it:                            */
  // Note:
  //    because of the doubling of the pattern buffer length this check
  //    is actually not needed and should never hit.
  if (offset + req->key_blksize < pat_length*2)
  {
    if (debugc) PTOD1("debugc pattern:  %p", pattern);
    if (debugc) PTOD1("debugc buf:      %p", buf);
    if (debugc) PTOD1("debugc xfersize: %d", req->key_blksize);
    if (debugc) PTOD1("debugc char*:    %p", (void*) ((char*) (pattern + offset)));
    memcpy(buf, (void*) ((char*) (pattern + offset)), req->key_blksize);
  }

  else
  {
    ABORT("Should not be here %d ", (int) 0);
    //if (debugc) PTOD1("debugc offset should not:     %d", offset);
    ///* Copy what we can from the end of the pattern and then */
    ///* copy the beginning of the pattern:                    */
    //int copy2 = offset + req->key_blksize - pat_length;
    //int copy1 = req->key_blksize - copy2;
    //memcpy(buf, (void*) ((char*) (pattern + offset)), copy1);
    //memcpy((void*) ((char*) buf + copy1), (void*) pattern, copy2);
  }
  if (debug) PTOD("debugc End fill compression_pattern");

  //snap(env, "pattfill", (void*) pattern, 512+64);
  // snap(env, "fillbuff", (void*) buffer, 64);
}


/**
 * Duplicate block. Consists of a pure compression pattern with only the first
 * two words changed to dedup_set.
 */
static void store_dedup_set(JNIEnv *env, struct Request *req)
{
  int i;

  if (debug) PTOD("Start store dedup_set");

  SECTOR_START();
  for (i = 0; i < req->sectors; i++ )
  {
    req->sector->lba1 = req->dedup_set >> 32;
    req->sector->lba2 = (uint) req->dedup_set;
    //PTOD2("lba: %08X %08x", req->sector->lba1, req->sector->lba2);

    SECTOR_PLUS();
  }

  if (debug) PTOD("End store dedup_set");
}


/**
 * Fill a 'key block' worth of data.
 *
 * True key blocks are filled for Data Validation and Dedup, for compression
 * patterns there is only one key covering the proper xfersize.
 *
 */
extern void fill_key_block(JNIEnv         *env,
                           struct Request *req)
{
  int debugf = 0;
  if (debug) PTOD1("start fill whole_buffer for lba 0x%08x", req->sector_lba);


  while (1)   /* while() only to allow for break */
  {

    /* Dedup implies Data Validation: */
    if (debugf) PTOD("debugf 1");
    if (req->data_flag & FLAG_VALIDATE)
    {
      /* See below: just an extra paranoia check: */
      if (req->buffer == 0)
        ABORT("Invalid 'use pattern buffer' request1)", "");

      /* Regular Data Validation (no dedup or compression) use DV+LFSR pattern: */
      if (req->data_flag & FLAG_VALIDATE_NORMAL)
      {
        if (debugf) PTOD("debugf 2");
        fill_dv_pattern(env, req, 1);
        break;
      }

      /* Compression only? Fill compression pattern and DV headers */
      if (req->data_flag & FLAG_VALIDATE_COMP)
      {
        if (debugf) PTOD("debugf 3");
        fill_compression_pattern(env, req);
        fill_dv_pattern(env, req, 0);
        break;
      }

      /* Dedup with a unique block? Overlay the 32-byte DV headers */
      if (req->data_flag & FLAG_DEDUP && req->dedup_set & UNIQUE_BLOCK_MASK)
      {
        if (debugf) PTOD("debugf 4");
        //PTOD1("req->data_flag1: %08x", req->data_flag);
        fill_compression_pattern(env, req);
        fill_dv_pattern(env, req, 0);
        break;
      }

      /* Dedup with a duplicate block. Only store dedup_set number: */
      if (req->data_flag & FLAG_DEDUP && !(req->dedup_set & UNIQUE_BLOCK_MASK))
      {
        if (debugf) PTOD("debugf 5");
        //PTOD1("req->data_flag2: %08x", req->data_flag);
        fill_compression_pattern(env, req);
        store_dedup_set(env, req);
        break;
      }


      PTOD1("req->data_flag1: %04x", req->data_flag);
      ABORT("fill whole_buffer1: Invalid data_flag contents","");
    }

    /* Compression only? */
    // This could be just 'else', but let's leave it for a bit.
    // This means that ANY write that is not DV/Dedup goes through here.
    // Of course, a pattern file does not go through here anyway
    // Maybe have a debugging option to force real pattern copy?
    else if (req->data_flag & FLAG_COMPRESSION)
    {

      /**
       * Performance enhancement filling write buffers: If the write buffer address is
       * zero it will be replaced by an address inside of the data pattern
       * buffer. This eliminates the need to copy gigabuckets of data from the
       * pattern buffer to the write buffer, removing a huge amount of cpu
       * cycles.
       *
       * This of course can ONLY be done when Data Validation or Dedup is not
       * involved.
       *
       * BTW: yes, the 'prevent_dedup' call below copying into the pattern
       * buffer will lose maybe a very slight level of accuracy as far as the
       * no-dedup change, but likely is so little that this will just be
       * noise.
       */
      if (debugf) PTOD("debugf 6");
      if (req->caller_buffer == 0)
      {
        if (debugf) PTOD("debugf 6a");
        req->buffer = ((jlong) shared_mem->pattern + req->compression);
        ABORT("Invalid 'use pattern buffer' request4", "");
      }
      else
      {
        // will this ever be called this way?
        // Really, this should only be called now for DV/Dedup.
        //printRequest(env, req);
        //ABORT("Invalid 'use pattern buffer' request3", "");
        fill_compression_pattern(env, req);
      }

      prevent_dedup(env, req->fhandle, req->pattern_lba, req->buffer, req->key_blksize);

      break;
    }

    PTOD1("req->data_flag2: %04x", req->data_flag);
    ABORT("fill whole_buffer2: Invalid data_flag contents","");
  }

  if (debug) PTOD("End fill whole_buffer");
}


/**
 * We have a bad sector. The reporting of this is done in Java.
 * Information for each sector is stored in java, and reporting is not done
 * until 'report_io_error()' returns a 60003 error to java.
 */
static void report_bad_sector(JNIEnv *env,
                              struct Request *req,
                              int    error_flag)
{

  jclass clx;
  jmethodID report;
  jintArray sector_array;

  CHECK(1);
  clx = (*env)->FindClass(env, "Vdb/BadSector");
  CHECK(2);
  report = (*env)->GetStaticMethodID(env, clx, "signalBadSector",
                                     "([IJJJJJJJJJJJJJ)V");
  CHECK(3);

  sector_array = (*env)->NewIntArray(env, 128);
  (*env)->SetIntArrayRegion(env, sector_array, 0, 128, (jint*) req->sector);


  (*env)->CallStaticVoidMethod(env, clx, report,
                               sector_array,
                               (jlong) req->fhandle,
                               (jlong) req->file_lba,
                               (jlong) req->pattern_lba,
                               (jlong) req->file_start_lba,
                               (jlong) req->sector_lba,
                               (jlong) req->offset_in_key_block,
                               (jlong) req->compression,
                               (jlong) req->dedup_set,
                               (jlong) req->data_flag,
                               (jlong) req->key,
                               (jlong) req->key_blksize,
                               (jlong) req->data_length,
                               (jlong) error_flag) ;
}


/**
 * Validate the contents of the DV sector header.
 */
static int check_dv_header(JNIEnv         *env,
                           struct Request *req)
{
  int error_flag = 0;

  uchar ts_check;
  jlong time = ((jlong) req->sector->time1 << 32) + req->sector->time2;
  CHECKSUM(ts_check, time);

  //PTOD1("req->sector->lba1: %08x", req->sector->lba1);
  //PTOD1("req->sector->lba2: %08x", req->sector->lba2);
  //PTOD1("req->sector_lba:   %016I64x", req->sector_lba);

  /* Compare the fixed portion of the sector header: */
  if (req->key != req->sector->bytes >> 24)
    error_flag |= BAD_KEY;
  if (ts_check != (uchar) (req->sector->bytes >> 16) )
    error_flag |= BAD_CHECKSUM;
  if (req->sector->lba1 != req->sector_lba >> 32)
    error_flag |= BAD_LBA;
  if (req->sector->lba2 != (uint) req->sector_lba)
    error_flag |= BAD_LBA;
  if (memcmp(req->sector->name, req->dv_text, 8 ) != 0 )
    error_flag |= BAD_NAME;
  if (req->sector->bytes &0xffff != 0)
    error_flag |= BAD_ZERO;

  // See note in vdb_jni.h
  //if (req->sector->pid != 0)
  //  error_flag |= BAD_ZERO;
  //if (req->sector->pid != shared_mem->pid)
  //  error_flag |= BAD_PID;

  if (debug) PTOD1("check_dv_header: %08x", error_flag);

  return error_flag;
}

/**
 */
extern int validate_dv_sector(JNIEnv *env,
                              struct Request *req)
{
  uint i, seed, error_flag;

  uint* data_ptr = (uint*) &req->sector->data;
  uint  error_accum;     /* LFSR field */
  uint  expected_value;  /* LFSR field */
  uint  actual_value;    /* LFSR field */
  uint  poly_bits  = vd_polynomial_coefficients[ req->key ];
  int   loop_count = 480 / sizeof( uint );

  if (debug) PTOD("start validate dv_sector");

  /* Check the fixed pieces: */
  error_flag = check_dv_header(env, req);


  /* The 480 remaining bytes have been filled using LFSR.                      */
  /* Use that again for comparison:                                            */
  /* The following value will contain 0 only if every bit of data was correct. */
  error_accum = 0;

  /* Add the SD or FSD name to the seed: */
  seed = (*(uint*) &req->dv_text[0] ^ *(uint*) &req->dv_text[4]);

  /* Seed the sequence with the LBA so each sector contains unique data. */
  expected_value = ((req->sector_lba ^ seed) >> 9) * poly_bits;

  /* A zero data_value will generate all zeros. Use something else: */
  if (expected_value == 0)
    expected_value = (uint) req->sector_lba;

  do
  {
    /* Start read early so that it overlaps with generating the expected value. */
    actual_value = *data_ptr++;

    /* This generates the next value in a Linear Feedback Shift Register Sequence. */
    expected_value = (expected_value >> 1) ^ ( poly_bits & -(expected_value & 1U) );

    /* OR a non-zero value into error_accum if the data doesn't match. */
    error_accum |= actual_value ^ expected_value;

  } while ( --loop_count != 0 );

  /* Do we have an error? */
  if (error_accum != 0)
    error_flag |= BAD_DATA;

  if (debug) PTOD1("validate_dv_sector: %08x", error_flag);

  if ( error_flag != 0 )
  {
    //PTOD1("compression: %lld", req->compression);
    //PTOD1("report bad_sector from validate dv_sector. error_flag: %08x", error_flag);
    report_bad_sector(env, req, error_flag);
  }


  return error_flag;
}



/**
 * Validate a compression (no dedup) data pattern.
 * This contains the standard DV sector header, but the data comes from the
 * compression data pattern, and not from LFSR
 */
extern int validate_comp_sector(JNIEnv *env,
                                struct Request *req)
{
  int   i;
  jlong pattern    = (jlong) shared_mem->pattern;
  int   pat_length = shared_mem->pattern_length;
  int   error_flag = 0;
  jlong data       = (jlong) req->sector;

  if (debug) PTOD("start validate comp_sector");

  /* Check the fixed pieces: */
  if ( (req->dedup_set & UNIQUE_MASK) != UNIQUE_BLOCK_ACROSS_NO)
  {
    error_flag = check_dv_header(env, req);

    /* Check the 480 byte data pattern: */
    for (i = 0 ; i < 120; i ++)
    {
      int   offset = (i << 2) + 32;
      uint* datptr = (uint*) (data + offset);
      uint* patptr = (uint*) (pattern + ((req->compression + offset + req->offset_in_key_block) % pat_length));
      if (*datptr == *patptr)
        continue;

      error_flag |= BAD_COMP;
      if (debug) PTOD1("mismatch1: %08x", error_flag);
      break;
    }
  }
  else
  {
    if (req->sector->lba1 != req->sector_lba >> 32)
      error_flag |= BAD_LBA;
    if (req->sector->lba2 != (uint) req->sector_lba)
      error_flag |= BAD_LBA;

    if (debug) PTOD1("halfway: %08x", error_flag);

    /* Check the 504 byte data pattern: */
    for (i = 0 ; i < 126; i ++)
    {
      int   offset = (i << 2) + 8;
      uint* datptr = (uint*) (data + offset);
      uint* patptr = (uint*) (pattern + ((req->compression + offset + req->offset_in_key_block) % pat_length));
      //PTOD1("offset: %d", offset);
      //PTOD1("datptr: %p", datptr);
      //PTOD1("patptr: %p", patptr);
      //PTOD1("patptd: %lld", ((char*) patptr - pattern));
      //PTOD1("datpt*: %08x", *datptr);
      //PTOD1("patpt*: %08x", *patptr);
      if (*datptr == *patptr)
        continue;

      error_flag |= BAD_COMP;
      if (debug) PTOD1("mismatch2: %08x", error_flag);
      break;
    }
  }


  if (error_flag != 0)
  {
    /* The key with dedup is unused. */
    //PTOD1("dedup_set: %lld", req->dedup_set);
    //PTOD1("lba 1: %08x", req->sector->lba1);
    //PTOD1("lba 2: %08x", req->sector->lba2);
    //PTOD1("report bad_sector from validate comp_sector. error_flag: %08x", error_flag);
    report_bad_sector(env, req, error_flag);
  }
  //PTOD("debug5");
  //
  if (debug) PTOD("end validate comp_sector");

  return error_flag;
}


/**
 * The same as validate comp_sector(), but now straight forward the whole
 * sector, except for the first 8 bytes which must contain the dedup set.*
 */
extern int validate_duplicate_sector(JNIEnv* env, struct Request *req)
{
  jlong pattern    = (jlong) shared_mem->pattern;
  int   pat_length = shared_mem->pattern_length;
  int   error_flag = 0;
  int   i, rc;


  /* For simplicity sake I just do a word-for-word compare here.     */
  /* At some point in time maybe change this to memcmp()?            */
  /* The ' + & -2 is to skip the first two words containing dedupset */
  int   words = (512 - 8) >> 2;
  jlong data  = (jlong) req->sector;
  //jlong patt  = (jlong) (pattern + offset_in_block + 32);   // skip 32 for the dv header

  //PTOD("debug2");
  //PTOD1("ofinbl: %lld", offset_in_block);
  //PTOD1("patstart: %p", pattern);
  //PTOD1("patend:   %p", (pattern + 1048576-1));
  //PTOD1("sector: %p", sector);
  //PTOD1("data:   %p", (void*) (data + 32));


  if (debug) PTOD("start validate duplicate_sector");

  //snap(env, "patt", (void*) patt, 64);
  //snap(env, "data", sector, 64);

  /* Compare word-for-word: */
  for (i = 0 ; i < words; i ++)
  {
    int   offset = (i << 2) + 8;
    uint* datptr = (uint*) (data + offset);
    uint* patptr = (uint*) (pattern + ((req->compression + offset + req->offset_in_key_block) % pat_length));
    //PTOD1("offset: %d", offset);
    //PTOD1("datptr: %p", datptr);
    //PTOD1("patptr: %p", patptr);
    //PTOD1("patptd: %lld", ((char*) patptr - pattern));
    //PTOD1("datpt*: %08x", *datptr);
    //PTOD1("patpt*: %08x", *patptr);
    if (*datptr == *patptr)
      continue;

    error_flag |= BAD_COMP;
    //PTOD1("mismatch3: %08x", error_flag);
    break;
  }

  /* Compare the dedup set#:                                                        */
  /* Technically I should compare the dedupset and the key inside of it separately! */
  /* However, I can also make that distinction in the reporting.                    */
  if (req->sector->lba1 != req->dedup_set >> 32)
    error_flag |= BAD_DEDUPSET;
  if (req->sector->lba2 != (uint) req->dedup_set)
    error_flag |= BAD_DEDUPSET;


  //PTOD("debug4");
  //PTOD1("error_flag: %08x", error_flag);
  if (error_flag != 0)
  {
    /* The key with dedup is unused. */
    int key = 0;
    //PTOD1("dedup_set: %lld", req->dedup_set);
    //PTOD1("lba 1: %08x", req->sector->lba1);
    //PTOD1("lba 2: %08x", req->sector->lba2);
    //PTOD1("report bad_sector from validate duplicate_sector. error_flag: %08x", error_flag);
    report_bad_sector(env, req, error_flag);
  }
  //PTOD("debug5");

  if (debug) PTOD("end validate duplicate_sector");

  return error_flag;
}


/**
 * Validate a complete buffer.
 * If the key has bit 0x8000 set we first forcibly zap the sector.
 */
extern int validate_key_block(JNIEnv *env, struct Request *req)
{
  int i, rc;
  int errors_in_data_block = 0;
  int force_error = (req->key & 0x8000);
  //debug = 1;
  if (debug) PTOD("start of validate whole_buffer");


  /* A trick to force data validation errors: Change contents AFTER read. */
  /* This will be caught for normal DV, Dedup unique and duplicate, and compression */
  if (force_error)
  {
    SECTOR_START();
    for (i = 0; i < req->sectors; i++ )
    {
      if (i == 0)
        req->sector->lba1           = 0x0bad0bad;  // mess up lba
      else if (i == 1)
        req->sector->data[119]      = 0x0bad0bad;  // mess up last bytes of data
      else if (i == 2)
        req->sector->time1          = 0x0bad0bad;  // mess up first byte of timestamp
      else if (i == 3)
        req->sector->bytes          = 0x0bad0bad;  // mess up checksum
      else if (i == 4)
        *(uint*) &req->sector->name = 0x0bad0bad;  // mess up name
      SECTOR_PLUS();
    }

    req->key &= 0xff;
    PTOD1("Forcing Data Validation error due to 'force_error_after'. key: %d", req->key);
  }


  /* Compare data in each sector: */
  SECTOR_START();
  if (debug) PTOD1("req->sectors: %d", req->sectors);
  for ( i = 0; i < req->sectors; i++ )
  {
    if (debug) PTOD1("start sector %d", i);

    /* Normal DV implies no compression */
    if (req->data_flag & FLAG_VALIDATE_NORMAL)
      rc = validate_dv_sector(env, req);

    /* This is a unique block: */
    else if (req->data_flag & FLAG_DEDUP && req->dedup_set & UNIQUE_BLOCK_MASK)
      rc = validate_comp_sector(env, req);

    /* This is a duplicate block: */
    else if (req->data_flag & FLAG_DEDUP && !(req->dedup_set & UNIQUE_BLOCK_MASK))
      rc = validate_duplicate_sector(env, req);

    /* Compression only: */
    else if (req->data_flag & FLAG_COMPRESSION)
      rc = validate_comp_sector(env, req);

    else
    {
      PTOD1("req->dedup_set: %016I64x", req->dedup_set);
      PTOD1("req->data_flag: %04x", req->data_flag);
      ABORT("validate whole_buffer: invalid data_flag contents","");
    }

    if (rc != 0)
      errors_in_data_block ++;
    SECTOR_PLUS();

    if (debug) PTOD1("end sector %d", i);
  }

  if (debug) PTOD1("end of validate whole. Errors: %d", errors_in_data_block);
  return errors_in_data_block;
}




/**
 * Data Validation.
 *
 * Read the whole block, and then compare the data for each key
 * block.
 *
 * file_start_lba: the offset of this block within this FSD (zero for SD)
 * file_lba:       the offset of this block within the file/SD
 *
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_multiKeyReadAndValidate(JNIEnv     *env,
                                                                jclass     this,
                                                                jlong      handle,
                                                                jint       data_flag,
                                                                jlong      f_start_lba,
                                                                jlong      f_lba,
                                                                jint       d_length,
                                                                jlong      buffer,
                                                                jint       key_count,
                                                                jintArray  keys,
                                                                jlongArray compressions,
                                                                jlongArray dedup_sets,
                                                                jstring    dv_text_in,
                                                                jint       jni_index)
{
  jlong  rc = 0;
  jlong  tod1;
  int i, j;
  jlong  bypass_read   = 0;
  struct Workload *wkl = (jni_index < 0) ? 0 : &shared_mem->workload[jni_index];

  char*  dv_text       = (char*) (*env)->GetStringUTFChars(env, dv_text_in, 0);
  jsize  len           = (*env)->GetArrayLength(env, keys);
  jint*  key_array     = (*env)->GetIntArrayElements(env, keys, 0);
  jlong* cmp_array     = (*env)->GetLongArrayElements(env, compressions, 0);
  jlong* ded_array     = (*env)->GetLongArrayElements(env, dedup_sets, 0);
  int    offset_in_block = 0;
  int    errors_found    = 0;
  struct Request request;
  struct Request *req = &request;

  /* When handle is negative, bypass the read.
     This is a nice way to help create corruptions for debugging:
     - read the block and validate
     - the data buffer will be corrupted in java code
     - call this again, but skip read.
     - this should trigger some fun corruptions!  */
  if (handle < 0)
  {
    handle = handle * -1;
    bypass_read = 1;
  }


  /* Specify fixed 'Request' info: */
  req->data_length    = d_length;
  req->key_blksize    = d_length / key_count;
  req->fhandle        = handle;
  req->pattern_lba    = f_start_lba + f_lba;
  req->file_lba       = f_lba;
  req->file_start_lba = f_start_lba;
  req->dv_text        = dv_text;
  req->data_flag      = data_flag;

  //PTOD1("req->pattern_lba: %08I64d", req->pattern_lba);
  //PTOD1("f_start_lba:      %08I64d", f_start_lba);
  //PTOD1("f_lba:            %08I64d", f_lba);

  /* Read the whole block: */
  tod1 = START_WORKLOAD_STATS(env, wkl);
  //PTOD("debug: r0");
  if (!bypass_read)
    rc = file_read(env, handle, f_lba, req->data_length, buffer);
  UPDATE_WORKLOAD_STATS(env, wkl, 1, req->data_length, tod1, rc);


  //PTOD("debug: r1");

  if (rc == 0)
  {
    //PTOD("debug: r2");
    /* Validate each key block: */
    for (i = 0; i < key_count; i++)
    {
      //PTOD("debug: r3");
      /* Only those with a non-zero key: */
      if (key_array[i] > 0)
      {
        //PTOD("debug: r4");
        /* Fill in info for validate whole_buffer(): */
        req->buffer      = req->key_blksize * i + buffer;
        req->pattern_lba = req->key_blksize * i + f_lba + f_start_lba;
        req->key         = key_array[i];
        req->compression = cmp_array[i];
        req->dedup_set   = ded_array[i];

        if (validate_key_block(env, req) > 0)
          errors_found++;
      }
    }

    //PTOD("debug: r5");
    if (errors_found > 0)
    {
      //PTOD("debug: r6");
      report_io_error(env, 1, handle, req->file_lba, req->data_length, 60003, buffer);
      rc = 60003;
    }
  }
  else
  {
    //PTOD("debug: r7");
    report_io_error(env, 1, handle, req->file_lba, req->data_length, rc, buffer);
  }
  //PTOD("debug: r8");

  (*env)->ReleaseStringUTFChars(env, dv_text_in, dv_text);
  (*env)->ReleaseIntArrayElements(env,  keys,         key_array, JNI_ABORT);
  (*env)->ReleaseLongArrayElements(env, compressions, cmp_array, JNI_ABORT);
  (*env)->ReleaseLongArrayElements(env, dedup_sets,   ded_array, JNI_ABORT);

  return rc;
}


/**
 * Data Validation.
 *
 * Fill and write the whole block.
 *
 * file_start_lba: the offset of this block within this FSD (zero for SD)
 * file_lba:       the offset of this block within the file/SD
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_multiKeyFillAndWrite(JNIEnv     *env,
                                                             jclass     this,
                                                             jlong      handle,
                                                             jlong      tod,
                                                             jint       d_flag,
                                                             jlong      f_start_lba,
                                                             jlong      f_lba,
                                                             jint       d_length,
                                                             jlong      p_lba,
                                                             jint       p_length,
                                                             jlong      buffer,
                                                             jint       key_count,
                                                             jintArray  keys,
                                                             jlongArray compressions,
                                                             jlongArray dedup_sets,
                                                             jstring    dv_text_in,
                                                             jint       jni_index)
{
  int i;
  jlong  rc, tod1;
  struct Request request;
  struct Request *req  = &request;
  struct Workload *wkl = (jni_index < 0) ? 0 : &shared_mem->workload[jni_index];
  char*  dv_text       = (char*) (*env)->GetStringUTFChars(env, dv_text_in, 0);
  jint*  key_array     = (*env)->GetIntArrayElements(env, keys, 0);
  jlong* cmp_array     = (*env)->GetLongArrayElements(env, compressions, 0);
  jlong* ded_array     = (*env)->GetLongArrayElements(env, dedup_sets, 0);

  int debugw = 0;

  /* The 'name' must be 8 bytes: */
  // maybe remove this or have a constant in wkl->, better yet, pass a64-bit LONG from java.?
  if (dv_text == NULL)
  {
    PTOD("fill buffer(): NULL dv_text.");
    ABORT("fill buffer(): NULL dv_text. ", "");
  }
  if (strlen(dv_text) != 8)
  {
    PTOD1("fill buffer(): String passed must be 8 bytes long: >>>%s<<<", dv_text);
    PTOD1("fill buffer(): String length: %d", strlen(dv_text));
    ABORT("fill buffer(): String passed must be 8 bytes long: ", dv_text);
  }

  if (debugw) PTOD("debugw 1");

  if (debug) PTOD("start multiKey FillAndWrite");

  /* Copy the fixed portion of the info: */
  req->fhandle        = handle;
  req->data_length    = d_length;
  req->pattern_length = p_length;
  req->key_blksize    = p_length / key_count;
  req->dv_text        = dv_text;
  req->file_lba       = f_lba;
  req->data_flag      = d_flag;
  req->write_time_ms  = tod;
  req->file_start_lba = f_start_lba;
  req->caller_buffer  = buffer;
  if (debugw) PTOD("debugw 2");

  /* Fill each 'key_block_size' piece of the buffer: */
  for (i = 0; i < key_count; i++)
  {
    if (debugw) PTOD("debugw 3");
    req->key         = key_array[i];
    req->buffer      = (i * req->key_blksize) + buffer;
    req->pattern_lba = (i * req->key_blksize) + p_lba;
    req->sector_lba  = req->pattern_lba;
    req->compression = cmp_array[i];
    req->dedup_set   = ded_array[i];
    if (debugw) PTOD("debugw 4");
    if (debugw) printRequest(env,req);

    /* Fill a key block: */
    fill_key_block(env, req);
    if (debugw) PTOD("debugw 5");
  }

  if (debugw) PTOD("debugw 5x1");
  (*env)->ReleaseStringUTFChars(env, dv_text_in, dv_text);
  if (debugw) PTOD("debugw 5x2");
  (*env)->ReleaseIntArrayElements(env,  keys,         key_array, JNI_ABORT);
  if (debugw) PTOD("debugw 5x3");
  (*env)->ReleaseLongArrayElements(env, compressions, cmp_array, JNI_ABORT);
  if (debugw) PTOD("debugw 5x4");
  (*env)->ReleaseLongArrayElements(env, dedup_sets,   ded_array, JNI_ABORT);
  if (debugw) PTOD("debugw 5x5");


  /* The req->buffer pointer may have been modified by fill_key_block: */
  if (buffer == 0)
  {
    if (debugw) PTOD("debugw 5a");
    req->buffer = ((jlong) shared_mem->pattern + req->compression);
    if (debugw) PTOD("debugw 5b");
  }
  else
  {
    if (debugw) PTOD("debugw 5c");
    req->buffer = buffer;
    if (debugw) PTOD("debugw 5d");
  }
  if (debugw) PTOD1("debugw 6a %d", req->key_blksize);
  if (debugw) PTOD1("debugw 6b %d", req->pattern_length);


  /* To allow Dedup to write any xfersize it wants (without DV), we some times */
  /* build a data pattern that has extra bytes BEFORE or AFTER the actual      */
  /* block to be written. This is done by creating FULL Key Block patterns.    */
  /* The buffer address now needs to be adjusted so that we start at the       */
  /* proper real byte, and not at the begiining of a Key Block.                */

  /* If file_lba does not start on a multiple of the key block size it means */
  /* that we have created 'n' bytes of extra data pattern. Adjust buffer: */
  if (req->data_flag & FLAG_DEDUP && req->file_lba % req->key_blksize)
    req->buffer += req->file_lba % req->key_blksize;

  if (debugw) PTOD("debugw 7");
  if (debugw) PTOD2("buffer: %08llx file_lba: %08llx", req->buffer, req->file_lba);

  tod1 = START_WORKLOAD_STATS(env, wkl);
  if (debugw) PTOD("debugw 8");

  //snap(env, "data", (void*) req->buffer, 16);

  rc   = file_write(env, handle, req->file_lba, req->data_length, req->buffer);


  if (debugw) PTOD("debugw 9");
  UPDATE_WORKLOAD_STATS(env, wkl, 0, req->data_length, tod1, rc);
  if (debugw) PTOD("debugw 10");

  if (rc != 0)
  {
    report_io_error(env, 0, handle, req->file_lba, req->data_length, rc, req->buffer);
  }

  return rc;
}


/**
 * Call to Java to report i/o error or Data Validation error.
 * For Data Validation this is only called once per data block.
 */
void report_io_error(JNIEnv *env, jlong read_flag, jlong fhandle,
                     jlong   lba, jint  xfersize,  jlong error, jlong buffer)
{
  jclass clx;
  jmethodID report;

  CHECK(1);
  clx = (*env)->FindClass(env, "Vdb/IO_task");
  CHECK(2);
  report = (*env)->GetStaticMethodID(env, clx, "io_error_report", "(JJJJJJ)V");
  CHECK(3);

  (*env)->CallStaticVoidMethod(env, clx, report, read_flag,
                               fhandle, lba, (jlong) xfersize, error, buffer);
}



/**
 * Fills a Java int[] array with LFSR data.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_fillLfsrArray(JNIEnv    *env,
                                                     jclass    this,
                                                     jintArray array,
                                                     jlong     lba,
                                                     jint      key,
                                                     jstring   name_in)
{
  char *name   = (char*) (*env)->GetStringUTFChars(env, name_in, 0);
  int  bytes   = (*env)->GetArrayLength(env, array) * sizeof(uint);
  uint *buffer = (uint*) alloc_buffer(env, bytes);

  if (buffer == 0)
    ABORT("Java_Vdb_Native_fillLFSRArray", "memory allocation failed");

  generate_lfsr_data(env, buffer, bytes, lba, key, name);

  (*env)->SetIntArrayRegion(env, array, 0, bytes / sizeof(uint), (jint*) buffer);

  (*env)->ReleaseStringUTFChars(env, name_in, name);

  free_buffer(bytes, (jlong) buffer);
}


/**
 * Fills a native buffer with LFSR data.
 */
JNIEXPORT void JNICALL Java_Vdb_Native_fillLfsrBuffer(JNIEnv    *env,
                                                      jclass    this,
                                                      jlong     buffer,
                                                      jint      bytes,
                                                      jlong     lba,
                                                      jint      key,
                                                      jstring   name_in)
{
  char *name   = (char*) (*env)->GetStringUTFChars(env, name_in, 0);

  if (buffer == 0)
    ABORT("Java_Vdb_Native_fillLFSRBuffer", "memory allocation failed");

  generate_lfsr_data(env, (uint*) buffer, (int) bytes, lba, key, name);

  (*env)->ReleaseStringUTFChars(env, name_in, name);
}


/**
 * Function to overlay the first 8 bytes of each 4k portion of the data buffer.
 * This prevents accidental dedup results.
 */
extern void prevent_dedup(JNIEnv *env,
                          jlong   fhandle,
                          jlong   file_lba,
                          jlong   buffer,
                          int     xfersize)
{
  int    i;
  int    pieces = (xfersize + 4095) >> 12;
  jlong  tod1   = GET_SIMPLE_TOD();

  //PTOD1("buffer: %I64d", buffer);
  //PTOD1("xfersize: %d", xfersize);

  for (i = 0; i < pieces; i++)
  {
    uint  *ptr;
    uint  *ptr2;
    int    offset = i << 12;
    //PTOD("debugp 1");
    //PTOD1("offset: %d", offset);

    ptr  = (uint*) (buffer + offset);
    ptr2 = (uint*) (buffer + offset);

    //PTOD("debugp 2");
    //PTOD1("file_lba %16I64d", file_lba);
    *ptr = (uint) ((file_lba + offset) / 4096) ^ (fhandle << 16);
    //PTOD("debugp 3");

    ptr  = (uint*) (buffer + offset + 4);
    //PTOD("debugp 4");
    *ptr = tod1;
    //PTOD("debugp 5");

    //PTOD1("handle: %08I64x", fhandle);
    //PTOD1("tod1: %08I64x", tod1);
    //snap(env, "prevent", ptr2, 8);
  }
}



