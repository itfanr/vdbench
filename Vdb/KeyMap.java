package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
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



/**
 * This class handles all DV_map manipulations around the fact that for
 * file system testing a DV_map entry of one block does not just support
 * the xfersize, but supports the lowest xfersize used.
 * All other xfersizes must be a multiple of this lowest xfersize.
 *
 * Terminology: - Key:            the DV key
 *              - Key block size: the lowest xfersize used.
 *              - Key block:      a block of 'key block size'.
 *              - Key map:        an array of keys, up to a length of
 *                                (xfersize / key block size).
 */
public class KeyMap
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private DV_map map            = null;
  private int    key_block_size = 0;
  private int    sum_of_keys    = 0;
  private long   logical_lba    = 0;
  private long   file_lba       = 0;
  private int[]  key_map        = null;
  private int    key_count      = 0;   /* (xfersize / key block size) */
  private boolean format;


  public KeyMap(DV_map mp, long start_lba, int key_blk_size, int max_xfersize)
  {
    map            = mp;
    logical_lba    = start_lba;
    key_block_size = key_blk_size;

    key_map = new int[max_xfersize / key_block_size];

    format = SlaveWorker.work.format_run;
  }

  /**
   * Clear DV_map entries for a file.
   * This is needed when a file is either deleted or created, though at
   * creation we should actually already find everything cleared.
   */
  public void clearMapForFile(long file_size)
  {
    for (long size = 0; size < file_size; size += key_block_size)
      map.dv_set(logical_lba + size, 0);
  }


  /**
   * Get the DV keys for each key block inside of an xfersize
   */
  public boolean getKeysFromMap(long f_lba, int xfersize)
  {
    file_lba = f_lba;
    if (file_lba % key_block_size != 0)
      common.failure("Unexpected lba: " + file_lba + "/"+ key_block_size);

    if (xfersize % key_block_size != 0)
      common.failure("Unexpected xfersize: " + xfersize + ", key block size: "+ key_block_size);

    key_count = xfersize / key_block_size;
    long lba  = logical_lba + file_lba;

    sum_of_keys = 0;
    for (int i = 0; i < key_count; i++)
    {
      int key = map.dv_get(lba + (i * key_block_size));
      sum_of_keys += key;

      /* A bad block will never be reused! */
      if (key == DV_map.DV_ERROR)
        return false;

      key_map[i] = key;
    }

    return true;
  }

  public int[] getKeys()
  {
    return key_map;
  }
  public int getKeyCount()
  {
    if (key_count == 0)
      common.failure("zero key_count");

    return key_count;
  }
  public int getKeyBlockSize()
  {
    return key_block_size;
  }


  /**
   * If all keys are zero we can't read and validate the block.
   */
  public boolean preReadNeeded()
  {
    return sum_of_keys != 0;
  }


  /**
   * Increment keys for a write operation.
   */
  public boolean incrementKeys()
  {
    long lba = logical_lba + file_lba;

    for (int i = 0; i < key_count; i++)
    {
      /* If during the read-before-write the key was set to 'error', skip block: */
      if (map.dv_get(lba + i * key_block_size) == DV_map.DV_ERROR)
        return false;
      key_map[i] = DV_map.dv_increment(key_map[i]);
    }

    return true;
  }

  public void storeKeys()
  {
    long lba = logical_lba + file_lba;

    for (int i = 0; i < key_count; i++)
    {
      if (map.dv_get(lba + i * key_block_size) != DV_map.DV_ERROR)
      {
        map.dv_set(lba + i * key_block_size, key_map[i]);
      }
    }
  }

  /**
   * If the 'validate=time' option is used, save the last successful TS.
   */
  public void saveTimestamp(boolean read)
  {
    if (!Validate.isStoreTime())
      return;

    long lba = logical_lba + file_lba;

    for (int i = 0; i < key_count; i++)
      map.save_timestamp(lba + i * key_block_size, read);
  }


  public void countReadAndValidates()
  {
    /* Count the number of key blocks read and validated: */
    for (int i = 0; i < key_count; i++)
    {
      if (key_map[i] != 0)
        map.key_reads[key_map[i]]++;
    }
  }

  public void countWrites()
  {
    /* Count the number of key blocks read and validated: */
    for (int i = 0; i < key_count; i++)
    {
      if (key_map[i] != 0)
        map.key_writes[key_map[i]]++;
    }
  }


  /**
   * Write before/after journal image.
   * Journal update NOT done for a format operation. WHY NOT???
   */
  public void writeBeforeJournalImage()
  {
    if (Validate.isMapOnly() || format)
      return;

    synchronized (map.journal)
    {
      for (int i = 0; i < key_count; i++)
      {
        int block = (int) (logical_lba + file_lba) / key_block_size + i;
        map.journal.writeJournalEntry(key_map[i], block, (i + 1) == key_count);
      }
    }
  }

  private static int debug_writes = 1000;
  private static int debug_skips  = 10;
  private final static boolean debug_clean = common.get_debug(common.DEBUG_DV_CLEANUP);
  public void writeAfterJournalImage()
  {
    if (Validate.isMapOnly() || format)
      return;

    /* For journal writes, write the first 'n' After images, */
    /* but then skip some After images writes: */
    if (debug_clean && Validate.isJournaling())
    {
      common.ptod("debug_writes: " + debug_writes + " " + debug_skips);

      /* Since for FSD we don't set the block busy we can handle only one error: */
      if (debug_writes-- < 0)
        common.failure("Debugging Journaling. Limit reached");
    }


    synchronized (map.journal)
    {
      for (int i = 0; i < key_count; i++)
      {
        int block = (int) (logical_lba + file_lba) / key_block_size + i;
        map.journal.writeJournalEntry(0, block, (i + 1) == key_count);
      }
    }
  }

  public static void main(String[] args)
  {
    /*
    String FSD_NAME = "henkie  ";
    Native.alloc_jni_shared_memory(false, common.get_shared_lib());
    DV_map.create_patterns(4096);

    int KEY_BLOCK_SIZE = 512;
    long FILE_LBA = 0l;
    long handle = Native.openfile("c:\\junk\\vdb1", 0, 1);
    DV_map dv_map = DV_map.allocateMap(FSD_NAME, 409600, 512);
    KeyMap keymap = new KeyMap(dv_map, 0L, KEY_BLOCK_SIZE, 4096);
    long   buffer = Native.allocBuffer(4096);

    int[] map = keymap.buildKeys(FILE_LBA, 4096);
    int keys  = keymap.getKeyCount();

    //for (int i = 0; i < keys; i++)
    //  common.ptod("key1: " + map[i]);

    for (int x = 0; x < 55; x++)
    {
      //keymap.incrementKeys();
      long rc = Native.fillAndWrite(handle, 0l, FILE_LBA, 4096, buffer, keys, map, FSD_NAME);
      common.ptod("rc: " + rc);

      common.sleep_some(2000);

      for (int i = 0; i < keys; i++)
        common.ptod("key2: " + map[i]);

      rc = Native.readAndValidate(handle, 0l, FILE_LBA, 4096, buffer, keys, map, FSD_NAME);
      common.ptod("rc: " + rc);
    }
    */
  }
}

