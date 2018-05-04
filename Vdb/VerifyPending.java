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

import java.io.File;
import java.util.Vector;

import Utils.Format;
import Utils.printf;


/**
 * Verify the contents of those writes that were pending during the shutdown of
 * the run that created the journal file.
 * This is for raw functionality only.
 */
public class VerifyPending
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



  /**
   * Look through the recovered DV map and find out which blocks had a wrete
   * pending.
   */
  public static void findPendingBlocks(DV_map before_map, DV_map new_map, SD_entry sd)
  {
    /* The before_map table contains all entries that were in  progress,      */
    /* and will have the key of the block that we were trying to write.       */
    /* We will allocate an array containing the lbas that have an io pending. */
    int lba_entries_needed = 0;
    for (int j = 0; j < new_map.byte_map.length; j++)
    {
      int key = before_map.dv_get((long) j * new_map.getBlockSize());
      if (key != 0)
        lba_entries_needed++;
    }

    /* Create the lba array: */
    before_map.pending_write_lbas = new long[lba_entries_needed];
    int index = 0;
    for (int j = 0; j < new_map.byte_map.length; j++)
    {
      long lba = (long) j * new_map.getBlockSize();
      int key = before_map.dv_get(lba);

      if (key != 0)
        before_map.pending_write_lbas[index++] = lba;
    }

    log("There were " + lba_entries_needed + " pending writes " +
        "found in this journal");
  }


  /**
   * Check the pending blocks for this SD_entry.
   */
  public static void checkSdPendingBlocks(DV_map   before_map,
                                          DV_map   new_map,
                                          SD_entry sd)
  {
    printf pf;

    /* Check all pending writes to see what the current status is: */
    log("Journal recovery complete for sd=" + sd.sd_name + ",lun=" + sd.lun);

    if (before_map.pending_write_lbas.length > 0)
    {
      log("The following lbas had a write operation in progress " +
          "at the end of the previous Vdbench run.");
      log("The I/O completion status is being checked.");
      log("");
    }

    /* Now read and check all the pending blocks: */
    for (int i = 0; i < before_map.pending_write_lbas.length; i++)
    {
      long lba       = before_map.pending_write_lbas[i];
      int before_key = before_map.dv_get(lba);

      /* If the before key was marked 'error' it means the block had */
      /* never been written and therefore should be set to zero:    */
      if (before_key == DV_map.DV_ERROR)
        before_key = 0;

      int after_key  = DV_map.dv_increment(before_key);

      int before_keys = checkSdKeys(sd, lba, new_map.getBlockSize(), before_key);
      int after_keys  = checkSdKeys(sd, lba, new_map.getBlockSize(), after_key);
      int sectors     = new_map.getBlockSize() / 512;

      /* Decide on the status of this block: */
      compareKeyCounts(new_map, before_key, before_keys,
                       after_key, after_keys, sectors, lba);
    }
  }


  /**
   * We have read the block and counted the key values.
   * Report the status and/or discrepancies.
   *
   */
  private static void compareKeyCounts(DV_map new_map,
                                       int    before_key,
                                       int    before_keys,
                                       int    after_key,
                                       int    after_keys,
                                       int    sectors,
                                       long   lba)
  {
    printf pf;

    /* If the key is what we expect, hurray. It means that the block */
    /* indeed has been written. new_map already contains that key    */
    if (after_keys == sectors)
    {
      pf = new printf("VerifyPending(): lba 0x%012X contains " +
                      "%d 'after' 0x%02x keys. Keys are valid.");
      pf.add(lba);
      pf.add(after_keys);
      pf.add(after_key);
      log(pf.print());
    }

    /* If the key is the previous key, it means that the write did NOT */
    /* complete. We need to reset the key value:                       */
    else if (before_keys == sectors)
    {
      pf = new printf("VerifyPending(): lba 0x%012X contains " +
                      "%d 'before' 0x%02x keys. Keys are valid.");
      pf.add(lba);
      pf.add(before_keys);
      pf.add(before_key);
      log(pf.print());
      new_map.dv_set(lba, before_key);
    }

    else
    {
      pf = new printf("VerifyPending(): lba 0x%012X contains " +
                      "%d 'before' 0x%02x keys: and %d 'after' 0x%02x keys " +
                      "and %d 'other' keys. Block is invalid.");
      pf.add(lba);
      pf.add(before_keys);
      pf.add(before_key);
      pf.add(after_keys);
      pf.add(after_key);
      pf.add(sectors - before_keys - after_keys);

      /* For a quick fix, mark a hald-done block 'in error' until I write code to */
      /* handle in BadSector() the fact that we CAN have half blocks.             */
      new_map.dv_set(lba, DV_map.DV_ERROR);

      log(pf.print());
      //log("VerifyPending(): This block will be called out later by Data Validation.");
      log("VerifyPending(): This block will be bypassed from further validation ");
      log("                 until code is written to handle 'half good / half bad' blocks. ");
    }
  }



  /**
   * Check the pending blocks for any file that owns one of these pending blocks.
   */
  public static void checkFsdPendingBlocks(FileAnchor anchor,
                                           DV_map     before_map,
                                           DV_map     new_map)
  {
    /* If nothing's pending, that's just too easy: */
    if (before_map.pending_write_lbas.length == 0)
      return;

    int lba_index  = 0;
    int file_index = 0;
    Vector files   = anchor.getFileList();

    /* Scan through all files, looking for matching lbas: */
    for (file_index = 0;
        file_index < files.size() && lba_index < before_map.pending_write_lbas.length;
        file_index++)
    {
      FileEntry fe   = (FileEntry) files.elementAt(file_index);
      long start_lba = fe.getFileStartLba();
      long end_lba   = start_lba + fe.getCurrentSize();
      while (before_map.pending_write_lbas[lba_index] < fe.getFileStartLba())
        lba_index++;

      if (before_map.pending_write_lbas[lba_index] >= end_lba)
        continue ;

      /* I have now the file that this lba belongs to. Read it: */
      lba_index = checkKeyValueForFile(before_map, new_map, fe, lba_index);
    }

    if (blocks_read != before_map.pending_write_lbas.length)
      common.failure("Error: not all 'pending write' blocks were read and checked: " +
                     before_map.pending_write_lbas.length + "/" + blocks_read);
  }



  /**
   * Read a block from the file where the 'pending write' block belongs.
   */
  private static int blocks_read = 0;
  private static int checkKeyValueForFile(DV_map before_map, DV_map new_map,
                                          FileEntry fe,     int lba_index)
  {
    int  xfersize  = before_map.getBlockSize();
    long start_lba = fe.getFileStartLba();
    long end_lba   = start_lba + fe.getCurrentSize();

    /* Read all blocks for this file: */
    for (; lba_index < before_map.pending_write_lbas.length; lba_index++)
    {
      long check_lba = before_map.pending_write_lbas[lba_index];
      long file_lba  = check_lba - fe.getFileStartLba();

      /* If the next lba to be checked is outside of this file, stop: */
      if (file_lba >= fe.getCurrentSize())
        break;

      int before_key = before_map.dv_get(check_lba);

      /* If the before key was marked 'error' it means the block had */
      /* never been written and therefore should be set to zero:    */
      if (before_key == DV_map.DV_ERROR)
        before_key = 0;

      int after_key   = DV_map.dv_increment(before_key);
      blocks_read++;
      int before_keys = checkFsdKeys(fe, file_lba, xfersize, before_key);
      int after_keys  = checkFsdKeys(fe, file_lba, xfersize, after_key);
      int sectors     = xfersize / 512;

      /* Decide on the status of this block: */
      compareKeyCounts(new_map, before_key, before_keys,
                       after_key, after_keys, sectors, check_lba);
    }

    return lba_index;
  }

  /**
   * Open the file and then have the contents of the block checked.
   */
  private static int checkSdKeys(SD_entry sd, long lba, int xfersize, int expected_key)
  {
    long handle = Native.openFile(sd.lun);
    if (handle < 0)
      common.failure("Unable to open lun=" + sd.lun);
    File_handles.addHandle(handle, sd);

    int valid_keys = countKeys(handle, lba, xfersize, expected_key);
    if (valid_keys < 0)
      common.failure("VerifyPending(): Unable to read lba " + lba + "for lun=" + sd.lun);

    Native.closeFile(handle);
    File_handles.remove(handle);

    return valid_keys;
  }




  /**
   * Open the file and then have the contents of the block checked.
   */
  private static int checkFsdKeys(FileEntry fe, long lba, int xfersize, int expected_key)
  {
    long handle = Native.openFile(fe.getName());
    if (handle < 0)
      common.failure("Unable to open lun=" + fe.getName());
    File_handles.addHandle(handle, fe.getName());

    int valid_keys = countKeys(handle, lba, xfersize, expected_key);
    if (valid_keys < 0)
      common.failure("VerifyPending(): Unable to read lba " + lba + "for file=" + fe.getName());

    Native.closeFile(handle);
    File_handles.remove(handle);

    return valid_keys;
  }



  /**
   * Read a block and count the amount of valid keys in the sectors of that block
   */
  private static int countKeys(long handle, long lba, int xfersize, int expected_key)
  {
    int sectors = xfersize / 512;
    int valid_keys = 0;

    int[] data_array  = new int[ xfersize / 4];
    long  data_buffer = Native.allocBuffer(xfersize);

    if (Native.readFile(handle, lba, xfersize, data_buffer) != 0)
      return -1;

    Native.buffer_to_array(data_array, data_buffer, xfersize);

    for (int i = 0; i < sectors; i++)
    {
      int offset = (i*128);
      int current_key = data_array[offset+4] >> 24;

      if (current_key == expected_key)
        valid_keys++;
    }

    Native.freeBuffer(xfersize, data_buffer);

    return valid_keys;
  }


  private static void log(String txt)
  {
    String rd = "rd=" + SlaveWorker.work.work_rd_name;
    ErrorLog.sendMessageToLog(rd + ": " + txt);
  }

  private static void log(String format, Object ... args)
  {
    log(String.format(format, args));
  }

}



