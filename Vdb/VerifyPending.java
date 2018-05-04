package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import Utils.Format;
import Utils.printf;


/**
 * Verify the contents of those writes that were pending during the shutdown of
 * the run that created the journal file.
 * This is for both raw and file system functionality.
 *
 * Note 07/01/15:
 * Since this code currently is not Dedup aware shall I just postpone the
 * reading of these pending blocks until the normal journal recovery sequential
 * reads and THEN deal with them?
 * At that point, do not call the JNI DV read, but a normal read.
 * I still have the problem even then though that I can not use keys to validate
 * each sector of a block.
 *
 * Maybe call DV Jni code with an extra flag to NOT report a corruption and then
 * call it twice with the before and then the after key and then if there is an
 * error with BOTH call it a corruption?
 *
 * The issue then is: a 1024k write can that technically end up being split,
 * half AFTER, half BEFORE?
 * And, since I no longer have keys, can I even figure that out?
 *
 * Still thinking.
 */
public class VerifyPending
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Look through the recovered DV map and find out which blocks had a write
   * pending.
   */
  public static void findPendingBlocks(DV_map before_map, DV_map new_map, String type, String which)
  {
    Elapsed elapsed = new Elapsed("findPendingBlocks", 100*1000*1000);


    /* Create the pending list, later to be array: */
    ArrayList <Long> lba_list  = new ArrayList(1024);
    ArrayList <Byte> flag_list = new ArrayList(1024);
    int blksize                = new_map.getKeyBlockSize();
    boolean first              = true;
    for (long j = 0; j < new_map.key_blocks; j++)
    {
      long lba  = j * blksize;
      int  flag = before_map.dv_get_nolock(lba);

      elapsed.track();

      if (flag != 0)
      {
        if (first)
        {
          first = false;
          ErrorLog.plog("");
          ErrorLog.plog("Note that i/o still pending at the end of the previous "+
                        "run's NORMAL completion may be included here.");
          ErrorLog.plog("");
        }

        if (flag == DV_map.PENDING_KEY_0)
        {
          ErrorLog.plog("I/O pending for key block at lba 0x%08x flag 0x%02x key 0x%02x. "+
                        "This was the block's first write. Block marked 'unknown'.",
                        lba, flag, new_map.dv_get(lba));
          new_map.dv_set(lba, 0);
          continue;
        }

        ErrorLog.plog("I/O pending for key block at lba 0x%08x flag 0x%02x key 0x%02x",
                      lba, flag, new_map.dv_get(lba));

        lba_list.add(lba);
        flag_list.add((byte) flag);
        if (lba_list.size() > Integer.MAX_VALUE - 1)
          common.failure("Too many (%,d) pending key blocks", lba_list.size());

      }
    }

    ErrorLog.plog("%s=%s %,d pending writes found in this journal need to be checked.",
                  type, which, lba_list.size());
    if (which == null)
      common.failure("Unknown SD or FSD name");



    if (Validate.ignorePending())
    {
      before_map.pending_write_lbas  = new long[0];
      before_map.pending_write_flags = new byte[0];
      ErrorLog.plog("User opted to ignore all pending writes. Data contents set to 'unknown'");

      for (int i = 0; i < lba_list.size(); i++)
        new_map.dv_set(lba_list.get(i), 0x00);

      elapsed.end(5);
      return;
    }

    before_map.pending_write_lbas  = new long[lba_list.size()];
    before_map.pending_write_flags = new byte[lba_list.size()];

    for (int i = 0; i < lba_list.size(); i++)
    {
      before_map.pending_write_lbas  [ i ] = lba_list.get(i);
      before_map.pending_write_flags [ i ] = flag_list.get(i);
    }


    /* Translate this into a hash map: */
    before_map.pending_map = new HashMap(before_map.pending_write_lbas.length);
    for (int i = 0; i < before_map.pending_write_lbas.length; i++)
    {
      //common.ptod("pending_map: 0x%08x %02x", before_map.pending_write_lbas[i],
      //                                        before_map.pending_write_flags[i]);
      before_map.pending_map.put(before_map.pending_write_lbas[i],
                                 before_map.pending_write_flags[i]);
    }

    elapsed.end(5);
  }



  /**
   * Check the pending blocks for any file that owns one of these pending blocks.
   */
  public static void checkFsdPendingBlocks(FileAnchor anchor,
                                           DV_map     before_map)
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

      lba_index = checkKeyValuesForFile(before_map, fe, lba_index);
    }
  }



  /**
   * We now have a file that has pending key blocks. Add all it's keyblocks to a
   * map for this file.
   *
   * I could have stored the hashmap in the FileEntry, but I did not want to add
   * an other 8 bytes minimum to the size of FileEntry.
   */
  private static int checkKeyValuesForFile(DV_map    before_map,
                                           FileEntry fe,
                                           int       lba_index)
  {
    /* if this is the first pending key block create the anchor map and array: */
    FileAnchor anchor = fe.getAnchor();
    if (anchor.pending_file_lba_map == null)
    {
      anchor.pending_file_lba_map = new HashMap(8);
      anchor.pending_files    = new ArrayList(8);
    }

    /* This is a new file: create the HashMap and store it in the anchor: */
    HashMap <Long, Long> pending_lbas = new HashMap(8);
    if (anchor.pending_file_lba_map.put(fe, pending_lbas) != null)
      common.failure("Duplicate pending file name: " + fe.getShortName());
    anchor.pending_files.add(fe);


    /* Get all pending key blocks for this file: */
    for (; lba_index < before_map.pending_write_lbas.length; lba_index++)
    {
      long key_lba   = before_map.pending_write_lbas[lba_index];
      long file_lba  = key_lba - fe.getFileStartLba();

      /* If the next lba to be checked is outside of this file, stop: */
      if (file_lba >= fe.getCurrentSize())
        break;

      /* Add the pending write for this key block to this file: */
      if (pending_lbas.put(file_lba, file_lba) != null)
        common.failure("Duplicate pending lba found: " + file_lba);

      ErrorLog.plog("Pending write found to file %s, file lba 0x%08x", fe.getFullName(), file_lba);
    }

    return lba_index;
  }
}



