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

import java.io.*;
import java.util.*;
import Utils.*;


/**
 * This class handles Data Validation.
 */
public class DV_map
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String map_name;
  Jnl_entry journal = null;    /* Possible Journal belonging to this map      */
  private int  map_blksize;            /* Blocksize covered for each entry            */
  int  map_status;             /* 0: empty                                    */
                               /* 1: recovered from journal                   */
  int  map_entries;            /* Number of entries in map                    */
  byte byte_map[];             /* map: bit0:   read or write active           */
                               /*      bit1-7: values 0-126                   */
                               /*              0:     never written           */
                               /*              1-126: key value               */
                               /*                     Rollover to 1 allowed   */
                               /* Value 127 means that block is in error      */

  int  map_busy = 0;           /* # of consecutive 'busy' returns             */

  public static final int DV_ERROR = 127;

  long[] timestamp_map       = null;
  long[] pending_write_lbas  = null;
  FileAnchor recovery_anchor = null;

  static Object print_lock = new Object();

  static String pattern_dir      = null;
  static double compression_rate  = -1;
  static long   compression_seed  = 0;
  private static Vector patterns = new Vector(126+1);

  static boolean dv_headers_printed = false;

  public static long[] key_reads  = new long[256];
  public static long[] key_writes = new long[256];


  /* This table gives you the randomizer value needed for compression. */
  /* For instance, if you want a 7.4% compression, you need to use the */
  /* value '4' when storing zeroes in the complete random buffer.      */
  /* see createCompressionBuffer() below.                              */
  static double[] randomizer_value = new double[]
  {
    0.2,  2.2,  4.4,  5.6,  7.4,  8.8, 10.2, 10.9, 13.0, 16.0, 15.9, 15.7,
    18.9, 19.7, 21.0, 22.4, 24.0, 25.7, 25.4, 27.7, 28.6, 29.7, 32.1, 30.7,
    33.4, 35.2, 36.8, 37.1, 37.5, 38.9, 41.0, 41.3, 42.8, 43.7, 44.2, 46.0,
    47.1, 46.8, 49.7, 49.2, 50.8, 50.9, 53.1, 53.2, 54.1, 57.4, 57.9, 57.3,
    59.8, 60.3, 61.2, 61.3, 62.3, 63.2, 64.8, 65.2, 66.8, 67.7, 68.4, 68.7,
    70.1, 71.3, 72.0, 72.7, 72.1, 74.5, 75.4, 75.8, 76.6, 78.4, 78.0, 79.5,
    79.7, 81.1, 81.7, 83.5, 83.3, 83.9, 85.3, 86.1, 86.4, 87.5, 88.5, 89.0,
    89.5, 90.9, 90.7, 91.8, 93.0, 93.8, 94.3, 94.7, 96.0, 96.0, 96.9, 97.6,
    98.4, 98.8, 99.2, 99.8, 100.0
  };


  private static HashMap all_maps = new HashMap(8);


  /**
   * Allocate Data Validation for 'blocks' records of 'blksize' length records
   */
  public DV_map(String name, long entries, int xfersize)
  {
    map_name = name;
    if (SlaveJvm.isWdWorkload() && entries < 3000)
      common.failure("The amount of data blocks in a lun must " +
                     "be at least 3000 times the data transfersize when " +
                     "using Data Validation. Current amount of blocks: " + entries);

    map_blksize = xfersize;
    map_entries = (int) entries;

    byte_map    = new byte[(map_entries + 3) & ~3];

    /* Create timestamp map? */
    if (Validate.isStoreTime())
    {
      common.ptod("Data Validation. Allocating timestamp map for SD; " +
                  Format.f("%.3f MB", (byte_map.length * 8. / 1024. / 1024.)));
      timestamp_map = new long[ byte_map.length ];
    }

    for (int i = 0; i < map_entries; i++)
      byte_map[ i ] = 0;

    common.ptod("Allocating Data Validation map: " + byte_map.length +
                " one-byte entries for each " + xfersize + "-byte block.");
  }


  /**
   * Create a new map, or reuse an existing one.
   */

  public static DV_map allocateMap(String map_name, long lun_size, long xfersize)
  {
    DV_map old_map = (DV_map) all_maps.get(map_name);
    if (old_map != null)
      return old_map;

    //common.ptod("lun_size: " + lun_size);
    //common.ptod("xfersize: " + xfersize);
    DV_map new_map = new DV_map(map_name, lun_size / xfersize, (int) xfersize);
    all_maps.put(map_name, new_map);

    new_map.map_name = map_name;

    return new_map;
  }



  public static DV_map findMap(String map_name)
  {
    DV_map old_map = (DV_map) all_maps.get(map_name);
    if (old_map != null)
      return old_map;

    //common.ptod("map_name: " + map_name + "<<<");
    //String[] names = (String[]) all_maps.keySet().toArray(new String[0]);
    //for (int i = 0; i < names.length; i++)
    //  common.ptod("names: " + names[i] + "<<<");

    return null;
  }

  public static DV_map[] getAllMaps()
  {
    return(DV_map[]) all_maps.values().toArray(new DV_map[0]);
  }

  public static void removeMap(String map_name)
  {
    DV_map old_map = (DV_map) all_maps.get(map_name);
    if (old_map == null)
      common.failure("Trying to remove unknown map");

    all_maps.remove(map_name);
  }

  public void setBlockSize(int blk)
  {
    map_blksize = blk;
  }
  public int getBlockSize()
  {
    return map_blksize;
  }


  /**
    * Mark a block of data 'in use'.
    *<pre>
    * The randomizer used to generate the random seek addresses for the Workload
    * Generator can create duplicate seek addresses of i/o's that are already
    * in progress.
    * Since it is not known which i/o will complete first it is necessary to
    * prevent this from happening. A block of data therefore is marked 'busy'
    * at the time of the generation of the i/o. If the block is already busy the
    * workload Generator is instructed to bypass this randomly generated seek
    * address, and continue to the next randomly generated block.
    *
    * Warning: if the total size of the WD and SD's defined is very small, it
    * can and will happen that all the blocks are marked busy (the i/o's are
    * not completed yet) and that this method described here is forcing the
    * next random block to be used which in turn is also already marked busy.
    * This will cause excessive waste of cpu cycles.
    *
    * A Data Validation table entry is one byte per block:
    * bit0:   read or write active
    * bit1-7: values 0-126
    *         0:     block has never been written
    *         1-126: key value: Which data pattern is in block.
    *                Rollover to 1 allowed
    * Value 127 means that block is in error and may NOT be read or written again
    *
    * Data Validation maps are created on an 'SD' basis. Once a DV_map is
    * created it will continue to be used for all later Run definitions (RDs),
    * unless the xfersize changes, then the old map is thrown away and a new one
    * started.
    *
    * The data transfer sizes for all operations against an SD that has data
    * validation activated must be identical, also across RDs.
    */
  public synchronized int dv_set_busy(long lba)
  {
    byte entry;

    /* Value 127 means block in error: */
    entry = byte_map[ (int) (lba / map_blksize) ];
    if ( (entry & 0x7f) == DV_map.DV_ERROR)
    {
      map_busy = 0;
      //common.ptod("DV_map.dv_set_busy(): LBA already marked in error: " + lba);
      return -1;
    }

    /* Test busy flag. If not busy, set and return key: */
    if ( (entry & 0x80) == 0)
    {
      entry |= 0x80;
      byte_map [ (int) (lba / map_blksize) ] = entry;
      map_busy = 0;
      return entry & 0x7f;
    }

    /* Aborting after n busy attempts does not appear to be a decent solution */
    /* so let's try to sleep for a while to let some ios complete:            */
    /* Should not have an impact on performance too much!                     */
    if (++map_busy > 100)
    {
      //common.ptod("sleeping map_busy: " + map_busy);
      common.sleep_some(1);
    }


    /* To warn for any loops, tell them about returning too many busies: */
    //if (++map_busy > 2000)
    //  common.failure("Too many 'busy' statuses reported. " +
    //                 "Likely caused because the target file/volume size is so " +
    //                 "small that ALL records are currently in use");

    /* Block was busy. I/O needs to be skipped: */
    return -1;
  }

  /**
   * Mark all map entries not busy.
   * At the end of a run some entries can be marked busy. Since the i/o will
   * not be done and therefore will not reset it, we will do it ourselves.
   * (Caused by i/o's that are still in the fifos when the run completes)
   */
  public static void dv_set_all_unbusy(Vector sd_list)
  {
    if (!Validate.isValidate())
      return;

    /* WD entry, one for each SD: */
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);
      DV_map dv = DV_map.findMap(sd.sd_name);
      if (dv != null)
      {
        for (int j = 0; j < dv.map_entries; j++)
          dv.byte_map[ j ] &= 0x7f;
      }
    }
  }

  /**
   * This method increments a data pattern key by one. The value of the key
   * will roll over from
   * the maximum value of 126 back to one. (Remember, value '0' means the
   * block has never been written).
   */
  public static int dv_increment(int key)
  {
    return( (key == 126) ? 1 : ++key);
  }

  /**
   * Decrement key
   * Used during recovery where we have the BEFORE image of an inflight write,
   * but the key does not match and we need to try to compare it with the
   * previous key.
   * If the key equals ONE, we don't know if the previous key was ZERO or 126!!
   * In that case we return 0 and just skip validation.
   */
  public static int dv_decrement(int key)
  {
    if (key == 1)
      return 0;
    if (key == 0)
      common.failure("Trying to decrement a key with a value of zero");

    return key - 1;
  }


  /**
   * Get entry from DV map
   */
  public int dv_get(long lba)
  {
    //common.ptod("byte_map: %8d %08x %d", lba, lba, byte_map.length);
    int key = byte_map [ (int) (lba / map_blksize) ];

    //common.ptod("dv_get. lba: %08x key: 0x%02x %d %s", lba, key, map_blksize,map_name);
    return key;
  }


  /**
   * Set entry in DV map
   */
  public void dv_set(long lba, int key)
  {
    //common.where(8);
    //common.ptod("dv_set. lba: %08x key: 0x%02x %d %s", lba, key, map_blksize, map_name);
    //if (key == 127)
    //  common.where(8);
    byte_map [ (int) (lba / map_blksize) ] = (byte) (key & 0x7f);
  }


  public void eraseMap()
  {
    common.ptod("Erasing Data Validation map: " + map_name);
    for (int i = 0; i < byte_map.length; i++)
      byte_map[i] = 0;
  }

  public long getLastTimestamp(long lba)
  {
    if (timestamp_map == null)
      return 0;
    return timestamp_map [ (int) (lba / map_blksize) ];
  }

  /**
   * Mark block of data 'not in use'.
   */
  public synchronized void dv_set_unbusy(long lba, int key, boolean read) throws Exception
  {
    /* get current entry: */
    byte entry = byte_map[ (int) (lba / map_blksize) ];

    /* This should be the next key: */
    //byte next  = (byte) dv_increment(entry & 0x7f);

    /* Make sure that it is indeed busy: */
    if ( (entry & 0x80) == 0)
    {
      /* If this block is marked DV_ERROR we just set it to unbusy after */
      /* reporting the error, so that is accepted.                       */
      if ( (entry & 0x7f) == DV_ERROR)
        return;

      throw new Exception("dv_set_unbusy(): entry not busy: " + entry + " key: " + key + " lba: " + lba);
    }

    /* This check was only possible if a write had occurred.                     */
    /* If this was only a read, then there is no need to increment the key.      */
    /* Since we don't know if the original request was a read or write           */
    /* we do not have a choice to replace the key, whether it has changed or not */

    // Compare what they want it to be and what is should be:
    //if (next != key)
    //  common.failure("dv_set_unbusy(): request to set new key to " + key +
    //                 " but we expect it to be set at " + next);

    /* Update the key in the map and mark it not busy: */
    int tmp = byte_map [ (int) (lba / map_blksize) ] = (byte) (key & 0x7f);

    //common.ptod(this + " tmp: " + tmp + " " + (lba / map_blksize));

    return;
  }

  /**
   * Store timestamp of last successful read/write.
   * No lock is necessary, since we use a full 8 bytes and the block is still busy.
   *
   * Positive timestamp: write; negative: read
   */
  public void save_timestamp(long lba, boolean read_flag)
  {
    /* Store timestamp of last successful i/o if needed: */
    if (timestamp_map != null)
    {
      timestamp_map[ (int) (lba / map_blksize) ] = new Date().getTime();
      if (read_flag)
        timestamp_map[ (int) (lba / map_blksize) ] *= -1;
    }
  }


  /**
   * A new I/O Cmd_entry has been generated, see what needs to be done.
   * A new i/o is marked busy, if it is already busy, tell the caller that
   * this block must be skipped and a new i/o generated.
   * If this i/o is a write of a block that has been written at least one time
   * change the i/o into a read so that the data in the previous written block
   * can be validated. The write will then be done AFTER the data has been
   * validated.
   */
  public boolean dv_new_cmd(Cmd_entry cmd)
  {

    /* Set the block in dv busy. If already busy, i/o needs to be skipped: */
    cmd.dv_key = dv_set_busy(cmd.cmd_lba);
    if (cmd.dv_key == -1)
      return false;

    /* If this is a read, we can return */
    if (cmd.cmd_read_flag)
      return true;

    /* It is a write. If block was never written, set new key and return: */
    if (cmd.dv_key == 0)
    {
      cmd.dv_key = 1;
      return true;
    }

    /* There was a need to do Data Validation without re-reading before   */
    /* each new write. The added reads changed a workload so much that an */
    /* existing error never showed up. Beware that if we do that we will  */
    /* miss any possible lost writes!                                     */
    if (Validate.isNoPreRead())
      return true;

    /* We need to change this write to a read. IO completion will then */
    /* request a write once the read is done:                          */
    cmd.cmd_read_flag = true;
    cmd.dv_pre_read   = true;


    return true;
  }


  /**
   * Insert a special RD in the beginning that takes care of all journal
   * recoveries and the re-reading of all data:
   */
  public static void setupSDJournalRecoveryRun()
  {
    /* WD entry, one for each SD: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      WD_entry wd = new WD_entry();
      Vdbmain.wd_list.addElement(wd);

      wd.wd_name       = Jnl_entry.RECOVERY_RUN_NAME + "_" + sd.sd_name;
      wd.wd_sd_name    = sd.sd_name;
      wd.sd_names      = new String[] { sd.sd_name};
      wd.setSkew(0);
      wd.lowrange      = -1;
      wd.highrange     = -1;
      wd.seekpct       = Jnl_entry.RECOVERY_READ; // Sequential until EOF */
      wd.rhpct         = 0;
      wd.whpct         = 0;
      wd.readpct       = 100;
      wd.xfersize      = 513; // will be replaced in slave (int) sd.max_xfersize;
    }

    /* Allocate RD_entry and insert it at the front of the RD list: */
    RD_entry rd = new RD_entry();
    Vdbmain.rd_list.insertElementAt(rd, 0);

    rd.rd_name = Jnl_entry.RECOVERY_RUN_NAME;
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.distribution = 2;
    rd.iorate     = RD_entry.MAX_RATE;
    rd.iorate_req = RD_entry.MAX_RATE;

    rd.wd_names = new String[ 1 ];
    rd.wd_names[ 0 ] = Jnl_entry.RECOVERY_RUN_NAME + "*";
  }


  /*
fwd=recover_fsds,fsd=*,
operation=read,
fileselect=sequential,
fileio=sequential,
threads=1,    will be higher!
xfersize=64k
  */

  public static void setupFsdJournalRecoveryRun()
  {
    FwdEntry fwd = new FwdEntry();
    FwdEntry.getFwdList().add(fwd);
    fwd.fwd_name      = Jnl_entry.RECOVERY_RUN_NAME;
    fwd.fsd_names     = new String[] { "*"};
    fwd.setOperation(Operations.READ);
    fwd.sequential_io = true;
    fwd.select_random = false;
    fwd.xfersizes     = new double[] { 65536 };
    fwd.threads       = 8;

    /* Overrides: */
    if (FwdEntry.recovery_fwd != null)
    {
      fwd.xfersizes = FwdEntry.recovery_fwd.xfersizes;
      fwd.threads   = FwdEntry.recovery_fwd.threads;
    }

    /* Allocate RD_entry and insert it at the front of the RD list: */
    RD_entry rd = new RD_entry();
    Vdbmain.rd_list.insertElementAt(rd, 0);

    rd.rd_name      = Jnl_entry.RECOVERY_RUN_NAME;
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.distribution = 2;
    rd.fwd_rate     = RD_entry.MAX_RATE;

    if (RD_entry.recovery_rd != null)
    {
      rd.setInterval(RD_entry.recovery_rd.getInterval());
      rd.fwd_rate = RD_entry.recovery_rd.fwd_rate;
    }

    rd.fwd_names = new String[ ] { Jnl_entry.RECOVERY_RUN_NAME} ;
  }


  /**
   * Allocate a DV map for each SD.
   *
   * Maps only needed when DV is active; maps only allocated once, unless the
   * xfersize for an SD changes. In that case, old map is deleted and new one
   * created.
   *
   * A map does already exist after journal recovery.
   */
  public static void allocateSDMapsIfNeeded(Vector sd_list)
  {
    if (!Validate.isValidate())
      return;


    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);

      /* Do we already have a map for this SD? */
      DV_map map = DV_map.findMap(sd.sd_name);
      if (map != null)
      {
        /* If we reuse a map, the blocksize needs to be the same: */
        if (map.map_blksize != sd.wg_for_sd.xfersize)
        {
          common.ptod("");
          common.ptod(sd.sd_name + ": " + Format.f("xfersize from previous run: %7d", map.map_blksize));
          common.ptod(sd.sd_name + ": " + Format.f("xfersize for this run:      %7d", sd.wg_for_sd.xfersize));
          common.ptod("Data validation xfersize changed. Data validation map for lun will be cleared");

          if (Validate.isJournaling() &&
              !SlaveWorker.work.work_rd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
            common.failure("Data Validation with journaling requires that data transfer sizes used for an SD are identical");

          /* Delete the old map: */
          DV_map.removeMap(sd.sd_name);
          map = null;
          System.gc();
        }
      }


      /* If we don't have a map, allocate: */
      if (map == null)
      {
        map = DV_map.allocateMap(sd.sd_name, sd.end_lba, sd.wg_for_sd.xfersize);

        if (Validate.isJournaling())
        {
          if (map.journal == null)
          {
            map.journal = new Jnl_entry(sd);
            map.journal.storeMap(map);
          }
        }
      }
    }
  }


  /**
   * Create data patterns for each of the 126 keys
   * pattern[key 0] will always serve as the default
   *
   * 127 patterns are created, but normally only one is needed. Only for DV
   * will we need more patterns.
   *
   * Currently only pattern[0] is used for compression.
   *
   * Note: there should be only 126 patterns, since that is the max amount
   * of keys used by DV?
   */
  private static boolean created = false;
  public static void create_patterns(int xfersize)
  {
    pattern_dir = SlaveWorker.work.pattern_dir;
    if (created)
      return;

    created = true;

    int pattern_buffer[];
    int patternsfound = 0;

    /* Allocate default pattern buffer and store 0,1,2,3,4 etc: */
    patterns.removeAllElements();
    pattern_buffer = new int[xfersize / 4];
    for (int i = 0; i < xfersize / 4; i++)
      pattern_buffer[i] = i;

    /* If requested, override default pattern from pattern directory: */
    if (pattern_dir != null)
    {
      int ret[] = read_and_store_pattern("default");
      if (ret != null)
      {
        pattern_buffer = ret;
        patternsfound++;
      }
    }

    /* If needed, override default with a compression pattern: */
    if (Validate.getCompression() >= 0)
    {
      if (xfersize == 0)
        common.failure("Invalid xfersize for data pattern");

      pattern_buffer = new int[xfersize / 4];
      createCompressionPattern(pattern_buffer, Validate.getCompression(),
                               Validate.getCompSeed());

      /* Use the compression pattern for each DV key */
      for (int i = 0; i < 127; i++)
        patterns.addElement(pattern_buffer);
      patternsfound = 127;

      /* Override pattern[1] if we do dedup: */
      if (Validate.getDedupRate() > 0)
      {
        pattern_buffer = new int[xfersize / 4];
        createCompressionPattern(pattern_buffer, Validate.getCompression(),
                                 Validate.getDedupSets());
        patterns.set(1, pattern_buffer);
      }
    }

    else
    {
      /* Store this default pattern for each key: */
      for (int i = 0; i < 127; i++)
        patterns.addElement(pattern_buffer);
    }

    /* Now read the key specific patterns from disk if requested and */
    /* override what we just did:                                    */
    if (pattern_dir != null)
    {
      for (int i = 1; i < 127; i++)
      {
        int ret[] = read_and_store_pattern("pattern" + i);
        if (ret != null)
        {
          patterns.set(i, ret);
          patternsfound++;
        }
      }
    }


    /* Copy these patterns to JNI (though usually only one will be used): */
    for (int i = 0; i < 127; i++)
    {
      int[] buf = (int[]) patterns.elementAt(i);
      Native.store_pattern((int[]) patterns.elementAt(i), i);

      /* If we did not get any patterns, store only the standard default: */
      if (patternsfound == 0)
        break;
    }

    if (pattern_dir != null && patternsfound == 0)
      common.failure("No patterns found in pattern directory '" + pattern_dir + "'");

  }


  public static int[] get_pattern(int key)
  {
    return(int[]) patterns.elementAt(key);
  }


  /**
   * Read pattern from pattern file and translate it to int array
   */
  private static int[] read_and_store_pattern(String fname)
  {
    long max_size = SlaveWorker.work.maximum_xfersize;

    /* if the file does not exist, that's easy: */
    File fptr = new File(pattern_dir, fname);
    if (!fptr.exists())
      return null;

    int pattern[] = new int[(int) max_size / 4];
    int buffer[]  = new int[(int) max_size];
    int byteindex = 0;

    try
    {
      FileInputStream file_in = new FileInputStream(fptr);
      DataInputStream data_in = new DataInputStream(file_in);

      while (byteindex < max_size)
      {
        try
        {
          int abyte = data_in.readUnsignedByte();
          buffer[byteindex++] =  abyte;
        }
        catch (IOException e)
        {
          break;
        }
      }

      if (byteindex == 0)
        common.failure("Pattern file empty: " + fptr.getAbsolutePath());
      data_in.close();

      /* Must be a 4 byte boundary; truncate (likely cr/lf): */
      if (byteindex % 4 != 0)
      {
        common.ptod("Data pattern length for file '" + fptr.getAbsolutePath() +
                    "' must be a multiple of 4. Input length of " +
                    byteindex + " truncated. Possible cr/lf? ");
        byteindex -= byteindex % 4;
      }

      /* We now have our bytes; translate them to ints: */
      for (int i = 0; i < SlaveWorker.work.maximum_xfersize ; i += 4)
      {
        long word = 0;
        word += buffer[ (i+0) % byteindex ] << 24;
        //common.ptod(Format.f("word1: %08x", word));
        word += buffer[ (i+1) % byteindex ] << 16;
        //common.ptod(Format.f("word2: %08x", word));
        word += buffer[ (i+2) % byteindex ] <<  8;
        //common.ptod(Format.f("word3: %08x", word));
        word += buffer[ (i+3) % byteindex ];
        //common.ptod(Format.f("word4: %08x", word));
        pattern[ i / 4 ] = (int) word;
        //common.ptod(Format.f("buffer: %02x", buffer[ (i + 0) % byteindex ]));
        //common.ptod(Format.f("buffer: %02x", buffer[ (i + 1) % byteindex ]));
        //common.ptod(Format.f("buffer: %02x", buffer[ (i + 2) % byteindex ]));
        //common.ptod(Format.f("buffer: %02x", buffer[ (i + 3) % byteindex ]));
        //common.ptod("byteindex: " + byteindex);
      }

      common.ptod("Successfully loaded data pattern from " + fptr.getAbsolutePath());
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    return pattern;
  }


  /**
   * Create unique character based name for Data Validation for this SD and Lun
   * Accumulate all characters of the lun and sd name, and then add to that
   * the current microsecond based timestamp
   */
  public static void create_unique_dv_names(Vector sd_list)
  {

    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);

      int checksum = 0;

      for (int j = 0; j < sd.lun.length(); j++)
        checksum += sd.lun.charAt(j);

      for (int j = 0; j < sd.sd_name.length(); j++)
        checksum += sd.sd_name.charAt(j);

      checksum += (int) Native.get_simple_tod();

      checksum &= 0xffffff;

      sd.unique_dv_name = Format.f("sd%06x", checksum);

      ErrorLog.printMessageOnLog("Unique Data Validation name '" + sd.unique_dv_name +
                                 "' created for " + sd.sd_name + " (" + sd.lun + ")");
    }
  }


  public static void old_main(String args[])
  {
    //String num = "03e04b8641b4c8";    0x03e0d54fc49898
    String num = args[0];
    common.ptod("num: " + num);

    int ck = 0;
    for (int i = 0; i < num.length(); i+=2)
    {
      long tmp = Long.parseLong(num.substring(i, i+2), 16);
      //common.ptod("tmp: " + tmp);
      ck += tmp;
    }
    common.ptod(Format.f("Checksum: %x", ck & 0xff));
  }


  /**
   * Create a data pattern that results in a proper compression rate.
   *
   * input:  - the compression: 100 bytes in, 'n' bytes out
   */
  private static void createCompressionPattern(int[] pattern_buffer,
                                               double comp,
                                               long   seed)
  {
    Random compression_random = new Random(seed);


    /* Determine which randomizer limit to use: */
    double limit = 0;
    for (int i = 0; i < randomizer_value.length; i++)
    {
      if ( randomizer_value[i] > comp)
        break;
      limit = i;
    }


    /* Fill buffer with random numbers: */
    for (int j = 0; j < pattern_buffer.length; j++)
      pattern_buffer[j] = (int) (compression_random.nextDouble() * Integer.MAX_VALUE);

    /* Now replace 'comp'% of buffer with zeros: */
    for (int j = 0; j < pattern_buffer.length; j++)
    {
      if (compression_random.nextDouble() > limit / 100.)
        pattern_buffer[j] = 0;
    }
  }


  /**
   * Check to see if any part of this file has ever been written.
   */
  public boolean anyValidBlocks(long logical_lba, long size)
  {
    long lba = logical_lba;
    long max = lba + size;
    while (lba < max)
    {
      if (dv_get(lba) != 0)
        return true;
      lba += map_blksize;
    }

    return false;
  }


  /**
   * Check to see if there are any bad blocks on the file.
   */
  public boolean anyBadBlocks(long logical_lba, long size)
  {
    long lba = logical_lba;
    long max = lba + size;
    while (lba < max)
    {
      if (dv_get(lba) == DV_map.DV_ERROR)
        return true;
      lba += map_blksize;
    }

    return false;
  }



  public static void printCounters()
  {
    if (!Validate.isValidate())
      return;

    Vector txt = new Vector(16, 0);
    int validation_reads = 0;
    txt.add("Data Validation counters: ");
    for (int i = 0; i < key_reads.length; i++)
    {
      if (key_reads[i] + key_writes[i] > 0)
      {
        if (i == 0)
          txt.add(Format.f("Reads of blocks that were never written: %6d ",
                           key_reads[i]));
        else
        {
          txt.add(Format.f("Key %3d: ", i) +
                  Format.f("reads: %8d ", key_reads[i]) +
                  Format.f("writes: %8d ", key_writes[i]));
          validation_reads += key_reads[i];
        }

        /* Clear counters for next run: */
        key_reads[i] = key_writes[i] = 0;
      }
    }

    //SlaveJvm.sendMessageToConsole(txt);
    for (int i = 0; i < txt.size(); i++)
      common.ptod(txt.elementAt(i));

    SlaveJvm.sendMessageToConsole("Total amount of blocks read and validated: " + validation_reads);

    if (common.get_debug(common.ACCEPT_DV_NOREADS))
      return;

    /* Tape testing is always sequential, so we can't read during write: */
    if (SlaveWorker.work.tape_testing || common.get_debug(common.ACCEPT_DV_NOREADS))
      return;

    if (validation_reads == 0)
    {
      if (SlaveWorker.work.work_rd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
        return;
      if (SlaveWorker.work.format_run)
        return;

      txt.removeAllElements();
      txt.add("No read validations done during a Data Validation run.");
      txt.add("This means very likely that your run was not long enough to");
      txt.add("access the same data block twice. ");
      txt.add("There are several solutions to this: ");
      txt.add("- increase elapsed time. ");
      txt.add("- use larger xfersize. ");
      txt.add("- use only a subset of your lun by using the 'sd=...,size=' ");
      txt.add("  parameter or the 'sd=...,range=' parameter.");
      SlaveJvm.sendMessageToConsole(txt);
      common.failure("No read validations done during a Data Validation run.");
    }
  }

  /**
   * When there is at least one DV or i/o error, terminate after 'xx' seconds
   */
  public static void checkDVStatus()
  {
    if (ErrorLog.getErrorCount() == 0)
      return;

    int maximum_dv_wait = Validate.getMaxErrorWait();
    if (maximum_dv_wait == 0)
      return;

    long tod = System.currentTimeMillis();
    if (tod < ErrorLog.getLastErrorTod() + maximum_dv_wait * 1000)
      return;

    common.ptod("*");
    common.ptod("It has been more than " + maximum_dv_wait +
                " seconds since the last Data Validation or I/O error.");
    common.ptod("Total Data Validation or I/O error count: " + ErrorLog.getErrorCount());
    common.ptod("*");
    common.failure("Vdbench terminating due to Data Validation or I/O errors. See errorlog.html.");
  }
}

