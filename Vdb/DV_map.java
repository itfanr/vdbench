package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.*;


/**
 * This class handles Data Validation information for one SD or ONE FSD.
 */
public class DV_map
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String map_name;
  Jnl_entry journal = null;    /* Possible Journal belonging to this map      */
  private int  key_blksize;            /* Blocksize covered for each entry            */
  int   map_status;            /* 0: empty                                    */
                               /* 1: recovered from journal                   */
  long  key_blocks;            /* Number of keyblocks in map                  */
  long  map_length;            /* Same, rounded to 4                          */

  long  blocks_in_error = 0;   /* ++ done when an entry is set to DV_ERROR.   */
                               /* Completely regenerated during setAllUnBusy  */
  long  blocks_busy     = 0;
  long  blocks_known    = 0;

  /* For true Data Validation we need one byte of memory per block.           */
  /* For dedup we only need one bit to handle flipflop.                       */
  /* Theoretically it would be possible to not have a flipflop map at all     */
  /* when flipflop is not requested. However, let's not do this because of    */
  /* the possibly huge java heap differences when someone switches.           */
  private DedupBitMap flipflop_bitmap = null;
  private MapFile[] byte_maps; /* map: bit0:   read or write active           */
                               /*      bit1-7: values 0-126                   */
                               /*              0:     never written           */
                               /*              1-126: key value               */
                               /*                     Rollover to 1 allowed   */
                               /* Value 127 means that block is in error      */



  int  map_busy = 0;           /* # of consecutive 'busy' returns             */

  public static final int DV_ERROR      = 0x7f; // 127;

  /* These flags for journal recovery. Busy flag not needed, so 0x80 is OK: */
  public static final int PENDING_KEY_0          = 0x80;
  public static final int PENDING_WRITE          = 0x81;
  public static final int PENDING_KEY_ROLL       = 0x82; /* (rollover from 127 to 1) */
  public static final int PENDING_KEY_ROLL_DEDUP = 0x83; /* (rollover from   2 to 1) */
  public static final int PENDING_KEY_REREAD     = 0x84; /* (key reset to 'before' ) */

  private Timestamp timestamp_map = null;

  long[] pending_write_lbas  = null;
  byte[] pending_write_flags = null;
  HashMap <Long, Byte> pending_map = null;

  FileAnchor recovery_anchor = null;

  private int bad_sectors_found = 0;

  private Dedup dedup = null;


  public HashMap <Long, BadDataBlock> bad_data_map = null;

  static Object print_lock = new Object();

  static long   compression_seed  = 0;

  static boolean dv_headers_printed = false;

  public static long[] key_reads  = new long[256];
  public static long[] key_writes = new long[256];

  private static HashMap <String, DV_map> all_maps = new HashMap(8);


  /**
   * Allocate Data Validation for 'blocks' records of 'blksize' length records
   */
  public DV_map(String jnl_dir_name, String name, long entries, int xfersize)
  {
    map_name     = name;
    int fifosize = Fifo.getSizeNeeded(0);

    // removed. This should be caught by calculateContext()
    // if (SlaveJvm.isWdWorkload())
    // {
    //   if (entries < fifosize * 1.5)
    //     common.failure("The amount of data blocks in a lun must " +
    //                    "be at least %d times the data transfersize when " +
    //                    "using Data Validation or Dedup. Current amount of blocks: %d",
    //                    (int) (fifosize * 1.5), entries);
    // }

    //if (entries > Integer.MAX_VALUE)
    //  common.failure("Data Validation supports no more than %,d blocks. You requested %,d",
    //                 Integer.MAX_VALUE, entries);

    key_blksize = xfersize;
    key_blocks = entries;
    map_length  = (key_blocks + 3) & ~3;

    //common.ptod("map_blksize: %,16d", map_blksize);
    //common.ptod("map_entries: %,16d", map_entries);
    //common.ptod("map_length : %,16d", map_length );

    /* This combination is required to prevent a unique */
    /* mmap file name from being created:               */
    if (Validate.isContinueOldMap() && jnl_dir_name == null)
      common.failure("'validate=continue_old_map' also requires 'journal=xxx");

    /* For true DV we need one byte per block: */
    if (Validate.isRealValidate())
    {
      if (Validate.isContinueOldMap())
        byte_maps = MapFile.openOldFile(jnl_dir_name, map_name, map_length);
      else
        byte_maps = MapFile.createNewFile(jnl_dir_name, map_name, map_length);
    }

    /* For Dedup however we only need one bit per block:                     */
    /* (Even though flipflop may be turned off, still allocated one, this to */
    /* avoid java heap issues when the user turns it on)                     */
    /* Also keep in mind that flipflop is REQUIRED with real DV!             */
    else
      flipflop_bitmap = new DedupBitMap().createMapForFlipFlop(dedup, map_length, map_name);


    /* Create timestamp map? */
    if (Validate.isStoreTime())
      timestamp_map = new Timestamp(map_length);

    if (Validate.isRealValidate())
      ErrorLog.plog("Allocating Data Validation map: %,d one-byte entries for each %,d-byte block.",
                    map_length, xfersize);
  }


  public void setDedup(Dedup ded)
  {
    dedup = ded;
  }
  public Dedup getDedup()
  {
    return dedup;
  }

  /**
   * This won't work on Windows. MappedByteBuffer does not have a proper
   * API to have the file unmapped. It therefore stays open until the JVM
   * terminates.
   */
  public void deleteByteMapFile()
  {
    if (byte_maps != null)
    {
      for (MapFile byte_map : byte_maps)
      {
        byte_map.closeMapFile();
        boolean rc = new File(byte_map.getFilename()).delete();
      }
      byte_maps = null;
    }
    else
      flipflop_bitmap = null;
  }

  /**
   * Get entry from DV map.
   */
  public synchronized int dv_get(long lba)
  {
    //common.ptod("dv_get: %08x ", lba);

    int key = 0;
    if (byte_maps != null)
    {
      long block     = lba / key_blksize;
      int  map       = (int) (block >> MapFile.BYTE_SHIFT);
      int  remainder = (int) (block  & MapFile.BYTE_AND);

      //common.ptod("block    : %,16d", block    );
      //common.ptod("map      : %,16d", map      );
      //common.ptod("remainder: %,16d", remainder);

      key            = byte_maps[ map ].get( remainder );
    }

    else
    {
      //common.ptod("lba:               %,16d", lba);
      //common.ptod("map_blksize:       %,16d", map_blksize);
      //common.ptod("lba / map_blksize: %,16d", lba / map_blksize);
      boolean bit_set = flipflop_bitmap.getBit(lba / key_blksize);
      if (bit_set)
        key = 1;
      else
        key = 0;
    }

    //if (lba == 32768)
    //{
    //  common.ptod("dv_get: %08x %02x", lba, key);
    //  common.where(4);
    //}

    return key;
  }

  public int dv_get_nolock(long lba)
  {
    //common.ptod("dv_get: %08x ", lba);

    int key = 0;
    if (byte_maps != null)
    {
      long block     = lba / key_blksize;
      int  map       = (int) (block >> MapFile.BYTE_SHIFT);
      int  remainder = (int) (block  & MapFile.BYTE_AND);

      key            = byte_maps[ map ].get( remainder );
    }

    else
    {
      boolean bit_set = flipflop_bitmap.getBit(lba / key_blksize);
      if (bit_set)
        key = 1;
      else
        key = 0;
    }

    return key;
  }


  /**
   * Set entry in DV map
   */
  public synchronized void dv_set(long lba, int key)
  {
    //common.ptod("dv_set: %08x ", lba);

    /* Make sure we don't get more than what we want: */
    if (key >>> 8 != 0)
      common.failure("Data validation key larger than 8 bits: %08x", key);

    //if (lba == 32768)
    //{
    //  common.ptod("dv_set: %08x %02x", lba, key);
    //  common.where(4);
    //}

    if (byte_maps != null)
    {
      long block     = lba / key_blksize;
      int  map       = (int) (block >> MapFile.BYTE_SHIFT);
      int  remainder = (int) (block  & MapFile.BYTE_AND);
      int  oldkey    = byte_maps[ map ].get( remainder );

      byte_maps[ map ].put( remainder, key);

      if (key == DV_ERROR)
        blocks_in_error++;
    }

    else
    {
      /* Store the current value, whether it changed or not: */
      if (key == 0)
        flipflop_bitmap.setBit(lba / key_blksize, false);
      else
      {
        //common.where(8);
        flipflop_bitmap.setBit(lba / key_blksize, true);
      }
      //common.failure("what's this?");
      //common.where();
      //flipflop_bitmap.setBit(lba / map_blksize, !bit_set);
    }
  }


  public void dv_set_nolock(long lba, int key)
  {
    //common.ptod("dv_set: %08x ", lba);

    /* Make sure we don't get more than what we want: */
    if (key >>> 8 != 0)
      common.failure("Data validation key larger than 8 bits: %08x", key);

    //if (lba == 32768)
    //{
    //  common.ptod("dv_set: %08x %02x", lba, key);
    //  common.where(4);
    //}

    if (byte_maps != null)
    {
      long block     = lba / key_blksize;
      int  map       = (int) (block >> MapFile.BYTE_SHIFT);
      int  remainder = (int) (block  & MapFile.BYTE_AND);
      int  oldkey    = byte_maps[ map ].get( remainder );

      byte_maps[ map ].put( remainder, key & 0xff);
    }

    else
    {
      /* Store the current value, whether it changed or not: */
      if (key == 0)
        flipflop_bitmap.setBit(lba / key_blksize, false);
      else
      {
        //common.where(8);
        flipflop_bitmap.setBit(lba / key_blksize, true);
      }
      //common.failure("what's this?");
      //common.where();
      //flipflop_bitmap.setBit(lba / map_blksize, !bit_set);
    }
  }


  /**
   * Create a new map, or reuse an existing one.
   */

  public static DV_map allocateMap(String jnl_dir_name, String map_name, long lun_size, int xfersize)
  {
    DV_map old_map = (DV_map) all_maps.get(map_name);
    if (old_map != null)
      return old_map;

    if (lun_size / xfersize == 0)
      common.failure("unexpected block count");

    /* This is the first time this map is created on this slave:                    */
    /* (This shows how important it is KEEP devices on the same slave with REAL DV) */
    DV_map new_map = new DV_map(jnl_dir_name, map_name, lun_size / xfersize, xfersize);
    all_maps.put(map_name, new_map);

    new_map.map_name = map_name;

    if (Validate.isRealValidate())
      ErrorLog.plog("Created new DV map: %s size: %,d bytes; key block size: %d; entries: %,d",
                    map_name, lun_size, xfersize, new_map.key_blocks);

    return new_map;
  }

  public static DV_map findExistingMap(String map_name)
  {
    if (all_maps.size() == 0)
      return null;
    DV_map old_map = (DV_map) all_maps.get(map_name);
    if (old_map != null)
      return old_map;

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

  public int getKeyBlockSize()
  {
    return key_blksize;
  }

  /**
   * Create a 'nice' xfersize that can be used during journal recovery.
   * Since we are reading sequentially using the actual key block size may cause
   * things to be too slow if we have a small key block size.
   *
   * Initially just return an xfersize that is a multiple of the key block size
   * and still no larger than 128k. 1MB may be too large if we are dealing with
   * Swiss Cheese: we will bypass a read for any xfersize that has no known
   * data, and with one MB we may be reading a whole MB with only one key block
   * having known data.
   *
   * In the future we may have some other ideas.
   */
  public int determineJnlRecoveryXfersize(int max_used)
  {
    /* If the user has already specified a larger xfersize, use that instead: */
    int MAX = 128 * 1024;

    // Not sure if this will work, so ignore for now
    //if (max_used > MAX)
    //  return MAX;

    /* Loop until we reach a value of 128k or if larger, the map_blksize: */
    int xfersize = 0;
    while (xfersize < MAX)
      xfersize += key_blksize;

    return xfersize;
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
    * Note: this last paragraph is no longer the case and xfersizes can be
    *       different as long as they are all a muliple of the smallest size.
    *
    * The data transfer sizes for all operations against an SD that has data
    * validation activated must be identical, also across RDs.
    */
  public synchronized int getKeyAndSetBusy(long lba)
  {
    /* A specific check for when/why an lba is locked: */
    //if (lba == 0x00016000)
    //{
    //  common.ptod("getKeyAndSetBusy: 0x%08x %02x", lba, dv_get(lba));
    //  common.where(8);
    //}

    /* Value 127 means block in error: */
    int key = dv_get(lba);
    if ( (key & 0x7f) == DV_map.DV_ERROR)
    {
      map_busy = 0;
      //common.ptod("DV_map.getKeyAndSetBusy(): LBA already marked in error: " + lba);
      return -1;
    }

    /* Test busy flag. If not busy, set and return key: */
    if ( (key & 0x80) == 0)
    {
      key |= 0x80;
      dv_set(lba, key);
      map_busy = 0;

      return key & 0x7f;
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
    if (++map_busy > 10000)
      common.failure("Too many 'busy' statuses reported. " +
                     "Likely caused because the target file/volume size is so " +
                     "small that ALL blocks are currently in use");

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
    /* WD entry, one for each SD: */
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);
      if (sd.dv_map != null)
        sd.dv_map.setAllUnBusy();
    }
  }


  /**
   * Reset all busy flags.
   * While we're at it, also count blocks in error, saving us a trip later on.
   */
  public synchronized void setAllUnBusy()
  {
    if (byte_maps == null)
      return;

    Elapsed elapsed = new Elapsed("DV_map.setAllUnbusy");

    /* Start one async thread for each MapFile: */
    ArrayList <MapUnbusyThread> async_list = new ArrayList(8);
    for (MapFile byte_map : byte_maps)
    {
      MapUnbusyThread mt = new MapUnbusyThread(byte_map);
      async_list.add(mt);
      mt.start();
    }

    /* Wait for them: */
    while (true)
    {
      int running = 0;
      for (MapUnbusyThread mt : async_list)
      {
        if (mt.isAlive())
          running++;
      }
      if (running == 0)
        break;
      common.sleep_some(100);
    }

    /* Pick up the counters from each MapFile: */
    blocks_in_error = 0;
    blocks_busy     = 0;
    blocks_known    = 0;
    for (MapFile byte_map : byte_maps)
    {
      blocks_in_error += byte_map.counter.bad_blocks;
      blocks_busy     += byte_map.counter.blocks_busy;
      blocks_known    += byte_map.counter.blocks_known;
    }


    elapsed.end(5);
    //common.ptod("blocks_in_error: " + blocks_in_error);
    //common.ptod("blocks_busy:     " + blocks_busy);
    //common.ptod("blocks_known:    " + blocks_known);
  }

  /**
   * This method increments a data pattern key by one. The value of the key
   * will roll over from the maximum value of 126 back to one. (Remember, value
   * block has never been written).
   */
  public int dv_increment(int key, long set)
  {
    /* Normal processing: */
    if (!Dedup.isDedup())
      return( (key == 126) ? 1 : ++key);

    if (!dedup.isFlipFlop() && Dedup.isDuplicate(set))
      common.failure("Duplicate set key value should never been incremented without flipflop");

    /* For Dedup with DV we MUST flipflop. Without flipflop we would never */
    /* change data patterns making DV useless:                             */
    //else if (!dedup.isFlipFlop()) // (!Validate.isRealValidate() && !dedup.isFlipFlop())
    //  return key;

    /* We won't be called when flipflop is not needed! */
    synchronized (dedup)
    {
      dedup.flipflops++;
      //common.where();
    }
    return( (key >= dedup.getMaxKey()) ? 1 : ++key);
  }

  /**
   * Flipflop:
   * - if old key value equals zero it is the first write and we return a '1'.
   * - if it is a one we return '2'
   * - if it is a two we again return 1'.'
   *
   */
  public synchronized int flipflop(int key)
  {
    //common.failure("there should not be any incrementing here");
    dedup.flipflops++;
    //common.where(8);
    if (key == 0)
      return 1;
    else if (key == 1)
      return 2;
    else if (key == 2)
      return 1;
    else
    {
      common.failure("Unexpected flipflop value: " + key);
      return -1;
    }
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

  public void eraseMap()
  {
    common.ptod("Erasing Data Validation map: " + map_name);
    for (long i = 0; i < key_blocks; i++)
      dv_set(i * key_blksize, 0);
  }

  public long getLastTimestamp(long lba)
  {
    if (timestamp_map == null)
      return 0;

    long ret = timestamp_map.getTime(lba / key_blksize);
    return ret;
  }


  public String getLastOperation(long lba)
  {
    if (timestamp_map == null)
      return "n/a";
    else
    {
      String ret = timestamp_map.getLastOperation(lba / key_blksize);
      return ret;
    }
  }

  /**
   * Mark block of data 'not in use'.
   */
  public synchronized void dv_set_unbusy(long lba, int key) throws Exception
  {
    //common.ptod("dv_set_ubsy: 0x%08x %2d", lba, key);

    /* get current entry: */
    int entry = dv_get(lba);

    /* Make sure that it is indeed busy: */
    if ( (entry & 0x80) == 0)
    {
      /* If this block is marked DV_ERROR we just set it to unbusy after */
      /* reporting the error, so that is accepted.                       */
      if ( (entry & 0x7f) == DV_ERROR)
        return;

      String txt = String.format("dv_set_unbusy(): entry not busy: lba: 0x%08x old: %2d new: %2d", lba, entry, key);
      throw new Exception(txt);
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
    dv_set(lba, key & 0x7f);

    //common.ptod(this + " tmp: " + tmp + " " + (lba / map_blksize));

    return;
  }


  /**
   * Mark block as no longer busy, without changing the key.
   */
  public synchronized void setUnbusy(long lba) throws Exception
  {
    //common.ptod("dv_set_ubsy: 0x%08x %2d", lba, key);

    /* get current entry: */
    int key = dv_get(lba);

    /* Make sure that it is indeed busy: */
    if ( (key & 0x80) == 0)
    {
      /* If this block is marked DV_ERROR we just set it to unbusy after */
      /* reporting the error, so that is accepted.                       */
      if ( (key & 0x7f) == DV_ERROR)
        return;

      String txt = String.format("dv_set_unbusy(): entry not busy: lba: 0x%08x key: %2d ", lba, key);
      common.ptod("txt: " + txt);
      throw new Exception(txt);
    }

    /* Update the key in the map and mark it not busy: */
    dv_set(lba, key & 0x7f);

    //common.ptod(this + " tmp: " + tmp + " " + (lba / map_blksize));

    return;
  }

  /**
   * Store timestamp of last successful read/write.
   * No lock is necessary, since we use a full 8 bytes and the block is still busy.
   */
  public void save_timestamp(long lba, long type)
  {
    /* Store timestamp of last successful i/o if needed: */
    if (timestamp_map != null)
      timestamp_map.storeTime(lba / key_blksize, type);
  }



  /**
   * Allocate a DV map for each real SD.
   *
   * Maps only needed when DV is active; maps only allocated once, unless the
   * xfersize for an SD changes. In that case, old map is deleted and new one
   * created.
   *
   * DV is activated for dedup so that we can keep track of what data
   * pattern is written where using key 1+2 values only.
   *
   * A map does already exist after journal recovery.
   */
  public static void allocateSDMaps(Vector <SD_entry> sd_list)
  {
    for (SD_entry sd : SD_entry.getRealSds(sd_list))
    {
      /* Do we already have a map for this SD? */
      sd.dv_map = DV_map.findExistingMap(sd.sd_name);

      /* If we don't have a map, allocate: */
      if (sd.dv_map == null)
      {
        sd.dv_map = DV_map.allocateMap(sd.jnl_dir_name, sd.sd_name, sd.end_lba, sd.getKeyBlockSize());

        if (Validate.isJournaling())
        {
          if (sd.dv_map.journal == null)
          {
            sd.dv_map.journal = new Jnl_entry(sd.sd_name, sd.jnl_dir_name, "sd");
            sd.dv_map.journal.storeMap(sd.dv_map);
          }
        }
      }

      /* If there is dedup, copy it from the SD: */
      sd.dv_map.setDedup(sd.dedup);

      /* This accomodates a problem in Vdbench where with very complex workloads    */
      /* an SD can move from one slave to an other and then back, with the problem  */
      /* being after the 'back' where the slave is not aware of data having been    */
      /* modified and the maps therefore not being in sync.                         */
      /* This was the easiest fix for a bug that took 12 years show up.             */
      /* Even if the SD did not make it back to a previous slave, the other slaves  */
      /* ended up with a clear map file anyway, so always forcing a clean map is OK */
      // Problem has been fixed in RD_entry, but may as well keep around, just in case.....
      if (common.get_debug(common.ALWAYS_ERASE_MAPS))
        sd.dv_map.eraseMap();
    }
  }




  public static void main(String args[])
  {
    String stamp1 = args[0];
    String stamp2 = args[1];
    long   tod1   = Long.parseLong(stamp1, 16);
    long   tod2   = Long.parseLong(stamp2, 16);
    long   tod    = (tod1 << 32) | tod2;
    common.ptod("tod: 0x%x", tod);

    SimpleDateFormat df = new SimpleDateFormat( "EEEE, MMMM d, yyyy HH:mm:ss.SSS zzz" );
    df.setTimeZone(TimeZone.getTimeZone("PST"));
    common.ptod("date: " + df.format(new Date(tod/1000)));

    //int ck = 0;
    //for (int i = 0; i < stamp.length(); i+=2)
    //{
    //  long tmp = Long.parseLong(stamp.substring(i, i+2), 16);
    //  //common.ptod("tmp: " + tmp);
    //  ck += tmp;
    //}
    //common.ptod(Format.f("Checksum: %x", ck & 0xff));
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
      lba += key_blksize;
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
      lba += key_blksize;
    }

    return false;
  }



  public static void printCounters()
  {
    if (!Validate.isRealValidate())
      return;

    Vector txt = new Vector(16, 0);
    long validation_reads = 0;
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

    if (Validate.ignoreZeroReads())
      return;

    /* Count the total of bad blocks. Each map's counting is done during setAllUnbusy() */
    long bad_blocks = 0;
    for (DV_map map : getAllMaps())
      bad_blocks += map.blocks_in_error;


    ErrorLog.ptod("Total amount of key blocks read and validated: %,8d; "+
                  "key blocks marked in error: %4d ", validation_reads, bad_blocks);


    if (validation_reads == 0)
    {
      if (SlaveWorker.work.work_rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
        return;
      if (SlaveWorker.work.format_run)
        return;
      if (SlaveWorker.work.only_eof_writes)
        return;
      if (Validate.skipRead())
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
      txt.add("Or, you never did any writes so Vdbench does not know what to ");
      txt.add("compare the data with. In that case, change the rdpct= parameter.");
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

    ErrorLog.plog("Total Data Validation or I/O error count: " + ErrorLog.getErrorCount());
    ErrorLog.plog("*");

    common.failure("Vdbench terminating due to Data Validation or I/O errors. See errorlog.html.");
  }

  public synchronized void countBadSectors()
  {
    bad_sectors_found++;
  }
  public int getBadSectorCount()
  {
    return bad_sectors_found;
  }

  public static boolean anyBadSectorsFound()
  {
    DV_map[] maps = getAllMaps();
    for (int i = 0; i < maps.length; i++)
    {
      if (maps[i].bad_sectors_found > 0)
        return true;
    }
    return false;
  }

  public static void xx_main(String args[])
  {
    String fname = "/export/dedup/quick_vdbench_test";
    int BUFSIZE = 1024*1024;
    int BLOCKS = 40960;
    long buffer = Native.allocBuffer(BUFSIZE);
    int[] array = new int[ BUFSIZE / 4 ];


    for (int i = 30; i < 100; i+=10)
    {
      new File(fname).delete();

      /* Fill buffer with random numbers: */
      Random compression_random = new Random(0);
      //for (int j = 0; j < array.length; j++)
      //  array[j] = (int) (compression_random.nextDouble() * Integer.MAX_VALUE);

      /* Now clear some words: */
      int zeros = BUFSIZE / 4 * i / 100 ;
      for (int j = 0; j < zeros; j++)
      {
        array[j] = 0;
      }

      //common.ptod("zeros: %6d %6d ", zeros, z);

      Native.arrayToBuffer(array, buffer);

      long handle = Native.openfile(fname, 0, 1);
      for (int j = 0; j < BLOCKS; j++)
      {
        long lba = j * (long) BUFSIZE;
        if (Native.writeFile(handle, lba, BUFSIZE, buffer) != 0)
        {
          common.ptod("lba: " + lba);
          common.failure("write error");
        }
      }

      if (common.onSolaris())
      {
        long rc = Native.fsync(handle);
        if (rc != 0)
          common.failure("Native.closeFile(fhandle): fsync failed, rc= " + rc);
      }
      Native.closeFile(handle);

      OS_cmd ocmd = new OS_cmd();
      ocmd.addText("zfs get compressratio pool-0/local/default/dedup");
      ocmd.execute(false);
      String[] lines = ocmd.getStdout();
      for (int j = 0; j < lines.length; j++)
      {
        if (lines[j].indexOf("compressratio") != -1)
          common.ptod("lines: %3d %8d %s", i, (zeros*4), lines[j]);
      }
    }
  }



  /**
   * Fill a 512-byte int array sector with LFSR data.
   * The first 32 bytes are cleared to zero after.
   */
  public static void obsolete_fillLFSRSector(int[]  sector_array,
                                             long   lba,
                                             int    key,
                                             String name)
  {
    if (sector_array.length != 512/4)
      common.failure("fillLFSRSector(): Invalid length. " + sector_array.length);
    Native.fillLfsrArray(sector_array, lba, key, name);

    for (int i = 0; i < 32/4; i++)
      sector_array[i] = 0;
  }
}

/*17:17:35.712 lines:   0        0 pool-0/local/default/dedup  compressratio  1.00x  -
17:19:19.828 lines:   2     5242 pool-0/local/default/dedup  compressratio  1.00x  -
17:21:01.650 lines:   4    10485 pool-0/local/default/dedup  compressratio  1.00x  -
17:22:42.326 lines:   6    15728 pool-0/local/default/dedup  compressratio  1.00x  -
17:24:24.821 lines:   8    20971 pool-0/local/default/dedup  compressratio  1.00x  -
17:26:06.573 lines:  10    26214 pool-0/local/default/dedup  compressratio  1.00x  -
17:27:47.041 lines:  12    31457 pool-0/local/default/dedup  compressratio  1.00x  -
17:29:29.070 lines:  14    36700 pool-0/local/default/dedup  compressratio  1.00x  -
17:31:11.913 lines:  16    41943 pool-0/local/default/dedup  compressratio  1.00x  -
17:32:54.011 lines:  18    47185 pool-0/local/default/dedup  compressratio  1.00x  -
17:34:36.450 lines:  20    52428 pool-0/local/default/dedup  compressratio  1.00x  -
17:36:19.446 lines:  22    57671 pool-0/local/default/dedup  compressratio  1.00x  -
17:38:02.520 lines:  24    62914 pool-0/local/default/dedup  compressratio  1.00x  -
17:39:45.290 lines:  26    68157 pool-0/local/default/dedup  compressratio  1.00x  -
17:41:28.857 lines:  28    73400 pool-0/local/default/dedup  compressratio  1.00x  -
17:43:12.974 lines:  30    78643 pool-0/local/default/dedup  compressratio  1.00x  -
17:44:56.963 lines:  32    83886 pool-0/local/default/dedup  compressratio  1.00x  -
17:46:40.484 lines:  34    89128 pool-0/local/default/dedup  compressratio  1.00x  -
17:48:18.442 lines:  36    94371 pool-0/local/default/dedup  compressratio  1.15x  -
17:49:55.923 lines:  38    99614 pool-0/local/default/dedup  compressratio  1.17x  -
17:51:31.404 lines:  40   104857 pool-0/local/default/dedup  compressratio  1.20x  -
17:53:05.518 lines:  42   110100 pool-0/local/default/dedup  compressratio  1.23x  -
17:54:38.042 lines:  44   115343 pool-0/local/default/dedup  compressratio  1.26x  -
17:56:09.516 lines:  46   120586 pool-0/local/default/dedup  compressratio  1.29x  -
17:57:39.227 lines:  48   125829 pool-0/local/default/dedup  compressratio  1.33x  -
17:59:08.147 lines:  50   131072 pool-0/local/default/dedup  compressratio  1.36x  -
18:00:35.945 lines:  52   136314 pool-0/local/default/dedup  compressratio  1.40x  -
18:02:06.885 lines:  54   141557 pool-0/local/default/dedup  compressratio  1.45x  -
18:03:35.861 lines:  56   146800 pool-0/local/default/dedup  compressratio  1.50x  -
18:04:59.969 lines:  58   152043 pool-0/local/default/dedup  compressratio  1.55x  -
18:06:25.751 lines:  60   157286 pool-0/local/default/dedup  compressratio  1.61x  -
18:07:49.425 lines:  62   162529 pool-0/local/default/dedup  compressratio  1.68x  -
18:09:10.796 lines:  64   167772 pool-0/local/default/dedup  compressratio  1.75x  -
18:10:30.039 lines:  66   173015 pool-0/local/default/dedup  compressratio  1.83x  -
18:11:47.355 lines:  68   178257 pool-0/local/default/dedup  compressratio  1.92x  -
18:13:02.910 lines:  70   183500 pool-0/local/default/dedup  compressratio  2.03x  -
18:14:16.066 lines:  72   188743 pool-0/local/default/dedup  compressratio  2.15x  -
18:15:26.750 lines:  74   193986 pool-0/local/default/dedup  compressratio  2.28x  -
18:16:35.037 lines:  76   199229 pool-0/local/default/dedup  compressratio  2.44x  -
18:17:40.972 lines:  78   204472 pool-0/local/default/dedup  compressratio  2.62x  -
18:18:46.203 lines:  80   209715 pool-0/local/default/dedup  compressratio  2.85x  -
18:19:46.896 lines:  82   214958 pool-0/local/default/dedup  compressratio  3.11x  -
18:20:44.941 lines:  84   220200 pool-0/local/default/dedup  compressratio  3.45x  -
18:21:39.847 lines:  86   225443 pool-0/local/default/dedup  compressratio  3.86x  -
18:22:31.910 lines:  88   230686 pool-0/local/default/dedup  compressratio  4.39x  -
18:23:21.456 lines:  90   235929 pool-0/local/default/dedup  compressratio  5.12x  -
18:24:08.133 lines:  92   241172 pool-0/local/default/dedup  compressratio  6.12x  -
18:24:51.820 lines:  94   246415 pool-0/local/default/dedup  compressratio  7.62x  -
18:25:32.898 lines:  96   251658 pool-0/local/default/dedup  compressratio  10.15x  -
18:26:10.424 lines:  98   256901 pool-0/local/default/dedup  compressratio  14.98x  -
(sbm-fugu-a) /net/129.147.9.187/export/home1/16/hv104788/vdbench503:

*/
