package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;


/**
 * Thread used to allow for async dumping or restoration of portions of a DV_map
 */
public class JournalThread extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Jnl_entry jnl           = null;
  private DV_map    map           = null;
  private long      handle        = 0;
  private String    fname         = null;
  private long      start_key_blk = 0;
  private long      key_blocks    = 0;
  private long      key_blksize   = 0;
  private boolean   restore       = false;

  private long      jnl_lba       = 0;
  private long      bad_blocks    = 0;
  private long      known_blocks  = 0;


  /**
   * A thread handling a maximum of one GB worth of journal restore.
   *
   * Start and end are in 512-byte journal records!
   */
  public JournalThread(Jnl_entry jnl,
                       DV_map    map,
                       long      handle,
                       long      start_key_blk,
                       long      key_blocks,
                       long      key_blksize,
                       boolean   restore)
  {
    this.jnl           = jnl;
    this.map           = map;
    this.handle        = handle;
    this.fname         = File_handles.getLastName(handle);;
    this.start_key_blk = start_key_blk;
    this.key_blocks    = key_blocks;
    this.key_blksize   = key_blksize;
    this.restore       = restore;

    /* Calculate starting journal lba: */
    jnl_lba  = 512;
    jnl_lba += start_key_blk / 480 * 512;

    // This check is here to prove that we do not need to pass the blksize!!!
    if (key_blksize != map.getKeyBlockSize())
      common.failure("mismatch in key block size: %d %d", key_blksize, map.getKeyBlockSize());
  }


  /**
   */
  public void run()
  {
    try
    {
      if (restore)
        restorePortionOfMap();
      else
        dumpPortionOfMap();
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * Dump a subset of the Data Validation map to either the .jnl or the .map
   * file.
   *
   * Though we process up to 'n' times 480 key blocks for 'n*512' bytes of data
   * to be written to the file, we must not write more that allowed, because
   * journal recovery depends on that.
   */
  private void dumpPortionOfMap()
  {
    int   HOW_MANY     = 8;
    int   BUFFER_SIZE  = HOW_MANY * 512;
    long  buffer       = Native.allocBuffer(BUFFER_SIZE);
    int[] array        = new int[ HOW_MANY * 512 / 4];

    Elapsed elapsed = new Elapsed("dumpPortionOfMap of " + fname, 250*1000);

    for (long key_block = start_key_blk; key_block < start_key_blk + key_blocks;)
    {
      int array_offset = 0;
      int records_done = 0;
      for (int which = 0; which < HOW_MANY; which++)
      {
        //common.ptod("key_block: " + key_block);
        if (key_block >= start_key_blk + key_blocks)
          break;

        //common.ptod("array_offset: " + array_offset);
        array[ array_offset + 0 ] = Jnl_entry.MAP_EYE_CATCHER;
        array[ array_offset + 1 ] = Jnl_entry.MAP_EYE_CATCHER;
        array_offset             += Jnl_entry.MAP_SKIP_HDR;

        /* Dump the map in chunks of x integers after byte to int translation: */
        for (int j = Jnl_entry.MAP_SKIP_HDR; j < 128 && key_block < map.key_blocks; j++)
        {
          array[ array_offset ] = map.dv_get_nolock(key_block++ * key_blksize) << 24 |
                                  map.dv_get_nolock(key_block++ * key_blksize) << 16 |
                                  map.dv_get_nolock(key_block++ * key_blksize) <<  8 |
                                  map.dv_get_nolock(key_block++ * key_blksize);
          array_offset++;
        }
        elapsed.track(480);
        records_done++;
      }


      /* Write no more than the amount of key blocks we processed: */
      //common.ptod("jnl_lba: %08x %,12d", jnl_lba, jnl_lba);
      Native.arrayToBuffer(array, buffer);
      jnl.jnl_write(handle, jnl_lba, records_done * 512, buffer);
      jnl_lba += records_done * 512;
      //common.ptod("records_done: " + records_done);
      //common.ptod("array_offset: " + array_offset);
    }

    //common.failure("debugging");


    elapsed.end(5);

    Native.freeBuffer(BUFFER_SIZE, buffer);
  }



  /**
   * Restore only piece of the map.
   * Note that this can also be done in 'n' times 512 bytes, but for now I do
   * not want to worry about possibly reading beyond EOF.
   */
  private void restorePortionOfMap()
  {
    long  buffer = Native.allocBuffer(512);
    int[] array  = new int[ 512 / 4 ];

    Elapsed elapsed = new Elapsed("restorePortionOfMap of " + fname, 250*1000);

    /* Read the map in chunks of x integers and do int to byte translation: */
    for (long key_block = start_key_blk; key_block < start_key_blk + key_blocks;)
    {
      /* Read: */
      try
      {
        jnl.jnl_read(handle, jnl_lba, 512, buffer, array, Jnl_entry.MAP_EYE_CATCHER);
        elapsed.track(480);
      }
      catch (Exception e)
      {
        common.ptod("Error while restoring the journal map. Journal likely is corrupted");
        common.ptod(e);
        common.failure(e);
      }

      if (array[0] != Jnl_entry.MAP_EYE_CATCHER)
        common.failure("Missing eye catcher in journal file at lba 0x%08x", jnl_lba);

      for (int j = Jnl_entry.MAP_SKIP_HDR; j < 128 && key_block < start_key_blk + key_blocks; j++)
      {
        int key1 = array[ j ] >> 24 & 0xff;
        int key2 = array[ j ] >> 16 & 0xff;
        int key3 = array[ j ] >>  8 & 0xff;
        int key4 = array[ j ]       & 0xff;

        // nolock went from 2500/sec to 70000/sec
        map.dv_set_nolock(key_block++ * key_blksize, key1);
        map.dv_set_nolock(key_block++ * key_blksize, key2);
        map.dv_set_nolock(key_block++ * key_blksize, key3);
        map.dv_set_nolock(key_block++ * key_blksize, key4);

        if (key1 == DV_map.DV_ERROR) bad_blocks++;
        if (key2 == DV_map.DV_ERROR) bad_blocks++;
        if (key3 == DV_map.DV_ERROR) bad_blocks++;
        if (key4 == DV_map.DV_ERROR) bad_blocks++;

        if (key1 != 0) known_blocks++;
        if (key2 != 0) known_blocks++;
        if (key3 != 0) known_blocks++;
        if (key4 != 0) known_blocks++;
      }

      jnl_lba += 512;
    }



    /* Note: if the USED map was never written to the jnl and map file  there       */
    /*       indeed will be ZERO mod_blocks. They'll all be in the journal records! */

    /* Note: even if the journal is not officially closed, if we used journal_max,  */
    /*       any block in error at that time MAY be in the map.                     */

    Jnl_entry. plog("Data Validation map restored for %s. Map contains %,16d known blocks.",
                    fname, known_blocks);
    if (bad_blocks > 0)
      Jnl_entry.plog(String.format("Key blocks found in journal that are in error: %d. "+
                                   "They will NOT be re-verified.", bad_blocks));

    elapsed.end(5);

    Native.freeBuffer(512, buffer);
  }


  /**
   * Wait for a bunch of threads (likely from above) to complete.
   */
  public static void waitForList(ArrayList <Thread> threads)
  {
    while (true)
    {
      int running = 0;
      for (Thread thread : threads)
      {
        if (thread.isAlive())
          running++;
      }
      if (running == 0)
        break;
      //common.where();
      common.sleep_some(100);
    }
  }
}





