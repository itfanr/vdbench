package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.HashMap;
import java.util.Vector;

import Utils.Getopt;


/**
 * This class handles statistics counting for dedup sets and possible some other
 * dedupset stuff.
 *
 * Understand though that statistics counting will only be useful if the current
 * execution knowns the CURRENT state of the storage device, meaning that the
 * current execution must have at least done the complete formatting of the
 * storage (seek=eof) or needs access to a preserved mmap file.
 *
 * To avoid users misunderstanding the output I should likely not have it active
 * by default.
 *
 * And of course, statistics will only be maintained when hot sets have
 * been requested.
 */
public class DedupSet
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public int    set_number     = 0;
  public int    blocks_in_set  = 0;
  public int    switches       = 0;
  public int    references     = 0;
  public byte[] keys           = null;
  public long   starting_block = 0;



  public static int[] createHotBlockArray(Dedup dedup)
  {
    int[] hot_blocks = new int[ (int) dedup.hot_dedup_blocks ];

    int block_index = 0;
    int set_number  = 0;
    for (int i = 0; i < dedup.hot_dedup_parms.length; i+=2)
    {
      for (int x = 0; x < dedup.hot_dedup_parms[ i ]; x++)
      {
        for (int y = 0; y < dedup.hot_dedup_parms[ i+1 ]; y++)
        {
          hot_blocks[ block_index++ ] = set_number;
        }
        set_number++;
      }
    }

    //common.ptod("dedicates.length: " + dedicates.length);
    //for (int i = 0; i < dedicates.length; i++)
    //  common.ptod("dedicates: block: %3d %4x", i, dedicates[i]);

    return hot_blocks;
  }


  /**
   * Create a list with all requested hot sets.
   *
   * Note that when using HotBand AND Data Validation, a block will be marked
   * busy. The most frequently used blocks, guess what, are already busy.
   * This counteracts the requested hotband logic.......
   * Unless of course you have a large amount of hot sets.
   */
  public static ArrayList <DedupSet> createHotSetList(Dedup dedup)
  {
    ArrayList <DedupSet> list = new ArrayList(dedup.hot_dedup_parms.length / 2);

    int  set_number = 0;
    long block      = 0;
    for (int i = 0; i < dedup.hot_dedup_parms.length; i+=2)
    {
      for (int x = 0; x < dedup.hot_dedup_parms[ i ]; x++)
      {
        DedupSet set       = new DedupSet();
        set.set_number     = set_number;
        set.blocks_in_set  = dedup.hot_dedup_parms[ i+1 ];
        set.starting_block = block;
        set.keys           = new byte[set.blocks_in_set];
        list.add(set);

        //common.ptod("set.set_number: %4d %4d %4d", set.set_number, set.blocks_in_set, set.starting_block);
        set_number++;
        block += set.blocks_in_set;
      }
    }

    return list;
  }


  /**
   * DedupSet statistics.
   *
   * All we do here is count how many of each flipflop exist.
   * If a dedupset is very deep then of course this can get expensive!
   *
   * Note: without DV, blocks are not locked. This means that mutliple
   * concurrent writes can be going on to the same block using the same key,
   * allowing for a small discrepancy when reporting switches.
   */
  private static Object testing_lock = new Object();
  public synchronized void addStatistics(DV_map dvmap)
  {
    //synchronized (testing_lock)
    {
      references++;

      /* Without flipflop we really never switch so also do not need to count: */
      // this check has already been done!
      //if (!dvmap.dedup.isFlipFlop())
      //  return;

      /* Count the CURRENT state of this set: */
      int zeros  = 0;
      int ones   = 0;
      int others = 0;

      for (int i = 0; i < blocks_in_set; i++)
      {
        long lba = (starting_block + i) * dvmap.getKeyBlockSize();
        int  key = dvmap.dv_get(lba) & 0x7f;
        if (key == 0)
          zeros++;
        else if (key == 1)
          ones++;
        else
          others++;
      }


      if (zeros == blocks_in_set)
      {
        switches++;
        //debugit(zeros, ones, others, 0);
      }
      else if (ones == blocks_in_set)
      {
        switches++;
        //debugit(zeros, ones, others, 1);
      }
      //else
      //  debugit(zeros, ones, others, 99);
    }
  }


  /**
   * Report counters.
   * Scary thought: IO spent an hour trying to figure out why there never were
   * any switches, until I remembered that statistically the chance that a set
   * with 8 or 16 blocks are ALL ones or twos is very, very small.
   * That's why we need to keep the block count low and/or use hotbands!!!!!
   */
  private void debugit(int zeros, int ones, int others, int which)
  {
    common.ptod("Switched to all %d for set %3d; rel: %4d -%4d; switches %5d; zeros: %2d ones: %2d others: %2d",
                which, set_number, starting_block, starting_block + blocks_in_set -1,

                switches, zeros, ones, others);
  }
}

