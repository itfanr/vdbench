package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import Utils.Getopt;


/**
 * This class stores and handles SD (or maybe FSD, tbd) specific information.
 */
public class SdDedup implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String      name            = "SdDedup";
  int         dedup_unit      = 0;
  long        rel_byte_start  = 0;
  long        rel_byte_end    = 0;
  long        rel_block_start = 0;
  long        rel_block_end   = 0;
  Dedup       dedup           = null;
  DedupBitMap uniques_bitmap  = null;

  ArrayList <Integer> hot_blocks = new ArrayList(0);


  public SdDedup(String n)
  {
    name = n;
  }

  /**
   * Return the proper hotset if any have been defined.
   * Otherwise, go to the main Dedup class for the translation.
   */
  public long translateDuplicateBlockToSet(long rel_block)
  {
    boolean debug = false;

    if (rel_block < rel_block_start)
      common.failure("Invalid call: block %,d for %s below relative start of %,d",
                     rel_block, name, rel_block_start);
    if (rel_block >= rel_block_end)
      common.failure("Invalid call: block %,d for %s beyond relative end of %,d",
                     rel_block, name, rel_block_end);

    /* If we are beyond the hot sets, call the dedup general code: */
    long my_block = rel_block - rel_block_start;
    if (my_block >= hot_blocks.size())
    {
      long set = dedup.translateBlockToSet(rel_block);
      if (debug) common.ptod("nothot: %6d %6d %6d %s", rel_block_start, rel_block, set, name);
      return set;
    }

    /* Return the requested hot set number: */
    long hot_block = rel_block - rel_block_start;
    long set       = hot_blocks.get((int) hot_block);
    if (debug) common.ptod("hotset: %6d %6d %6d %s", rel_block_start, rel_block, set, name);

    return set;
  }


  /**
  * The dedupsets= parameter has a direct impact on the way dedup= is calculated.
  *
  * 1,000,000 data blocks with sets=1 and dedup=50 will result in
  * 500,000 unique blocks with 500,000 duplicate blocks
  *
  * 1,000,000 data blocks with sets=100,000 and dedup=50 results in
  * 500,000 unique blocks with 500,000 duplicate blocks, but 100,000 of those
  * are unique duplicates, so in reality we have 500,000 + 100,000 unique blocks
  * or dedup=60.
  *
  * This is clearly wrong, so we can no longer make 50% of the blocks unique,
  * instead we must make 50% minus 100,000 blocks unique, or 40%.
  *
  * The code below estimates the amount of blocks that the SDs contain, and
  * adjusts the 'percentage of unique blocks' with it.
  */

  public static void adjustSdDedupValues()
  {
    int dedup_unit = Validate.getDedupUnit();

    /* When manipulating all this make sure that we always keep SDs   */
    /* in the same sorted order. This is needed for journal recovery  */
    /* and  keeps relative offsets in tact.                           */
    SD_entry[] sorted_sds = Vdbmain.sd_list.toArray(new SD_entry[0]);
    Arrays.sort(sorted_sds, new SdSort());

    /* Create a map with all Dedup instances: */
    HashMap <Dedup, Dedup> used_dedup_map = new HashMap(4);
    for (SD_entry sd : sorted_sds)
    {
      if (!sd.concatenated_sd)
        used_dedup_map.put(sd.dedup, sd.dedup);
    }

    /* For all those instances treat the combined SDs using this instance as ONE: */
    for (Dedup dedup : used_dedup_map.values())
    {
      /* for this Dedup instance, look for the SDs using it: */
      HashMap <String, SD_entry> sds_for_dedup_map = new HashMap(8);
      for (SD_entry sd : sorted_sds)
      {
        if (sd.dedup == dedup)
          sds_for_dedup_map.put(sd.sd_name, sd);
      }

      /* Calculate total size while also creating an SdDedup instance: */
      long total_size = 0;
      String[] sds_using_dedup = sds_for_dedup_map.keySet().toArray(new String[0]);
      for (String sd_name : sds_using_dedup)
      {
        SD_entry sd              = sds_for_dedup_map.get(sd_name);

        SdDedup sdd = sd.sdd     = new SdDedup(sd.sd_name);
        sdd.dedup                = dedup;
        sdd.rel_byte_start       = total_size;
        sdd.rel_byte_end         = total_size + sd.end_lba;
        sdd.rel_block_start      = total_size / dedup_unit;
        sdd.rel_block_end        = (total_size + sd.end_lba) / dedup_unit;
        total_size              += sd.end_lba;
      }

      dedup.total_size = total_size;
      dedup.adjustSdDedupValue();
    }
  }

  /**
   * Hot block to hot set translation.
   * Each new element here indicates that this dedupunit/block represents a
   * specific hot set e.g. two blocks of hotset1, three blocks of hotset2, etc.
   */
  public void receiveHotset(int set)
  {
    hot_blocks.add(set);
  }
}






