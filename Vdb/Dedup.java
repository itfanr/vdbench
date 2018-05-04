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
 * This class contains Dedup specific code.
 *
 * Note: all attempts to create a reliable hash failed. The accuracy just was
 * not good enough. When for instance needing 40960 unique dedup_set values
 * only about 25000 were set, with 15000 duplicates. That was not good enough.
 * Now just using block%dedup_sets_used.
 */
public class Dedup implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private double  dedup_ratio      = -1;
  private double  dedup_pct        = Double.MIN_VALUE;
  private double  dedup_adjusted   = 100;  /* Set to 100 to accomodate compression only */
  private long    dedup_sets_reqd  = Long.MAX_VALUE;   /* Defaults to dedup_pct */
  private long    dedup_sets_used;
  private boolean dedup_across     = true;
  private int     dedup_maxkey     = 2;
  private long    dedup_seed       = 0;
  public  long    total_size;      /* For SDs and FSDs the proper size */

  private static int dedup_type_seq = 1;
  private        int dedup_type     = dedup_type_seq++;

  private long    uniques_written;
  private long    uniques_written_acr_no;
  private long    duplicates_written;
  public  long    flipflops;
  private long    unique_blocks;
  private long    duplicate_blocks;

  private long    unit_blocks;
  private double  sets_pct;
  private boolean flipflop = false;
  private boolean hotflop  = false;

  public  int[]   hot_dedup_parms = new int[0]; /* Hot sets off by default */
  //{
  //  10,2,     // Default total 20 + 60 + 120 = 200 hot blocks
  //  20,3,
  //  30,4
  //};

  /* The stuff here basically belongs on the slaves: */
  public  long    hot_dedup_sets;
  public  long    hot_dedup_blocks;
  private int[]   hot_blocks = null;
  public  ArrayList <DedupSet> hot_set_stats = null;

  public  static Dedup    dedup_default = new Dedup();
  public  static boolean  any_hotsets_requested = false;


  /* These fields are also used in vdbjni.h and vdb_dv.c
     (first 32 bits)
     .... .... .... .... .... .... .... ....
     .... ....                               0xff00000000000000 DV key, flipflop for Dedup
               1                             0x0080000000000000 'Unique' flag
               11                            0x00C0000000000000 UNIQUE_BLOCK_ACROSS_YES
               111                           0x00E0000000000000 UNIQUE_BLOCK_ACROSS_NO
               1111                          0x00F0000000000000 DEDUP_NO_DEDUP
                         .... .... .... .... 0X0000FFFF00000000 Dedup type, 65536 max

     The remaining 32bits are dedup_set number, or 4 billion max

  */
  public static long UNIQUE_KEY_MASK         = 0x7F00000000000000l;
  public static long UNIQUE_MASK             = 0x00F0000000000000l;
  public static long UNIQUE_BLOCK_MASK       = 0x0080000000000000l;
  public static long UNIQUE_BLOCK_ACROSS_YES = 0x00C0000000000000l;
  public static long UNIQUE_BLOCK_ACROSS_NO  = 0x00E0000000000000l;
  public static long DEDUP_NO_DEDUP          = 0x00F0000000000000l;
  public static long DEDUPSET_TYPE_MASK      = 0x0000FFFF00000000l;
  public static long DEDUPSET_NUMBER_MASK    = 0x00000000FFFFFFFFl;
  //public static long NO_COMPRESSION         = 0x8000000000000004l;
  //public static long RANDOM_LFSR            = 0x8000000000000005l;


  private static final long   MB  = 1024*1024l;


  public Object clone()
  {
    try
    {
      Dedup dedup           = (Dedup) super.clone();
      dedup.hot_dedup_parms = (int[]) hot_dedup_parms.clone();
      dedup.dedup_type      = dedup_type_seq++;
      return dedup;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  /**
   * This is static data, and belongs in Validate.
   * However, in the code it will read easier to get this via Dedup
   */
  public static boolean isDedup()
  {
    return Validate.isDedup();
  }
  public static int getDedupUnit()
  {
    return Validate.getDedupUnit();
  }

  public double getDedupPct()
  {
    return dedup_pct;
  }
  public boolean isDedupAcross()
  {
    return dedup_across;
  }
  public int getMaxKey()
  {
    return dedup_maxkey;
  }
  public double getAdjustedPct()
  {
    return dedup_adjusted;
  }
  public double getDedupRatio()
  {
    return dedup_ratio;
  }


  /**
   * Reporting all Dedup information.
   *
   * There is one big problem: though theoretically it is possible for each SD
   * or FSD to have its own unique Dedup instance, it is very unlikely that that
   * is the case.
   * It is therefore not (easily) possible to go to each DV_map and report the
   * current state related to flips or flops.
   * I therefore no longer will report that, though in the future I may do so
   * again.
   */
  public static void reportAllSdCounters()
  {
    HashMap <Dedup, Dedup> dedup_map = new HashMap(8);
    for (SD_entry sd : SD_entry.getRealSds(SlaveWorker.sd_list))
      dedup_map.put(sd.dedup, sd.dedup);

    for (Dedup dedup : dedup_map.keySet())
      dedup.reportCounters(null);
  }

  public static void reportAllFsdCounters()
  {
    HashMap <Dedup, Dedup> dedup_map = new HashMap(8);
    for (FwgEntry fwg : SlaveWorker.work.fwgs_for_slave)
      dedup_map.put(fwg.dedup, fwg.dedup);

    for (Dedup dedup : dedup_map.keySet())
      dedup.reportCounters(null);
  }

  public void reportCounters(DV_map dv_map)
  {
    long flips = 0;
    long flops = 0;
    long zeroes = 0;

    if (dv_map != null)
    {

      for (long i = 0; i < dv_map.key_blocks; i++)
      {
        int key = dv_map.dv_get(i * dv_map.getKeyBlockSize());
        if (key == 1)
          flips++;
        else if (key == 2)
          flops++;
        else
          zeroes++;
      }
    }

    int    dedupunit = getDedupUnit();
    long   total  = uniques_written + uniques_written_acr_no + duplicates_written;
    double result = 100. - (duplicates_written * 100. / total) + sets_pct;
    common.ptod("Dedup counters. Bytes written: %s across: %s (%.1f%%) on_sd: %s (%.1f%%) " +
                "dups: %s (%.1f%%) resulting dedup=%.1f%% (%.2f:1)",
                FileAnchor.whatSize(total * dedupunit),
                FileAnchor.whatSize(uniques_written * dedupunit),
                uniques_written * 100. / total,
                FileAnchor.whatSize(uniques_written_acr_no * dedupunit),
                uniques_written_acr_no * 100. / total,
                FileAnchor.whatSize(duplicates_written * dedupunit),
                duplicates_written * 100. / total,
                result, 100. / result);
    if (dv_map != null)
      common.ptod("Key=01: %6d Key=02: %6d zeroes: %d", flips, flops, zeroes);

    common.ptod("Dedup options: " + printDedupInfo());


    /* Shortcut for now: no info for hot sets and FSDs: */
    if (SlaveJvm.isWdWorkload())
      printHotCounts();

  }



  /**
   * Dedup setup for one specific block.
   *
   * - Unique yes/no based on DedupBitMap, which is on an SD/FSD level.
   * - Dedup_set based on total SD/FSD block# % dedup_sets_used
   * - Offset within (compression) pattern based on total SD/FSD block#%1048576
   *   for unique blocks, based on dedup_set for duplicates.
   *
   * The result of all this is used in vdb_dv.c.fill_buffer()
   *
   * Note that maybe this should be changed to do everything on an SD/FSD level
   * instead half of it on SD/FSD and the other half on TOTAL SD/SD.
   * The DV_map and DedupBitMap are on SD/FSD level, it's just the dedup_sets_used
   * that needs to be moved to the SD/FSD.
   * Once we do that we can even handle different dedup rations per SD/FSD.
   * (Just dreamin' here)  :-)
   *
   */
  /**
   * Get the proper compression and dedup info for each key block.
   *
   * Since the data pattern and/or dedup status is determined on a key block
   * level, we need to calculate this for each key block and not just for the
   * whole block.
   */
  public void dedupFsdBlock(ActiveFile afe)
  {
    if (!Dedup.isDedup())
      common.failure("Is this an illegal call?");

    int    dedup_unit    = getDedupUnit();
    KeyMap key_map       = afe.getKeyMap();
    FileAnchor anchor    = afe.getFileEntry().getAnchor();
    long   anchor_offset = afe.getFileEntry().getAnchor().relative_dedup_offset;
    int    key_count     = key_map.getKeyCount();
    long   compression;
    long   dedup_set;

    /* Do calculations for each Key: */
    for (int i = 0; i < key_count; i++)
    {
      /* Uniqueness must be calculated using 'dedupunit=' boundaries: */
      // some of this stuff maybe should be put in keymap?
      long pattern_lba = afe.getFileEntry().getFileStartLba() +
                         afe.next_lba + key_map.getKeyBlockSize() * i;
      long pattern_blk = pattern_lba / dedup_unit;
      int  rel_block   = (int) ((pattern_lba / dedup_unit) + anchor_offset / dedup_unit);
      boolean unique   = dedup_pct == 100 || anchor.getDedupBitMap().isUnique(pattern_blk);


      /* Uniqueness must be calculated using 'dedupunit=' boundaries: */
      //long pattern_lba  = key_map.pattern_lba + dedup_unit * i;
      //long pattern_blk  = pattern_lba / dedup_unit;
      //int  rel_block    = (int) (pattern_blk + sd.relative_dedup_offset / dedup_unit);
      //boolean unique    = dedup_pct == 100. || cmd.sd_ptr.dedup_bitmap.isUnique((int) pattern_blk);



      /* If this is a unique block */
      if (unique)
      {
        /* With 'compression only' we come here too: */
        // see isDedup() call above ???
        if (!isDedup())
          dedup_set = DEDUP_NO_DEDUP;
        else if (dedup_across)
          dedup_set = UNIQUE_BLOCK_ACROSS_YES;
        else
          dedup_set = UNIQUE_BLOCK_ACROSS_NO;

        /* Set compression offset.                                          */
        compression = getUniqueCompressionOffset(pattern_lba);
      }

      /* For a duplicate, compression offset and set number are related: */
      else
      {
        dedup_set   = rel_block % dedup_sets_used;
        compression = getDuplicateCompressionOffset(dedup_set);
      }

      /* For Data Validation writes, add the key to the set: */
      // we would not be here without Validate
      //if (Validate.isValidate())
      dedup_set |= ((long) key_map.getKeys()[i] << 56);
      dedup_set |= (dedup_type << 32);

      //common.ptod("dedup_set: %016x %d", dedup_set, dedup_type);

      key_map.getCompressions()[i] = compression;
      key_map.getDedupsets()[i]    = dedup_set;

      //if (false)
      //  common.ptod("dedupFsdBlock: %s flba: %08x dlba: %08x set: %08x comp: %08x kbs: %d unit: %d uniq: %-5b setsu: %d",
      //              afe.getFileEntry().getShortName(), afe.next_lba,
      //              dedup_lba, dedup_set, compression, key_map.getKeyBlockSize(),
      //              dedup_unit, unique, dedup_sets_used);
    }
  }


  /**
   * Set compression offset for a (key) block.
   * (Data pattern is a minimum of 1mb).
   *
   * THIS CODE ALSO RESIDES IN JNI!!!!!!
   */
  public static long getUniqueCompressionOffset(long lba)
  {
    long compression = lba % (Patterns.getBufferSize());
    //common.ptod("getUniqueCompressionOffset: lba: 0x%08x comp: 0x%08x %d",
    //            lba, compression, Patterns.getPattern().length);
    return compression;
  }
  private static long getDuplicateCompressionOffset(long dedup_set)
  {
    long compression  = (dedup_set * 347) << 2;
    compression      &= (MB -1);
    return compression;
  }


  /**
   * Prepare this block to be written with it's proper dedup and compression
   * information.
   *
   * Remember, from KeyMap: the data pattern is based on mutliples of dedupunit,
   * but the data block ultimately written can be a subset of this.
   */
  public void dedupSdBlock(KeyMap key_map, Cmd_entry cmd)
  {
    int dedup_unit = getDedupUnit();
    SD_entry sd    = cmd.sd_ptr;
    int  key_count = key_map.getKeyCount();
    long compression;
    long dedup_set;

    if (!Dedup.isDedup())
      common.failure("Is this an illegal call?");

    /* Do calculations for each Key: */
    for (int i = 0; i < key_count; i++)
    {
      /* Uniqueness must be calculated using 'dedupunit=' boundaries: */
      long pattern_lba  = key_map.pattern_lba + dedup_unit * i;
      long pattern_blk  = pattern_lba / dedup_unit;
      long rel_block    = pattern_blk + sd.sdd.rel_byte_start / dedup_unit;
      boolean unique    = dedup_pct == 100. || sd.sdd.uniques_bitmap.isUnique(pattern_blk);

      /* The first hot blocks may of course NOT be unique: */
      // this has been taken care of in the bitmap already?
      //if (unique_block && rel_block < hot_dedup_blocks)
      //  common.ptod("rel_block: " + rel_block);

      /* If this is a unique block */
      if (unique)
      {
        /* With 'compression only' we come here too: */
        // Sure?
        if (!isDedup())
          dedup_set = DEDUP_NO_DEDUP;
        else if (dedup_across)
          dedup_set = UNIQUE_BLOCK_ACROSS_YES;
        else
          dedup_set = UNIQUE_BLOCK_ACROSS_NO;

        /* Set compression offset. */
        compression = getUniqueCompressionOffset(pattern_lba);
      }

      /* For a duplicate, compression offset and set number are related: */
      else
      {
        dedup_set   = sd.sdd.translateDuplicateBlockToSet(rel_block);
        compression = getDuplicateCompressionOffset(dedup_set);
      }



      /* Dedup does not use a 'key', it has a 'flipflop': */
      int key    = key_map.getKeys()[i];
      dedup_set |= ((long) key << 56);
      dedup_set |= ((long) dedup_type << 32);

      //common.ptod("dedup_type: " + dedup_type);

      // quick test: */
      //if (cmd.sd_ptr.sd_name.equals("sd1"))
      //  common.ptod("dedup_set sd1: %016x %d", dedup_set, dedup_type);
      //else
      //  common.ptod("dedup_set sd2: %016x %d", dedup_set, dedup_type);

      key_map.getCompressions()[i] = compression;
      key_map.getDedupsets()[i]    = dedup_set;

      //if (isDuplicate(dedup_set) && getKey(dedup_set) > 0)
      //  common.failure("not allowed: %016x", dedup_set);

      //common.ptod("dedup_set: %016x", dedup_set);

      //if (false)
      ////if (cmd.cmd_lba == 16384)
      //{
      //  common.ptod("dedupSdBlock: %s "+
      //              "flba: 0x%08x "+
      //              "dlba: %08x "+
      //              "set: %016x "+
      //              "comp: %08x "+
      //              "kbs: %d "+
      //              "unit: %d "+
      //              "uniq: %-5b "+
      //              "setsu: %d",
      //              cmd.sd_ptr.sd_name,
      //              cmd.cmd_lba,
      //              dedup_lba,
      //              dedup_set,
      //              compression,
      //              key_map.getKeyBlockSize(),
      //              dedup_unit,
      //              unique_block,
      //              dedup_sets_used);
      //}
    }
  }




  /**
   * Adjust dedup info for ONE Dedup instance.
   * No longer are we treating everything as one HUGE set of dedupable data, we
   * now have two types:
   * - The default Dedup puts them all in one big bucket.
   * - Individual SD level Dedup is unique its own.
   *
   */
  public void adjustSdDedupValue()
  {
    int dedup_unit = getDedupUnit();

    /* Calculate block count and #dedupsets: */
    double blocks = (total_size / dedup_unit);
    double sets   = (dedup_sets_reqd > 0) ? dedup_sets_reqd :
                    blocks * Math.abs(dedup_sets_reqd) / 100;

    /* Create an adjusted dedup%: */
    dedup_adjusted  = Math.max((dedup_pct - (sets * 100. / blocks)), 0);
    dedup_adjusted  = (dedup_pct == 100) ? 100 : dedup_adjusted;
    dedup_sets_used = Math.max(1, (long) sets);
    unit_blocks = (long) blocks;
    sets_pct    = sets * 100 / blocks;

    unique_blocks    = (long) (blocks * dedup_adjusted / 100.);
    duplicate_blocks = unit_blocks - unique_blocks;

    if (hotflop && hot_dedup_parms.length == 0)
      common.failure("Requesting 'dedupflipflop=hot' without specifying 'deduphotsets=nnn'");


    /*
    common.ptod("dedup_adjusted: " + dedup_adjusted);
    dedup_adjusted = (long) (unique_blocks * 100 / blocks);
    common.ptod("dedup_adjusted: " + dedup_adjusted);
    */

    if (sets > blocks)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("adjustDedupValue(): requesting more dedupsets (%.0f) than " +
                     "we have data blocks (%.0f, estimated)", sets, blocks);
    }

    if (sets > blocks * dedup_pct / 100)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("adjustDedupValue(): requesting more dedupsets (%.0f) than " +
                     "we have non-unique data blocks (%.0f, estimated)",
                     sets, blocks * dedup_pct / 100);
    }

    common.plog("Dedup options: " + printDedupInfo());

    //slaveLevelSetup(false);



    /* Calculate a total of the amount of blocks in hot sets: */
    hot_dedup_blocks = 0;
    hot_dedup_sets   = 0;
    for (int i = 0; i < hot_dedup_parms.length; i+=2)
    {
      hot_dedup_sets   += hot_dedup_parms[i];
      hot_dedup_blocks += hot_dedup_parms[i] * hot_dedup_parms[i+1];
    }

    if (hot_dedup_blocks > 1000000)
      common.failure("More than one million hot dedup blocks: " + hot_dedup_blocks);

    if (dedup_sets_used <= hot_dedup_sets)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("You do not have enough dedup sets. You are requested more "+
                     "hot dedup sets (%d) than you have dedup sets (%d)",
                     hot_dedup_sets, dedup_sets_used);
    }
  }


  public static void adjustFsdDedupValues(Vector <FwgEntry> fwgs_for_rd)
  {
    HashMap <Dedup, Dedup> dedup_map = new HashMap(8);

    for (FwgEntry fwg : fwgs_for_rd)
      dedup_map.put(fwg.dedup, fwg.dedup);

    for (Dedup dedup : dedup_map.values().toArray(new Dedup[0]))
      dedup.adjustFsdDedupValue();
  }

  /**
  * The dedup= value given needs to be adjusted for the amount of dedupsets.
  * For instance, if we ask for dedup=50 and dedupsets=10%, then the amount of
  * unique blocks will have to be 40% (adding dedupsets=10% then makes again for
  * 50).
  */
  public void adjustFsdDedupValue()
  {
    int dedup_unit = getDedupUnit();

    /* This 'total size' code depends on the order of FSDs not changing */
    /* when doing journal recovery. Sort them first                     */
    ArrayList <String> names = new ArrayList(16);
    for (FileAnchor anchor : FileAnchor.getAnchorList())
      names.add(anchor.getAnchorName());
    Collections.sort(names);

    // We have a problem here:
    // The sizes used below are ESTIMATES, and the estimates may be off if we
    // use variable file sizes using a randomizer.
    // This must be fixed in the future by no longer doing an ESTIMATE, but
    // by indeed just doing the proper file size calculations
    // which are currently only done on the slave during createFileList().
    // (There is a check in FsdEntry.readParms() to avoid this problem for now)
    //
    // BTW: this code here runs on the MASTER.

    /* Calculate total size of all the anchors and set relative starting lba for each: */
    total_size = 0;
    for (String name : names)
    {
      FileAnchor anchor = FileAnchor.findAnchor(name);
      anchor.relative_dedup_offset = total_size;
      total_size += anchor.bytes_in_file_list;
    }

    /* Calculate block count and #dedupsets: */
    double blocks = (total_size / dedup_unit);
    double sets   = (dedup_sets_reqd > 0) ? dedup_sets_reqd : blocks * Math.abs(dedup_sets_reqd) / 100;

    /* Create an adjusted dedup%: (why rounded?) */
    dedup_adjusted  = Math.max(Math.round(dedup_pct - (sets * 100. / blocks)), 0);
    dedup_adjusted  = Math.max((dedup_pct - (sets * 100. / blocks)), 0);
    dedup_sets_used = (long) Math.max(sets, 1);
    unit_blocks = (long) blocks;
    sets_pct    = sets * 100 / blocks;

    unique_blocks    = (long) (blocks * dedup_adjusted / 100);
    duplicate_blocks = unit_blocks - unique_blocks;

    if (sets > blocks)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("adjustDedupValue(): requesting more dedupsets (%.0f) than " +
                     "we have data blocks (%.0f, estimated)", sets, blocks);
    }

    if (sets > blocks * dedup_pct / 100)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("adjustDedupValue(): requesting more dedupsets (%.0f) than " +
                     "we have non-unique data blocks (%.0f, estimated)",
                     sets, blocks * dedup_pct / 100);
    }

    if (dedup_sets_used < hot_dedup_sets)
    {
      common.ptod("Dedup options: " + printDedupInfo());
      common.failure("You do not have enough dedup sets. You are requested more "+
                     "hot dedup sets (%d) than you have dedup sets (%d)",
                     hot_dedup_sets, dedup_sets_used);
    }

    common.plog("Dedup options: " + printDedupInfo());
  }


  /**
   * This gets called from either:
   * - MiscParms
   * - SD_entry
   * - FSD_entry
   *
   */
  public void parseDedupParms(Vdb_scan prm, boolean dflt)
  {
    boolean debug  = false;
    String keyword = prm.keyword;


    /* Newer dedup parameter setup:                                              */
    /* I decided that during experimentation, both for me and for the user,      */
    /* allowing commented out single-line parameters is much more user friendly. */
    /* Allowing BOTH just gets too confusing!                                    */
    /* I'll keep the code here though; the 'extractXXX() code may be useful.     */
    if (keyword.equals("xxxdedup"))
    {
      /* ANY dedup parameter ACTIVATES Dedup: */
      Validate.setDedup();

      /* Split subparameters, e.g. dedup=(flipflop=hotsets,ratio=3.5,hotsets=(2,2,10,4)) */
      ArrayList <String> parsed = Vdb_scan.splitRawParms(prm.raw_value);

      for (String parm : parsed)
      {
        if (debug) common.ptod("parm: " + parm);
        keyword = parm.trim().split("=+")[0];
        if (debug) common.ptod("keyword: " + keyword);

        //if (keyword.equals("dedup"))
        //  continue;

        if (keyword.equals("flipflop"))
        {
          String value = Vdb_scan.extractpair(parm).toLowerCase();
          if (value.startsWith("y"))
            flipflop = true;
          else if (value.startsWith("n"))
            flipflop = false;
          else if (value.startsWith("hot"))
            flipflop = hotflop = true;
          else
            common.failure("Dedup parameter scan: unexpected input: " + parm);
          continue;
        }

        if (keyword.equals("ratio"))
        {
          dedup_ratio =  Vdb_scan.extractDouble(Vdb_scan.extractpair(parm));
          dedup_pct   = (100. / dedup_ratio);
          if (dedup_ratio < 1)
            common.failure("Minimum value for dedupratio=%d is dedupratio=1",
                           dedup_ratio);
          continue;
        }

        if (keyword.equals("unit"))
        {
          if (!dflt)
            common.failure("'unit=' may not be specified as an SD or FSD parameter");
          Validate.setDedupUnit(Vdb_scan.extractInt(Vdb_scan.extractpair(parm)));
          continue;
        }

        if (keyword.equals("hotsets"))
        {
          any_hotsets_requested = true;
          String value = Vdb_scan.extractpair(parm).toLowerCase();
          double[] doubles = Vdb_scan.extractDoubles(value);

          if (doubles.length % 2 != 0)
            common.failure("Specifying deduphotsets= must be done in pairs "+
                           "(10 sets of 3 blocks, hotsets=(10,3,. . . .)");
          hot_dedup_parms = new int[ doubles.length ];
          for (int i = 0; i < doubles.length; i++)
          {
            hot_dedup_parms[i] = (int) doubles[i];
            if (hot_dedup_parms[i] == 0)
              common.failure("hotsets: no 'zero' value allowed in parameters");
          }
          continue;
        }

        if (keyword.equals("sets"))
        {
          dedup_sets_reqd = Vdb_scan.extractLong(Vdb_scan.extractpair(parm));
          continue;
        }

        else
          common.failure("Unexpected dedup parameter: " + parm);
      }


      //common.ptod("flipflop: " + flipflop);
      //common.ptod("hotflop:  " + hotflop);
      //common.ptod("dedup_ratio: " + dedup_ratio);
      //common.ptod("Validate.getDedupUnit: " + Validate.getDedupUnit());
      //System.exit(777);

      return;
    }



    /* ANY dedup parameter ACTIVATES Dedup: */
    Validate.setDedup();

    if (keyword.equals("dedupratio"))
    {
      dedup_ratio = prm.getDouble();
      dedup_pct   = (100. / dedup_ratio);
      if (dedup_ratio < 1)
        common.failure("Minimum value for dedupratio=%d is dedupratio=1",
                       dedup_ratio);
    }

    else if (keyword.equals("dedupunit"))
    {
      if (!dflt)
        common.failure("'dedupunit=' may not be specified as an SD or FSD parameter");
      Validate.setDedupUnit(prm.getInt());
    }

    else if (keyword.equals("dedupacross"))
      dedup_across = prm.getString().toLowerCase().startsWith("y");

    else if (keyword.equals("dedupmaxkey"))
      dedup_maxkey = prm.getInt();

    /* If we only have ONE integer, use all the defaults. (5)       */
    /* otherwise, pick up the matrix and keep using the 5% default. */
    else if (keyword.equals("dedupsets"))
    {
      if (prm.num_count == 1)
      {
        dedup_sets_reqd = prm.getInt();
        if (dedup_sets_reqd == 0)
          common.failure("'dedupsets=' must be a minimum of one: " + dedup_sets_reqd);
      }
    }

    else if (keyword.equals("deduphotsets"))
    {
      any_hotsets_requested = true;
      if (prm.num_count % 2 != 0)
        common.failure("Specifying deduphotsets= must be done in pairs "+
                       "(10 sets of 3 blocks, hotsets=(10,3,. . . .)");
      hot_dedup_parms = new int[ prm.num_count ];
      for (int i = 0; i < prm.num_count; i++)
      {
        hot_dedup_parms[i] = (int) prm.numerics[i];
        if (hot_dedup_parms[i] == 0)
          common.failure("hotsets: no 'zero' value allowed in parameters");
      }
    }

    else if (keyword.equals("xxdedupseed"))
      dedup_seed = prm.getLong();

    else if (keyword.equals("dedupflipflop"))
    {
      String value = prm.alphas[0];
      if (value.startsWith("y"))
        flipflop = true;
      else if (value.startsWith("n"))
        flipflop = false;
      else if (value.startsWith("hot"))
        flipflop = hotflop = true;
      else
        common.failure("Dedup parameter scan: unexpected input: " + value);
    }

    else
      common.failure("Invalid dedupxxx parameter: " + keyword);
  }


  public boolean isFlipFlop()
  {
    return flipflop;
  }
  public boolean isHotFlop()
  {
    return hotflop;
  }
  public boolean isHotSet(long set)
  {
    long set_number = set & DEDUPSET_NUMBER_MASK;
    if (set_number < hot_dedup_sets)
      return true;
    else
      return false;
  }

  /**
   * flipflop has three possibilities:
   * - no
   * - yes
   * - for hot sets only
   */
  public boolean xxisFlipFlop(long dedup_set)
  {
    if (!flipflop)
      return false;

    // I may have a problem with REAL DV????

    if (hotflop)
    {
      /* Hot flop is only for duplicates, ignore uniques: */
      if ( (dedup_set & UNIQUE_MASK) == UNIQUE_BLOCK_ACROSS_YES)
        return false;

      /* And of course only for hot sets: */
      long set_number = dedup_set & DEDUPSET_NUMBER_MASK;
      if (set_number < hot_dedup_sets)
        return true;
      else
        return false;
    }
    else
      return true;
  }


  public void checkDedupParms()
  {
    int dedup_unit = getDedupUnit();

    if (!isDedup())
      return;

    if (dedup_unit == 0)
      common.failure("Requesting Dedup without specifying dedupunit=nnn");

    if (dedup_unit % 512 != 0)
      common.failure("dedupunit= value must be a multiple of 512: " + dedup_unit);

    // It should not be needed, but when the dedupunit= is specified as
    // a general parameter, code also asks for dedupratio=, which we could
    // possibly find in the sd-level dedup set of parameters.
    // Just let it be!
    if (dedup_pct == Double.MIN_VALUE)
      common.failure("dedupratio= parameter is required. ");

    if (dedup_pct < 0 || dedup_pct > 100)
      common.failure("Dedup rate must be between 0 and 100: " + dedup_pct);

    /* Dedupsets default is 5% */
    if (dedup_sets_reqd == Long.MAX_VALUE)
      dedup_sets_reqd = -5;

    if (Validate.isRealValidate() && !flipflop)
    {
      BoxPrint.printOne("Data Validation with Dedup requires 'dedup=flipflop'. Flipflop now activated.");
      flipflop = true;
    }
  }


  /**
   * Maintain dedup-write statistics.
   *
   * There is also a set of statistics to count how often a hot dedupset
   * goes from all flips to all flops.
   * This logic depends on the actual key setting in the DV_map to be OLD.
   * This then allows the code below to determine whether:
   * - the key=1 write was a switch from key=0
   * - the key=1 write was a switch from key=2
   * This then eliminates the need for the DedupSets statistics logic to
   * remember what the state is of each hot set and block.
   */
  public synchronized void countDedup(KeyMap key_map, DV_map dvmap)
  {
    int dedup_unit = getDedupUnit();
    int key_count  = key_map.getKeyCount();
    int last_set = -1;

    /* The key-count here identifies the # of keys when xfersize is  */
    /* a multiple of dedupunit. Info therefore is unreliable if not. */
    for (int i = 0; i < key_count; i++)
    {
      long set = key_map.getDedupsets()[i];
      //common.ptod("set: %016x %016x", key_map.getDedupsets()[i], set);

      if (isUnique(set))
      {
        if ( ( set & UNIQUE_MASK) == UNIQUE_BLOCK_ACROSS_YES)
        {
          uniques_written ++;
          //common.ptod("uniques_written_across: %8d %8d", uniques_written_across, dedup_unit);
        }
        else
          uniques_written_acr_no ++;
      }

      else
      {
        duplicates_written ++;
        //common.ptod("duplicates_written: %8d %8d", duplicates_written, dedup_unit);

        /* Set statistics are only maintained for the hot sets: */
        int set_number = dvmap.getDedup().getSet(set);


        /* if this is a hot set we update statistics for this set ONCE: */
        //if (set_number < hot_dedup_sets) // && set_number != last_set)
        if (isHotSet(set) && set_number != last_set)
        {
          hot_set_stats.get(set_number).addStatistics(dvmap);
          last_set = set_number;
        }
      }
    }
  }


  private static ArrayList <String> lines = new ArrayList(25);
  public String printDedupInfo()
  {
    int dedup_unit = getDedupUnit();
    lines.clear();
    String txt = "";
    String flip = "no";
    if (isHotFlop())
      flip = "hot";
    else if (isFlipFlop())
      flip = "yes";

    addLine("dedup_type",       "%,15d",  dedup_type);
    addLine("dedup_ratio",      "%15.2f", dedup_ratio);
    addLine("dedup_unit",       "%,15d",  dedup_unit);
    addLine("flipflop",         "%15s",   flip);
    addLine("hot_dedup_sets",   "%,15d",  hot_dedup_sets);
    addLine("hot_dedup_blocks", "%,15d",  hot_dedup_blocks);
    if (dedup_sets_reqd < 0)
      addLine("dedup_sets_reqd", "%15s", (dedup_sets_reqd*-1 + "%"));
    else
      addLine("dedup_sets_reqd", "%,15d", (dedup_sets_reqd));
    addLine("dedup_sets_used",  "%,15d",  dedup_sets_used);

    lines.add("");
    addLine("unique_blocks",    "%,15d",  unique_blocks);
    addLine("duplicate_blocks", "%,15d (including %,d originals)",  duplicate_blocks, dedup_sets_used);



    addLine("dedup_pct",        "%15.2f", dedup_pct);
    addLine("dedup_adjusted",   "%15.2f", dedup_adjusted);
    addLine("total_size",       "%15s",   FileAnchor.whatSize(total_size));
    addLine("unit_blocks",      "%,15d",  unit_blocks);


    //addLine("dedup_across",     "%15b", dedup_across);

    lines.add("\nInfo only reliable if xfersize is multiple of dedupunit:");

    addLine("uniques_written",    "%,15d; expected %,d; delta %,d ",
            uniques_written, unique_blocks, uniques_written - unique_blocks);
    addLine("duplicates_written", "%,15d; expected %,d; delta %,d ",
            duplicates_written, duplicate_blocks, duplicates_written - duplicate_blocks);
    addLine("Total writes",       "%,15d; delta %,d",
            uniques_written + duplicates_written, uniques_written + duplicates_written - unit_blocks);

    addLine("flipflops",          "%,15d", flipflops);
    //addLine("est_sets_pct",       "%15.2f", est_sets_pct);


    /* Note that rounding issues can cause a MINOR delta with the requested ratio */
    double calc_ratio  =  ((double) unit_blocks / (unique_blocks + dedup_sets_used));
    double calc_ratio2 =  ((double) unit_blocks / (unique_blocks + dedup_sets_used * 2));
    double calc_ratio3 =  ((double) unit_blocks / (unique_blocks + dedup_sets_used + hot_dedup_sets));

    lines.add("");
    addLine("Dsim counts: Hash size",    "%,15d (%,d uniques + %,d sets)",
            (unique_blocks + dedup_sets_used),
            unique_blocks, dedup_sets_used);
    addLine("Dsim counts: Duplicates", "%,15d", (duplicate_blocks - dedup_sets_used));
    addLine("Dsim counts: Ratio",      "%15.2f (%.5f)", calc_ratio, calc_ratio);

    if (isFlipFlop())
    {
      if (!isHotFlop())
      {
        addLine("Dsim counts: Max hash size possible due to flipflop", "%,d (unique_blocks + dedup_sets_used * 2)",
                unique_blocks + dedup_sets_used*2);
        addLine("Dsim counts: Min ratio possible due to flipflop", "%.2f (%.5f)", calc_ratio2, calc_ratio2);
      }

      else
      {
        addLine("Dsim counts: Max hash size possible due to hotflop", "%,d (unique_blocks + dedup_sets_used + hot_dedup_sets)",
                unique_blocks + dedup_sets_used + hot_dedup_sets);
        addLine("Dsim counts: Min ratio possible due to hotflop", "%.2f (%.5f)", calc_ratio3, calc_ratio3);
      }
    }

    lines.add("");


    String tmp = "";
    for (String line : lines)
      tmp += "\n" + line;

    return tmp;
  }

  public static void addLine(String label, String format, Object ... args)
  {
    //common.ptod("label: " + label);
    //common.ptod("format: " + format);
    String tmp = String.format("%-24s ", label + ":");
    //common.ptod("tmp: " + tmp);
    tmp = String.format(tmp + format, args);
    lines.add(tmp);
  }



  /**
   * If dedup is used, check some info
   */
  public static void checkSdDedup()
  {
    if (!Validate.isDedup())
      return;

    /* Sort the SDs to make sure they are used in the same order as */
    /* they have been in an earlier journal creation run:           */
    SD_entry[] sds = Vdbmain.sd_list.toArray(new SD_entry[0]);
    Arrays.sort(sds, new SdSort());

    /* Each SD either uses his OWN Dedup instance or gets to use the default: */
    for (SD_entry sd : sds)
    {
      if (sd.dedup == null)
        sd.dedup = Dedup.dedup_default;
      //else if (Validate.sdConcatenation())
      //  common.failure("SD specific Dedup currently not allowed for concatenated SDs");

      sd.dedup.checkDedupParms();
    }
  }

  public static void checkFsdDedup()
  {
    if (!Validate.isDedup())
      return;

    /* Sort the SDs to make sure they are used in the same order as */
    /* they have been in an earlier journal creation run:           */
    FsdEntry[] fsds = FsdEntry.getFsdList().toArray(new FsdEntry[0]);
    Arrays.sort(fsds, new FsdSort());

    /* Each SD either uses his OWN Dedup instance or gets to use the default: */
    for (FsdEntry fsd : fsds)
    {
      if (fsd.dedup == null)
        fsd.dedup = Dedup.dedup_default;

      fsd.dedup.checkDedupParms();
    }
  }


  /**
   * A LUN or file's lba must be translated to a dedupset number.
   *
   */
  public long translateBlockToSet(long rel_block)
  {
    long set = ((rel_block) % (dedup_sets_used - hot_dedup_sets)) + hot_dedup_sets ;


    //common.ptod("set: %4d %4d %4d", rel_block, set, dedup_sets_used);

    // we should never return anything less than the hot_blocks count!
    //  if (set == 0)
    //  {
    //    common.ptod("rel_block:        " + rel_block);
    //    common.ptod("hot_dedup_blocks: " + hot_dedup_blocks);
    //    common.ptod("dedup_sets_used:  " + dedup_sets_used);
    //    common.ptod("hot_dedup_sets:   " + hot_dedup_sets);
    //    common.ptod("rel_block - hot_dedup_blocks:     " + (rel_block - hot_dedup_blocks));
    //    common.ptod("dedup_sets_used - hot_dedup_sets: " + (dedup_sets_used - hot_dedup_sets));
    //    common.ptod("xxxxxx:                           " + ((rel_block - hot_dedup_blocks) % (dedup_sets_used - hot_dedup_sets)));
    //
    //    common.failure("oops?");
    //  }
    //
    //common.ptod("translateBlockToSet: %6d %6d", rel_block, set);

    if (set >= dedup_sets_used)
      common.failure("translateBlockToSet: set# %d >= %d", set, dedup_sets_used);
    if (set < 0)
      common.failure("translateBlockToSet: negative set#: %d", set);

    return set;
  }


  /**
   * High level info about Dedup is created by the Master, low level detail
   * however has to be created on the slaves to eliminate the need to pass it
   * from the Master via the java sockets.
   *
   * A little bit of stuff will be done on the master to allow quick generation
   * of error messages though.
   */
  public static void slaveLevelSetup(Vector <SD_entry> sd_list)
  {
    /* When manipulating all this make sure that we always keep SDs   */
    /* in the same sorted order. This is needed for journal recovery  */
    /* and  keeps relative offsets in tact.                           */
    SD_entry[] sorted_sds = SD_entry.getRealSds(sd_list);

    /* Create a map with all Dedup instances: */
    HashMap <Dedup, Dedup> used_dedup_map = new HashMap(4);
    for (SD_entry sd : sorted_sds)
      used_dedup_map.put(sd.dedup, sd.dedup);


    /* Now loop through all of those:                */
    /* (There is usually one, but there can be more) */
    for (Dedup dedup : used_dedup_map.values())
    {
      /* We need a map for hot block (OK to be zero) */
      dedup.hot_blocks = DedupSet.createHotBlockArray(dedup);

      /* Also need some statistics (optional) */
      dedup.hot_set_stats = DedupSet.createHotSetList(dedup);

      //common.ptod(dedup.printDedupInfo());

      /* Read the possible old DV_map and set the newly created statistics: */
      /* Note that the call to addStatistics() expects the OLD key, while   */
      /* the DV_map has the current NEW key:                                */
      //for (int block = 0; block < hot_dedup_blocks; block++)
      //{
      //  int set = (int) translateLbaToSet(block);
      //  int  key = dvmap.dv_get(block * dvmap.getBlockSize());
      //  if (key == 1)
      //    stats_list.get(set).addStatistics(0);
      //  else if (key == 2)
      //    stats_list.get(set).addStatistics(1);
      //}

      /* Tell hotband to keep writes at the start of the LUN: */
      if (dedup.hot_dedup_sets > 0)
        HotBand.doNotAddOnePct();


      /* Take all the hotsets and round-robin those to each SD. */
      /* But first I need to know WHICH SDs use this Dedup instance: */
      ArrayList <SD_entry> dedup_users = new ArrayList(8);
      for (SD_entry sd : sorted_sds)
      {
        if (sd.dedup == dedup)
          dedup_users.add(sd);
      }

      int sd_index = 0;
      for (int i = 0; i < dedup.hot_blocks.length; i++)
      {
        int set = dedup.hot_blocks[i];
        SD_entry sd = dedup_users.get( sd_index++ % dedup_users.size() );
        sd.sdd.receiveHotset(set);
      }
    }


    /* Read the possible old DV_map and set the newly created statistics: */
    /* Note that the call to addStatistics() expects the OLD key, while   */
    /* the DV_map has the current NEW key:                                */
    for (SD_entry sd : sorted_sds)
    {
      DV_map dvmap = sd.dv_map;
      Dedup  dedup = sd.dedup;
      //for (int block = 0; block < dedup.hot_dedup_blocks; block++)
      //{
      //  int set = (int) dedup.translateLbaToSet(block);
      //  int key = dvmap.dv_get(block * dvmap.getBlockSize());
      //  if (key == 1)
      //    dedup.hot_stats_list.get(set).addStatistics(dvmap);
      //  else if (key == 2)
      //    dedup.hot_stats_list.get(set).addStatistics(dvmap);
      //}


      for (DedupSet set : dedup.hot_set_stats)
      {
        set.addStatistics(dvmap);
        set.references = 0;
        set.switches   = 0;
      }
      //common.where();
    }

  }


  /**
   * Print out hot set statistics.
   *
   * Note that if a smaller block is written that the dedupunit=size the counts
   * will not be valid.
   * e.g.: 8*1024 writes to dedupunit=8k count 8 times...... and 'switches' also
   * will be wrong.
   *
   * This is mainly a debugging tool when everything is 'perfect'.
   */
  private void printHotCounts()
  {
    long total = 0;
    for (DedupSet set : hot_set_stats)
      total += set.references;

    double cum_pct = 0;
    for (DedupSet set : hot_set_stats)
    {
      double pct = set.references * 100. / total;
      cum_pct += pct;
      common.ptod("printHotCounts(): set %5d; blocks: %4d; switches %5d; references: %6d pct: %5.2f  %6.2f",
                  set.set_number, set.blocks_in_set,
                  set.switches, set.references, pct, cum_pct);
    }
    if (total > 0)
      common.ptod("printHotCounts(): total references: %,d", total);
  }


  //public static long UNIQUE_BLOCK_MASK       = 0x0080000000000000l;
  public static boolean isUnique(long set)
  {
    return( ( set & UNIQUE_BLOCK_MASK) != 0);
  }
  public static boolean isDuplicate(long set)
  {
    return( ( set & UNIQUE_BLOCK_MASK) == 0);
  }
  public static String xlate(long set)
  {
    return(isUnique(set)) ? "unq" : "dup";
  }
  public static int getKey(long set)
  {
    return(int) (set >> 56);
  }
  public static int getSet(long set)
  {
    return(int) set;
  }


  public static void main(String[] args)
  {
    long dedup_unit      = 4096;
    long dedup_sets_used = 161061169;
    long hot_dedup_sets  = 0;
    long MB              = 1024*1024l;
    long GB              = 1024*1024*1024l;

    HashMap <Long, Long> dupmap = new HashMap(10240);

    for (long lba = 0*GB; lba < 8200*GB; lba+=dedup_unit)
    {
      long rel_block = lba / dedup_unit;
      long set = ((rel_block) % (dedup_sets_used - hot_dedup_sets)) + hot_dedup_sets ;

      //if (dupmap.put(set, set) != null)
      if (set == 0)
        common.ptod("duplicate: lba: %,20d %,16d", lba, set);
      //if (lba % GB == 0)
      //  common.ptod("lba: %,16d", lba / GB);

    }
  }
  public static void main3(String[] args)
  {
    int[] set_sizes  = new int[]
    {
      10,2,     // Default total 20 + 60 + 120 = 200 hot blocks
      20,3,
      30,4
    };

    /* Calculate a total of the amount of blocks in hot sets: */
    long hot_dedup_blocks = 0;
    long hot_dedup_sets   = 0;
    for (int i = 0; i < set_sizes.length; i+=2)
    {
      hot_dedup_sets   += set_sizes[i];
      hot_dedup_blocks += set_sizes[i] * set_sizes[i+1];
    }

    long[] dedicates = new long[ (int) hot_dedup_blocks ];

    int block_index = 0;
    int set_number  = 0;
    for (int i = 0; i < set_sizes.length; i+=2)
    {
      for (int x = 0; x < set_sizes[ i ]; x++)
      {
        for (int y = 0; y < set_sizes[ i+1 ]; y++)
        {
          dedicates[ block_index++ ] = set_number;
        }
        set_number++;
      }
    }


    for (int i = 0; i < dedicates.length; i++)
      common.ptod("dedicates: block: %3d set: %3d", i, dedicates[i]);

    //top:
    //for (int a = 0; a < 40; a++)
    //{
    //
    //  long  dedup_unit = 1024;
    //  //                            nn sets with mm blocks
    //  //                            10 sets of 2, 20 sets of 3, .....
    //
    //  long requested_block = a;
    //
    //  long pair_block_start = 0;
    //  long pair_block_end   = 0;
    //  long pair_set         = 0;
    //
    //  long set_start        = 0;
    //  for (int i = 0; i < set_sizes.length; i+=2)
    //  {
    //    pair_block_end += (set_sizes[i] * set_sizes[i+1]);
    //
    //    if (requested_block < pair_block_end)
    //    {
    //      common.ptod("set for block: %,6d: set# %d ", requested_block, i / 2);
    //      continue top;
    //    }
    //    pair_block_start += set_sizes[i+1];
    //    pair_set         += set_sizes[i  ];
    //  }
    //
    //  common.ptod("not found, ending loop");
    //  return;
    //}
  }


  /**
   * vdbench Vdb.Dedup dir1 dir2 file -u 128k -s nn
   *    * For now this only handles dedup=100 compression=100 sets=1
   */
  public static void main2(String[] args)
  {
    Getopt g = new Getopt(args, "u:s:", 99);
    g.print("Dedup");

    String dir1  = g.get_positional(0);
    String dir2  = g.get_positional(1);
    String file1 = dir1 + File.separator + g.get_positional(2);
    String file2 = dir2 + File.separator + g.get_positional(2);
    int unit = 128*1024;

    long h1 = Native.openFile(file1);
    if (h1 < 0)
      common.failure("Unable to open file: " + file1);

    long h2 = Native.openFile(file2);
    if (h2 < 0)
      common.failure("Unable to open file: " + file2);

    long size1 = Native.getSize(h1, file1);
    long size2 = Native.getSize(h2, file2);
    if (size1 != size2)
      common.failure("File sizes must be identical: %d %d", size1, size2);

    long buffer1 = Native.allocBuffer(unit);
    long buffer2 = Native.allocBuffer(unit);
    int[] array1 = new int[unit / 4];
    int[] array2 = new int[unit / 4];
    long  blocks = size1 / unit;

    int uniq1 = 0;
    int dups1 = 0;
    int uniq2 = 0;
    int dups2 = 0;
    for (int i = 1; i < blocks; i++)
    {
      long lba = i * unit;
      if (Native.readFile(h1, lba, unit, buffer1) != 0)
        common.failure("Error reading %s", file1);
      if (Native.readFile(h2, lba, unit, buffer2) != 0)
        common.failure("Error reading %s", file2);

      Native.buffer_to_array(array1, buffer1, unit);
      Native.buffer_to_array(array2, buffer2, unit);

      boolean zero1 = (array1[0] == array1[1] && array1[0] == 0);
      boolean zero2 = (array2[0] == array2[1] && array2[0] == 0);
      if (zero1) dups1++;
      else uniq1++;
      if (zero2) dups2++;
      else uniq2++;
      if (zero1 != zero2)
        common.failure("zero status mismatch. lba: 0x%08x", lba);

      /* Duplicate blocks must match: */
      if (zero1)
      {
        for (int j = 0; j < array1.length; j++)
        {
          if (array1[j] != array2[j])
          {
            common.ptod("mismatch: lba 0x%08x %08x %08x %08x %08x %06x ",
                        lba, array1[0], array1[1], array1[j], array2[j], j*4);
            //System.exit(999);
            break;
          }
        }
      }
    }

    common.ptod("uniq1: " + uniq1);
    common.ptod("dups1: " + dups1);
    common.ptod("uniq2: " + uniq2);
    common.ptod("dups2: " + dups2);
  }



  public static void main1(String[] args)
  {
    //Validate.setDedupRate(Integer.parseInt(args[0]));
    //Dedup ded = new Dedup(256*1024*1024*1024l, 1024);

    int[] col = new int[10];
    int loop = Integer.parseInt(args[0]);

    for (int i = 0; i < loop; i++)
    {
      long hash = i * 2654435761l;
      col[(int)(hash % 10)]++;
      common.ptod("hash: " + hash + " " + hash % 10);
    }

    for (int i = 0; i < col.length; i++)
      common.ptod("col: " + i + " " + col[i]);
  }
}







