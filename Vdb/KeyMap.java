package Vdb;
import java.text.SimpleDateFormat;
import java.util.Date;

import Utils.Fput;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

/**
 * This class handles all DV_map manipulations around the fact that a DV_map
 * entry of one block does not just support the xfersize, but supports the
 * lowest xfersize used. All other xfersizes must be a multiple of this lowest
 * xfersize.
 *
 * Terminology: - Key:            the DV key
 *              - Key block size: the lowest xfersize used.
 *              - Key block:      a block of 'key block size'.
 *              - Key map:        an array of keys, up to a length of
 *                                (xfersize / key block size).
 *
 * To support Dedup and Compression a KeyMap is always created regardless of
 * Data Validation being active. This is needed to allow for 'common' code to
 * handle the compression and dedup_set fields passed to JNI.
 */
public class KeyMap
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private DV_map dv_map         = null;
  private int    key_block_size = 0;
  private int    sum_of_keys    = 0;   /* If non-zero some key block has key > 0 */
  private boolean block_in_error = false;
  private int[]  key_map        = null;
  private int    key_count      = 0;   /* (xfersize / key block size) */

  private long[] compressions   = null;
  private long[] dedup_sets     = null;

  private boolean for_compression_only = false;


  /* For File System testing: contains relative lba from the start of the FSD: */
  public  long   file_start_lba = 0;

  /* The lba within the file or the SD: */
  public  long   file_lba       = 0;

  /* The lba used for pattern generation for the current data block.       */
  /* This is usuall equal to file_lba + file_start_lba, but at times, when */
  /* data blocks are written on a non-dedupunit boundary, may contain the  */
  /* lba that is a multiple of said dedupunit.                             */
  public  long   pattern_lba    = 0;

  /* The length of the data pattern that must be generated.                        */
  /* Usually equal to xfersize, but may need adjustment due to dedupunit mismatch. */
  public  int    pattern_length = 0;

  /* This is the TRUE xfersize: */
  public  long   xfersize       = 0;

  private static Object counter_lock = new Object();

  private SimpleDateFormat df_log  = new SimpleDateFormat( "MMddyy-HH:mm:ss.SSS" );
  private static boolean do_rw_log = common.get_debug(common.CREATE_READ_WRITE_LOG);


  public KeyMap(long start_lba, int key_blk_size, int max_xfersize)
  {
    file_start_lba = start_lba;
    key_block_size = key_blk_size;

    if (max_xfersize == 0)
      common.failure("No max_xfersize established yet");

    /* Due to Dedup data pattern generation we can straddle across key blocks, */
    /* e.g. bytes 4000-4999 using dedupunit=4096 and xfersize=1000. Add one    */
    //int max_keys = (max_xfersize + max_xfersize - 1) / key_block_size + 1;
    int max_keys = (max_xfersize + key_block_size * 2) / key_block_size;
    key_map      = new int  [ max_keys ];
    compressions = new long [ max_keys ];
    dedup_sets   = new long [ max_keys ];

    //common.ptod("key_block_size: " + key_block_size);
    //
    //
    //common.failure("max_xfersize: " + max_xfersize);

    /* Set the defaults for compression and dedup sets. */
    /* Will be overridden with proper values if needed. */
    for (int i = 0; i < compressions.length; i++)
      compressions[i] = 0;
    for (int i = 0; i < dedup_sets.length; i++)
      dedup_sets[i] = Dedup.DEDUP_NO_DEDUP;

    //if (dv_map != null && key_block_size != dv_map.getBlockSize())
    //{
    //  common.ptod("");
    //  common.ptod("Mismatch between key block size (%d) and Data Validation map size (%d). File: %s",
    //              key_block_size, dv_map.getBlockSize(), mp.journal.jnl_file_name);
    //  common.ptod("Did you recover a journal but your new xfersize parameters do not match?");
    //  common.failure("Mismatch between key block size (%d) and Data Validation map size (%d). File: %s",
    //                 key_block_size, dv_map.getBlockSize(), mp.journal.jnl_file_name);
    //}

    /* Set default for those cases where we don't call getKeysFromMap(): */
    setKeyCount(1);
  }


  /**
   * KeyMap to be used for a 'compression only' write.
   * key_block_size is set here each time getKeysFromMap() is called.
   */
  public KeyMap()
  {
    for_compression_only = true;

    /* One extra key for pattern buffer and dedup pattern overlap: */
    //key_count    = 1 + 1;
    setKeyCount(1);
    key_map      = new int[  key_count ];
    compressions = new long[ key_count ];
    dedup_sets   = new long[ key_count ];

    for (int i = 0; i < dedup_sets.length; i++)
      dedup_sets[i] = Dedup.DEDUP_NO_DEDUP;
  }


  /**
   * Clear DV_map entries for a file.
   * This is needed when a file is either deleted or created, though at
   * creation we should actually already find everything cleared.
   */
  public void clearMapForFile(long file_size)
  {
    for (long size = 0; size < file_size; size += key_block_size)
      dv_map.dv_set(file_start_lba + size, 0);
  }


  /**
   * Pass on information about the current data block being written.
   *
   * This includes specific Key Block info when dedup and/or Data Validation is
   * used.
   *
   * If ANY key block is in error, return FALSE: "don't touch this block"
   */
  public boolean storeDataBlockInfo(long f_lba, long xfer, DV_map dvmap)
  {
    dv_map   = dvmap;
    file_lba = f_lba;
    xfersize = xfer;

    /* If this KeyMap is only to help with compression pattern, fake the key block size */
    if (for_compression_only)
    {
      pattern_lba    = file_lba + file_start_lba;
      pattern_length = key_block_size = (int) xfersize;
    }

    else
    {
      /* Set proper pattern lba and length. This code allows straddling of dedupunits */
      /* of blocks that are not a multiple of dedupunit.                              */
      /* e.g. unit=4096, lba=2048, xfersize=4096. This straddles two dedup units.     */
      /* Jni code will build the complete 2*units pattern, but still writes from      */
      /* the proper lba=2048,xfersize=4096.                                           */
      pattern_lba    = file_lba + file_start_lba;
      pattern_length = (int) xfersize;
      long remainder = pattern_lba % key_block_size;
      if (remainder != 0)
      {
        pattern_lba    -= remainder;
        pattern_length += remainder;
      }

      remainder = pattern_length % key_block_size;
      if (remainder != 0)
        pattern_length += (key_block_size - remainder);
      setKeyCount((int) (pattern_length / key_block_size));
      //common.ptod("pattern_length: " + pattern_length);
      //common.ptod("key_block_size: " + key_block_size);

      /* Checks: */
      if (pattern_lba % key_block_size != 0)
        common.failure("Unexpected pattern_lba: %d/%d/%d/%d", file_lba, file_start_lba, key_block_size, pattern_lba);
      if (pattern_length % key_block_size != 0)
        common.failure("Unexpected pattern_length: %d/%d/%d/%d/%d", file_lba, file_start_lba, key_block_size, pattern_lba, pattern_length);
    }

    if (key_count > key_map.length+1)
    {
      common.ptod("pattern_length: " + pattern_length);
      common.ptod("key_block_size: " + key_block_size);
      common.ptod("key_map.length: " + key_map.length);
      common.ptod("key_count:      " + key_count);
      common.ptod("xfersize:       " + xfersize);
      common.failure("Invalid key count");
    }

    long lba  = file_start_lba + file_lba;

    sum_of_keys    = 0;
    block_in_error = false;

    /* Proper key values are only needed for DV and Dedup: */
    if (!Validate.isRealValidate() && !Validate.isValidateForDedup())
      return true;


    //common.ptod("pattern_length: " + pattern_length);
    //common.ptod("key_block_size: " + key_block_size);
    //common.ptod("key_map.length: " + key_map.length);
    //common.ptod("key_count:      " + key_count);
    //common.ptod("xfersize:       " + xfersize);

    for (int i = 0; i < key_count; i++)
    {
      int key      = dv_map.dv_get(lba + (i * key_block_size));
      key         &= 0x7f;
      sum_of_keys += key;

      /* A bad block will never be reused! */
      if (key == DV_map.DV_ERROR)
      {
        block_in_error = true;
        return false;
      }

      key_map[i] = key;

      //if (key > 0)
      //  common.failure("No kidding?");

      //common.ptod("key_map[i]: " + key_map[i]);
    }

    return true;
  }

  public boolean badDataBlock()
  {
    return block_in_error;
  }

  /**
   * Get the DV keys for each key block inside of an xfersize, and mark them busy.
   * Any busy or block in error will be skipped.
   *
   * This is only for SD workloads since with FSDs we have only one thread.
   * (That last statement is no longer correct as of the introduction of
   * fileselect=shared, though by now that check is made BEFORE the run).
   */
  public boolean getKeysFromMapAndSetBusy(long f_lba, long xfersize)
  {
    try
    {
      file_lba = f_lba;
      if (file_lba % key_block_size != 0)
        common.failure("Unexpected lba: %d / %d / %d",
                       file_lba, key_block_size,  file_lba % key_block_size);

      if (xfersize % key_block_size != 0)
        common.failure("Unexpected xfersize: " + xfersize + ", key block size: "+ key_block_size);

      /* The amount of keys needed for this xfersize: */
      setKeyCount((int) (xfersize / key_block_size));

      sum_of_keys = 0;

      synchronized (dv_map)
      {
        for (int i = 0; i < key_count; i++)
        {
          long key_lba = file_start_lba + file_lba + (i * key_block_size);
          int  key     = dv_map.getKeyAndSetBusy(key_lba);
          key_map[i]   = key & 0x7f;
          sum_of_keys += key & 0x7f;

          /* A bad block will never be reused; a busy block will be skipped */
          /* So for both: Clear busy status of earlier key blocks:          */
          if (key < 0) /* -1 means block is busy */
          {
            for (int j = 0; j < i; j++)
            {
              try
              {
                dv_map.setUnbusy(file_start_lba + file_lba + (j * key_block_size));
              }
              catch (Exception e)
              {
                long keylba = file_start_lba + file_lba + (j * key_block_size);
                common.ptod(e);
                common.ptod("keylba:         " + keylba);
                common.ptod("file_start_lba: " + file_start_lba);
                common.ptod("file_lba:       " + file_lba);
                common.ptod("i:              " + i);
                common.ptod("j:              " + j);
                common.failure(e);
              }
            }

            return false;
          }
        }
      }
    }
    catch (Exception e)
    {
      common.ptod("file_lba: " + file_lba);
      common.ptod("xfersize: " + xfersize);
      common.ptod("sum_of_keys: " + sum_of_keys);
      common.failure(e);
    }
    return true;
  }


  public int[] getKeys()
  {
    return key_map;
  }

  public void setKeyCount(int count)
  {
    key_count = count;
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
  public long[] getCompressions()
  {
    if (compressions == null)
      common.failure("NULL compression values");
    return compressions;
  }
  public long[] getDedupsets()
  {
    return dedup_sets;
  }

  /**
   * If all keys are zero we can't validate the block so we won't even bother to
   * pre-read it.
   */
  public boolean anyDataToCompare()
  {
    if (!Validate.isRealValidate() && Validate.isValidateForDedup())
      return false;
    return sum_of_keys != 0;
  }


  /**
   * Increment keys for a write operation.
   * Keys are only updated within the Key map, not in DV map, so no harm done
   * when we have to bail out of this block because of an existing DV_ERROR.
   *
   * Data Validation without Dedup:
   * - increment every single block
   *
   *
   * Dedup, without or without Data Validation:
   * - unique blocks: always increment/flipflop
   * - duplicate blocks:
   *    - without flipflop: no flipflop.
   *    - hotflop:          only flipflop hotblocks
   *    - with flipflop:    flipflop
   *
   *
   * Return: false if any key block in the current data block is marked in
   * error.
   */
  public boolean incrementKeys()
  {
    /* This field needs to be rebuilt: */
    sum_of_keys = 0;

    /* Loop through all key blocks: */
    for (int i = 0; i < key_count; i++)
    {
      long lba   = file_start_lba + file_lba + i * key_block_size;
      long block = lba / key_block_size;
      long set   = dedup_sets[i];

      //common.ptod("set0: %016x", set);

      if (Dedup.isDedup() && set == Dedup.DEDUP_NO_DEDUP)
        common.failure("not expected: %d", i);

      /* If during the read-before-write the key was set to 'error', skip block: */
      if (dv_map.dv_get(lba) == DV_map.DV_ERROR)
        return false;


      /* Data Validation without Dedup: increment every single block */
      if (!Validate.isDedup())
        key_map[i] = dv_map.dv_increment(key_map[i], set);

      /* Dedup for unique blocks: always increment/flipflop */
      else if (Dedup.isUnique(set))
      {
        if (Validate.isRealValidate())
        {
          key_map[i] = dv_map.dv_increment(key_map[i], set);
          if (key_map[i] == 0)
            common.failure("Increment keys results in a zero key");
        }

        /* flipflop really is a mechanism for DUPLICATES. UNIQUES change */
        /* regardless of the flip/flop state. */
        //else
        //{
        //  key_map[i] = dv_map.flipflop(key_map[i]);
        //  //common.ptod("flipflop1: %,12d %016x %s", block, set, Dedup.xlate(set));
        //}
      }

      /* Dedup for duplicate blocks: without flipflop: no flipflop */
      else if (!dv_map.getDedup().isFlipFlop())
      {
        //if (Dedup.getKey(set) != 0)
        //  common.failure("During THIS test key should not be other than zero: %016x", set);

        continue;
      }

      /* Dedup for duplicate blocks: hotflop: only flipflop hotblocks */
      else if (dv_map.getDedup().isHotFlop())
      {
        if (!dv_map.getDedup().isHotSet(set))
          continue;
        {
          key_map[i] = dv_map.flipflop(key_map[i]);
          if (key_map[i] == 0)
            common.failure("Increment keys results in a zero key");
          //common.ptod("flipflop2: %,12d %016x %4d", block, set, dv_map.dedup.getSet(set));
        }
      }

      /* Dedup for duplicate blocks: with flipflop: flipflop */
      else
      {
        //common.ptod("key_map[i]1: " + key_map[i]);
        key_map[i] = dv_map.flipflop(key_map[i]);
        //common.ptod("key_map[i]2: " + key_map[i]);
        if (key_map[i] == 0)
          common.failure("Increment keys results in a zero key");
        //common.ptod("flipflop3: %,12d %016x", block, set);
      }

      /* The new DV key must be replaced in the set: */
      //common.ptod("set1: %016x", set);
      set          &= ~ Dedup.UNIQUE_KEY_MASK;
      //common.ptod("set2: %016x", set);
      set          |= (((long) key_map[i]) << 56);
      //common.ptod("set3: %016x", set);
      dedup_sets[i] = set;


      /* Paranaoia? OK for a while: */
      if (Validate.isRealValidate() && key_map[i] == 0)
        common.failure("Increment keys results in a zero key");

      sum_of_keys += key_map[i];
    }



    //if (!dv_map.dedup.isFlipFlop())
    //{
    //  for (int i = 0; i < key_count; i++)
    //  {
    //    long set = dedup_sets[i];
    //    if (Dedup.isDuplicate(set) && Dedup.getKey(set) == 1)
    //      common.failure("During THIS test key should not be other than zero: %016x", set);
    //  }
    //}


    return true;
  }

  /**
   * Store the keys, and in the process, mark them unbusy.
   * Storing the key is only needed after a write operation, but since we have to
   * reset the busy bit anyway, replacing the key with the same value will
   * accompish this for us.
   */
  public void storeKeys()
  {
    if (!Validate.isRealValidate() && !Validate.isValidateForDedup())
      common.failure("Invalid call for Key manipulation");

    synchronized (dv_map)
    {
      // Problem!!!! ????
      // if the first key block of this Data block is NOT in error, but a
      // different Key block is, then I won't recognize it.
      // This goes both for SD and FSD!

      // Is there even a reason for marking OTHER Key blocks in error?
      // Bad_sector() has already set the bad Key block in error, so the code
      // below, as far as the Key block is concerned, is redundant.

      // However, marking a few extra 'not in error' key blocks in error has not
      // caused any problems at all so I do not see a need to make any changes here.

      // Also the fact that indeed data blocks whose first Key block is not in error
      // but only an other key block, has not caused any problems.

      // Just leave asis.


      boolean bad_block = (dv_map.dv_get(file_start_lba + file_lba) == DV_map.DV_ERROR);

      for (int i = 0; i < key_count; i++)
      {
        long lba    = file_start_lba + file_lba + i * key_block_size;
        int  oldkey = dv_map.dv_get(lba);

        /* Don't update if the key is already marked BAD */
        if (oldkey != DV_map.DV_ERROR)
        {
          if (!Validate.isValidateForDedup() && SlaveJvm.isWdWorkload() && (oldkey & 0x80) == 0)
            common.failure("storeKeys(): lba 0x%08x should be busy %02x", lba, oldkey);
          dv_map.dv_set(lba, key_map[i]);

          oldkey = (oldkey & 0x7f);
          sum_of_keys = sum_of_keys - oldkey + key_map[i];
        }

        /* If the block's lba is in error, set all key blocks in error: */
        // See notes above
        if (bad_block)
          dv_map.dv_set(lba, DV_map.DV_ERROR);
      }
    }
  }

  public void markDataBlockBad(DV_map dvmap, long data_lba)
  {
    dv_map = dvmap;
    for (int i = 0; i < key_count; i++)
    {
      long lba    = file_start_lba + data_lba + i * key_block_size;
      dv_map.dv_set(lba, DV_map.DV_ERROR);
    }
  }

  /**
   * If the 'validate=time' option is used, save the last successful TS.
   * The flags below are stored in byte0 over the timestamp.
   */
  public void saveTimestamp(long type)
  {
    if (!Validate.isStoreTime())
      return;

    long lba = file_start_lba + file_lba;

    for (int i = 0; i < key_count; i++)
    {
      dv_map.save_timestamp(lba + i * key_block_size, type);
    }
  }

  public void countRawReadAndValidates(SD_entry sd, long lba)
  {
    synchronized (counter_lock)
    {
      /* Count the number of key blocks read and validated: */
      for (int i = 0; i < key_count; i++)
      {
        if (key_map[i] != 0)
          dv_map.key_reads[ key_map[i] & 0xff ]++;
      }
    }

    /* For some serious debugging, log all reads and writes: */
    if (do_rw_log)
    {
      Fput   fp  = sd.rw_log;
      String now = df_log.format(new Date());

      /* Only one writer per FSD: */
      synchronized (fp)
      {
        for (int i = 0; i < key_count; i++)
        {
          fp.println("%s r %02d %d ",
                     now,
                     key_map[i],
                     lba + (key_block_size * i));
        }
      }
    }
  }

  public void countFileReadAndValidates(FileEntry fe, long lba)
  {
    synchronized (counter_lock)
    {
      /* Count the number of key blocks read and validated: */
      for (int i = 0; i < key_count; i++)
      {
        if (key_map[i] != 0)
          dv_map.key_reads[ key_map[i] & 0xff ]++;
      }

      /* For some serious debugging, log all reads and writes: */
      Fput fp = fe.getAnchor().rw_log;
      if (fp != null)
      {
        String now = df_log.format(new Date());

        /* File# and file lba should be next to each other for easier search: */
        for (int i = 0; i < key_count; i++)
        {
          fp.println("%s r %02d %d %d %d ",
                     now,
                     key_map[i],
                     fe.getFileNoInList(),
                     lba + (key_block_size * i),
                     file_start_lba + lba + (key_block_size * i));
        }
      }
    }
  }

  public void countRawWrites(SD_entry sd, long lba)
  {
    synchronized (counter_lock)
    {
      /* Count the number of key blocks read and validated: */
      for (int i = 0; i < key_count; i++)
      {
        if (key_map[i] != 0)
          dv_map.key_writes[ key_map[i] &0xff ]++;
      }
    }

    /* For some serious debugging, log all reads and writes: */
    if (do_rw_log)
    {
      Fput   fp  = sd.rw_log;
      String now = df_log.format(new Date());

      /* Only one writer per FSD: */
      synchronized (fp)
      {
        /* File# and file lba should be next to each other for easier search: */
        for (int i = 0; i < key_count; i++)
        {
          fp.println("%s w %02d %d ",
                     now,
                     key_map[i],
                     lba + (key_block_size * i));
        }
      }
    }
  }

  public void countFileWrites(FileEntry fe, long lba)
  {
    synchronized (counter_lock)
    {
      /* Count the number of key blocks read and validated: */
      for (int i = 0; i < key_count; i++)
      {
        if (key_map[i] != 0)
          dv_map.key_writes[ key_map[i] &0xff ]++;
      }
    }

    /* For some serious debugging, log all reads and writes: */
    Fput fp  = fe.getAnchor().rw_log;
    if (fp != null)
    {
      String now = df_log.format(new Date());

      /* Only one writer per FSD: */
      synchronized (fp)
      {
        /* File# and file lba should be next to each other for easier search: */
        for (int i = 0; i < key_count; i++)
        {
          fp.println("%s w %02d %d %d %d ",
                     now,
                     key_map[i],
                     fe.getFileNoInList(),
                     lba + (key_block_size * i),
                     file_start_lba + lba + (key_block_size * i));
        }
      }
    }
  }


  /**
   * Write before/after journal image.
   *
   * Journal update NOT done for a format operation.
   * This is done to speed up the format, however, unless it is properly
   * documented and understood this is the wrong thing to do. User just will have
   * to start using MapOnly() through validate=maponly or -vm
   *
   * Now using mmap() do we even NEED a journal if we know the OS will not go
   * down?
   *
   * Note: there should not be a need for each Key block size journal write to
   * be synchronous! This is taken care of already in writeJournalEntry() using
   * the 'last' flag!
   */
  public void writeBeforeJournalImage()
  {
    /* The usefulness of 'maponly' is there, but has not been used or tested */
    /* in a long time. Leave it though.                                      */
    if (Validate.isMapOnly()) // || format)
      return;

    synchronized (dv_map.journal)
    {
      for (int i = 0; i < key_count; i++)
      {
        long lba   = file_start_lba + file_lba;
        long block = (lba / key_block_size + i);
        if (key_map[i] == 0)
          common.failure("writeBeforeJournalImage trying to write a zero key");

        dv_map.journal.writeJournalEntry(key_map[i], block, (i + 1) == key_count);
      }
    }

    HelpDebug.abortAfterCount("writeBeforeJournalImage");
  }

  public void writeAfterJournalImage()
  {
    if (Validate.isMapOnly()) // || format)
      return;

    /* If the block is in error, don't write AFTER image: */
    /* This will leave this block as 'write pending'. */
    if (dv_map.dv_get(file_start_lba + file_lba) == DV_map.DV_ERROR)
      return;

    HelpDebug.abortAfterCount("writeAfterJournalImage");

    synchronized (dv_map.journal)
    {
      for (int i = 0; i < key_count; i++)
      {
        long lba   = file_start_lba + file_lba;
        long block = (lba / key_block_size + i);
        dv_map.journal.writeJournalEntry(0, block, (i + 1) == key_count);
      }
    }
  }


  /**
   * Set compression offset when compression is requested without Dedup
   */
  public void setFsdCompressionOnlyOffset(ActiveFile afe)
  {
    /* Do calculations for each Key: */
    for (int i = 0; i < getKeyCount(); i++)
    {
      long lba = afe.getFileEntry().getFileStartLba() +
                 afe.next_lba + getKeyBlockSize() * i;

      compressions[i] = Dedup.getUniqueCompressionOffset(lba);
    }
  }

  public void setSdCompressionOnlyOffset(long lba_in)
  {
    /* Do calculations for each Key: */
    for (int i = 0; i < key_count; i++)
    {
      long lba = lba_in + getKeyBlockSize() * i;

      //common.ptod("compressions: " + compressions.length);
      //common.ptod("lba: " + lba + " " + lba % (Patterns.getBufferSize()));
      compressions[i] = Dedup.getUniqueCompressionOffset(lba);
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

