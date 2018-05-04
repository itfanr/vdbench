package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;



/**
 * Describes one bad Key block discovered by Data Validation.
 *
 * It is pointed to by BadDataBlock and points to BadSector.
 */
public class BadKeyBlock
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public BadDataBlock owning_bdb;

  public long        dedup_set;
  public long        key_lba;
  public int         sectors;
  public int         sectors_added;
  public BadSector[] bad_sectors = null;
  public long        last_used_tod = -1;
  public String      last_used_txt;
  public String      last_used_op;
  public long        first_error_found = System.currentTimeMillis();

  public String      fname;



  public void addSector(BadSector bads)
  {
    if (bad_sectors == null)
    {
      bad_sectors   = new BadSector[ bads.key_blksize / 512 ];
      key_lba       = bads.key_lba;
      sectors       = bads.key_blksize / 512;
      dedup_set     = bads.dedup_set;
      if (Validate.isStoreTime())
        last_used_tod = bads.dv.getLastTimestamp(key_lba);
      last_used_op  = bads.dv.getLastOperation(key_lba);
      last_used_txt = BadSector.dv_df.format(new Date(last_used_tod));

      if (bads.sd != null)
        fname = bads.sd.sd_name;
      else
        fname = bads.afe.getFileEntry().getFullName();
    }

    if (bad_sectors[ bads.sector_in_keyblock ] != null)
      throw new RuntimeException("Duplicate reporting of sector");

    bad_sectors[ bads.sector_in_keyblock ] = bads;
    sectors_added++;
  }


  /**
   * Report the status of a corrupted key block.
   *
   * When all sectors have the same error_flag, timestamp etc, report only the
   * very first sector.
   *
   * Any other way: report ALL sectors.
   */
  public void reportBadKeyBlock()
  {
    BadSector first = getSectors().get(0);

    plog("Key block lba: 0x%08x", key_lba);
    plog("   Key block of %,d bytes has %d 512-byte sectors.", first.key_blksize, sectors);


    if ((first.data_flag & Validate.FLAG_PENDING_REREAD) != 0)
    {
      plog("   The last key block write was pending during journal shutdown. ");
      plog("   All sectors contained a valid 'before' key, but other errors ");
      plog("   were found when the block was read and checked for a second time. ");
      plog("");
    }

    reportTimeLine();

    if (sectors == sectors_added)
    {
      plog("   All %d sectors in this key block are corrupted.", sectors);
      if (matchingErrorFlags())
      {
        plog("   All corruptions are of the same type: ");
        for (String flag : first.getFlagText())
          plog("   ===> " + flag);
        plog("   Only the FIRST sector will be reported:");
        first.printOneSector();

        //plog("key:      %02x ",        first.getKey());
        //plog("lba:      %x ",          first.getLba());
        //plog("checksum: %02x ",        first.getChecksum());
        //plog("flag:     %08x ",        first.data_flag);
        //plog("time:     %016x ",       first.getTimeStampRead());
        //plog("name:     %016x ",       first.getNameHex());
        //plog("name:     >>>>%s<<<< ",  first.getNameString());
        //plog("getTimeStampText: " + first.getTimeStampText());

        /* Report the time that the block was last written: */
        if (first.dv.getLastTimestamp(key_lba) != 0)
        {
          plog("Time that this key block 0x%08x was last used for %s: %s",
               key_lba,
               first.dv.getLastOperation(key_lba),
               BadSector.dv_df.format(new Date(first.dv.getLastTimestamp(key_lba))));
        }
      }

      /* All sectors corrupted, but the errorflags do not match: */
      else
      {
        plog("   Corruption types do not match. Reporting all sectors: ");
        reportFlagCounts();
        for (BadSector bads : getSectors())
          bads.printOneSector();
      }
    }
    else
    {
      plog("   %d of %d sectors are corrupted.", sectors_added, sectors);
      plog("   All %d corrupted sectors will be reported.", sectors_added);
      for (BadSector bads : getSectors())
        bads.printOneSector();
    }
  }

  public boolean allMatching()
  {
    //common.ptod("sectors/added       : " + (sectors_added == sectors) );
    //common.ptod("matchingChecksums() : " + matchingChecksums() );
    //common.ptod("matchingDedupsets() : " + matchingDedupsets() );
    //common.ptod("matchingErrorFlags(): " + matchingErrorFlags());
    //common.ptod("matchingKeys()      : " + matchingKeys()      );
    //common.ptod("validLbas()         : " + validLbas()      );
    //common.ptod("matchingNames()     : " + matchingNames()     );
    //common.ptod("matchingTimestamps(): " + matchingTimestamps());

    return( sectors_added == sectors &&
            matchingChecksums()      &&
            matchingDedupsets()      &&
            matchingErrorFlags()     &&
            matchingKeys()           &&
            validLbas()              &&
            matchingNames()          &&
            matchingTimestamps());
  }

  public boolean matchingErrorFlags()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.error_flag != bads.error_flag)
        return false;
    }
    return true;
  }
  public boolean matchingTimestamps()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.getTimeStampRead() != bads.getTimeStampRead())
        return false;
    }
    return true;
  }
  public boolean matchingNames()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.getNameString().startsWith("garbage") ||
          bads.getNameString().startsWith("garbage"))
        return false;
      if (!first.getNameString().equals(bads.getNameString()))
        return false;
    }
    return true;
  }
  public boolean matchingChecksums()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.getChecksum() != bads.getChecksum())
        return false;
    }
    return true;
  }
  public boolean matchingDedupsets()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.dedup_set != bads.dedup_set)
        return false;
    }
    return true;
  }
  public boolean matchingKeys()
  {
    BadSector first = getSectors().get(0);
    for (BadSector bads : bad_sectors)
    {
      if (first.getKey() != bads.getKey())
        return false;
    }
    return true;
  }

  public boolean validLbas()
  {
    for (BadSector bads : bad_sectors)
    {
      if (bads.getLba() != bads.sector_lba)
        return false;
    }
    return true;
  }


  /**
   * Return ONLY those sectors that we have received.
   *
   * Don't confuse this with getting ALL sectors, though usually all sectors
   * will be there.
   */
  public ArrayList <BadSector> getSectors()
  {
    ArrayList <BadSector> list = new ArrayList(sectors_added);
    for (BadSector bads : bad_sectors)
    {
      if (bads != null)
        list.add(bads);
    }

    if (list.size() == 0)
      throw new RuntimeException("No sectors found");
    return list;
  }

  private static void plog(String format, Object ... args)
  {
    ErrorLog.add(String.format(format, args));
  }

  /**
   * Report the timeline of this key block, preferably in the TOD order found.
   *
   * Explanation for the '%032d0001' mask:
   * %032d: a long enough decimal string to properly be sorted on milliseconds.
   * '0000/0001/0002' With the timestamp being in milliseconds I want to make
   * sure that if we end up with identical milliseconds the --sector--- is
   * sorted first.
   *
   * Primitive, but it works.
   *
   * My hope is to get back to including, in each sector, a timestamp using
   * microseconds, though at this time I decided that this must
   * wait.
   * There is also a wish to use the SAME timestamp for the write as I use for
   * saving the timestamp for the last read/write. TBD.
   */
  private void reportTimeLine()
  {
    BadSector first = getSectors().get(0);
    ArrayList <String> sort_helper = new ArrayList(8);

    plog("   Timeline: ");
    if (first.bad_checksum)
      plog("   No valid timestamp found in first corrupted sector");
    if (!Validate.isStoreTime())
    {
      //plog("   validate=time not used.");
    }
    else if (Validate.isStoreTime() && last_used_tod == 0)
      plog("   This key block has not been used (yet) during this vdbench test.");

    else if (Validate.isStoreTime())
      sort_helper.add(String.format("%032d0001 %s Key block was last %s. " +
                                    "(Timestamp is taken just AFTER the actual read or write).",
                                    last_used_tod, last_used_txt, last_used_op));

    if (!first.bad_checksum && Dedup.isUnique(first.dedup_set))
    {
      sort_helper.add(String.format("%032d0000 %s Sector last written. (As found in the first corrupted sector, " +
                                    "timestamp is taken just BEFORE the actual write).",
                                    first.getTimeStampRead(), first.getTimeStampText()));
    }


    if ((first.data_flag & Validate.FLAG_PENDING_READ) != 0)
      plog(">>>>>>> This key block was read because journal recovery determined a write was pending");

    String why = "n/a";
    if ((first.data_flag & Validate.FLAG_NORMAL_READ) != 0)
      why = "workload requested read";
    else if ((first.data_flag & Validate.FLAG_PRE_READ) != 0)
      why = "read-before-write";
    else if ((first.data_flag & Validate.FLAG_READ_IMMEDIATE) != 0)
      why = "read-after-write";
    else if ((first.data_flag & Validate.FLAG_PENDING_READ) != 0)
      why = "pending-write-read";
    else if ((first.data_flag & Validate.FLAG_PENDING_REREAD) != 0)
      why = "pending-write-reread";
    sort_helper.add(String.format("%032d0002 %s Key block first found to be corrupted during a %s.",
                                  first_error_found, BadSector.dv_df.format(first_error_found), why));

    /* We now have the proper TOD order. */
    Collections.sort(sort_helper);
    for (String help : sort_helper)
      plog("   " + help.substring(help.indexOf(" ")).trim());
    plog("");
  }


  public void reportFlagCounts()
  {
    int bad_key      = 0;
    int bad_checksum = 0;
    int bad_lba      = 0;
    int bad_name     = 0;
    int bad_data     = 0;
    int bad_comp     = 0;
    int bad_dedupset = 0;
    int bad_zero     = 0;

    for (BadSector bads : getSectors())
    {
      if ((bads.error_flag & BadSector.BAD_KEY      ) != 0) bad_key      ++;
      if ((bads.error_flag & BadSector.BAD_CHECKSUM ) != 0) bad_checksum ++;
      if ((bads.error_flag & BadSector.BAD_LBA      ) != 0) bad_lba      ++;
      if ((bads.error_flag & BadSector.BAD_NAME     ) != 0) bad_name     ++;
      if ((bads.error_flag & BadSector.BAD_DATA     ) != 0) bad_data     ++;
      if ((bads.error_flag & BadSector.BAD_COMP     ) != 0) bad_comp     ++;
      if ((bads.error_flag & BadSector.BAD_DEDUPSET ) != 0) bad_dedupset ++;
      if ((bads.error_flag & BadSector.BAD_ZERO     ) != 0) bad_zero     ++;
    }

    plog("   Error type counts:");
    if (bad_key      != 0) plog("   %-40s %3d", "===> Data Validation Key miscompare:",    bad_key      );
    if (bad_checksum != 0) plog("   %-40s %3d", "===> Checksum error on timestamp:",       bad_checksum );
    if (bad_lba      != 0) plog("   %-40s %3d", "===> Logical byte address miscompare:",   bad_lba      );
    if (bad_name     != 0) plog("   %-40s %3d", "===> SD or FSD name miscompare:",         bad_name     );
    if (bad_data     != 0) plog("   %-40s %3d", "===> Data miscompare:",                   bad_data     );
    if (bad_comp     != 0) plog("   %-40s %3d", "===> Compression pattern miscompare:",    bad_comp     );
    if (bad_dedupset != 0) plog("   %-40s %3d", "===> Bad dedup set value:",               bad_dedupset );
    if (bad_zero     != 0) plog("   %-40s %3d", "===> Bad reserved field contents (mbz):", bad_zero     );
  }



  /**
   * Corruption found by JNI during journal recovery.
   * However, it can be that the only reason for the corruption is the fact that
   * this was a PENDING write that never completed and we therefore have to
   * revert back to the previous key used.
   */
  public boolean checkPendingKeyBlock(long file_lba,
                                      long file_start_lba,
                                      long buffer)
  {
    /* Point to the first corrupted sector: */
    BadSector bads = getSectors().get(0);
    //long      file_start_lba = 0;
    //if (bads.afe != null)
    //  file_start_lba = bads.afe.getFileEntry().getFileStartLba();

    int pending_flag = (bads.dv.journal.before_map.pending_map.get(key_lba)) & 0xff;

    plog("checkPendingKeyBlock for %s lba 0x%08x key 0x%02x "+
         "bad_data_flag 0x%04x pending_flag 0x%02x", fname,
         file_lba, bads.key_expected, bads.data_flag, pending_flag);

    /**
     * After the journal recovery the map contains the key of the pending write.
     * The data block then can contain previous or new contents.
     *
     * Scenarios (ignoring dedup):
     * - all new
     * - all old
     * - half new, second half old, in that order.
     * Any other scenarios are invalid.
     */
    int after_key = bads.key_expected;
    int before_key = 0;
    if (pending_flag == DV_map.PENDING_KEY_0)
      before_key = 0;

    else if (pending_flag == DV_map.PENDING_KEY_ROLL)
      before_key = 126;

    else if (pending_flag == DV_map.PENDING_KEY_ROLL_DEDUP)
    {
      common.ptod("proof that we have PENDING_KEY_ROLL_DEDUP");
      before_key = 2;
    }

    else if (pending_flag == DV_map.PENDING_WRITE)
      before_key = after_key - 1;

    else
      common.failure("Invalid pending flag: 0x%02x", pending_flag);

    long lba = file_lba + file_start_lba;

    long        block  = lba / bads.key_blksize;
    DedupBitMap bitmap;
    if (bads.sd != null)
      bitmap = DedupBitMap.findUniqueBitmap("sd=" + bads.sd.sd_name);
    else
      bitmap = DedupBitMap.findUniqueBitmap("fsd=" + bads.afe.getAnchor().fsd_name_active);


    boolean unique = true;
    if (Validate.isDedup())
    {
      //common.ptod("bads.dv.getDedup(): " + bads.dv.getDedup());
      //common.ptod("bitmap: " + bitmap);
      unique = bads.dv.getDedup().getDedupPct() == 100. || bitmap.isUnique(block);
    }

    int before_keys = countKeys(lba, buffer, bads.key_blksize, before_key, unique);
    int after_keys  = countKeys(lba, buffer, bads.key_blksize, after_key,  unique);

    plog("sectors: %d; before_key: %d; after_key: %d; before_keys: %d; after_keys: %d ",
         sectors, before_key, after_key, before_keys, after_keys);

    /**
      * Note from March 4 to SolidFire: I concluded that if the pending write
      * found during journal recovery is the very first write to that block
      * within a Vdbench test I can not trust that block.  Data Validation is
      * based on a Data Validation Key, key values 1 through 126, and rolling
      * over back to 1 after reaching 126.  If I would consider that maybe the
      * first write (key = 0x01) of this block was completed it is theoretically
      * possible that I find the proper DV key value of 0x01 from a run hours or
      * days or weeks earlier with the exact same data content.  There is
      * therefore not a GUARANTEE that this block was valid so I do not want to
      * accept it, and will mark the block's contents as 'unknown', but not
      * 'corrupt'.
      *
      * This above does bring up a small theoretical 'hole' in Vdbench: What if
      * I successfully write key 0x01 to a lun, and then later read it and
      * indeed get the correct key, the correct lba, and the correct sd/fsd
      * name?  In theory, the previous write could have been lost and I may
      * have read a block that was written hours/days/weeks earlier.  I will
      * leave the current beta code alone, don't want to risk stability, but
      * this is what I think of doing: each block, or each UNIQUE block when
      * using Dedup, contains a timestamp of the last write.  I should compare
      * that timestamp to be within 'start of Vdbench', or 'start of initial
      * journal creation' and the current time of day.  That then can be an
      * other paranoia check.  Again, I'll leave current code alone.
      *
      */

    /* If the previous key was 0 we can't really trust the data at all.             */
    /* We could find garbage from last week that just happens to have the same key, */
    /* meaning that we could be lucky and have the same LBA and sd or fsd name in   */
    /* the block, or a mismatch in those values.                                    */
    /* We therefore can never trust the contents.                                   */

    /* Note: this block will never get this far. */
    /* It has been removed in findPendingBlocks  */
    if (pending_flag == DV_map.PENDING_KEY_0)
    {
      plog("checkPendingKeyBlock for key lba 0x%08x key 0x%02x. "+
           "This was the first write, results always unpredictable. Blocki set to 'unknown'",
           key_lba, bads.key_expected);
      bads.dv.dv_set(key_lba, 0x00 | 0x80);
      return true;
    }


    // need to check all the stored BadSectors to make sure we have no other errors
    // than just bad key. (they WOULD be thrown out later, but it is still better
    // to know they were corrupted NOW.
    //
    // the block should actually be re-checked because the data can be bad when using the proper key!



    /* All sectors 'old': set back to old key, but keep busy: */
    if (before_keys == sectors)
    {
      plog("checkPendingKeyBlock for key lba 0x%08x key 0x%02x. "+
           "Write never completed, key set back to 0x%02x",
           key_lba, bads.key_expected, before_key);
      plog("Key Block will be re-checked.");

      bads.dv.dv_set(key_lba, before_key | 0x80);
      if (bads.dv.journal.before_map.pending_map.put(key_lba, (byte) DV_map.PENDING_KEY_REREAD) == null)
        common.failure("Invalid pending state");

      /* Block is fine. */
      return true;
    }


    /* The original journal pending read starts with using the AFTER key. */
    /* If that block is OK we should never arrive here:                   */
    if (after_keys == sectors)
    {
      plog("checkPendingKeyBlock for lba 0x%08x key 0x%02x. "+
           "All keys are valid, but there are other errors. "+
           "Block will be marked in error.",
           file_lba, after_key);
      bads.dv.dv_set(file_lba, DV_map.DV_ERROR);

      /* Bad block: */
      return false;
    }



    /* All other errors: */
    plog("checkPendingKeyBlock valid before/after keys: %d/%d out of %d sectors",
         before_keys, after_keys, sectors);
    plog("checkPendingKeyBlock for lba 0x%08x key 0x%02x. "+
         "Unacceptable mismatch between before/after keys. Block will be marked in error.",
         file_lba, bads.key_expected, after_key);
    bads.dv.dv_set(file_lba, DV_map.DV_ERROR);

    /* Bad block: */
    return false;
  }

  /**
   * Read a block and count the amount of valid keys in the sectors of that block
   */
  private int countKeys(long lba, long buffer, int key_blksize, int count_key, boolean unique)
  {

    int valid_keys = 0;

    int[] data_array  = new int[ key_blksize / 4];
    Native.buffer_to_array(data_array, buffer, key_blksize);

    for (int i = 0; i < sectors; i++)
    {
      int offset      = (i*128);
      int current_key;
      if (unique)
        current_key = data_array[offset+4] >> 24;
      else
        current_key = data_array[offset+0] >> 24;
      //common.ptod("current_key: 0x%02x", current_key);

      if (current_key == count_key)
        valid_keys++;
    }

    return valid_keys;
  }
}




