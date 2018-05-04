package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.nio.ByteOrder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;



/**
 * Describes data communicated from JNI back to java when a Data Validation
 * error has occurred.
 *
 * This is the new BadSector, replacing the orignal Bad_sector.
 */
public class BadSector
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /* These values are also hardcoded in vdb_dc.c */
  public  static int BAD_KEY      = 0x0001;
  public  static int BAD_CHECKSUM = 0x0002;
  public  static int BAD_LBA      = 0x0004;
  public  static int BAD_NAME     = 0x0008;
  public  static int BAD_DATA     = 0x0010;
  public  static int BAD_COMP     = 0x0020;
  public  static int BAD_DEDUPSET = 0x0040;
  public  static int BAD_ZERO     = 0x0080;
  public  static int BAD_PID      = 0x0100;

  public  BadKeyBlock  owning_bkb;
  public  BadDataBlock owning_bdb;

  /* Info coming directly or indirectly from the read call: */
  public  int    data_flag;           /* Data flag from read request              */
  public  long   handle;              /* File handle                              */
  public  long   file_lba;            /* Lba of data block read in lun or file    */
  public  long   key_lba;             /* Lba of current key block                 */
  public  long   rel_data_lba;        /* Relative Lba of data block read          */
  public  long   file_start_lba;      /* Relative start of file or 0 for lun      */
  public  long   sector_lba;          /* Relative lba for data pattern generation */
  public  int    offset_in_key_block; /* Byte offset of failing sector            */
  public  long   compression;         /* Compression option from READ             */
  public  long   dedup_set;           /* Which dedupset was expected from READ    */
  public  int    key_expected;        /* What key was expected in this sector     */
  public  int    key_blksize;         /* Key block size                           */
  public  int    data_blksize;        /* Data block size                          */
  public  String name_left;           /* SD or FSD name expected                  */

  /* Info coming from INSIDE of the read buffer */
  public  int    error_flag;          /* Which errors did JNI find                */
  public  int[]  sector_array;        /* The data copied from the read buffer     */


  public  boolean bad_key      = false;
  public  boolean bad_checksum = false;
  public  boolean bad_lba      = false;
  public  boolean bad_name     = false;
  public  boolean bad_data     = false;
  public  boolean bad_comp     = false;
  public  boolean bad_dedupset = false;
  public  boolean bad_zero     = false;
  public  boolean bad_pid      = false;
  public  boolean unique       = false;
  public  boolean duplicate    = false;

  public  SD_entry   sd  = null;
  public  ActiveFile afe = null;
  public  String     lun = null;
  public  DV_map     dv  = null;

  public  int    sector_in_keyblock;
  public  int    sector_in_datablock;
  public  int    keyblock_in_datablock;

  private static int[]  expected_pattern;
  private static int[]  lfsr_sector  = new int[512/4];

  public  static SimpleDateFormat dv_df = new SimpleDateFormat( "EEE MMM dd yyyy HH:mm:ss.SSS zzz" );



  /**
   * This is called from JNI when an data corruption is found.
   */
  static void signalBadSector(int[]  in_sector_array,         // 512     bytes
                              long   in_handle,               // (jlong) req->fhandle,
                              long   in_file_lba,             // (jlong) req->data_lba,
                              long   in_key_lba,              // (jlong) req->key_lba,
                              long   in_file_start_lba,       // (jlong) req->file_start_lba,
                              long   in_sector_lba,           // (jlong) req->sector_lba,
                              long   in_offset_in_key_block,  // (jlong) req->offset_in_key_block,
                              long   in_compression,          // (jlong) req->compression,
                              long   in_dedup_set,            // (jlong) req->dedup_set,
                              long   in_data_flag,            // (jlong) req->data_flag,
                              long   in_key,                  // (jlong) req->key,
                              long   in_key_blksize,          // (jlong) req->key_blksize,
                              long   in_data_blksize,         // (jlong) req->data_blksize,
                              long   in_error_flag)           // (jlong) error_flag)
  {

    try
    {

      /* Only ONE at the time, including the printing of the data: */
      synchronized (BadDataBlock.reporting_lock)
      {
        HashMap <Long, BadDataBlock> bad_data_map = null;

        /* Copy everything locally: */
        common.where(16);
        BadSector bads = new BadSector();
        bads.sector_array          = in_sector_array;
        bads.handle                = in_handle;
        bads.file_lba              = in_file_lba;
        bads.rel_data_lba          = in_file_lba + in_file_start_lba;
        bads.key_lba               = in_key_lba;
        bads.file_start_lba        = in_file_start_lba;
        bads.offset_in_key_block   = (int) in_offset_in_key_block;
        bads.sector_lba           = in_sector_lba;
        bads.compression           = in_compression;
        bads.dedup_set             = in_dedup_set;
        bads.error_flag            = (int) in_error_flag;
        bads.data_flag             = (int) in_data_flag;
        bads.key_expected          = (int) in_key;
        bads.data_blksize          = (int) in_data_blksize;
        bads.key_blksize           = (int) in_key_blksize;

        bads.sector_in_keyblock    = bads.offset_in_key_block / 512;
        bads.sector_in_datablock   = (int) ((bads.key_lba - bads.file_lba)
                                            / 512 + bads.sector_in_keyblock);
        bads.keyblock_in_datablock = (int) ((bads.key_lba - bads.file_start_lba - bads.file_lba) / bads.key_blksize);

        bads.interpretFlags();

        /* Figure out what we are dealing with: SD or FSD, and create, if needed, a map: */
        Object obj = File_handles.findHandle(bads.handle);
        if (obj instanceof SD_entry)
        {
          SD_entry sd  = bads.sd = (SD_entry) obj;
          bads.lun     = sd.lun;
          bad_data_map = sd.bad_data_map;
          if (bad_data_map == null)
            bad_data_map = sd.bad_data_map = new HashMap(8);
          bads.name_left = sd.sd_name8;
        }

        else if (obj instanceof ActiveFile)
        {
          ActiveFile afe = bads.afe = (ActiveFile) obj;
          bads.name_left = afe.getFileEntry().getAnchor().fsd_name_8bytes;
          bads.lun       = afe.getFileEntry().getFullName();
          bad_data_map   = afe.getFileEntry().getAnchor().bad_data_map;
          if (afe.getFileEntry().getAnchor().bad_data_map == null)
            bad_data_map = afe.getFileEntry().getAnchor().bad_data_map = new HashMap(8);

          /* Mark this sector BAD as soon as we can: */
          afe.getFileEntry().setBlockBad(512);
        }

        else
          throw new RuntimeException("reportBadSector(): unexpected file handle return: " + obj);

        /* Create or reuse a BadDataBlock instance: */
        BadDataBlock bdb = bad_data_map.get(bads.rel_data_lba);
        if (bdb == null)
        {
          bdb = new BadDataBlock(bads);
          bad_data_map.put(bads.rel_data_lba, bdb);
        }

        /* Find the DV map for later use: */
        if ((bads.dv = DV_map.findExistingMap(bads.name_left.trim())) == null)
          common.failure("Unable to find Data Validation map for '%s'", bads.name_left.trim());
        bads.dv.countBadSectors();

        /* Immediately mark this key block bad:     */
        // no can't do, journal recovery may still happen
        //bads.dv.dv_set(bads.file_start_lba + bads.key_lba, DV_map.DV_ERROR);

        /* Add this bad sector to the bad block: */
        bdb.addSector(bads);

        //debugging
        if (common.get_debug(common.DV_PRINT_SECTOR_IMMED))
        {
          plog("Reporting due to 'DV_PRINT_SECTOR_IMMED'");
          bads.printOneSector();
          ErrorLog.flush();
        }
      }
    }
    catch (Exception e)
    {
      common.ptod("Exception during signalBadSector()");
      common.ptod(e);
      common.ptod("Flushing local errorlog");
      ErrorLog.flush();
      common.failure(e);
    }
  }




  public void printOneSector()
  {
    String left;
    String right;
    String hex_ts = Format.hex(getTimeStampRead(), 14);

    /* If we receive garbage the hex timestamp can be larger than 14: */
    if (hex_ts.length() > 14)
      hex_ts = hex_ts.substring(0,14);


    /* This line serves only for DVPost(): */
    //String txt = String.format("dvpost: %s %s %s 0x%08x 0x%08x %d 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x %d %d",
    //                           lun,                                               //  3
    //                           name_left.trim(),                                  //  4
    //                           getNameString(),                                   //  5
    //                           file_start_lba,                                    //  6
    //                           key_lba,                                           //  7
    //                           key_blksize,                                       //  8  This is Key block size
    //                           offset_in_key_block / 512,                         //  9
    //                           getTimeStampRead(),                                    // 10
    //                           error_flag,                                        // 11
    //                           key,                                               // 12
    //                           getChecksum(),                                     // 13
    //                           (ts < 0) ? ts * -1 : ts,                           // 14
    //                           compression,                                       // 15
    //                           dedup_set);                                        // 16
    // what to do with this?
    //print(txt);


    long sector = offset_in_key_block / 512;


    /* First header line: */
    plog("");
    if (afe != null)
    {
      plog("        Data Validation error for fsd=%s; "+
           "FSD lba: 0x%08x; Key block size: %d; " +
           "relative sector in data block: 0x%02x ",
           name_left.trim(), sector_lba, key_blksize, sector);
      plog("        File name: %s; file block lba: 0x%08x; bad sector file lba: 0x%08x",
           lun, file_lba, sector_lba - file_start_lba);
    }
    else
    {
      plog("        Data Validation error for sd=%s,lun=%s ", name_left.trim(), lun);
      plog("        Block lba: 0x%08x; sector lba: 0x%08x; Key block size: %d; " +
           "relative sector in data block: 0x%02x (%2d); current pid: %d (0x%x)",
           key_lba, sector_lba, key_blksize, sector,
           sector, common.getProcessId(), common.getProcessId());
    }

    if (!owning_bkb.allMatching())
      report_flags(error_flag);


    /* Second header line: */
    if ((error_flag & BAD_NAME) != 0)
      plog("        SD or FSD name in block expected: '%s'; received: '%s'.",
           name_left.trim(), getNameString());


    /* Third header line: */
    if (!Dedup.isDedup() || dedup_set == Dedup.UNIQUE_BLOCK_ACROSS_YES)
    {
      if (!owning_bkb.allMatching())
      {
        String todstr = dv_df.format(new Date(getTimeStampRead()) );
        plog("        Timestamp found in sector: %s", todstr);
      }
    }

    /* Checksum line: */
    if ((error_flag & BAD_CHECKSUM) != 0)
    {
      plog("        Checksum on timestamp failed. Timestamp: 0x%s; checksum %s; should be %s",
           hex_ts, Format.hex(getChecksum(), 2), Format.hex(calc_checksum(hex_ts)));
    }

    /* First data lines: */
    expected_pattern = getExpectedPattern();
    printFirstLine();
    printSecondLine();


    /* We are now going to report the good or bad data. If we had a bad key */
    /* or bad lba then we should not bother with the data we received from  */
    /* JNI. Just get the data pattern matching the lba and key found and    */
    /* report that, if there is a difference.                               */
    if (!checkOtherContent())
    {
      /* Suppress redundant garbage. However, is the statement below still */
      /* correct when using compression? */
      if ((error_flag & BAD_KEY) != 0 && (error_flag & BAD_DATA) != 0)
        plog("        Key miscompare always implies Data miscompare. Remainder of data suppressed.");

      else
      {
        /* Third through last line: */
        int lines_suppressed = 0;
        for (int i = 8; i < 128; i+=4)
        {
          /* Left portion: */
          left = String.format("%08x %08x %08x %08x",
                               expected_pattern[i+0],
                               expected_pattern[i+1],
                               expected_pattern[i+2],
                               expected_pattern[i+3]);

          /* Right portion: */
          right = String.format("%08x %08x %08x %08x",
                                sector_array[i+0],
                                sector_array[i+1],
                                sector_array[i+2],
                                sector_array[i+3]);

          combine_and_print(i, left, right);
        }
      }
    }

    ErrorLog.flush();
  }


  private static void plog(String format, Object ... args)
  {
    ErrorLog.add(format, args);
  }

  private void report_flags(int error_flag)
  {
    ArrayList <String> flags = getFlagText();
    for (String flag : flags)
      plog("        ===> " + flag);
  }

  public ArrayList <String> getFlagText()
  {
    ArrayList <String> flags = new ArrayList(4);
    if (bad_key      ) flags.add(String.format("Data Validation Key miscompare. "+
                                               "Expecting key=0x%02x received key=0x%02x (%d/%d)",
                                               key_expected, getKey(),
                                               key_expected, getKey()));
    if (bad_checksum ) flags.add(String.format("Checksum error on timestamp. "+
                                               "Checksum: 0x%02x, timestamp: 0x%016x",
                                               getChecksum(), getTimeStampRead()));
    if (bad_lba      ) flags.add(String.format("Logical byte address miscompare. "+
                                               "Expecting 0x%08x, receiving 0x%08x",
                                               sector_lba, getLba()));
    if (bad_name     ) flags.add(String.format("SD or FSD name miscompare. "+
                                               "Expecting '%s', receiving '%s' (0x%016x)",
                                               name_left, getNameString(), getNameHex()));
    if (bad_data     ) flags.add("Data Corruption beyond fixed sector header.");
    if (bad_comp     ) flags.add("Compression pattern miscompare.");
    if (bad_dedupset ) flags.add("Bad dedup set value.");
    if (bad_zero     ) flags.add("Bad reserved field contents (mbz).");
    if (bad_pid      ) flags.add("Bad process id.");
    if (unique       ) flags.add("This is a unique (dedup) block.");
    if (duplicate    ) flags.add("This is a duplicate (dedup) block.");

    return flags;
  }

  public void interpretFlags()
  {
    bad_key      = ((error_flag & BAD_KEY      ) != 0);
    bad_checksum = ((error_flag & BAD_CHECKSUM ) != 0);
    bad_lba      = ((error_flag & BAD_LBA      ) != 0);
    bad_name     = ((error_flag & BAD_NAME     ) != 0);
    bad_data     = ((error_flag & BAD_DATA     ) != 0);
    bad_comp     = ((error_flag & BAD_COMP     ) != 0);
    bad_dedupset = ((error_flag & BAD_DEDUPSET ) != 0);
    bad_zero     = ((error_flag & BAD_ZERO     ) != 0);
    bad_pid      = ((error_flag & BAD_PID     ) != 0);

    /* Key miscompare ALWAYS implies data miscompare, so 'bad_data' is irrelevant: */
    if (bad_key)
      bad_data = false;

    if (Dedup.isDedup())
    {
      if ((dedup_set & Dedup.UNIQUE_BLOCK_MASK) != 0)
        unique = true;
      else
        duplicate = true;
    }
  }


  private static int calc_checksum(String hex)
  {
    int check = 0;

    if (hex.length() != 14)
      throw new RuntimeException("hex string must be 14 digits: '" + hex + "'");


    for (int i = 0; i < 7; i++)
    {
      check += Integer.parseInt(hex.substring(i*2,i*2+2), 16);
      //common.ptod("hex: " + hex.substring(i*2,i*2+2) + "  " + check);
    }

    check &= 0xff;

    //common.ptod(Format.f(" hex:      0x%s",   hex));
    //common.ptod(Format.f(" check:    0x%02x", check));

    return check;
  }

  /**
   * Determine what the pattern is that we compare with.
   */
  private int[] getExpectedPattern()
  {
    int[] pattern = new int[512/4];
    if (!Validate.isCompression())
    {
      //common.ptod("sector_lba1: %016x", sector_lba);
      //common.ptod("key:               " + key);
      //common.ptod("name_left:         >>>" + name_left + "<<<");
      Native.fillLfsrArray(pattern, sector_lba, key_expected, name_left);

      /* fillLfsr fills bytes 0-511; we need bytes 32-511 placed at offset 32: */
      int[] p2 = new int[512/4];
      System.arraycopy(pattern, 0, p2, 32/4, 480/4);

      return p2;
    }

    /* Compression pattern is needed. For that we need the 'compression' field: */
    int[] comp_pattern = Patterns.getPattern();

    if (false) // once++ == 0)
    {
      for (int i = 0; i < 128; i+=4)
        common.ptod("%4d %03x %08x %08x %08x %08x", i*4, i*4,
                    comp_pattern[i+0],
                    comp_pattern[i+1],
                    comp_pattern[i+2],
                    comp_pattern[i+3]);
    }

    if (compression < 0)
      throw new RuntimeException("Unexpected negative compression field: " + compression);

    for (int i = 0; i < pattern.length; i++)
    {
      int pattern_offset = (int) (compression + offset_in_key_block + i*4) % (comp_pattern.length * 4) / 4;
      pattern[i] = comp_pattern[ pattern_offset ];
    }

    return pattern;
  }


  private void printFirstLine()
  {
    /* Without dedup, or unique across: */
    if (!Dedup.isDedup() || (dedup_set & Dedup.UNIQUE_BLOCK_MASK) != 0)
    {
      plog("0x%03x%s  %08x %08x %s %s   %08x %08x %08x %08x",
           0,
           ((error_flag & BAD_LBA) != 0 || (error_flag & BAD_CHECKSUM) != 0) ? "*" : " ",
           (int) (sector_lba >> 32),
           (int) sector_lba,
           "........",
           "........",
           sector_array[0],
           sector_array[1],
           sector_array[2],
           sector_array[3]);
    }

    /* Dedup, unique not across: */
    else if (dedup_set == Dedup.UNIQUE_BLOCK_ACROSS_NO)
    {
      plog("0x%03x%s  %08x %08x %08x %08x   %08x %08x %08x %08x",
           0,
           ((error_flag & BAD_LBA) != 0) ? "*" : " ",
           (int) (sector_lba >> 32),
           (int) sector_lba,
           expected_pattern[2],
           expected_pattern[3],
           sector_array[0],
           sector_array[1],
           sector_array[2],
           sector_array[3]);
    }

    /* Dedup, duplicates: */
    else
    {
      plog("0x%03x%s  %08x %08x %08x %08x   %08x %08x %08x %08x",
           0,
           ((error_flag & BAD_DEDUPSET) != 0) ? "*" : " ",
           (int) (dedup_set >> 32),
           (int) dedup_set,
           expected_pattern[2],
           expected_pattern[3],
           sector_array[0],
           sector_array[1],
           sector_array[2],
           sector_array[3]);
    }
  }

  private void printSecondLine()
  {
    /* Without dedup, or unique across: */
    if (!Dedup.isDedup() || (dedup_set & Dedup.UNIQUE_BLOCK_MASK) != 0)
    {
      String hex = name_to_hex(name_left);
      plog("0x%03x   %02x..0000 %-8s %-8s %08x   %08x %08x %08x %08x",
           16,
           key_expected,
           hex.substring(0, 8),
           hex.substring(8),
           0,
           sector_array[4],
           sector_array[5],
           sector_array[6],
           sector_array[7]);
    }

    else
    {
      String left = String.format("%08x %08x %08x %08x",
                                  expected_pattern[4],
                                  expected_pattern[5],
                                  expected_pattern[6],
                                  expected_pattern[7]);
      String right = String.format("%08x %08x %08x %08x",
                                   sector_array[4],
                                   sector_array[5],
                                   sector_array[6],
                                   sector_array[7]);
      plog("0x%03x%s  %s   %s", 16, (left.equals(right)) ? " " : "*", left, right);
    }
  }



  /**
   * Combine left and right text, add the offset and print but only if there are
   * differences.
   */
  private static void combine_and_print(int index, String left, String right)
  {
    String diff = "*";

    /* Any differences? */
    if (left.equals(right))
    {
      diff = " ";
      return;
    }

    /* Combine left and right: */
    plog("0x%03x%s  %s   %s ", index*4, diff, left, right);
  }

  /**
   * Check to see if it is worth it to print the rest of the data pattern.
   */
  private boolean checkOtherContent()
  {
    boolean mismatch = false;

    /* If all the rest of the data is fine, do not report: */
    for (int i = 8; i < 128; i++)
    {
      if (sector_array[i] != expected_pattern[i])
        mismatch = true;
    }
    if (!mismatch)
    {
      plog("        There are no mismatches in bytes 32-511\n");
      return true;
    }

    /* Don't bother with LFSR: */
    if ((data_flag & Validate.FLAG_COMPRESSION) != 0)
      return false;

    /* When either the key or lba were bad, try different LFSR pattern: */
    if ((error_flag & BAD_KEY) != 0 || (error_flag & BAD_LBA) != 0)
    {
      /* Whatever 'name' we found in the block, it must be 8 bytes: */
      if (getNameString().length() < 8)
        return false;

      int  use_key = sector_array[4] >>> 24;
      long use_lba = Jnl_entry.make64(sector_array[0], sector_array[1]);

      /* Create an LFSR array using this data: */
      Native.fillLfsrArray(lfsr_sector, use_lba, use_key, getNameString());

      /* If this data matches, just report and continue: */
      for (int i = 8; i < 128; i++)
      {
        if (sector_array[i] != lfsr_sector[i])
          mismatch = true;
      }
      if (!mismatch)
      {
        plog("        Data pattern matches the incorrect key and/or lba that was read.\n");
        return true;
      }
    }

    return false;
  }


  private static String name_to_hex(String name)
  {
    String txt = "";

    if (name.length() != 8)
      name = (name + "        ").substring(0, 8);

    if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
    {
      for (int i = 0; i < name.length(); i++)
      {
        txt = txt + Format.hex(name.charAt(i), 2);
      }
    }

    else
    {
      /* Take the first 4 bytes and reverse them: */
      txt = txt + Format.hex(name.charAt(3), 2);
      txt = txt + Format.hex(name.charAt(2), 2);
      txt = txt + Format.hex(name.charAt(1), 2);
      txt = txt + Format.hex(name.charAt(0), 2);

      /* Then follow with the next 4: */
      txt = txt + Format.hex(name.charAt(7), 2);
      txt = txt + Format.hex(name.charAt(6), 2);
      txt = txt + Format.hex(name.charAt(5), 2);
      txt = txt + Format.hex(name.charAt(4), 2);
    }

    return txt;
  }

  public String getTimeStampText()
  {
    //if (!Dedup.isDedup() || (dedup_set & Dedup.UNIQUE_MASK) == Dedup.UNIQUE_BLOCK_ACROSS_YES)
    if (!Dedup.isDedup() || Dedup.isUnique(dedup_set))
      return dv_df.format(new Date(getTimeStampRead()) );
    else
      return null;
  }

  public int getKey()
  {
    return sector_array[4] >>> 24;
  }
  public int getChecksum()
  {
    return sector_array[4] << 8 >>> 24;
  }
  public long getLba()
  {
    return Jnl_entry.make64(sector_array[0], sector_array[1]);
  }
  public long getTimeStampRead()
  {
    return Jnl_entry.make64(sector_array[2], sector_array[3]);
  }
  public long getNameHex()
  {
    return Jnl_entry.make64(sector_array[5], sector_array[6]);
  }

  /**
   * Translate long value to String.
   * Properly handle big-endian and little-endian.
   *
   * Returning EIGHT characters
   */
  public String obsolete_getNameString()
  {
    /* BIG_ENDIAN SPARC: */
    if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
    {
      throw new RuntimeException("tbd: BIG_ENDIAN");
    }


    /* LITTLE_ENDIAN X86: */
    int word0 = sector_array[5];
    int word1 = sector_array[6];
    char[] chars = new char[8];
    for (int i = 0; i < 4; i++)
      chars[ i ] = (char) ( word0 << (i*8) >>> 24);
    for (int i = 0; i < 4; i++)
      chars[ 4 + i ] = (char) ( word1 << (i*8) >>> 24);

    char[] special_chars = new char[] {'-', '_', '\'', ' '};

    String name = "";

    for (int i = 3; i >= 0; i--)
    {
      if (Character.isLetterOrDigit(chars[i]) ||
          new String(special_chars).indexOf(chars[i]) >= 0)
        name = String.format("%s%s", name, new Character(chars[i]));
      else
        return "garbage ";
    }
    for (int i = 7; i >= 4; i--)
    {
      if (Character.isLetterOrDigit(chars[i]) ||
          new String(special_chars).indexOf(chars[i]) >= 0)
        name = String.format("%s%s", name, new Character(chars[i]));
      else
        return "garbage ";
    }

    name = name.trim();
    if (name.contains(" "))
      return "garbage ";

    String ret = (name + "         ").substring(0,8);

    common.ptod("ret: >>>%s<<<", ret);
    common.failure("testing");

    return ret;
  }

  /* Note there is a blank here, this to allow hex-to-ascii translation of  */
  /* trailing blanks                                                        */
  /* This blank of course should not be allowed for sd or fsd name checking */
  private static String allowed_chars = "0123456789" +
                                        "abcdefghijklmnopqrstuvwxyz" +
                                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                                        "-_\' ";


  /**
   * Translate long value to String.
   *
   * This is only needed for Data Validation where eight bytes of possible
   * garbage received after a corruption is translated back into an ASCII SD or
   * FSD name with the hope that we can verify that, although the data read is
   * the WRONG data, maybe it just is the wrong data but that wrong data in
   * itself is not corrupted.
   *
   * (Is it really worth the effort?)
   *
   *
   * Properly handle big-endian and little-endian.
   * The code is based onclick little endian, but onclick big endian we first
   * reverse the integers.
   *
   * Returning EIGHT characters.
   *
   * I have not been able to figure out why the original code ended up causing
   * problems in JNI code where the eight-letter translation done in JNI
   * resulted in more than 8 bytes.
   * This is a decent workaround.
   *
   * Keep in mind though that when there is garbage coming out of
   * 'sector_array', it is highly unlikely that the result is a true SD or FSD
   * name. However, at least the JNI code won't die.
   */
  public String getNameString()
  {
    /* LITTLE_ENDIAN X86: */
    int word0 = sector_array[5];
    int word1 = sector_array[6];

    /* BIG_ENDIAN SPARC: */
    if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
    {
      word0 = Integer.reverseBytes(word0);
      word1 = Integer.reverseBytes(word1);
    }

    char[] chars = new char[8];
    for (int i = 0; i < 4; i++)
      chars[ i ] = (char) ( word0 << (i*8) >>> 24);
    for (int i = 0; i < 4; i++)
      chars[ 4 + i ] = (char) ( word1 << (i*8) >>> 24);

    String name = "";

    for (int i = 3; i >= 0; i--)
    {
      char ch = chars[i];

      if (allowed_chars.indexOf(ch) != -1)
        name = String.format("%s%s", name, new Character(ch));
      else
        return "garbage1";
    }

    for (int i = 7; i >= 4; i--)
    {
      char ch = chars[i];
      if (allowed_chars.indexOf(ch) != -1)
        name = String.format("%s%s", name, new Character(ch));
      else
        return "garbage2";
    }

    name = name.trim();
    if (name.contains(" "))
      return "garbage1";

    String value = (name + "         ").substring(0,8);

    return value;

  }

  public static void main(String[] args)
  {
    BadSector bads = new BadSector();
    bads.sector_array = new int[8];

    bads.sector_array[5] = 0x79454d6e;
    bads.sector_array[6] = 0x3332344c;

    // data coming from big_endian
    bads.sector_array[5] = 0x73643120;   //    sd1
    bads.sector_array[6] = 0x20202020;


    // data coming from little_endian
    //bads.sector_array[5] = 0x20316473;   //    sd1
    //bads.sector_array[6] = 0x20202020;

    common.ptod("name: >>>%s<<<", bads.getNameString());

  }

  public static void main2(String[] args)
  {
    /* BIG_ENDIAN SPARC: */
    //if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
    //{
    //  throw new RuntimeException("tbd: BIG_ENDIAN");
    //}


    /* LITTLE_ENDIAN X86: */
    int word0;
    int word1;

    word0 =  0x4cddd59e;  //  sector_array[5];
    word1 =  0x5e274593;  //  sector_array[6];

    word0 =  0x9e65dd4c;  //  sector_array[5];
    word1 =  0x9345275e;  //  sector_array[6];

    Random rand = new Random(0);

    int valids = 0;
    top:
    for (int x = 0; x < 100000000; x++)
    {
      word0 = rand.nextInt();
      word1 = rand.nextInt();

      // word0 = 0x38d96666;
      // word1 = 0x79dddd35;
      //
      // word0 = 0x73643120;
      // word1 = 0x20202020;

      char[] chars = new char[8];
      for (int i = 0; i < 4; i++)
        chars[ i ] = (char) ( word0 << (i*8) >>> 24);
      for (int i = 0; i < 4; i++)
        chars[ 4 + i ] = (char) ( word1 << (i*8) >>> 24);

      String name = "";

      for (int i = 3; i >= 0; i--)
      {
        char ch = chars[i];

        if (allowed_chars.indexOf(ch) != -1)
          name = String.format("%s%s", name, new Character(ch));
        else
        {
          //common.ptod("garbage1 for %08x %08x", word0, word1);
          continue top;
        }
      }

      for (int i = 7; i >= 4; i--)
      {
        char ch = chars[i];
        if (allowed_chars.indexOf(ch) != -1)
          name = String.format("%s%s", name, new Character(ch));
        else
        {
          //common.ptod("garbage2 for %08x %08x", word0, word1);
          continue top;
        }
      }

      name = name.trim();
      if (name.contains(" "))
      {
        //common.ptod("garbage3 for %08x %08x '%s'", word0, word1, name);
        continue;
      }

      String value = (name + "         ").substring(0,8);

      common.ptod("valid: %4d >>>%s<<< for %08x %08x", valids++, value, word0, word1);
    }
    common.ptod("valids: " + valids);
  }
}
