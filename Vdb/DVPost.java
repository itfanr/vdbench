package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Fget;
import Utils.Getopt;



/**
 * Post processing program for Data Validation errorlog.html file.
 *
 * The objective is to glean more information from the error log avoiding the
 * need to wade through piles of data.
 * Changes in a post processing program are also easier to do than in the actual
 * Data Validation code which we don't want to break.
 *
 **/
public class DVPost
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static HashMap <String, DvKeyBlock> bad_keyblock_map = new HashMap(128);
  private static HashMap <String, Integer>  lun_keyblock_map = new HashMap(128);


  private static HashMap <String, DataBlock>  data_block_map = new HashMap(128);

  private static int linecount = 0;
  private static int sectorcount = 0;
  private static Fget fg;


  private static String line;
  private static ArrayList <String> overview = new ArrayList(256);

  protected static String name_mask = null;

  private static int duplicates = 0;


  private static String first_failure_date = null;

  private static SimpleDateFormat df = new SimpleDateFormat( "EEEE, MMMM d, yyyy HH:mm:ss.SSS" );


  /**
   * print method: adding text to the 'overview' ArrayList
   */
  public static void print(String txt)
  {
    overview.add(txt);
  }
  public static void print(String format, Object ... args)
  {
    overview.add(String.format(format, args));
  }


  /**
   * This is the 5.02 version of DVPost.
   * This version is NOT compatible with 5.01 errolog.html data.
   */
  public static void main(String[] args)
  {
    Getopt g = new Getopt(args, "db", 2);
    g.print("DVPost");
    if (!g.isOK())
      common.failure("Parameter scan error");

    String filename = ".";
    if (g.get_positionals().size() > 0)
      filename = g.get_positional(0);

    /* Needed in case any Native.xxx ends up doing a PTOD: */
    Native.allocSharedMemory();


    /* Prevent unimportant plog messages from showing up on the console: */
    common.ignoreIfNoPlog();

    if (args.length > 1)
      filename = args[1];
    else if ((filename = PostGui.askForFile(".", "Enter errorlog.html file name")) == null)
      return;

    try
    {
      DvKeyBlock bkb = null;

      /* Scan all the lines of this file until we find specific important lines: */
      if (Fget.dir_exists(filename))
        filename = new File(filename, "errorlog.html").getAbsolutePath();
      fg = new Fget(filename);
      //common.ptod("filename: " + filename);
      while ((line = fg.get()) != null)
      {
        linecount++;

        /* This is the line that identifies that we are finished reporting */
        /* all the bad sectors of a block. Without this line for a block   */
        /* we do not really know how many bad sectors there are, unless    */
        /* of course we have seen ALL sectors:                             */
        if (line.contains("op:"))
        {
          foundErrorLine();
          continue;
        }
        /*
      String txt = String.format("dvpost: %s %s %s 0x%08x 0x%08x %d 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x %d %d",
                   lun,                                               //  3
                   name_left.trim(),                                  //  4
                   Sector.check8byteString(name_right.trim()).trim(), //  5
                   file_start_lba,                                    //  6
                   file_lba,                                          //  7
                   xfersize,                                          //  8  This is Key block size
                   offset_in_block / 512,                             //  9
                   timestamp,                                         // 10
                   error_flag,                                        // 11
                   key,                                               // 12
                   checksum,                                          // 13
                   (ts < 0) ? ts * -1 : ts,                           // 14
                   compression,                                       // 15
                   dedup_set);                                        // 16
        */

        if (line.contains("Time of first failure:"))
        {
          String[] split = line.split(" +");
          first_failure_date = String.format("%s %s %s %s", split[6], split[7], split[8], split[9]);
          //common.ptod("first_failure_date: " + first_failure_date);
          continue;
        }


        /* Scan until we reach the dvpost line: */
        if (!line.contains("dvpost:"))
          continue;


        /* Rip this line apart: */
        //common.ptod("line: " + line);
        bkb            = new DvKeyBlock();
        String[] split = line.split(" +");

        /* For Kaminario: \\.\F:*/
        //if (split[4].endsWith("'") && split.length == 16)
        //if (split[3].endsWith(":") && split.length == 16)
        //{
        //  String newline = "";
        //  for (int i = 0; i < 5; i++)
        //    newline += split[i] + " ";
        //  newline += "kamiblnks ";
        //  for (int i = 5; i < 16; i++)
        //    newline += split[i] + " ";
        //  split = newline.split(" +");
        //}

        bkb.lun             = split[3];
        bkb.sd_wanted       = split[4];
        bkb.file_start_lba  = Long.parseLong(split[6].substring(2), 16);
        bkb.file_lba        = Long.parseLong(split[7].substring(2), 16);
        bkb.key_block_size  = Integer.parseInt(split[8]);
        bkb.key_wanted      = Integer.parseInt(split[12].substring(2), 16);

        bkb.sectors         = new Sector[bkb.key_block_size / 512];
        bkb.logical_lba     = bkb.file_start_lba + bkb.file_lba;
        if (first_failure_date != null)
          bkb.block_first_seen = df.parse(first_failure_date + " " + split[0]);

        int      offset_in_block = Integer.parseInt(split[9].substring(2), 16) * 512;
        long     timestamp       = (split[10].substring(2).equals("ffffffffffffffff")) ?
                                   0 : Long.parseLong(split[10].substring(2), 16);
        int      error_flag      = Integer.parseInt(split[11].substring(2), 16);
        int      checksum        = Integer.parseInt(split[13].substring(2), 16);

        bkb.dvpost_date = new Date(timestamp / 1000);

        /* Store the key block size for the current lun/file: */
        if (lun_keyblock_map.get(bkb.lun) == null)
          lun_keyblock_map.put(bkb.lun, bkb.key_block_size);


        /* Any error below, treat it as the errorlog.html having bad data. */
        /* Including maybe the file not being properly closed. */
        try
        {
          /* This is either a block that we already saw, or a new block: */
          bkb  = haveBadSector(bkb, offset_in_block);
          if (bkb == null)
            continue;
        }
        catch (Exception e)
        {
          common.ptod("Exception, treating it as end-of-file");
          common.ptod(e);
          break;
        }
      }

      if (duplicates > 0)
        common.failure("%d duplicate blocks found", duplicates);

      if (bad_keyblock_map.size() == 0)
        common.failure("No bad blocks found. No 'dvpost:' lines found?");

      reportSomeSectorStuff();

      if (!g.check('b'))
      {
        PostGui window = new PostGui(filename, sortBlocks("lun"), overview);
        window.setVisible(true);
        window.repaint();
        return;
      }

      for (String ov : overview)
        System.out.println(ov);
    }

    catch (Exception e)
    {
      DVPost.print("linecount: " + linecount);
      DVPost.print("line: " + line);
      common.failure(e);
    }
  }


  /**
   * We have a bad sector. Pick up as much information as possible.
   */
  private static DvKeyBlock haveBadSector(DvKeyBlock bkb_in, int offset_in_block)
  {
    /*
    line0:         Data Validation error at byte offset 0xd91b0000 (3642425344); 512 byte block offset 0x6c8d80 (7114112)
    line1:         Relative  32768-byte data block: 0x1b236; relative sector in block: 0x00
    or             Relative 131072-byte data block: 0x1;     block lba: 0x20000 ; relative sector in block: 0x38 (56)
    line2:         SD name miscompare.
    line3:         Lun: /dev/rdsk/c0t75d0s2; SD name in block expected: 'sd3'; read: 'sd2'.
    or             Lun: r:\junk\vdb2;           name in block expected: 'sd1'; read: 'sd1'.
    line4:         The disk block just read was written Tuesday, June 23, 2010 09:57:28.390
    line5: 0x000   00000000 d91b0000 ........ ........   00000000 d91b0000 00046d06 e2c14466
    line6: 0x010*  01..0000 73643320 20202020 00000000   01c40000 73643220 20202020 00000000
    line7: 0x020   4db23200 26d91900 136c8c80 09b64640   4db23200 26d91900 136c8c80 09b64640
    .....
    */

    String[] split;

    /* See if we already have this block, if not, add it: */
    String lun_lba = bkb_in.lun + " " + bkb_in.logical_lba;
    DvKeyBlock bkb = bad_keyblock_map.get(lun_lba);
    if (bkb == null)
    {
      bkb = bkb_in;
      bad_keyblock_map.put(lun_lba, bkb);
      if (bkb.first_dvpost_line_tod == null)
        bkb.first_dvpost_line_tod = line.split(" ")[0];
      //common.ptod("lun_lba: " + lun_lba);
    }

    /* Save the 'dvpost:' line, stripped of its overhead, but first save the message tod: */
    String tod = line.split(" ")[0];
    split = splitLine();
    bkb.raw_input.add("");
    bkb.raw_input.add(line);

    /* Continue until we find line5: */
    while ((line = fg.get()) != null)
    {
      linecount++;

      if (line.contains("The sector below was written"))
      {
        if (bkb.first_sector_written_tod == null)
        {
          bkb.first_sector_written_tod = parseWrittenDate(line);
        }
        //continue;
      }

      split = splitLine();
      bkb.raw_input.add(line);
      if (split.length == 0)
        continue;

      /* This comes from the 'read_immediately' option: */
      if (line.contains("Time that this block"))
      {
        String last = null;
        if (line.contains("read:"))
        {
          bkb.last_valid_rw = "read";
          last = line.substring(line.indexOf("read:") + 5).trim();
        }
        else
        {
          bkb.last_valid_rw = "write";
          last = line.substring(line.indexOf("write:") + 6).trim();
        }

        if (bkb.last_tod_valid == null)
          bkb.last_tod_valid = last;
        else if (!last.equalsIgnoreCase(bkb.last_tod_valid))
          bkb.last_tod_valid = "mixed";

        //common.ptod("bkb.last_tod_valid: " + bkb.last_tod_valid);
        bkb.last_valid = parseLastDate(bkb.last_tod_valid);
        //common.ptod("bkb.last_valid: " + bkb.last_valid);
      }

      /* Break at line5: */
      if (split[0].equals("0x000") || split[0].equals("0x000*"))
        break;
    }

    /* Pick up line5 and line6 and the (optional) next 30 lines: */
    String[] hex_lines = new String[32];
    String[] line0_split = splitLine();
    hex_lines[0]       = line;
    int count          = 1;
    for (int i = 16; i < 512; i+=16)
    {
      if ((line = fg.get()) == null)
        break;

      linecount++;
      bkb.raw_input.add(line.substring(26));
      split = splitLine();
      String tmp = String.format("0x%03x", i);
      if (!split[0].startsWith(tmp))
        break;
      hex_lines[i / 16] = line;
      count++;
    }

    /* See if we have all 32 lines: */
    //if (count != 32)
    //{
    //  DVPost.print("Did not receive a complete sector for sector # " + line0_split[1] + " " + line0_split[2]);
    //  return null;
    //}

    /* Now split the 32 lines of data that we have into 'expected' and 'read': */
    Sector sector        = new Sector();
    sector.lba_wanted    = bkb.logical_lba + offset_in_block;
    sector.tod_in_sector = bkb.dvpost_date;
    bkb.sectors[offset_in_block / 512] = sector;
    bkb.sectors_reported ++;

    for (int i = 0; i < count; i++)
    {
      line  = hex_lines[i];
      split = line.trim().split(" +");

      /* Replace the missing timestamp in line5: */
      if (split[3].equals("........"))
        split[3] = split[4] = "33333333";

      /* Replace the missing checksum in line6: */
      else if (split[1].indexOf("..") != -1)
        split[1] = split[1].substring(0, 2) + "33" + split[1].substring(4);

      /* Pick up all the data, both 'expected' and 'read': */
      sector.expected[i * 4 + 0] = (int) Long.parseLong(split[1], 16) >>> 32;
      sector.expected[i * 4 + 1] = (int) Long.parseLong(split[2], 16) >>> 32;
      sector.expected[i * 4 + 2] = (int) Long.parseLong(split[3], 16) >>> 32;
      sector.expected[i * 4 + 3] = (int) Long.parseLong(split[4], 16) >>> 32;

      sector.was_read[i * 4 + 0] = (int) Long.parseLong(split[5], 16) >>> 32;
      sector.was_read[i * 4 + 1] = (int) Long.parseLong(split[6], 16) >>> 32;
      sector.was_read[i * 4 + 2] = (int) Long.parseLong(split[7], 16) >>> 32;
      sector.was_read[i * 4 + 3] = (int) Long.parseLong(split[8], 16) >>> 32;

      /* Pick up the lba as was read: */
      if (i == 0)
      {
        try
        {
          sector.lba_read = Long.parseLong(split[5] + split[6], 16);
        }
        catch (NumberFormatException e)
        {
          DVPost.print("Unparsable lba. Needs further investigation: %s %s",split[5], split[6] );
          sector.lba_read =-1;
        }
      }
    }

    /* Translate the SD as it was read: */
    sector.sd_wanted = bkb.sd_wanted;
    sector.sd_read   = xlateSD(sector.was_read[5], sector.was_read[6], bkb.sd_wanted, hex_lines[1]);
    if (sector.sd_read.length() == 0)
    {
      common.ptod("line: " + line);
      common.failure("empty sd");
    }

    /* Pick up keys: */
    bkb.key_wanted     =
    sector.key_wanted = sector.expected[4] >> 24;
    sector.key_read   = sector.was_read[4] >> 24;
    bkb.key_read       = sector.key_read;
    //DVPost.print("sector.key_wanted: " + sector.key_wanted + " " + sector.key_read);

    bkb.different_words_in_block += sector.countDifferences();


    /* If the lba was wrong, get LFSR data belonging to bad lba: */
    if (sector.lba_wanted != sector.lba_read)
      sector.getBadLbaData();


    for (int i = 99990; i < 32; i++)
    {
      DVPost.print("              %08x %08x %08x %08x   %08x %08x %08x %08x ",
                   sector.expected[i * 4 + 0],
                   sector.expected[i * 4 + 1],
                   sector.expected[i * 4 + 2],
                   sector.expected[i * 4 + 3],
                   sector.was_read[i * 4 + 0],
                   sector.was_read[i * 4 + 1],
                   sector.was_read[i * 4 + 2],
                   sector.was_read[i * 4 + 3]);
    }

    // xx zz: 0x000*  00000000 00160000 ........ ........   00000000 0d55b000 00047363 6ac8d061
    // xx zz: 0x010*  01..0000 73643237 20202020 00000000   013d0000 38316473 20202020 00000000
    return bkb;
  }

  // 0  1  2  3   4    5    6                     7     8       9          10     11   12     13
  // xx yy zz op: read lun: r:\quick_vdbench_test lba:  5810176 0x0058A800 xfer:  1024 errno: 60003 A Data validation error was discovered
  /**
   * Parse a line labeled 'op:' with the real data block info:
   */
  private static void foundErrorLine()
  {
    String[] split = line.trim().split(" +");
    //common.ptod("line: " + line);

    /* Pick up some stuff: */
    long   lba     = Long.parseLong(split[8]);
    String lun     = split[6];
    String lun_lba = lun + " " + lba;

    if (!split[13].equals("60003"))
      return;

    /* Create a DataBlock instance for each real data block we found. */
    /* This is different from BadBlock which is only a Key block. */
    if (data_block_map.get(lun_lba) != null)
    {
      common.ptod("Duplicate i/o error reported: " + lun_lba);
      duplicates++;
      return;
    }
    DataBlock db     = new DataBlock();
    data_block_map.put(lun_lba, db);
    db.logical_lba   = lba;
    db.lun           = lun;
    db.key_blocksize = lun_keyblock_map.get(lun);
    db.data_xfersize = Integer.parseInt(split[11]);
    db.failure       = split[2];

    /* See if we already have this block from 'dvpost:' line: */
    DvKeyBlock bkb = (DvKeyBlock) bad_keyblock_map.get(lun_lba);
    if (bkb == null)
    {
      DVPost.print("Found an i/o error for an lba which was not reported by Data Validation:");
      DVPost.print("This can mean that the first Key block of a data block was NOT in error!");
      String tmp = common.replace(line, "  ", " ");
      DVPost.print(tmp.substring(tmp.indexOf("op:")));
      DVPost.print("");

      return;
    }

    /* Add the op: info to the BadBlock that we found earlier with dvpost dinfo: */
    bkb.failed_operation = split[4];
    bkb.error_code = Integer.parseInt(split[13]);
    if (bkb.error_code != 60003 && bkb.error_code != 803)
      bad("Invalid i/o error code: " + bkb.error_code);
  }




  private static String xlateSD(long sd1, long sd2, String wanted, String hex)
  {
    /* Just in case we read zeros: */
    if (sd1 == 0 || sd2 == 0)
      return "nulls";
    if (sd1 == -1 || sd2 == -1)
      return "neg-1";

    String sd = "";
    sd += (char) (sd1 >>> 24 & 0xff);
    sd += (char) (sd1 >>> 16 & 0xff);
    sd += (char) (sd1 >>>  8 & 0xff);
    sd += (char) (sd1 >>>  0 & 0xff);
    sd += (char) (sd2 >>> 24 & 0xff);
    sd += (char) (sd2 >>> 16 & 0xff);
    sd += (char) (sd2 >>>  8 & 0xff);
    sd += (char) (sd2 >>>  0 & 0xff);

    /* If the trimmed SD matches, we're happy: */
    sd = sd.trim();
    if (sd.equals(wanted))
      return wanted;


    /* Because of a hi-ending vs. low-ending issue during reporting, */
    /* try a reversal of the name:                                   */
    String reversed = "";
    for (int i = sd.length() - 1; i >= 0; i--)
      reversed += sd.charAt(i);

    /* If the reversed SD matches, we're happy: */
    if (reversed.equals(wanted))
      return wanted;

    /* A guess: if the reversed name starts with 'sd', return reversed: */
    if (reversed.startsWith("sd") || reversed.startsWith("SD"))
      return reversed;

    /* Was this sd really nothing but blanks? */
    if (sd.length() == 0)
      return "'blanks'";

    return sd;
  }

  private static void reportSomeSectorStuff()
  {
    name_mask = getLunNameMask();
    reportBadKeyBlockStatus();
    reportDataBlockStuff();

    reportErrorCodes();
    reportHighDeltas();
    reportWrongKeys();
    reportTimestamps();
    reportSingleBitErrors();
    reportSdNameWrong();
    //reportAllSectorsBad();
    //reportSomeSectorsBad(true);
    //reportSomeSectorsBad(false);
    reportWrongLbas();
    //reportWrongKeys();
  }


  private static DvKeyBlock[] sortBlocks(String order)
  {
    DvKeyBlock[] blocks   = (DvKeyBlock[]) bad_keyblock_map.values().toArray(new DvKeyBlock[0]);
    DvKeyBlock.sort_order = order;
    Arrays.sort(blocks);
    return blocks;
  }

  private static void reportErrorCodes()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");
    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];
      if (bkb.error_code != 0)
        count ++;
    }

    if (count == 0)
      return;

    DVPost.print("\n\n");
    DVPost.print("Blocks that have completed their error reporting: ");
    DVPost.print("%s %s", DvKeyBlock.header(), "error");

    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];
      if (bkb.error_code > 0)
        DVPost.print("%s %d", bkb.print(), bkb.error_code);
    }
  }



  private static void reportBadKeyBlockStatus()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    DVPost.print("\n\n");
    DVPost.print("Bad Key Block status:");
    DVPost.print("%s %s", DvKeyBlock.header(), "Errors");
    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];
      DVPost.print("%s %s", bkb.print(), bkb.getBlockStatusShort());
    }
  }

  private static void reportTimestamps()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    DVPost.print("\n\n");
    DVPost.print("Timestamps of 'last write' found in Key block:");
    DVPost.print("%s %s", DvKeyBlock.header(), "Timestamp found / last");

    HashMap times = new HashMap(16);
    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];
      String stamps = bkb.getTimestamps();
      if (bkb.last_tod_valid != null)
        stamps += " " + bkb.last_tod_valid;
      DVPost.print("%s %s", bkb.print(),  stamps);
    }
  }

  private static void reportWrongKeys()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].getWrongKeys() != null)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Blocks that have at least one wrong key: ");
      DVPost.print("%s %s", DvKeyBlock.header(), "(Expected) read high_delta");
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        String wrongs = bkb.getWrongKeys();

        String delta_info = "";
        int high_delta = bkb.getHighestKeyDelta();
        if (high_delta > 1)
          delta_info = "delta " + high_delta;

        if (wrongs == null)
          continue;
        DVPost.print("%s (%02x) %s %s", bkb.print(), bkb.key_wanted, wrongs, delta_info);
      }
    }
  }

  /**
   * Repoprt blocks that are more than one key generation behind
   */
  private static void reportHighDeltas()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].getWrongKeys() != null)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Key blocks that have at least TWO key delta: ");
      DVPost.print("%s %s", DvKeyBlock.header(), "(Expected) read");
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        String wrongs = bkb.getWrongKeys();
        if (wrongs == null)
          continue;

        int high_delta = bkb.getHighestKeyDelta();
        if (high_delta < 2)
          continue;

        DVPost.print("%s (%02x) %s delta=%d", bkb.print(), bkb.key_wanted, wrongs, high_delta);
      }
    }
  }


  private static void reportWrongLbas()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].getWrongLbas() != null)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Key blocks that have at least one wrong lba in their data:");
      DVPost.print("%s %s", DvKeyBlock.header(), "bad lba");
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        String wrongs = bkb.getWrongLbas();
        if (wrongs == null)
          continue;
        DVPost.print("%s %s", bkb.print(), wrongs);
      }
    }
  }

  /**
   * scan through all data blocks, and see how many of the keyblocks within that
   * datablock are bad.
   */
  private static void reportDataBlockStuff()
  {
    String hdr_mask = name_mask + " %10s %10s %10s %9s %9s %5s %7s %5s %12s %7s";
    String txt_mask = name_mask + " %10x %10x %10x %9d %9d %5d %7d %5d %12s %7s";
    String hdr = String.format(hdr_mask,
                               "lun/file",
                               "log_lba",
                               "start_lba",
                               "file_lba",
                               "xfersize",
                               "keyblocks",
                               "(bad)",
                               "sectors",
                               "(bad)",
                               "error_time",
                               "flag");

    DVPost.print("");
    DVPost.print("Data block information. A data block is divided into 'n' Key blocks, "+
                 "each Key block is divided into 512-byte sectors");
    DVPost.print("");
    DVPost.print(hdr);
    DVPost.print("");

    String[] keys = data_block_map.keySet().toArray(new String[0]);
    Arrays.sort(keys);
    for (String key : keys)
    {
      DataBlock db       = data_block_map.get(key);
      int keyblocks      = db.data_xfersize / db.key_blocksize;
      int sectors        = db.data_xfersize / 512;
      int bad_key_blocks = 0;
      int bad_sectors    = 0;
      long file_start_lba = 0;
      for (int i = 0; i < keyblocks; i++)
      {
        String lun_lba = db.lun + " " + (db.logical_lba + (i * db.key_blocksize));
        DvKeyBlock bkb = bad_keyblock_map.get(lun_lba);
        if (bkb != null)
        {
          file_start_lba = bkb.file_start_lba;
          bad_key_blocks++;
          for (Sector sector : bkb.sectors)
          {
            if (sector != null)
              bad_sectors++;
          }
        }
      }

      String flag = (sectors == bad_sectors) ? "" : "partial";

      String txt = String.format(txt_mask,
                                 db.lun,
                                 db.logical_lba,
                                 file_start_lba,
                                 db.logical_lba - file_start_lba,
                                 db.data_xfersize,
                                 keyblocks,
                                 bad_key_blocks,
                                 sectors,
                                 bad_sectors,
                                 db.failure,
                                 flag);
      DVPost.print(txt);
    }
  }

  private static String getLunNameMask()
  {
    int max_length = 1;
    String[] keys = data_block_map.keySet().toArray(new String[0]);
    for (String key : keys)
      max_length = Math.max(max_length, data_block_map.get(key).lun.length());
    return "%-" + max_length + "s";
  }



  private static void reportSingleBitErrors()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].countSingleBitWords() > 0)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Single bit errors: Blocks that have single bit errors beyond the 32-byte header:");
      DVPost.print("(Single bit error: if any 32-bit word is only one bit off)");
      DVPost.print("%s", DvKeyBlock.header());
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        if (bkb.countSingleBitWords() == 0)
          continue;
        DVPost.print("%s", bkb.print());
      }
    }
  }


  private static void reportSdNameWrong()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].getWrongSdNames() != null)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Mismatch in SD name:");
      DVPost.print("%s %s", DvKeyBlock.header(), "mismatch(es)");
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        String wrongs = bkb.getWrongSdNames();
        if (wrongs == null)
          continue;

        DVPost.print("%-20s %s", bkb.print(), wrongs);
      }
    }
  }


  private static void reportAllSectorsBad()
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];
      if (bkb.key_block_size / 512 == bkb.sectors_reported)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Blocks that have differences in all sectors:");
      DVPost.print("%s", DvKeyBlock.header());
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        if (bkb.key_block_size / 512 == bkb.sectors_reported)
          DVPost.print("%s", bkb.print());
      }
    }
  }


  /**
   * Report those blocks who had some sectors reported as bad.
   * It will report these separately for those blocks that we know were completely
   * reported because we found an 'op:' error line, and for those that we did
   * not see an 'op:' line for.
   */
  private static void reportSomeSectorsBad(boolean completed)
  {
    DvKeyBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      DvKeyBlock bkb = blocks[i];

      /* Look at those blocks we know had their reporting completed? */
      if (completed && bkb.failed_operation == null)
        continue;
      if (!completed && bkb.failed_operation != null)
        continue;
      if (bkb.key_block_size / 512 != bkb.sectors_reported)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Incomplete blocks that have differences in some sectors:");
      if (!completed)
      {
        DVPost.print("Since not all sectors have been reported by DV due to the data_errors= limit ");
        DVPost.print("this does not mean that these blocks may not have more bad sectors.");
        DVPost.print("(No 'op:' i/o error message was reported for this block).");
      }
      DVPost.print("%s %s", DvKeyBlock.header(), "sectors");
      for (int i = 0; i < blocks.length; i++)
      {
        DvKeyBlock bkb = blocks[i];
        if (bkb.key_block_size / 512 != bkb.sectors_reported)
          DVPost.print("%s %s", bkb.print(), bkb.getBadSectors());
      }
    }
  }

  private static void bad(String txt)
  {
    DVPost.print("\n\n");
    DVPost.print("Last line read: " + linecount);
    DVPost.print("bad line:");
    DVPost.print(line);
    common.failure(txt);
  }


  /**
   * Split the input line after removing the timestamp and slave name. ';' and
   * "'" are also removed. (Can not remove ':' or '.' since it is part of a
   * windows file name)
   */
  private static String[] splitLine()
  {
    /* First remove timestamp and host name: */
    line = line.substring(line.indexOf(": ") + 2).trim();

    /* Use StringTokenizer to do all the dirty work: */
    StringTokenizer st = new StringTokenizer(line.trim(), " ;'");
    String[] stuff = new String[ st.countTokens() ];
    int j = 0;
    while (st.hasMoreTokens())
      stuff[j++] = st.nextToken();

    for (int i = 99990; i < stuff.length; i++)
      System.out.println("stuff: " + i + " " + stuff[i]);

    return stuff;
  }


  public static Date parseLastDate(String line)
  {
    try
    {
      SimpleDateFormat df = new SimpleDateFormat( "EEEE MMMM d HH:mm:ss zzz yyyy" );
      return df.parse(line);
    }

    catch (ParseException e)
    {
      return null;
    }
  }

  public static Date parseWrittenDate(String line)
  {
    //The sector below was written Wednesday, June 24, 2015 14:04:40.956 MDT
    try
    {
      /* (Excluded the 'day of the week' from format) */
      SimpleDateFormat df1 = new SimpleDateFormat( "MMMM d, yyyy HH:mm:ss.SSS zzz" );

      String tmp = line.substring(line.indexOf(",") + 2);
      return df1.parse(tmp);
    }

    catch (ParseException e)
    {
      try
      {
        /* (Excluded the 'day of the week' from format) */
        SimpleDateFormat df2 = new SimpleDateFormat( "MMMM d, yyyy HH:mm:ss.SSS" );

        String tmp = line.substring(line.indexOf(",") + 2);
        return df2.parse(tmp);
      }

      catch (ParseException e2)
      {
        common.ptod(e2);
        return null;
      }
    }
  }

}



class DataBlock
{
  String lun;
  long   logical_lba;
  int    data_xfersize;
  int    key_blocksize;
  String failure;
}
