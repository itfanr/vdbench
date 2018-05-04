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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Fget;



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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static HashMap block_list = new HashMap(128);
  private static int linecount = 0;
  private static int sectorcount = 0;
  private static Fget fg;

  private static String line;
  private static ArrayList overview      = new ArrayList(256);


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
    String filename = ".";

    /* Prevent unimportant plog messages from showing up on the console: */
    common.ignoreIfNoPlog();

    if (args.length > 1)
      filename = args[1];
    else if ((filename = PostGui.askForFile(".", "Enter errorlog.html file name")) == null)
      return;

    try
    {
      /* Scan all the lines of this file until we find specific important lines: */
      fg = new Fget(filename);
      while ((line = fg.get()) != null)
      {
        linecount++;

        /* This is the line that identifies that we are finished reporting */
        /* all the bad sectors of a block. Without this line for a block   */
        /* we do not really know how many bad sectors there are, unless    */
        /* of course we have seen ALL sectors:                             */
        if (line.indexOf("op:") != -1)
        {
          foundErrorLine();
          continue;
        }
        /*
      String txt = String.format("dvpost: %s %s %s %x %x %d %x %x %x %x %xfersize",
                                 lun,
                                 name_left.trim(),
                                 name_right.trim(),
                                 file_start_lba,
                                 file_lba,
                                 xfersize,
                                 offset_in_block,
                                 timestamp,
                                 error_flag,
                                 key,
                                 checksum,
                                 ts);
        */

        /* Scan until we reach the dvpost line: */
        if (line.indexOf("dvpost:") == -1)
          continue;

        /* Rip this line apart: */
        //common.ptod("line: " + line);
        BadBlock bb    = new BadBlock();
        String[] split = line.split(" +");
        bb.lun         = split[3];
        bb.sd_wanted   = split[4];
        bb.file_start_lba  = Long.parseLong(split[6].substring(2), 16);
        bb.file_lba        = Long.parseLong(split[7].substring(2), 16);
        bb.xfersize        = Integer.parseInt(split[8]);
        bb.key_wanted      = Integer.parseInt(split[12].substring(2), 16);

        bb.sectors         = new Sector[bb.xfersize / 512];
        bb.logical_lba     = bb.file_start_lba + bb.file_lba;

        int      offset_in_block = Integer.parseInt(split[9].substring(2), 16) * 512;
        String   sd_right        = split[5];
        long     timestamp       = Long.parseLong(split[10].substring(2), 16);
        int      error_flag      = Integer.parseInt(split[11].substring(2), 16);
        int      checksum        = Integer.parseInt(split[13].substring(2), 16);
        long     last_stamp      = Long.parseLong(split[14].substring(2), 16);

        /* This is either a block that we already saw, or a new block: */
        bb  = haveBadSector(bb, offset_in_block);
        if (bb == null)
          continue;
      }

      if (block_list.size() == 0)
        common.failure("No bad blocks found. No 'dvpost:' lines found?");

      reportSomeSectorStuff();

      PostGui window = new PostGui(filename, sortBlocks("lun"), overview);
      window.setVisible(true);
      window.repaint();
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
  private static BadBlock haveBadSector(BadBlock bb_in, int offset_in_block)
  {
    /*
    line0:         Data Validation error at byte offset 0xd91b0000 (3642425344); 512 byte block offset 0x6c8d80 (7114112)
    line1:         Relative  32768-byte data block: 0x1b236; relative sector in block: 0x00
    or             Relative 131072-byte data block: 0x1;     block lba: 0x20000 ; relative sector in block: 0x38 (56)
    line2:         SD name miscompare.
    line3:         Lun: /dev/rdsk/c0t75d0s2; SD name in block expected: 'sd3'; read: 'sd2'.
    or             Lun: r:\junk\vdb2;           name in block expected: 'sd1'; read: 'sd1'.
    line4:         The disk block just read was written Tuesday, June 23, 2009 09:57:28.390
    line5: 0x000   00000000 d91b0000 ........ ........   00000000 d91b0000 00046d06 e2c14466
    line6: 0x010*  01..0000 73643320 20202020 00000000   01c40000 73643220 20202020 00000000
    line7: 0x020   4db23200 26d91900 136c8c80 09b64640   4db23200 26d91900 136c8c80 09b64640
    .....
    */

    Date last_date = null;
    String[] split;

    /* See if we already have this block, if not, add it: */
    BadBlock bb = (BadBlock) block_list.get(bb_in.lun + " " + bb_in.logical_lba);
    if (bb == null)
    {
      bb = bb_in;
      block_list.put(bb_in.lun + " " + bb_in.logical_lba, bb);
    }

    /* Save the 'dvpost:' line, stripped of its overhead: */
    split = splitLine();
    bb.raw_input.add("");
    bb.raw_input.add(line);

    /* Continue until we find line5: */
    while ((line = fg.get()) != null)
    {
      linecount++;
      split = splitLine();
      bb.raw_input.add(line);
      if (split.length == 0)
        continue;

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
      bb.raw_input.add(line.substring(26));
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
    Sector sector     = new Sector();
    sector.lba_wanted = bb.logical_lba + offset_in_block;
    sector.timestamp  = last_date;
    bb.sectors[offset_in_block / 512] = sector;
    bb.sectors_reported ++;

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
    sector.sd_wanted = bb.sd_wanted;
    sector.sd_read   = xlateSD(sector.was_read[5], sector.was_read[6], bb.sd_wanted, hex_lines[1]);

    /* Pick up keys: */
    bb.key_wanted     =
    sector.key_wanted = sector.expected[4] >> 24;
    sector.key_read   = sector.was_read[4] >> 24;
    //DVPost.print("sector.key_wanted: " + sector.key_wanted + " " + sector.key_read);

    bb.different_words_in_block += sector.countDifferences();


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
    return bb;
  }

// xx yy zz op: read   lun: r:\quick_vdbench_test          lba:      5810176 0x0058A800 xfer:     1024 errno: 60003 A Data validation error was discovered
  private static void foundErrorLine()
  {
    String[] split = line.trim().split(" +");

    /* Pick up some stuff: */
    long lba = Long.parseLong(split[8]);
    String lun = split[6];

    /* See if we already have this block, if not, add it: */
    BadBlock bb = (BadBlock) block_list.get(lun + " " + lba);
    if (bb == null)
    {
      DVPost.print("Found an i/o error for an lba which was not reported by Data Validation:");
      DVPost.print("line: " + common.replace(line, "  ", " "));
      return;
    }

    bb.failed_operation = split[4];
    bb.error_code = Integer.parseInt(split[13]);
    if (bb.error_code != 60003 && bb.error_code != 803)
      bad("Invalid i/o error code: " + bb.error_code);
  }




  private static String xlateSD(long sd1, long sd2, String wanted, String hex)
  {
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

    //DVPost.print("sd: >>>>" + sd + "<<<< " + hex);
    return sd;
  }

  private static void reportSomeSectorStuff()
  {
    reportErrorCodes();
    reportBlockStatus();
    reportTimestamps();
    reportSingleBitErrors();
    reportSdNameWrong();
    //reportAllSectorsBad();
    //reportSomeSectorsBad(true);
    //reportSomeSectorsBad(false);
    reportWrongLbas();
    reportWrongKeys();
  }


  private static BadBlock[] sortBlocks(String order)
  {
    BadBlock[] blocks   = (BadBlock[]) block_list.values().toArray(new BadBlock[0]);
    BadBlock.sort_order = order;
    Arrays.sort(blocks);
    return blocks;
  }

  private static void reportErrorCodes()
  {
    BadBlock[] blocks = sortBlocks("lun");
    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];
      if (bb.error_code != 0)
        count ++;
    }

    if (count == 0)
      return;

    DVPost.print("\n\n");
    DVPost.print("Blocks that have completed their error reporting : ");
    DVPost.print("%s %s", BadBlock.header(), "error");

    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];
      if (bb.error_code > 0)
        DVPost.print("%s %d", bb.print(), bb.error_code);
    }
  }



  private static void reportBlockStatus()
  {
    BadBlock[] blocks = sortBlocks("lun");

    DVPost.print("\n\n");
    DVPost.print("General bad block status:");
    DVPost.print("%s %s", BadBlock.header(), "Errors");
    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];
      DVPost.print("%s %s", bb.print(), bb.getBlockStatusShort());
    }
  }

  private static void reportTimestamps()
  {
    BadBlock[] blocks = sortBlocks("lun");

    DVPost.print("\n\n");
    DVPost.print("Timestamps of 'last write' found in block:");
    DVPost.print("%s %s", BadBlock.header(), "Key; timestamp");

    HashMap times = new HashMap(16);
    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];
      DVPost.print("%s %02x %s", bb.print(), bb.key_wanted, bb.getTimestamps());
    }
  }

  private static void reportWrongKeys()
  {
    BadBlock[] blocks = sortBlocks("lun");

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
      DVPost.print("%s %s", BadBlock.header(), "(Expected) read");
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        String wrongs = bb.getWrongKeys();
        if (wrongs == null)
          continue;
        DVPost.print("%s (%02x) %s", bb.print(), bb.key_wanted, wrongs);
      }
    }
  }


  private static void reportWrongLbas()
  {
    BadBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      if (blocks[i].getWrongLbas() != null)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Blocks that have at least one wrong lba in their data:");
      DVPost.print("%s %s", BadBlock.header(), "bad lba");
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        String wrongs = bb.getWrongLbas();
        if (wrongs == null)
          continue;
        DVPost.print("%s %s", bb.print(), wrongs);
      }
    }
  }



  private static void reportSingleBitErrors()
  {
    BadBlock[] blocks = sortBlocks("lun");

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
      DVPost.print("%s", BadBlock.header());
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        if (bb.countSingleBitWords() == 0)
          continue;
        DVPost.print("%s", bb.print());
      }
    }
  }


  private static void reportSdNameWrong()
  {
    BadBlock[] blocks = sortBlocks("lun");

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
      DVPost.print("%s %s", BadBlock.header(), "mismatch(es)");
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        String wrongs = bb.getWrongSdNames();
        if (wrongs == null)
          continue;

        DVPost.print("%-20s %s", bb.print(), wrongs);
      }
    }
  }


  private static void reportAllSectorsBad()
  {
    BadBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];
      if (bb.xfersize / 512 == bb.sectors_reported)
        count++;
    }

    if (count > 0)
    {
      DVPost.print("\n\n");
      DVPost.print("Blocks that have differences in all sectors:");
      DVPost.print("%s", BadBlock.header());
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        if (bb.xfersize / 512 == bb.sectors_reported)
          DVPost.print("%s", bb.print());
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
    BadBlock[] blocks = sortBlocks("lun");

    int count = 0;
    for (int i = 0; i < blocks.length; i++)
    {
      BadBlock bb = blocks[i];

      /* Look at those blocks we know had their reporting completed? */
      if (completed && bb.failed_operation == null)
        continue;
      if (!completed && bb.failed_operation != null)
        continue;
      if (bb.xfersize / 512 != bb.sectors_reported)
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
      DVPost.print("%s %s", BadBlock.header(), "sectors");
      for (int i = 0; i < blocks.length; i++)
      {
        BadBlock bb = blocks[i];
        if (bb.xfersize / 512 != bb.sectors_reported)
          DVPost.print("%s %s", bb.print(), bb.getBadSectors());
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
   * Split the inout line after removing the timestamp and slave name. ';' and "'"
   * are also removed. (Can not remove ':' or '.' since it is part of a windows
   * file name)
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
}

class BadBlock implements Comparable
{
  long     logical_lba;    /* file_start_lba + file_lba */
  long     file_start_lba;
  long     file_lba;
  String   lun;
  String   sd_wanted;
  int      sectors_reported;
  int      key_wanted;
  int      xfersize;
  Sector[] sectors;
  String   failed_operation;
  int      error_code = 0;
  int      different_words_in_block;
  ArrayList raw_input = new ArrayList(4096);

  static String sort_order = null;

  public BadBlock()
  {
  }

  public static Date parseLastDate(String line)
  {
    try
    {
      //Wednesday, December 31, 1969 19:23:09.934
      DateFormat df = new SimpleDateFormat( "E, MMMM d, yyyy HH:mm:ss.SSS" );
      String tmp = line.substring(line.indexOf("written") + 8);
      return df.parse(tmp);
    }

    catch (ParseException e)
    {
      return null;
    }
  }

  public int compareTo(Object obj)
  {
    BadBlock bb = (BadBlock) obj;

    if (sort_order.equals("lba"))
      return(int) (logical_lba - bb.logical_lba);

    /* For Lun, sort lun first, then lba: */
    if (sort_order.equals("lun"))
    {
      int diff = lun.compareTo(bb.lun);
      if (diff != 0)
        return diff;
      return(int) (logical_lba - bb.logical_lba);
    }
    else
      common.failure("Unknown sort flag");
    return 0;
  }

  public int countSingleBitWords()
  {
    int bitwords = 0;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].singlebit_words != 0)
        bitwords++;
    }
    return bitwords;
  }

  public String getBlockStatusShort()
  {
    String txt = "";
    if (getWrongKeys() != null)
      txt += "Key; ";
    if (getWrongLbas() != null)
      txt += "Lba; ";
    if (countSingleBitWords() > 0)
      txt += "Single bit; ";
    if (getWrongSdNames() != null)
      txt += "SD; ";
    if (getLFSRStatus())
      txt += "Bad lba data is bad; ";
    if (anyPartialSectors())
      txt += "Data, partial; ";
    else if (different_words_in_block > 0)
      txt += "Data; ";
    txt += getBadSectors() + " ";

    return txt;
  }

  public String getBlockStatus()
  {
    String txt = "";
    if (error_code == 0 && sectors_reported != (xfersize / 512))
      txt += "- Not all sectors have been reported.\n";
    if (getWrongKeys() != null)
      txt += "- Invalid key(s) read.\n";
    if (getWrongLbas() != null)
      txt += "- Invalid lba read.\n";
    if (countSingleBitWords() > 0)
      txt += "- At least one single bit error.\n";
    if (getWrongSdNames() != null)
      txt += "- Invalid SD name read.\n";
    if (getLFSRStatus())
      txt += "- Data corruption even when using wrong lba or key.\n";
    if (anyPartialSectors())
      txt += "- At least one sector is partially correct.\n";
    else if (different_words_in_block > 0)
      txt += "- Data corruption.\n";
    txt += "- " + getBadSectors() + " ";

    return txt;
  }

  /**
   * Return the numbers of the sectors that are bad in rages.
   */
  public String getBadSectors()
  {
    Vector numbers = new Vector(sectors.length);

    /* Put all bad sector numbers in a row: */
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null)
        numbers.add(new Integer(i));
    }

    /* Identify whether we received all the sectors. Having an error_code */
    /* confirms that, but also having seen the last sector confirms that: */
    String txt = error_code != 0 ? "Bad sectors: " : "Bad sectors: (incomplete) ";
    if (sectors[ sectors.length - 1 ] != null)
      txt = "Bad sectors: ";

    txt += compressSectorNumbers(numbers, sectors.length);

    return txt;
  }


  /**
   * Return the Integers that are in the received Vector as a String,
   * reporting them for instance as 0-15,16,18-20 etc.
   */
  public static String compressSectorNumbers(Vector numbers, int max)
  {
    String txt = "";

    if (numbers.size() == 0)
      return "All " + max + " sectors good (based on lba and key that was read)";
    if (numbers.size() == max)
      return "All " + max + " sectors bad ";

    /* Convert Vector to array: */
    int[] array = new int[numbers.size()];
    for (int i = 0; i < numbers.size(); i++)
      array[i] = ((Integer) numbers.elementAt(i)).intValue();

    int first = array[0];
    int last  = array[0];
    for (int i = 1; i < array.length; i++)
    {
      //DVPost.print("loop: " + array[i] + " " + last);
      if (array[i] != (last + 1))
      {
        if (first == last)
          txt += first + ",";
        else
          txt += first + "-" + last + ",";
        first = array[i];
      }
      last = array[i];
    }

    if (first == last)
      txt += first;
    else
      txt += first + "-" + last;

    return txt + " (" + array.length + " of " + max + ")";
  }

  public String getWrongKeys()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].key_read != sectors[i].key_wanted)
        wrongs.put(new Integer(sectors[i].key_read), new Integer(sectors[i].key_read));
    }

    if (wrongs.size() == 0)
      return null;

    Integer[] ints = (Integer[]) wrongs.keySet().toArray(new Integer[0]);
    Arrays.sort(ints);
    String txt = "";
    for (int i = 0; i < ints.length; i++)
      txt += String.format("%02x ", ints[i].intValue() & 0xff);
    return txt;
  }

  public boolean anyPartialSectors()
  {
    boolean partial = false;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].different_words_in_sector != 120)
        partial = true;
    }

    return partial;
  }
  public String getTimestamps()
  {
    DateFormat df = new SimpleDateFormat( "(MM/dd/yy HH:mm:ss.SSS) " );
    HashMap times = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].timestamp != null)
        times.put(sectors[i].timestamp, sectors[i].timestamp);
    }

    if (times.size() == 0)
      return null;

    Date[] dates = (Date[]) times.keySet().toArray(new Date[0]);
    Arrays.sort(dates);
    String txt = "";
    for (int i = 0; i < dates.length; i++)
      txt += df.format(dates[i]);
    return txt;
  }

  public String getWrongLbas()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].lba_read != sectors[i].lba_wanted)
        wrongs.put(new Long(sectors[i].lba_read), new Long(sectors[i].lba_read));
    }

    if (wrongs.size() == 0)
      return null;

    Long[] longs = (Long[]) wrongs.keySet().toArray(new Long[0]);
    Arrays.sort(longs);
    String txt = "";
    for (int i = 0; i < longs.length; i++)
    {
      if (i > 2)
        return txt + "...";
      else
        txt += String.format("0x%x ", longs[i].longValue());
    }
    return txt;
  }


  public String getWrongSdNames()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].sd_read != sectors[i].sd_wanted)
      {
        wrongs.put(sectors[i].sd_read, sectors[i].sd_read);
      }
    }

    if (wrongs.size() == 0)
      return null;

    String[] sds = (String[]) wrongs.keySet().toArray(new String[0]);
    Arrays.sort(sds);
    String txt = "";
    for (int i = 0; i < sds.length; i++)
      txt += sds[i] + " ";
    return txt;
  }

  public boolean getLFSRStatus()
  {
    boolean any_bad = false;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].lfsr_of_bad_lba_bad)
        any_bad = true;
    }

    return any_bad;
  }

  public void reReadSectors()
  {
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null)
        sectors[i].reReadSector(this);
    }
  }

  public static String header()
  {
    return String.format("\n%-40s %-6s %8s %-12s", "Lun", "sd", "xfersize", "lba");
  }
  public String print()
  {
    return String.format("%-40s %-6s %-8d 0x%010x", lun, sd_wanted, xfersize, logical_lba);
  }
}


class Sector
{
  long   lba_wanted;
  long   lba_read;
  String sd_wanted;
  String sd_read;
  long   ts;
  int    key_wanted;
  int    key_read;
  int    checksum;
  int    different_words_in_sector;
  int    different_bits;
  int    singlebit_words;
  Date   timestamp;
  int[]  expected = new int[128];
  int[]  was_read = new int[128];
  int[]  lfsr_sector;
  int[]  re_read;
  boolean  lfsr_of_bad_lba_bad = false;


  /**
   * Count how many words (32bits) are different in this sector.
   * We are only looking beyond the first 32 bytes.
   */
  public int countDifferences()
  {
    different_words_in_sector = 0;

    /* Look only beyond byte 32: */
    for (int i = 8; i < expected.length; i++)
    {
      int expd = expected[i];
      int read = was_read[i];

      /* Ignore timestamp: */
      if (i == 2 || i == 3)
        continue;

      /* Ignore checksum: */
      if (i == 4)
      {
        expd &= 0xff00ffff;
        read &= 0xff00ffff;
      }

      /* Ignore bad SD name: */
      if ((i == 5 || i == 6) && !sd_wanted.equals(sd_read))
        continue;
      if (expd != read)
      {
        different_words_in_sector++;

        /* Count miscomparing bits and how often we just have one single bit error: */
        int bits = countBits(expd, read);
        different_bits += bits;
        if (bits == 1)
        {
          singlebit_words++;
        }
      }
    }

    //if (different_words != 0)
    // DVPost.print("different_words: " + different_words + " bits: " + different_bits);
    //
    //common.ptod("different_words_in_sector: " + different_words_in_sector);
    return different_words_in_sector;
  }

  private int countBits(int word1, int word2)
  {
    int bits = 0;
    int w1 = word1;
    int w2 = word2;
    for (int i = 0; i < 32; i++)
    {
      if ((w1 & 1) != (w2 & 1))
        bits++;
      w1 >>>= 1;
      w2 >>>= 1;
    }

    //if (bits == 1)
    //  DVPost.print("countBits: %08x %08x %08x %d", lba_wanted, word1, word2, bits);

    return bits;
  }

  /**
   * Get the data pattern that goes with the BAD lba that we read.
   */
  public void getBadLbaData()
  {
    /* Allocate memory for what we want to do: */
    lfsr_sector = new int[512/4];

    /* Create an LFSR array using this data, sd name must be 8 bytes: */
    Native.fillLFSR(lfsr_sector, lba_read, key_read, check8byteString(sd_read));

    /* Now compare that data so that we can report discrepancies: */
    for (int i = 8; i < 512/4; i++)
    {
      if (was_read[i] != lfsr_sector[i])
      {
        lfsr_of_bad_lba_bad = true;
      }
    }
    //common.ptod("lfsr_of_bad_lba_bad: " + lfsr_of_bad_lba_bad + " " + key_read);
  }


  /**
   * An attempt to immediately re-read the bad sector and validate it again.
   */
  public void reReadSector(BadBlock bb)
  {
    Vector lines = new Vector(64);

    long handle = Native.openFile(bb.lun);
    if (handle < 0)
      common.failure("Can't open disk file");

    long data_buffer = Native.allocBuffer(512);
    int[] data_array = new int[512 / 4];

    common.ptod("lba_wanted: " + lba_wanted);
    common.ptod("bb.file_start_lba: " + bb.file_start_lba);
    long rc = Native.readFile(handle, lba_wanted - bb.file_start_lba, 512, data_buffer);
    if (rc != 0)
      common.failure("Error reading block");
    Native.closeFile(handle);

    Native.buffer_to_array(data_array, data_buffer, 512);

    Native.freeBuffer(512, data_buffer);

    int[] return_array = new int[128];
    System.arraycopy(data_array, 0, return_array, (32/4), data_array.length);

    re_read = return_array;
  }


  /**
   * Check the content of the input String and return "garbage " if this
   * is not a valid String.
   * Note: String is first mad 8 bytes long.
   */
  public static String check8byteString(String str)
  {
    String string = (str + "        ").substring(0,8);
    for (int i = 0; i < string.length(); i++)
    {
      if (!Character.isDigit(string.charAt(i)))
        return "garbage ";
    }
    return string;
  }
}



