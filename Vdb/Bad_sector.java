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

import java.util.Vector;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import Utils.Format;
import Utils.printf;



/**
 * Describes data communicated from JNI back to java when a Data Validation
 * error has occurred.
 */
public class Bad_sector
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static boolean first_time = true;
  private static Vector messages = null;

  private static int BAD_KEY      = 0x01;
  private static int BAD_CHECKSUM = 0x02;
  private static int BAD_LBA      = 0x04;
  private static int BAD_NAME     = 0x08;
  private static int BAD_DATA     = 0x10;

  private static int[] lfsr_sector  = new int[512/4];


  /**
   * This is called from JNI when an error is found.
   */
  static synchronized void reportBadSector(int[]  sector_array,  // 512 bytes
                                           int[]  pattern_array, // 512 bytes
                                           long   handle,
                                           long   file_start_lba,
                                           long   file_lba,
                                           long   offset_in_block,
                                           long   timestamp,
                                           int    error_flag,
                                           int    key,
                                           int    xfersize,
                                           int    checksum,
                                           String name_left,
                                           String name_right)
  {
    /* This is the logical lba of the sector we're reporting: */
    long sector_lba = file_start_lba + file_lba + offset_in_block;

    /* Find the DV map for later use: */
    DV_map dv = DV_map.findMap(name_left.trim());

    /* Immediately mark (this portion of) the block bad: */
    /* (the whole block is marked bad, not just this sector) */
    dv.dv_set(file_start_lba + file_lba, DV_map.DV_ERROR);

    /* Using the handle, find the SD or file name: */
    String lun = "n/a";
    Object obj = File_handles.findHandle(handle);
    SD_entry   sd  = null;
    ActiveFile afe = null;
    if (obj instanceof SD_entry)
    {
      sd = (SD_entry) obj;
      lun = sd.lun;
    }

    else if (obj instanceof ActiveFile)
    {
      afe = (ActiveFile) obj;
      lun = afe.getFileEntry().getName();

      /* Mark this sector BAD as soon as we can: */
      afe.getFileEntry().setBlockBad(512);
    }

    else
      common.failure("reportBadSector(): unexpected file handle return: " + obj);


    try
    {
      messages = new Vector(32, 0);
      String left;
      String right;
      String hex_ts = Format.hex(timestamp, 14);

      /* If we receive garbage the hex timestamp can be larger than 14: */
      if (hex_ts.length() > 14)
        hex_ts = hex_ts.substring(0,14);

      if (first_time)
        print_header();


      /* This line serves only for DVPost(): */
      long ts = dv.getLastTimestamp(sector_lba);
      String txt = String.format("dvpost: %s %s %s 0x%08x 0x%08x %d 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x",
                                 lun,
                                 name_left.trim(),
                                 Sector.check8byteString(name_right.trim()),
                                 file_start_lba,
                                 file_lba,
                                 xfersize,
                                 offset_in_block / 512,
                                 timestamp,
                                 error_flag,
                                 key,
                                 checksum,
                                 (ts < 0) ? ts * -1 : ts);
      print(txt);

      DateFormat df = new SimpleDateFormat( "EEEE, MMMM d, yyyy HH:mm:ss.SSS" );
      String todstr = df.format( new Date(timestamp / 1000) );

      long sector = offset_in_block / 512;
      printf p, l, r;

      /* Report the time that the block was last written: */
      if (ts != 0)
      {
        if (ts < 0)
          print(String.format("Last successful read for block 0x%08x: %s",
                              file_lba, df.format(new Date(ts * -1))));
        else
          print(String.format("Last successful written for block 0x%08x: %s",
                              file_lba, df.format(new Date(ts))));
      }


      /* 'force_error_after' is the reason we have this error: */
      if (key == DV_map.DV_ERROR)
      {
        print("");
        print("'force_error_after' caused this error.");
      }

      /* First header line: */
      print("");
      if (afe != null)
      {
        print(String.format("        Data Validation error for fsd=%s; "+
                            "FSD lba: 0x%08x; DV xfersize: %d; " +
                            "relative sector in block: 0x%02x (%2d)",
                            name_left.trim(),
                            sector_lba, xfersize, sector, sector));
        print(String.format("        File name: %s; file lba: 0x%08x ",
                            lun, file_lba));
      }
      else
      {
        print(String.format("        Data Validation error for sd=%s,"+
                            "lun=%s; "+
                            "block lba: 0x%08x; xfersize: %d; " +
                            "relative sector in block: 0x%02x (%2d)",
                            name_left.trim(), lun,
                            sector_lba, xfersize, sector, sector));
      }

      report_flags(error_flag);


      /* Second header line empty: */


      /* Third header line: */
      p = new printf("        The sector below was written %s");
      p.add(todstr);
      print(p.text);


      /* Checksum line: */
      if (calc_checksum(hex_ts) != checksum)
      {
        p = new printf("        Checksum on timestamp failed. Timestamp: 0x%s; checksum %s; should be %s");
        p.add(hex_ts);
        p.add(Format.hex(checksum,2));
        p.add(Format.hex(calc_checksum(hex_ts)));
        print(p.text);
      }

      /* First data line left: */
      String hex = Format.hex(sector_lba, 16);
      l = new printf("%s %s %s %s");
      l.add(hex.substring(0,8));      // lba 0-3
      l.add(hex.substring(8));        // lba 4-7
      l.add("........");              // timestamp
      l.add("........");

      /* First data line right: */
      r = new printf("%8s %8s %8s %8s");
      r.add(Format.hex(sector_array[0],8));  // raw data read
      r.add(Format.hex(sector_array[1],8));
      r.add(Format.hex(sector_array[2],8));
      r.add(Format.hex(sector_array[3],8));

      p = new printf("0x%03x%s  %s   %s ");
      p.add(0);
      if ((error_flag & 0x04) != 0 ||
          (error_flag & 0x02) != 0)
        p.add("*");
      else
        p.add(" ");
      p.add(l.text);
      p.add(r.text);
      print(p.text);


      /* Second data line left: */
      hex = name_to_hex(name_left);
      l = new printf("%s%s%s %s %s %s");
      l.add(Format.hex(key, 2));      // key
      l.add("..");                    // checksum
      l.add("0000");                  // spare
      l.add(hex.substring(0, 8));     // name 0-3
      l.add(hex.substring(8));        // name 4-7
      l.add(Format.hex(0 ,8));

      /* Second data line right: */
      r = new printf("%s %s %s %s");
      r.add(Format.hex(sector_array[4],8));
      r.add(Format.hex(sector_array[5],8));
      r.add(Format.hex(sector_array[6],8));
      r.add(Format.hex(sector_array[7],8));

      p = new printf("0x%03x%s  %s   %s ");
      p.add(16);
      if ((error_flag & BAD_KEY)      != 0 ||
          (error_flag & BAD_CHECKSUM) != 0 ||
          (error_flag & BAD_NAME)     != 0 )
        p.add("*");
      else
        p.add(" ");
      p.add(l.text);
      p.add(r.text);
      print(p.text);

      /* We are now going to report the good or bad data. If we had a bad key */
      /* or bad lba then we should not bother with the data we received from  */
      /* JNI. Just get the data pattern matching the lba and key found and    */
      /* report that, if there is a difference.                               */
      if (!checkOtherContent(error_flag, sector_array, name_right))
      {

        /* Third data through last line: */
        int lines_suppressed = 0;
        for (int i = 8; i < 128; i+=4)
        {
          /* Left portion: */
          l = new printf("%s %s %s %s");
          l.add(Format.hex(pattern_array[i+0],8));
          l.add(Format.hex(pattern_array[i+1],8));
          l.add(Format.hex(pattern_array[i+2],8));
          l.add(Format.hex(pattern_array[i+3],8));

          /* Right portion: */
          r = new printf("%s %s %s %s");
          r.add(Format.hex(sector_array[i+0],8));
          r.add(Format.hex(sector_array[i+1],8));
          r.add(Format.hex(sector_array[i+2],8));
          r.add(Format.hex(sector_array[i+3],8));

          combine_and_print(i, l.text, r.text);
        }
      }

      for (int i = 0; i < messages.size(); i++)
        common.ptod("messages: " + messages.elementAt(i));

      ErrorLog.sendMessagesToMaster(messages);
    }

    catch (Exception e)
    {
      common.ptod("Error during error reporting:");
      common.failure(e);
    }
  }


  /**
   * Check to see if it is worth it do print the rest of the data pattern.
   */
  private static boolean checkOtherContent(int    error_flag,
                                           int[]  sector_array,
                                           String lfsr_name)
  {
    if ((error_flag & BAD_KEY) != 0 || (error_flag & BAD_LBA) != 0)
    {
      /* Whatever 'name' we found in the block, is must be 8 bytes: */
      if (lfsr_name.length() < 8)
        return false;

      int  use_key = sector_array[4] >>> 24;
      long use_lba = (sector_array[0] << 32) | sector_array[1];

      /* Create an LFSR array using this data: */
      Native.fillLFSR(lfsr_sector, use_lba, use_key, lfsr_name);

      /* If this data matches, just report and continue: */
      boolean mismatch = false;
      for (int i = 8; i < 128; i++)
      {
        if (sector_array[i] != lfsr_sector[i])
          mismatch = true;
      }
      if (!mismatch)
      {
        print("        Data pattern matches the incorrect key and/or lba that was read.");
        return true;
      }
    }

    return false;
  }

  /**
   * Combine left and right text, add the offset and print.
   */
  private static void combine_and_print(int index, String left, String right)
  {
    String diff = "*";

    /* Any differences? */
    if (left.equals(right))
      diff = " ";

    /* Combine left and right: */
    printf p = new printf("0x%03x%s  %s   %s ");
    p.add(index*4);
    p.add(diff);
    p.add(left);
    p.add(right);

    print(p.text);
  }


  private static void print_header()
  {
    first_time = false;

    print("");
    print("At least one Data Validation error detected.                    ");
    print("All data in the disk block (assumed to be 512 bytes long) will  ");
    print("be printed in the errorlog; lines that do not compare will be   ");
    print("marked with '*'.");
    //print("The first 32 bytes of each disk block will always be printed on ");
    //print("the console and logfile, with all lines that do not compare.    ");
    print("");
    print("Byte 0x00 -  0x07: Byte offset of this disk block");
    print("Byte 0x08 -  0x0f: Timestamp: number of microseconds since 1/1/1970");
    print("Byte 0x10        : Data Validation key from 1 - 126");
    print("Byte 0x11        : Checksum of timestamp");
    print("Byte 0x12 -  0x13: Reserved");
    print("Byte 0x14 -  0x1b: SD or FSD name in ASCII hexadecimal");
    print("Byte 0x1c -  0x1f: Reserved");
    print("Byte 0x20 - 0x1ff: 480 bytes of data");
    print("");
    print("On the left: the data that was expected ('.' marks unknown value).");
    print("On the right: the data that was found.");
    print("(When the data compare of the 480 data bytes results in a mismatch ONLY because of a wrong ");
    print(" Data Validation key and/or wrong lba the reporting of that data will be suppressed.)");
    print("");

  }

  public static void print(String txt)
  {
    messages.add(txt);
  }


  private static int calc_checksum(String hex)
  {
    int check = 0;

    if (hex.length() != 14)
      common.failure("hex string must be 14 digits: '" + hex + "'");


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

  private static String name_to_hex(String name)
  {
    String txt = "";

    if (name.length() != 8)
      name = (name + "        ").substring(0, 8);

    boolean windows = common.onWindows();
    if (!windows)
    {
      for (int i = 0; i < name.length(); i++)
      {
        txt = txt + Format.hex(name.charAt(i), 2);
      }
    }

    else
    {
      /* Windows does funny things with byte reversal of strings.        */
      /* We here simulate it to make the printout of the sd names equal: */

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

  private static void report_flags(int error_flag)
  {
    if ((error_flag & BAD_KEY) != 0)
      print("        ===> Data Validation Key miscompare.");
    if ((error_flag & BAD_CHECKSUM) != 0)
      print("        ===> Checksum error on timestamp.");
    if ((error_flag & BAD_LBA) != 0)
      print("        ===> Logical byte address miscompare.");
    if ((error_flag & BAD_NAME) != 0)
      print("        ===> SD or FSD name miscompare.");
    if ((error_flag & BAD_DATA) != 0)
      print("        ===> Data miscompare.");
  }


  public static void main2(String args[])
  {
    int check = calc_checksum(args[0]);
    common.ptod(Format.f("check: %02x", check));
  }
  public static void main(String args[])
  {
    String hex = args[0];

    long timestamp = Long.parseLong(hex, 16);

    DateFormat df = new SimpleDateFormat( "EEEE, MMMM d, yyyy HH:mm:ss.SSS" );
    String todstr = df.format( new Date(timestamp / 1000) );

    common.ptod("todstr: " + todstr);
  }
}

