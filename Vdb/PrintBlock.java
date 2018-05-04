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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import Utils.Format;
import Utils.Getopt;



/**
 * This class prints any data block.
 * Together with that it reports the timestamp found in the sector
 * header, and also reports the LFSR status of all data.
 */
public class PrintBlock
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static DateFormat df = new SimpleDateFormat( "(MM/dd/yy HH:mm:ss.SSS)" );
  private static boolean zapit = false;
  private static int     zapoffset = -1;
  private static int     zapvalue  = 0x01234567;
  private static boolean quiet = false;

  public static void print(String[] args)
  {
    /* Replace "-print" with "print" to avoid parse errors: */
    args[0] = "print";

    Getopt g = new Getopt(args, "qzo:v:", 99);
    Vector positionals = g.get_positionals();

    if (positionals.size() != 4)
      if (args.length < 4)
        common.failure("Bad print option: 'vdbench print device lba size [-q]'. "+
                       "(lba may be prefixed with 0x if needed)");

      /* Allow print to CHANGE the block: */
    zapit = g.check('z');
    quiet = g.check('q');
    if (g.check('o'))
      zapoffset = Integer.parseInt(g.get_string(), 16);
    if (g.check('v'))
      zapvalue = (int) Long.parseLong(g.get_string(), 16);

    /* (positional[0] contains "print") */
    String disk       = g.get_positional(1);
    String lba_string = g.get_positional(2);
    long lba;
    if (lba_string.startsWith("0x"))
      lba = Long.parseLong(lba_string.substring(2), 16);
    else if (lba_string.endsWith("k"))
      lba = Long.parseLong(lba_string.substring(0, lba_string.length() - 1)) * 1024l;
    else if (lba_string.endsWith("m"))
      lba = Long.parseLong(lba_string.substring(0, lba_string.length() - 1)) * 1024l * 1024l;
    else
      lba = Long.parseLong(lba_string);

    System.out.println(String.format("Device: %s; lba: 0x%08x", disk, lba));

    /* Make size multiple of 512, but use original size for printing: */
    int print_size = Integer.parseInt(g.get_positional(3));
    int read_size  = (print_size + 511) / 512 * 512;

    Vector lines = printit(disk, lba, read_size, print_size);
    for (int i = 0; i < lines.size(); i++)
      System.out.println((String) lines.elementAt(i));
  }


  public static Vector printit(String lun, long lba, int read_size, int print_size)
  {
    Vector lines  = new Vector(512);
    Vector bad_sector_numbers = new Vector(read_size / 512);
    int bad_sectors = 0;

    /* Allocate workarea for LFSR: */
    int[] lfsr_sector  = new int[512/4];

    long handle = Native.openFile(lun);
    if (handle < 0)
      common.failure("Can't open disk file: " + lun);

    long  data_buffer = Native.allocBuffer(read_size);
    int[] data_sector = new int[read_size / 4];

    long rc = Native.readFile(handle, lba, read_size, data_buffer);
    if (rc != 0)
      common.failure("Error reading block " + lun + " " + lba + " " + read_size);
    Native.closeFile(handle);

    Native.buffer_to_array(data_sector, data_buffer, read_size);

    lines.add("lba         blk    sector data read" +
              "                           " +
              ((!quiet) ? "Notes; LFSR data valid t/f " : ""));

    int sectors = read_size / 512;
    int sector_offset = 0;
    for (int sector = 0; sector < sectors; sector++)
    {
      boolean mismatch = false;
      lines.add("");

      /* Report the timestamp from this block: */
      String todstr;
      String wrong = "";
      int ts0 = data_sector[sector_offset + 2];
      int ts1 = data_sector[sector_offset + 3];
      long ts = ((long) ts0 << 32) | ((long) ts1 << 32 >>> 32);
      todstr  = df.format( new Date(ts / 1000) );


      /* Check lba: */
      int lba0 = data_sector[sector_offset + 0];
      int lba1 = data_sector[sector_offset + 1];
      long blk_lba = ((long) lba0 << 32) | (long) lba1 << 32 >>> 32;
      if (!quiet && blk_lba != lba)
      {
        wrong = "(wrong lba) ";
        mismatch = true;
        lines.add("Wrong lba read. LFSR pattern generated using this lba");
      }

      int newkey = data_sector[sector_offset + 4] >> 24;

      /* Create an LFSR array using the lba just read (not requested), */
      /* and the key just read (not requested):                        */
      String name = xlateToString(data_sector[5], data_sector[6]);
      Native.fillLFSR(lfsr_sector, blk_lba, newkey, name);


      /* Print one line for each 4 words (16 bytes), but end after 'print_size' bytes */
      for (int i = 0;
            i < 512 / 4 / 4 && i < (print_size + 15) / 4 / 4;
            sector_offset += 4, i++)
      {
        String line  = "";
        String match = "";
        String lfsr  = "";
        line += Format.f("0x%08x ", lba + sector_offset*4);
        line += Format.f("+0x%04x ",  (sector_offset*4) );
        line += Format.f("0x%03x  ",  (sector_offset*4) % 512);

        /* Print four words per line: */
        for (int line_offset = 0; line_offset < 4; line_offset++)
          line += Format.f("%08x ", data_sector[sector_offset+line_offset]);

        if (!quiet)
        {
          /* Check LFSR values for bytes 32-511: */
          for (int line_offset = 0; (sector_offset % 128) > 4 && line_offset < 4; line_offset++)
          {
            if (data_sector[  sector_offset        + line_offset ] ==
                lfsr_sector[ (sector_offset % 128) + line_offset ])
              match += "t";
            else
            {
              match += "f";
              mismatch = true;
              lfsr += Format.f(" %08x", lfsr_sector[ (sector_offset % 128) + line_offset]);
            }
          }

          /* Report the timestamp and lba good/bad: */
          if (sector_offset % 128 == 0)
          {
            line += wrong;
            line += todstr;
          }
          else if (sector_offset % 128 > 1)
            line += match + lfsr;
        }

        lines.add(line);
      }

      if (mismatch)
      {
        bad_sectors++;
        bad_sector_numbers.add(new Integer(sector));
      }
    }

    if (!quiet)
    {
      lines.insertElementAt("",0);
      lines.insertElementAt(String.format("With a block size of %d bytes, "+
                                          "%d of %d sectors had miscompares.",
                                          read_size, bad_sectors, sectors), 0);
    }
    lines.insertElementAt("", 0);

    if (!quiet)
      lines.insertElementAt(BadBlock.compressSectorNumbers(bad_sector_numbers, sectors), 0);


    if (!zapit)
      return lines;

    /* Modify the block: */
    handle = Native.openFile(lun, 1);
    if (handle < 0)
      common.failure("Can't open disk file");

    /* Format the block: */
    if (zapoffset < 0)
    {
      for (int i = 0; i < data_sector.length; i+=2)
      {
        data_sector[i] = 0x01234567;
        data_sector[i+1] = 0x89abcdef;
      }
      lines.add("Block has now een overwritten with values 0x0123456789abcdef");
    }
    else
    {
      lines.add(String.format("Offset 0x%08x contains      0x%08x", zapoffset, data_sector[zapoffset / 4]));
      data_sector[zapoffset / 4] = zapvalue;
      lines.add(String.format("Offset 0x%08x replaced with 0x%08x", zapoffset, zapvalue));
    }

    Native.array_to_buffer(data_sector, data_buffer);

    rc = Native.writeFile(handle, lba, read_size, data_buffer);
    if (rc != 0)
      common.failure("Error writing block");
    Native.closeFile(handle);


    return lines;
  }

  private static String xlateToString(int int1, int int2)
  {
    StringBuffer txt = new StringBuffer(9);
    String arch = System.getProperty("os.arch");

    if (arch.equals("x86"))
    {
      txt.append((char) (int1        & 0xff));
      txt.append((char) (int1 >>>  8 & 0xff));
      txt.append((char) (int1 >>> 16 & 0xff));
      txt.append((char) (int1 >>> 24 & 0xff));
      txt.append((char) (int2        & 0xff));
      txt.append((char) (int2 >>>  8 & 0xff));
      txt.append((char) (int2 >>> 16 & 0xff));
      txt.append((char) (int2 >>> 24 & 0xff));
    }
    else
    {
      txt.append((char) (int1 >>> 24 & 0xff));
      txt.append((char) (int1 >>> 16 & 0xff));
      txt.append((char) (int1 >>>  8 & 0xff));
      txt.append((char) (int1        & 0xff));
      txt.append((char) (int2 >>> 24 & 0xff));
      txt.append((char) (int2 >>> 16 & 0xff));
      txt.append((char) (int2 >>>  8 & 0xff));
      txt.append((char) (int2        & 0xff));
    }

    //common.ptod("xlateToString: " + txt);
    for (int i = 0; i < txt.length(); i++)
    {
      if (!Character.isDigit(txt.charAt(i)))
        return "nostring";
    }

    return txt.toString();
  }

  public static void main(String[] args)
  {
    int[] array = new int[128];
    array[5] = 0x61626364;
    array[6] = 0x65666700;

    String ret = xlateToString(array[5], array[6]);
    common.ptod("ret: " + ret + "<");
  }
}
