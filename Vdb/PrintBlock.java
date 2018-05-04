package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static DateFormat df = new SimpleDateFormat( "(MM/dd/yy HH:mm:ss.SSS)" );
  private static boolean zapit = false;
  private static int     zapoffset = -1;
  private static int     zapvalue  = 0x01234567;
  private static boolean quiet = true;
  private static int dedupunit = 0;

  public static void print(String[] args)
  {
    /* Replace "-print" with "print" to avoid parse errors: */
    args[0] = "print";

    Getopt g = new Getopt(args, "u:qzo:v:", 99);
    Vector positionals = g.get_positionals();

    if (positionals.size() != 4)
      if (args.length < 4)
        common.failure("Bad print option: 'print device lba size'. "+
                       "(lba may be prefixed with 0x if needed)");

      /* Need shared memory to issue PTOD() requests in JNI: */
    Native.allocSharedMemory();

    /* Allow -print to CHANGE the block: */
    zapit = g.check('z');
    //quiet = g.check('q');
    if (g.check('o'))  // in hex!!
      zapoffset = Integer.parseInt(g.get_string(), 16);
    if (g.check('v'))
      zapvalue = (int) Long.parseLong(g.get_string(), 16);
    if (g.check('u'))
      dedupunit = g.extractInt();

    String disk         = g.get_positional(1);
    String lba_string   = g.get_positional(2);
    String print_string = g.get_positional(3);
    long   lba;
    int    print_size;

    if (lba_string.startsWith("0x"))
      lba = Long.parseLong(lba_string.substring(2), 16);
    else if (lba_string.endsWith("k"))
      lba = Long.parseLong(lba_string.substring(0, lba_string.length() - 1)) * 1024l;
    else if (lba_string.endsWith("m"))
      lba = Long.parseLong(lba_string.substring(0, lba_string.length() - 1)) * 1024l * 1024l;
    else if (lba_string.endsWith("g"))
      lba = Long.parseLong(lba_string.substring(0, lba_string.length() - 1)) * 1024l * 1024l * 1024l;
    else
      lba = Long.parseLong(lba_string);

    /* Allow specification of non-512 aligned block by just clearing the remainder: */
    lba &= ~511;

    System.out.println(String.format("Device: %s; lba: 0x%08x", disk, lba));

    /* Make size multiple of 512, but use original size for printing: */
    if (print_string.endsWith("k"))
      print_size = Integer.parseInt(print_string.substring(0, print_string.length() - 1)) * 1024;
    else if (print_string.endsWith("m"))
      print_size = Integer.parseInt(print_string.substring(0, print_string.length() - 1)) * 1024 * 1024;
    else
      print_size = Integer.parseInt(print_string);
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

    OpenFlags oflags = new OpenFlags(new String[] { "directio"}, null);

    long handle = Native.openFile(lun, oflags, 0);
    if (handle < 0)
      common.failure("Can't open disk file: " + lun);

    File_handles.addHandle(handle, "Vdbench print " + lun);

    long  data_buffer = Native.allocBuffer(read_size);
    int[] data_sector = new int[read_size / 4];

    //common.ptod("handle:      " + handle);
    //common.ptod("lba:         " + lba);
    //common.ptod("read_size:   " + read_size);
    //common.ptod("data_buffer: " + data_buffer);

    long rc = Native.readFile(handle, lba, read_size, data_buffer);
    if (rc != 0)
      common.failure("Error reading block " + lun + " " + lba + " " + read_size);
    Native.closeFile(handle);

    Native.buffer_to_array(data_sector, data_buffer, read_size);

    if (dedupunit == 0)
    {
      lines.add("lba             blk      sector data read" +
                "                           " +
                ((!quiet) ? "Notes; LFSR data valid t/f " : ""));
    }
    else
    {
      lines.add("lba             blk       dedup    sector data read" +
                "                           " +
                ((!quiet) ? "Notes; LFSR data valid t/f " : ""));
    }

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
      long ts = Jnl_entry.make64(ts0, ts1); // ((long) ts0 << 32) | ((long) ts1 << 32 >>> 32);
      todstr  = df.format( new Date(ts / 1000) );


      /* Check lba: */
      int lba0     = data_sector[sector_offset + 0];
      int lba1     = data_sector[sector_offset + 1];
      long blk_lba = Jnl_entry.make64(lba0, lba1); //((long) lba0 << 32) | (long) lba1 << 32 >>> 32;
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

      // problem here with crazy data. Was not using it anyway!
      //Native.fillLfsrArray(lfsr_sector, blk_lba, newkey, name);

      /* fillLfsr fills bytes 0-511; we need bytes 32-511 placed at offset 32: */
      int[] p2 = new int[512/4];
      System.arraycopy(lfsr_sector, 0, p2, 32/4, 480/4);
      lfsr_sector = p2;

      /* Print one line for each 4 words (16 bytes), but end after 'print_size' bytes */
      for (int i = 0;
          i < 512 / 4 / 4 && i < (print_size + 15) / 4 / 4;
          sector_offset += 4, i++)
      {
        String line  = "";
        String match = "";
        String lfsr  = "";
        line += String.format("0x%012x ", lba + sector_offset*4);

        line += Format.f("+0x%06x ",  (sector_offset*4) );

        /* Including dedup reporting: */
        if (dedupunit != 0)
          line += Format.f("+0x%06x ",  (lba + sector_offset*4) % dedupunit);


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



        if (dedupunit != 0 && (lba + sector_offset*4) % dedupunit == 0)
        {
          line += String.format(" dedup %08x %08x",
                                data_sector[sector_offset + 0],
                                data_sector[sector_offset + 1]);
          long dlba = ((long) data_sector[sector_offset + 0] << 32) | data_sector[sector_offset + 1];


          //line += String.format(" dlba: %016x", dlba);
          //line += String.format(" lba:  %016x", lba + sector_offset*4);
          /* For Vdbench, if the first 8 bytes equal the lba: this must be a unique block: */
          if (dlba == lba + sector_offset*4)
            line += " unique";
          else
            line += " duplicate";
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
      lines.insertElementAt(DvKeyBlock.compressSectorNumbers(bad_sector_numbers, sectors), 0);


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
        data_sector[i]   = 0x01234567;
        data_sector[i+1] = 0x89abcdef;
      }
      lines.add("Block has now been overwritten with values 0x0123456789abcdef");
    }
    else
    {
      lines.add(String.format("Offset 0x%08x contains      0x%08x", zapoffset, data_sector[zapoffset / 4]));
      data_sector[zapoffset / 4] = zapvalue;
      lines.add(String.format("Offset 0x%08x replaced with 0x%08x", zapoffset, zapvalue));
    }

    Native.arrayToBuffer(data_sector, data_buffer);

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

    //common.ptod("int1: %08x %08x", int1, int2);
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
      //common.ptod("txt.charAt(i): " + txt.charAt(i));
      if (!Character.isSpace(txt.charAt(i)) && !Character.isLetterOrDigit(txt.charAt(i)))
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
