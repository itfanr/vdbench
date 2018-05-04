package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import Utils.Getopt;

public class csim
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static RandomAccessFile raf;
  //private static byte[]           input_buffer;
  private static int              xfersize   = 128*1024;
  private static double           pct        = 0.1;
  private static int              level      = 1;
  private static double           raw_used   = 100;
  private static double           subset     = 100;
  private static long             volsize;
  private static long             volblocks;
  private static boolean          raw_volume = false;
  private static int              need_header = 0;
  private static int              total_file_count = 0;
  private static long             total_file_size  = 0;

  private static int blocks_read = 0;

  private static long             bytes_read = 0;
  private static long             bytes_out  = 0;
  private static int              file_line_length = 0;
  private static int              null_files = 0;

  private static ArrayList <CsimEntry> file_list = new ArrayList(1024);

  private static String level_splitter = (common.onWindows()) ? "\\+" : "/+";


  private static Random randomizer = new Random(0); // maybe create seed?
  private static int max_fname_length = 0;


  public static void main(String[] args)
  {
    Getopt g = new Getopt(args, "l:p:u:d:r:s:", 10000);
    //g.print("csim");

    if (!g.isOK() || g.get_positionals().size() == 0)
    {
      common.ptod("Usage: ./vdbench csim [-l nnn] [-p nnn] [-x nnn] [-s nnn] disk1, disk2, file1, file2, .....");
      common.ptod("Where: ");
      common.ptod("     -l nnn: gzip compression level to use, default 1");
      common.ptod("     -p nnn: which percentage of data to read, default 0.1%");
      common.ptod("     -s nnn: subset percentage. e.g. -s10 reports compression for each 10% of the volume");
      common.ptod("     -u nnn: transfer size unit in bytes for blocks to be read and compressed. Default 128k");
      common.ptod("     disk1, file1, ...: up to 10000 disk or file names or windows drive letters (c)");

      common.failure("parameter error");
    }

    if (g.check('p')) pct      =       g.get_double();
    if (g.check('l')) level    = (int) g.get_long();
    //if (g.check('r')) raw_used = (int) g.get_double();
    if (g.check('s')) subset   =       g.get_double();

    if (g.check('u'))
      xfersize = g.extractInt();

    /* Find all the files: */
    file_line_length = 29;
    createFileList(g);

    common.ptod("");
    doFiles();
    System.exit(0);;
  }


  private static void doFiles()
  {
    /* Go through each file/volume: */
    for (int i = 0; i < file_list.size(); i++)
    {
      CsimEntry ce = file_list.get(i);

      for (int e = 0; e < ce.extents.size(); e++)
      {
        CsimExtent extent = ce.extents.get(e);

        long extent_blocks  = extent.size / xfersize;
        long extent_samples = (pct >= 99) ? extent_blocks : (int) (extent_blocks * pct / 100) + 1;

        Long[] blocks_to_read = createSampleList(extent_blocks, extent_samples);
        readExtent(blocks_to_read, ce, extent);

        if (ce.extents.size() > 1)
        {
          if (extent.extno == 1)
            common.ptod("");
          printit(String.format("%s (%d)", ce.fname, extent.extno), extent);
        }
      }

      printit(String.format("%s", ce.fname), ce.main_extent);
    }
  }

  private static void printit(String title, CsimExtent extent)
  {
    double cpct  = (extent.bytes_out * 100. / extent.bytes_in);
    double ratio = ((double) extent.bytes_in / extent.bytes_out);
    String mask  = "%-" + max_fname_length + "s";

    common.ptod(mask + " size: %6s samples: %5d in: %6s out: %6s pct: %5.1f compratio: %7.2f:1",
                title,
                whatSize(extent.size),
                extent.blocks_read,
                whatSize(extent.bytes_in),
                whatSize(extent.bytes_out),
                cpct, ratio);
  }


  /**
   * Create a list of lbas that we want to read, handle duplicate random numbers
   */
  private static Long[] createSampleList(long extent_blocks, long extent_samples)
  {

    randomizer = new Random(0); // maybe create seed?


    int dups2 = 0;
    HashMap <Long, Long> sample_candidates = new HashMap((int) extent_samples * 2);
    for (int i = 0; i < extent_samples; i++)
    {
      long lba = ((long) (randomizer.nextDouble() * extent_blocks) * xfersize);
      if (sample_candidates.get(lba) != null)
      {
        dups2++;
        i--;
        continue;
      }

      sample_candidates.put(lba, lba);
    }

    //common.ptod("total_blocks: " + extent_blocks);
    //common.ptod("dups2: " + dups2);

    Long[] lbas_to_read = (Long[]) sample_candidates.keySet().toArray(new Long[0]);
    Arrays.sort(lbas_to_read);
    sample_candidates = null; // clear memory

    //for (int i = 0; i < 10 && i < lbas_to_read.length; i++)
    //  common.ptod("lbas_to_read (only 10): %12d, %16x", lbas_to_read[i], lbas_to_read[i]);

    return lbas_to_read;
  }


  /**
   * A primitive way to determine the size of a file or lun.
   */
  private static long determineVolSize(String fname)
  {
    byte[] small_buffer = new byte[512];
    long last_low_lba  = 0;
    long last_high_lba = 1024l*1024l*1024l*1024l*1024l;   // 100tb. Maybe make MAX_VALUE?

    if (!fname.startsWith("\\\\") && new File(fname).isDirectory())
    {
      common.ptod("File name skipped; is a directory: " + fname);
      return -1;
    }

    /* If this is a disk file then it's mighty easy: */
    raw_volume = true;
    if (new File(fname).length() != 0)
    {
      raw_volume = false;
      return new File(fname).length();
    }

    /* Only some 'raw' disks are tried: */
    if (!fname.startsWith("\\\\") && !fname.startsWith("/dev"))
    {
      //common.ptod("Null file, ignored: " + fname);
      null_files++;
      return -1;
    }

    /* It must be a raw disk. Use binary search: */
    try
    {
      raf = new RandomAccessFile(fname, "r");
    }
    catch (Exception e)
    {
      common.ptod("Exception opening file/lun " + fname);
      common.failure(e);
    }

    int tries = 0;
    while (true)
    {
      /* Try the middle between the last OK and the last failed read: */
      long lba = last_low_lba + ((last_high_lba - last_low_lba) / 2) & ~0x1ff;
      tries++;
      //common.ptod("tries: %6d %,24d %s", tries, lba, fname);
      if (tryRead(small_buffer, lba))
      {
        //common.ptod("success lba: %,14d ll: %,14d lh: %,14d", lba, last_low_lba, last_high_lba);
        last_low_lba = lba;
      }
      else
      {
        //common.ptod("failed  lba: %,14d ll: %,14d lh: %,14d", lba, last_low_lba, last_high_lba);
        last_high_lba = lba;
      }

      /* If the low and high meet here we're done: */
      if (last_low_lba + 512 == last_high_lba)
        break;
    }

    try
    {
      raf.close();
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    //common.ptod("tries: " + tries);

    return(long) ((last_low_lba + 512) * raw_used / 100);
  }


  /**
   * Read 512 bytes to see if that block exists.
   */
  private static boolean tryRead(byte[] small_buffer, long lba)
  {
    try
    {
      raf.seek(lba);
      int bytes = raf.read(small_buffer);
      if (bytes > 0)
        return true;
      else
        return false;
    }
    catch (Exception e)
    {
      return false;
    }
  }

  /**
   * Get the input list of files, volumes or directories and put them into a new
   * list, which includes a recursive list of file names and their sizes.
   */
  private static void createFileList(Getopt g)
  {
    long start_time = System.currentTimeMillis();
    try
    {
      for (int i = 0; i < g.get_positionals().size(); i++)
      {
        String fname = g.get_positional(i);

        if (common.onWindows() && fname.length() == 2 && fname.endsWith(":"))
        {
          common.ptod("\n\n");
          common.ptod("Asking for file, directory or volume '%s'", fname);
          common.failure("Windows: specify either single drive letter 'c' or directory 'c:\\'. ");
        }

        File fptr = new File(fname);

        if (!common.onWindows() && fname.startsWith("/dev/"))
          addFile(fname);

        else if (fptr.isFile())
          addFile(fptr.getAbsolutePath());

        else if (fptr.isDirectory())
          common.ptod("Ignoring directory: " + fptr.getAbsolutePath());
        //scanDirectory(fptr);

        else if (common.onWindows() && fname.length() == 1)
        {
          String letter = fname;
          fname = String.format("\\\\.\\%s:", fname);
          addFile(fname);
          //common.ptod("\nChanging '%s' to windows raw device %s", letter, fname);
        }

        else
          common.ptod("Unknown file type? " + fname);
      }
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    //double elapsed = System.currentTimeMillis() - start_time;
    //common.ptod("");
    //common.ptod("createFileList took %.1f seconds", (elapsed / 1000.));
  }


  /**
   * Add a new file to the list after determining the size.
   */
  private static void addFile(String fname)
  {
    CsimEntry ce = new CsimEntry();
    ce.fname     = fname;
    long size    = determineVolSize(fname);
    if (size <= 0)
      return;

    max_fname_length = Math.max(max_fname_length, fname.length());

    ///* Using 'csim .' causes strange /dir/./xyz file names. Fix: */
    //if (!ce.fname.startsWith("\\\\"))
    //  ce.fname = common.replace(fname, "\\.\\", "\\");

    /* By default the only extent in the ArrayList is the main extent: */
    ce.main_extent = new CsimExtent(0, size, 0);
    ce.extents.add(ce.main_extent);

    /* If we ask for subsets, create all new extents: */
    if (subset != 100)
    {
      int  count    = 100 / (int)  subset;
      long ext_size = size / count;
      ce.extents.clear();
      for (int i = 0; i < count; i++)
      {
        long offset = i * ext_size;
        offset -= offset % xfersize;
        ce.extents.add(new CsimExtent(offset , ext_size, i+1));
      }
    }

    ce.size  = size;
    file_list.add(ce);

    //if (common.get_debug(common.DEBUG_FILES))
    //  common.ptod("Adding file %-70s %12d", ce.fname, size);
    total_file_size += size;
    total_file_count++;

    if (file_line_length > 80)
    {
      common.ptod("");
      file_line_length = 0;
    }

    if (total_file_count % 10000 == 0)
    {
      String tmp = String.format("%7d ", total_file_count);
      file_line_length += tmp.length();
      System.out.print(tmp);
    }
  }


  /**
   * Read the samples
   */
  private static void readExtent(Long[] blocks, CsimEntry ce, CsimExtent extent)
  {
    byte[] input_buffer = new byte[xfersize];
    long lba = 0;

    try
    {
      raf = new RandomAccessFile(ce.fname, "r");

      /* We read each requested block within this file. */
      for (int i = 0; i < blocks.length; i++)
      {
        lba = blocks[i] + extent.start_lba;

        /* Read this piece of the file that we need: */
        raf.seek(lba);
        int bytes = raf.read(input_buffer, 0, xfersize);


        if (bytes < 0)
        {
          common.ptod("ce: " + ce);
          common.failure("problem reading file");
        }


        long out          = compressBuffer(input_buffer);
        extent.bytes_in  += bytes;
        extent.bytes_out += out;
        extent.blocks_read++;

        if (ce.extents.size() > 1)
        {
          ce.main_extent.bytes_in  += bytes;
          ce.main_extent.bytes_out += out;
          ce.main_extent.blocks_read++;
        }

        //common.ptod("Just read: %-12s lba: %12d, read: %6d out: %6d", ce.fname, lba, bytes, out);
      }

      raf.close();
    }

    catch (IOException ex)
    {
      common.ptod("IOException: " + ex.getMessage());
      common.ptod("extent.start_lba: " + extent.start_lba);
      common.ptod("lba:            %12d %16x  ", lba, lba);
      //e.printStackTrace();
    }

    catch (Exception ex)
    {
      common.ptod("");
      common.failure(ex);
    }
  }


  private static int compressBuffer(byte[] buffer)
  {
    try
    {
      ByteArrayOutputStream byteout = new ByteArrayOutputStream(xfersize);
      BufferedOutputStream bostream = new BufferedOutputStream(byteout);
      DataOutputStream  out_stream  = new DataOutputStream(bostream);
      GZIPOutputStream  zip_out_stream;
      zip_out_stream = new GZIPOutputStream(out_stream, (int) xfersize)
      {
        { def.setLevel(level);}
      };

      zip_out_stream.write(buffer, 0, xfersize);
      zip_out_stream.close();

      /* We take the zip results and accumulate them. */
      /* We round upwards to 512, maybe will have to be 8k or larger. */
      int bytes_out = byteout.size();

      //common.ptod("bytes_out: " + bytes_out);
      return bytes_out;
    }
    catch (Exception e)
    {
      common.ptod(e);
    }

    return 0;
  }



  private static double KB = 1024.;
  private static double MB = 1024. * 1024.;
  private static double GB = 1024. * 1024. * 1024.;
  private static double TB = 1024. * 1024. * 1024. * 1024.;
  private static double PB = 1024. * 1024. * 1024. * 1024. * 1024.;
  public static String whatSize(double size)
  {
    if (size < KB)
      return "" + size;

    String txt;
    if (size < MB)
      txt = String.format("%.1fk", size / KB);
    else if (size < GB)
      txt = String.format("%.1fm", size / MB);
    else if (size < TB)
      txt = String.format("%.1fg", size / GB);
    else if (size < PB)
      txt = String.format("%.1ft", size / TB);
    else
      txt = String.format("%.1fp", size / PB);

    /* Remove '.000' if this is a 'nice' number: */
    String front = txt.substring(0, txt.length() - 3);
    String tail  = txt.substring(txt.length() - 3);
    if (tail.startsWith(".0"))
      txt = front + tail.substring(2);

    return txt;
  }
}



class CsimEntry
{
  private final static String c = "Copyright (c) 2000-2005 Sun Microsystems, Inc. " +
                                  "All Rights Reserved.";

  String    fname;
  long      size;
  CsimExtent    main_extent;
  ArrayList <CsimExtent> extents = new ArrayList(10);

  public CsimEntry()
  {
  }
}



class CsimExtent
{
  long start_lba;
  long size;
  long bytes_in;
  long bytes_out;
  long blocks_read;
  int  extno;

  public CsimExtent(long start_lba, long size, int extno)
  {
    //common.ptod("size: %12d start: %12d", size, start_lba);
    this.start_lba = start_lba;
    this.size      = size;
    this.extno     = extno;
  }
}





