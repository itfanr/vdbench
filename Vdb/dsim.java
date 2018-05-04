package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Semaphore;

import Utils.Getopt;
import Utils.OS_cmd;


/**
 * Read 'n' volumes or files, trying to come up with a reasonable estimate
 * of Dedup
 */
public class dsim
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  protected static int    dedupunit        = 128*1024;
  private   static int    total_file_count = 0;
  private   static long   total_file_size  = 0;
  public    static long   total_blocks     = 0;

  protected static long   sample_start;

  protected static int    files_read       = 0;
  protected static int    blocks_read      = 0;
  protected static int    blocks_hashed    = 0;

  protected static long   bytes_read       = 0;
  protected static long   bytes_hashed     = 0;
  private   static int    file_line_length = 0;
  private   static int    null_files       = 0;

  protected static Signal signal           = new Signal(60);;

  protected static long   last_mb_reported = 0;

  private   static ArrayList <DsimFile> file_list = new ArrayList(1024);
  private   static long   cum_filesize = 0;

  protected static HashMap <String, MessageDigest> dedup_map = new HashMap(16384);
  protected static HashMap <String, Long>          count_map = new HashMap(16384);
  protected static HashMap <Long,   Long>          set_map   = new HashMap(16384);


  private   static boolean quiet      = false;

  protected static int     max_workers = 4;
  protected static int     max_files   = 2;

  /* These two are for all files, just so that I can read a subset of a file. */
  protected static long    start_lba = 0;
  protected static long    end_lba   = Long.MAX_VALUE;


  protected static Vector <DsimFile> active_files = new Vector(6);

  protected static int xfersize = 0;


  public static void main(String[] args) throws NoSuchAlgorithmException
  {
    Getopt getopt = new Getopt(args, "qn:u:d:w:f:x:s:e:", 10000);
    quiet = getopt.check('q');
    //getopt.print("dsim");

    if (!getopt.isOK() || getopt.get_positionals().size() == 0)
    {
      common.ptod("Usage: ./vdbench dsim [-n sss] [-w nn] [-f nn] [-u nnnk] disk1, disk2, dir1, dir2, file1, file2, .....");
      common.ptod("Where: ");
      common.ptod("     -u nnn: dedup unit, amount of bytes to be used for deduplication");
      common.ptod("     -n sss: Notify about progress every 'sss' seconds, default 60.");
      common.ptod("     -w nnn: How many 'worker threads' hashing a block. Default 4");
      common.ptod("     -f nnn: How many 'lun/file threads' reading luns and files. Default 2");
      common.ptod("     disk1, file1, ...: up to 10000 disk or file names or windows drive letters (c)");

      common.failure("parameter error");
    }


    if (getopt.check('n'))
      signal = new Signal((int) getopt.get_long());

    if (getopt.check('w'))
      max_workers = (int) getopt.get_long();

    if (getopt.check('f'))
      max_files = (int) getopt.get_long();

    if (getopt.check('s'))
      start_lba = getopt.extractLong();
    if (getopt.check('e'))
      end_lba = getopt.extractLong();

    if (!getopt.check('u'))
      common.failure("'-u nnn' parameter is required to know what the dedup unit size is");

    dedupunit = getopt.extractInt();

    if (dedupunit % 512 != 0)
      common.failure("up unit '%d' not divisible by 512 (did you forget 'k'?)", dedupunit);

    /* Read data in 1MB chunks, but must be multiple of dedupunit: */
    xfersize = 1024 * 1024 / dedupunit * dedupunit;

    if (getopt.check('x'))
      xfersize = getopt.get_int();

    /* Find all the files: */
    if (!quiet)
    {
      System.out.print("Searching for file names ... ");
    }
    file_line_length = 29;
    createFileList(getopt);
    System.out.println();

    /* Calculate how many samples we need for everything together: */
    total_blocks = total_file_size / dedupunit;

    if (!quiet)
    {
      common.ptod("Total file count:       %,14d", total_file_count);
      common.ptod("Total file size:        %14s", FileAnchor.whatSize(total_file_size));
      common.ptod("Total block count:      %,14d", total_blocks);
      common.ptod("Amount of data to read: %14s", FileAnchor.whatSize(total_blocks * dedupunit));
      common.ptod("Worker threads:         %14d", max_workers);
      common.ptod("Concurrent files:       %14d", max_files);
      common.ptod("Dedup unit:             %14d", dedupunit);
      common.ptod("");
    }

    if (total_file_count == 0)
      common.failure("No files found");


    /* Read the files that contain at least one sample: */
    sample_start = System.currentTimeMillis();
    readFiles();

    /* Find out how many hashes have collisions: those are duplicates: */
    String[] hashes       = count_map.keySet().toArray(new String[0]);
    long duplicate_blocks = 0;
    long max_collisions   = 0;
    long dedup_sets       = 0;
    for (String hash : hashes)
    {
      long count = count_map.get(hash).longValue();
      if (count > 1)
      {
        dedup_sets       ++;
        duplicate_blocks += count;
      }
      max_collisions = Math.max(max_collisions, count);
    }
    long unique_blocks = blocks_hashed - duplicate_blocks;

    //long   duplicate_blocks = blocks_hashed - dedup_map.size();

    if (!quiet)
    {
      common.ptod("Reads done:       %,14d (of xfersize %d)", blocks_read, xfersize);
      common.ptod("Blocks_hashed:    %,14d (of dedupunit %d)", blocks_hashed, dedupunit);
      common.ptod("Hash size:        %,14d", dedup_map.size());
      common.ptod("Dedup sets:       %,14d", dedup_sets);
      common.ptod("Duplicate blocks: %,14d", duplicate_blocks);
      common.ptod("Unique blocks:    %,14d", unique_blocks);
      common.ptod("");
    }
    reportStats();
    common.ptod("Please realize that there is no absolute guarantee, especially with smaller");
    common.ptod("configurations, that each of the requested dedup sets is referenced.");



    if (!quiet)
    {
      //common.ptod("");
      //common.ptod("Duplicate blocks spread out over %,d duplicate hash(es): %,d",
      //            duplicates, duplicate_blocks);
      if (duplicate_blocks > 0)
      {
        common.ptod("Average duplicate count per duplicate hash: %,8d",
                    duplicate_blocks / duplicate_blocks);
        common.ptod("Maximum collisions:                         %,8d", max_collisions);
      }
    }


    Long[] sets = set_map.keySet().toArray(new Long[0]);
    Arrays.sort(sets);
    long total = 0;
    for (Long set : sets)
    {
      total += set_map.get(set);
      //common.ptod("set# %4d count: %4d", set, set_map.get(set));
    }

    common.ptod("Number of sets found: %,d counting %,d duplicates", set_map.size(), total);

    /* report here any hash that has more than 1 reference: */
    /* it gets ugly with large files though.......          */
    if (false)
    {
      /* First create Strings to sort later: */
      ArrayList <String> lines = new ArrayList(hashes.length);
      for (String hash : hashes)
      {
        long count = count_map.get(hash).longValue();
        if (count > 1)
          lines.add(String.format("Count: %06d hash: %s", count, hash));
      }
      Collections.sort(lines);

      int printed = 0;
      //for (int i = lines.size() - 1; i > 0 && printed++ < 30; i--)
      for (int i = 0; i < lines.size() && printed++ < 300; i++)
        common.ptod("lines: " + lines.get(i));
      common.ptod("Stopped detailed hash count reporting after %d lines", printed);
    }
  }



  /**
   * A primitive way to determine the size of a file or lun.
   */
  private static long determineVolSize(String fname)
  {
    RandomAccessFile raf = null;
    byte[] small_buffer = new byte[512];
    long last_low_lba  = 0;
    long last_high_lba = 100l*1024l*1024l*1024l*1024l;   // 100tb. Maybe make MAX_VALUE?

    if (!fname.startsWith("\\\\") && new File(fname).isDirectory())
    {
      common.ptod("File name skipped; is a directory: " + fname);
      return -1;
    }

    /* If this is a disk file then it's mighty easy: */
    if (new File(fname).length() != 0)
      return new File(fname).length();

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
      //common.ptod("tries: " + tries + " " + lba + " " + fname);
      if (tryRead(raf, small_buffer, lba))
      {
        //common.ptod("success lba: %14d ll: %14d lh: %14d", lba, last_low_lba, last_high_lba);
        last_low_lba = lba;
      }
      else
      {
        //common.ptod("failed  lba: %14d ll: %14d lh: %14d", lba, last_low_lba, last_high_lba);
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

    return(long) ((last_low_lba + 512) );
  }


  /**
   * Read 512 bytes to see if that block exists.
   */
  private static boolean tryRead(RandomAccessFile raf, byte[] small_buffer, long lba)
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

  public static synchronized void countReads(int bytes)
  {
    bytes_read  += bytes;
    blocks_read ++;
  }
  public static synchronized void countFiles()
  {
    files_read  ++;
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
          scanDirectory(fptr);

        else if (common.onWindows() && fname.length() == 1)
        {
          String letter = fname;
          fname = String.format("\\\\.\\%s:", fname);
          addFile(fname);
          common.ptod("\nChanging '%s' to windows raw device %s", letter, fname);
        }

        else
          common.ptod("Unknown file type? " + fname);
      }
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    double elapsed = System.currentTimeMillis() - start_time;
    if (!quiet)
    {
      System.out.println();
      common.ptod("createFileList took %.1f seconds", (elapsed / 1000.));
    }
  }

  /**
   * Scan a directory recursively and add all files.
   */
  private static int last_level = 0;
  private static void scanDirectory(File dirptr)
  {
    try
    {
      String dirname = dirptr.getAbsolutePath();

      if (isThisLink(dirname))
      {
        common.ptod("Directory appears to be a link. Ignored: " + dirname);
        return;
      }

      //if (common.get_debug(common.DEBUG_DIRECTORIES))
      //  common.ptod("Scanning directory " + dirname);

      //if (common.get_debug(common.DEBUG_LEVEL))
      //{
      //  StringTokenizer st = new StringTokenizer(dirname, File.separator);
      //  if (st.countTokens() != last_level)
      //    common.ptod("Scanning directory " + dirname);
      //  last_level = st.countTokens();
      //}

      String[] files = dirptr.list();
      if (files == null)
      {
        common.ptod("\nscanDirectory: " + dirptr.getAbsolutePath());
        common.ptod("You may not have the proper priviliges to read this directory. Directory is bypassed");
        return;
      }

      for (int i = 0; i < files.length; i++)
      {
        String fname = files[i];

        if (fname.equals("no_dismount.txt") || fname.equals("vdb_control.file"))
          continue;

        File fptr = new File(dirname, fname);
        if (fptr.isFile())
          addFile(fptr.getAbsolutePath());
        else
          scanDirectory(fptr);
      }
    }
    catch (Exception e)
    {
      common.ptod("\nscanDirectory: " + dirptr.getAbsolutePath());
      common.ptod("This directory is bypassed");
      common.ptod(e);
    }
  }

  public static void reportStats()
  {
    long   duplicate_blocks = blocks_hashed - dedup_map.size();
    double elapsed          = System.currentTimeMillis() - sample_start;
    double mbsec            = (elapsed > 0) ? (bytes_read / (elapsed / 1000.) / 1000000.) : 0;
    double ratio            = (blocks_hashed == 0) ? 0 : (double) blocks_hashed / dedup_map.size() ;

    common.ptod("Totals: Dedup ratio: %.2f:1 (%.5f) Files read: %,6d; Reads done: %,12d; mb/sec: %6.2f",
                ratio, ratio, files_read, blocks_read, mbsec);
  }


  /**
   * Unix level determination if this is a link.
   * May not be 100% accurate.
   */
  private static boolean isThisLink(String dirname)
  {
    if (common.onWindows())
      return false;

    /* Don't check links? */
    //if (common.get_debug(common.DEBUG_LINK))
    //  return false;

    /* Only check for link at the lower levels? */
    //if (common.get_debug(common.DEBUG_LINK4))
    //{
    //  if (dirname.split("/+").length > 4)
    //    return false;
    //}


    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("/usr/bin/ls -ld");
    ocmd.addQuot(dirname);
    ocmd.execute(false);

    /* An error is treated as a link: */
    if (!ocmd.getRC())
    {
      common.ptod("isThisLink: Unexpected results from %s", ocmd.getCmd());
      String[] stderr = ocmd.getStderr();
      for (int i = 0; i < stderr.length; i++)
        common.ptod("stderr: " + stderr[i]);
      return true;
    }

    String[] stdout = ocmd.getStdout();
    if (stdout.length != 1)
    {
      common.ptod("isThisLink: Unexpected results from %s", ocmd.getCmd());
      for (int i = 0; i < stdout.length; i++)
        common.ptod("stdout: " + stdout[i]);
      return true;
    }

    return stdout[0].startsWith("l");

  }

  /**
   * Add a new file to the list after determining the size.
   */
  private static void addFile(String fname)
  {
    DsimFile  fe   = new DsimFile();
    fe.fname       = fname;
    long file_size = determineVolSize(fname);
    if (file_size <= 0)
      return;

    ///* Using 'csim .' causes strange /dir/./xyz file names. Fix: */
    //if (!fe.fname.startsWith("\\\\"))
    //  fe.fname = common.replace(fname, "\\.\\", "\\");


    fe.true_size       = file_size;
    fe.rel_start_lba   = dsim.cum_filesize;
    dsim.cum_filesize += file_size;
    file_list.add(fe);

    //if (common.get_debug(common.DEBUG_FILES))
    //  common.ptod("Adding file %,12d %s", file_size, fe.fname);
    total_file_size += file_size;
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
   * Read the files that contain at least one sample
   */
  private static void readFiles()
  {
    long sample_start = System.currentTimeMillis();

    for (DsimFile df : file_list)
    {
      /* Wait for there to be room: */
      while (active_files.size() >= dsim.max_files)
        common.sleep_some(1);

      active_files.add(df);
      df.start();
    }


    /* Wait until all done: */
    while (active_files.size() > 0)
      common.sleep_some(500);

  }
}



class Request
{
  byte[]  byte_buffer;
  int []  int_buffer;
  int     word0;
  int     word1;
  long    rel_lba;
  long    buf_lba;
  int     offset;
  long    setno;       /* should inlcude dedup_type. tbd */
  long    begin;     /* The first 8 bytes of each dedupunit */
  boolean unique;
}


class DsimHash extends Thread
{

  MessageDigest hasher;
  DsimFile      dsf     = null;

  public DsimHash(DsimFile ds)
  {
    dsf = ds;
  }

  public void run()
  {

    Request req = null;
    try
    {
      hasher = MessageDigest.getInstance("MD5");
    }
    catch (NoSuchAlgorithmException e)
    {
      common.failure(e);
    }

    common.sleep_some(3);

    while (true)
    {
      try
      {
        dsf.hash_get_sema.acquire();
        synchronized(dsf.hash_queue)
        {
          //req = dsf.hash_queue.lastElement();
          req = dsf.hash_queue.get(0);
          if (!dsf.hash_queue.remove(req))
            common.failure("remove failed");
          if (req == null)
            break;
          dsf.hash_put_sema.release();
        }

        dedupBuffer(req, dsf);
      }
      catch (IllegalArgumentException e)
      {
        common.ptod(e.getMessage());
        //common.where();
        //return;
      }
      catch (InterruptedException e)
      {
        return;
      }
    }
  }


  /**
   * Create a hash for the current 'dedupunit=' set of data read by DsimFile().
   *
   * Note that the byte_buffer used here is always a new byte buffer allocated
   * after the read. The Native buffer therefore can be reused for the next
   * asynchrous read.
   */
  private void dedupBuffer(Request req, DsimFile dsf)
  {
    byte[] byte_buffer = req.byte_buffer;
    int[]  int_buffer  = req.int_buffer;
    int    offset      = req.offset;

    long start = System.nanoTime();

    //if (req.unique)
    //  common.ptod("unique: " + req.unique);

    //common.ptod("hash unit_lba: %016x", req.unit_lba);

    hasher.update(byte_buffer, offset, dsim.dedupunit);
    dsim.bytes_hashed += dsim.dedupunit;

    /* Statistic reporting is checked each time we handle an MB of data: */
    if ((dsim.bytes_hashed / 1048576l) > dsim.last_mb_reported)
    {
      synchronized (dsim.dedup_map)
      {
        if (dsim.signal.go())
          dsim.reportStats();
      }
      dsim.last_mb_reported = dsim.bytes_hashed / 1048576l;
    }

    byte[] hash_array = hasher.digest();
    String hash = new BigInteger(1, hash_array).toString(16);

    synchronized (dsim.dedup_map)
    {
      dsim.blocks_hashed++;

      /* if this is a new block, add it to the dedup_map: */
      if (dsim.dedup_map.get(hash) == null)
      {
        //common.ptod("new hash for lba %,12d word1: %08x %s %s",
        //            req.rel_lba, req.word1, dsf.fname, hash);
        dsim.dedup_map.put(hash, hasher);
        dsim.count_map.put(hash, 1l);
      }
      else
      {
        //common.ptod("dup hash for lba %,12d word1: %08x %s %s",
        //            req.rel_lba, req.word1, dsf.fname, hash);
        Long previous = dsim.count_map.get(hash);
        dsim.count_map.put(hash, previous.longValue() + 1);
      }

      if (!req.unique)
      {
        if (dsim.set_map.get(req.setno) == null)
        {
          dsim.set_map.put(req.setno, 1l);
        }
        else
        {
          Long previous = dsim.set_map.get(req.setno);
          dsim.set_map.put(req.setno, previous.longValue() + 1);
        }
      }


      long end = System.nanoTime();
      dsf.hash_elapsed += end - start;
      dsf.hash_counts  ++;
    }


    //synchronized (dsim.df)
    //{
    //  dsim.concurrent --;
    //}
  }
}

class DsimFile extends Thread
{
  private   ArrayList <DsimHash> hash_workers     = new ArrayList(5);
  protected Semaphore            hash_get_sema    = new Semaphore(0);
  protected Semaphore            hash_put_sema    = new Semaphore(16);
  protected Vector    <Request>  hash_queue       = new Vector(10);


  String    fname;
  long      true_size;
  long      rel_start_lba;

  long      hash_elapsed  = 0;
  long      hash_counts   = 0;

  long      read_resptime = 0;
  long      read_counts   = 0;

  public DsimFile()
  {
  }

  public void run()
  {
    int[] int_buffer = new int[ dsim.xfersize / 4];
    long       buf   = Native.allocBuffer(dsim.xfersize);

    double sum_of_qdepth = 0;

    Thread.currentThread().setPriority( Thread.MAX_PRIORITY );
    try
    {
      /* Start our workers: */
      for (int i = 0; i < dsim.max_workers; i++)
      {
        DsimHash ds = new DsimHash(this);
        hash_workers.add(ds);
        ds.start();
      }

      long   bytes_in_file  = true_size;

      long handle = Native.openfile(fname, 0, 0);
      dsim.countFiles();

      /* We read each block within this file. Once we reach xfersize  */
      /* bytes those bytes are passed to the compressor:              */
      for (long lba_in_file = dsim.start_lba;
          lba_in_file < bytes_in_file && lba_in_file < dsim.end_lba; )
      {
        //common.ptod("lba_in_file: %,16d", lba_in_file);
        int bytes_to_read = dsim.xfersize;

        /* Don't read the last partial block: */
        if (bytes_in_file - lba_in_file < dsim.xfersize) //  dsim.dedupunit)
        {
          bytes_to_read = (int) (bytes_in_file - lba_in_file);
          if (bytes_to_read % dsim.dedupunit != 0)
          {
            //common.ptod("Ignoring last incomplete dedup unit (%d bytes) for %s",
            //            (bytes_in_file - lba_in_file), fname);

            /* We clear the raw data buffer and read the short block. */
            /* That way when we dedup 'dedupunit' the end is all zero */
            for (int f = 0; f < int_buffer.length; f++)
              int_buffer[f] = 0;
            Native.arrayToBuffer(int_buffer, buf);

            //break;
          }
        }

        long start = System.nanoTime();
        //common.ptod("Reading %,8d bytes from file %s", bytes_to_read, fname);
        Native.readFile(handle, lba_in_file, bytes_to_read, buf);
        read_counts   ++;
        read_resptime += System.nanoTime() - start;

        Native.buffer_to_array(int_buffer, buf, bytes_to_read);

        ByteBuffer bb = ByteBuffer.allocate(dsim.xfersize);
        IntBuffer  ib = bb.asIntBuffer();
        ib.put(int_buffer);
        byte[] byte_buffer = bb.array();

        int bytes = byte_buffer.length;
        dsim.countReads(bytes);



        /* Dedup each piece separately: */
        int units = (bytes_to_read + dsim.dedupunit - 1) / dsim.dedupunit;
        //common.ptod("units: " + units);
        for (int key_index = 0; key_index < units; key_index++)
        {
          int  int_offset = key_index * (dsim.dedupunit / 4);
          int  word0      = int_buffer[int_offset + 0];
          int  word1      = int_buffer[int_offset + 1];

          long buf_lba    = ((long) word0 << 32) | (long) word1;
          long rel_lba    = lba_in_file + (key_index * dsim.dedupunit); // + rel_start_lba;

          Request req     = new Request();

          req.byte_buffer = byte_buffer;
          req.int_buffer  = int_buffer;
          req.offset      = key_index * dsim.dedupunit;
          req.buf_lba     = lba_in_file;
          req.rel_lba     = rel_lba;
          req.unique      = (buf_lba == lba_in_file + req.offset);
          req.word0       = word0;
          req.word1       = word1;
          if (!req.unique)
            req.setno = word1;

          //common.ptod("word0: %08x %08x buf_lba: %016x %08x %b", word0, word1, buf_lba, lba_in_file, req.unique);
          //common.ptod("unit_lba: %016x %016x", req.unit_lba, lba_in_file);


          hash_put_sema.acquire();
          hash_queue.add(req);
          hash_get_sema.release();

          //common.ptod("Sending hash out for file %s", fname);
        }

        lba_in_file        += bytes;
        sum_of_qdepth += hash_queue.size();

        // still need to handle the last block

      }

      /* Set last queue entry to null, telling worker he's done: */
      for (DsimHash ds : hash_workers)
      {
        hash_queue.add(null);
        hash_get_sema.release();
      }

      Native.closeFile(handle);

      /* Wait for hash request queue to be empty: */
      while (hash_queue.size() > 0)
        common.sleep_some(4);

      common.sleep_some(5);

      /* Wait for hash workers to be done: */
      int alives = 0;
      do
      {
        alives = 0;
        for (DsimHash ds : hash_workers)
        {
          if (ds.isAlive())
            alives++;
        }
      } while (alives != 0);

      double read_resp = (read_counts > 0) ? read_resptime / read_counts / 1000000. : 0;
      long   usecs     = (hash_counts > 0) ? hash_elapsed / hash_counts / 1000 : 0;

      //common.ptod("reads: %6d msecs: %7.3f hash: %6d usecs: %6d %s",
      //            read_counts, read_resp,
      //            hash_counts, usecs, fname);

      //common.ptod("read_counts: " + read_counts);
      //common.ptod("avg hash qdepth: " + sum_of_qdepth / read_counts);
      //
      //common.ptod("hash_counts:  " + hash_counts);
      //common.ptod("hash_elapsed: %,d", hash_elapsed / 1000);
      //common.ptod("hash_usecs:   " + hash_elapsed / hash_counts / 1000);
      //
      //common.ptod("read_counts:  " + read_counts);
      //common.ptod("read_elapsed: %,d", read_resptime / 1000);
      //if (read_counts > 0)
      //  common.ptod("read_usecs:   " + read_resptime / read_counts / 1000);
    }


    catch (Exception e)
    {
      common.ptod("");
      common.failure(e);
    }

    Native.freeBuffer(dsim.xfersize, buf);

    dsim.active_files.remove(Thread.currentThread());
  }
}

