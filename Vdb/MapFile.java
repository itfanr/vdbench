package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import Utils.*;


/**
 * Using a memory mapped ByteBuffer to handle the Data Validation byte map.
 * This eliminates the need to use journal files.
 * Of course, when the OS dies we still need journal recovery.
 */
public class MapFile extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String           filename;
  private int              bytes_in_map;
  private MappedByteBuffer bbuffer;
  private FileChannel      channel;

  public  MapCounts        counter = null;

  private static HashMap file_map = new HashMap(16);

  /* This must be a power of two: */
  public  final static long  MAX_BYTES  = 1024 * 1024 * 1024;
  public  final static int   BYTE_SHIFT = 30;
  public  final static long  BYTE_AND   = (1 << BYTE_SHIFT) - 1;


  /**
   * Create a new map after first deleting a possible old file.
   *
   * Note: This actually now creates 'n' maps so that we can now handle maps
   * larger than 2**31
   */
  public static MapFile[] createNewFile(String jnl_dir_name, String fname, long total_len)
  {
    /* One extra needed due to dedupunit straddling: */
    if (Dedup.isDedup())
      total_len++;

    /* See how many maps we need: */
    long      maps_needed = (total_len + MAX_BYTES - 1) / MAX_BYTES;
    MapFile[] maps        = new MapFile[ (int) maps_needed ];

    for (int map = 0; map < maps.length; map++)
    {
      String fullname = createMapFileName(jnl_dir_name, fname, map);

      if (file_map.get(fullname) != null)
        common.failure("Map file still open: " + fullname);

      File fptr = new File(fullname);
      if (fptr.exists())
      {
        if (!fptr.delete())
        {
          ErrorLog.plog("Unable to delete old map file: " + fullname);
          common.failure("Unable to delete old map file: " + fullname);
        }
      }

      /* Need to figure out when we can just throw away the mmap file: */
      /* Keep it when:                                                 */
      /* - journaling                                                  */
      /* - dedup                                                       */
      //if (!Validate.isJournaling() && !Validate.isValidateForDedup())
      //  fptr.deleteOnExit();


      int bytes_to_get = (int) Math.min(total_len - map * MAX_BYTES, MAX_BYTES);
      //common.ptod("maps_needed: " + maps_needed);
      //common.ptod("map:          " + map);
      //common.ptod("total_len:    " + total_len);
      //common.ptod("bytes_to_get: " + bytes_to_get);
      //common.sleep_some(20);

      MapFile mf = maps[ map ] = new MapFile();

      try
      {
        mf.filename     = fullname;
        mf.bytes_in_map = bytes_to_get;
        mf.channel      = new RandomAccessFile(mf.filename, "rw").getChannel();

        mf.bbuffer      = mf.channel.map(FileChannel.MapMode.READ_WRITE, 0, mf.bytes_in_map);
        file_map.put(mf.filename, mf);
      }

      catch (Exception e)
      {
        common.failure(e);
      }

      /* Initialize contents: */
      for (int i = 0; i < mf.bytes_in_map; i++)
        mf.bbuffer.put((byte) 0);



      ErrorLog.plog("Created new mmap file: %s mmap size: %s",
                    mf.filename, FileAnchor.whatSize(mf.bytes_in_map));
    }


    return maps;
  }

  /**
   * Create a new mapped after first deleting a possible old file.
   */
  public static MapFile[] openOldFile(String jnl_dir_name, String fname, long total_len)
  {
    /* See how many maps we need: */
    int count = (int) (total_len + MAX_BYTES - 1);
    MapFile[] maps = new MapFile[ count ];

    for (int map = 0; map < count; map++)
    {
      String fullname     = createMapFileName(jnl_dir_name, fname, map);
      int    bytes_to_get = (int) Math.min(total_len - map * MAX_BYTES, MAX_BYTES);

      File fptr = new File(fullname);
      if (!fptr.exists())
        common.failure("Opening of old map file failed: " + fullname);

      if (fptr.length() != bytes_to_get)
        common.failure("Map file size mismatch: %s current size: %d requested size: %d",
                       fullname, fptr.length(), bytes_to_get);

      if (file_map.get(fullname) != null)
        common.failure("Map file still open: " + fullname);

      MapFile mf  = new MapFile();
      try
      {
        mf.filename     = fullname;
        mf.bytes_in_map = bytes_to_get;
        mf.bbuffer      = new RandomAccessFile(mf.filename, "rw").getChannel().
                          map(FileChannel.MapMode.READ_WRITE, 0, mf.bytes_in_map);
        file_map.put(mf.filename, mf);
      }

      catch (Exception e)
      {
        common.failure(e);
      }

      ErrorLog.plog("Reusing mmap file: %s mmap size: %s",
                    mf.filename, FileAnchor.whatSize(mf.bytes_in_map));
    }

    return maps;
  }

  public void put(int index, int val)
  {
    //common.ptod("Mapfile.put(): %s %6d %02x", filename, index, val);
    bbuffer.put(index, (byte) val);
  }
  public int get(int index)
  {
    int ret = bbuffer.get(index) &0xff;
    //common.ptod("Mapfile.get(): %s %6d %02x", filename, index, ret);
    return ret;
  }

  public long get8(int index)
  {
    try
    {
      long value = bbuffer.getLong(index);
      return value;
    }
    catch (Exception e)
    {
      common.ptod("index: " + index);
      common.failure(e);
      return -1;
    }
  }


  public void closeMapFile()
  {
    try
    {
      bbuffer.force();
      channel.close();
      file_map.remove(filename);
    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public String getFilename()
  {
    return filename;
  }
  public int getBytesInMap()
  {
    return bytes_in_map;
  }


  /**
   * Determine what the map's file name will be.
   *
   * Default: systemtemp.vdbench.pid###. + 'label' + '.mmap.' + seq
   *
   * If journal= has been used, then of course no system temp, and also no PID.
   */
  private static String createMapFileName(String jnl_dir_name, String label, int seq)
  {
    /* Easy name when using journaling: */
    if (jnl_dir_name != null && !Jnl_entry.isRawJournal(jnl_dir_name))
      return String.format("%svdbench.%s.mmap.%d", Fget.separator(jnl_dir_name), label, seq);

    /* Process id: (java 1.5+) */
    String pid = common.getProcessIdString();

    return String.format("%svdbench.pid%s.%s.mmap.%d", Fput.getTmpDir(), pid, label, seq);
  }


  /**
   * Running multiple concurrent Vdbench executions using only the fsd= or sd=
   * name to make the mmap files unique is no longer enough when those same
   * names are reused across executions.
   * By adding the 'pidxxx' information these files now ARE unique, but must be
   * deleted after any Vdbench execution to prevent leaving too much garbage
   * behind.
   * A deliberate choice was made to NOT use deleteOnExit() so that the mmap
   * file can be used for diagnostics.
   *
   * At the start of Vdbench with data validation the temp directory is scanned
   * and any pid that no loner exists will be deleted.
   *
   * If the user is using his own journal= parameter then, because of the
   * validate=reuse option, these files no longer may be unique.
   */
  public static void cleanupOrphanMapFiles()
  {
    HashMap <String, String> pidmap = new HashMap(32);
    //pidmap.put("29771", "29771"); // Debugging, 'pretend'

    try
    {
      /* Create a HashMap with process ids: */
      if (common.onWindows())
      {
        OS_cmd    ocmd = OS_cmd.execute("tasklist /nh", false);
        String[] lines = ocmd.getStdout();
        for (String line : lines)
        {
          if (line.trim().length() == 0)
            continue;
          String[] split = line.trim().split(" +");
          pidmap.put(split[1], split[1]);
        }
      }

      /* Hoping that all other OS's have the same 'ps' syntax (hahahahah): */
      else
      {
        OS_cmd    ocmd = OS_cmd.execute("ps -eo pid", false);
        String[] lines = ocmd.getStdout();
        for (String line : lines)
        {
          if (line.trim().length() == 0)
            continue;
          pidmap.put(line.trim(), line.trim());  /* getting the PID header in there is OK */
        }
      }

      /* Now scan the temp file directory for mmap files to see if its pid is still there: */
      /* Example: vdbench.pid41540.sd2.mmap.0 */
      String temp     = Fput.getTmpDir();
      String[] fnames = new File(temp).list();
      for (String fname : fnames)
      {
        if (fname.startsWith("vdbench.pid") && fname.contains(".mmap"))
        {
          String[] split = fname.trim().split("\\.+");
          //common.ptod("fname: split: %d %s", split.length, new File(temp, fname).getAbsolutePath());

          if (split.length < 3)
            continue;

          String   pid   = split[1].substring(3);

          /* if the process is not there, delete the file: */
          if (pidmap.get(pid) == null)
          {
            if (!new File(temp, fname).delete())
              common.ptod("Unable to delete mmap file %s%s", temp, fname);
            else
              common.plog("Deleted orphan mmap file: %s%s", temp, fname);
          }
        }
      }
    }

    catch (Exception e)
    {
      common.ptod("Error cleaning up orphan mmap files. Continueing");
      common.ptod(e);
    }
  }


  /**
   * Reset all busy flags.
   * While we're at it, also count some stuff, saving us a trip later on.
   */
  public synchronized void setAllUnbusy()
  {
    counter = new MapCounts();

    Elapsed elapsed = new Elapsed("MapFile.setAllUnbusy", 250*1000*1000);

    for (long i = 0; i < getBytesInMap(); i+=1)
    {
      long block     = i;
      int  map       = (int) (block >> MapFile.BYTE_SHIFT);
      int  remainder = (int) (block  & MapFile.BYTE_AND);

      int key = get( remainder );
      if ( (key & 0x7f) == DV_map.DV_ERROR)
        counter.bad_blocks++;
      else if ( key != 0)
        counter.blocks_known++;

      if ((key & 0x80) != 0)
      {
        put( remainder, key & 0x7f);
        counter.blocks_busy++;
      }

      elapsed.track();


      //   long just8     = byte_maps[ map ].get8( remainder );
      //   for (int b = 0; b < 8; b++)
      //   {
      //     byte byt = (byte) (just8 >>> (56 - (b*8)));
      //     int  key = byt;
      //     if ( (key & 0x7f) == DV_ERROR)
      //       blocks_in_error++;
      //
      //     if ((key & 0x80) != 0)
      //       byte_maps[ map ].put( remainder + b, key & 0x7f);
      //
      //     elapsed.track();
      //   }

      //   for (int j = 0; j < bytes.length; j++)
      //   {
      //     int key = just8[j];
      //     if ( (key & 0x7f) == DV_ERROR)
      //       blocks_in_error++;
      //
      //     if ((key & 0x80) != 0)
      //       byte_maps[ map ].put( remainder, key & 0x7f);
      //
      //     elapsed.track();
      //   }

    }
    elapsed.end(5);
  }

}

class MapCounts
{
  long bad_blocks   = 0;
  long blocks_busy  = 0;
  long blocks_known = 0;
}


class MapUnbusyThread extends Thread
{
  private MapFile mf;
  public MapUnbusyThread(MapFile mf)
  {
    this.mf = mf;
  }

  public void run()
  {
    try
    {
     mf.setAllUnbusy();
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }
}
