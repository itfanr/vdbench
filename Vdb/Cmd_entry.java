package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.Comparator;

import Utils.Format;

/**
 * This class contains all information needed to start and complete an i/o
 * request.
 *
 */
public class Cmd_entry
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public long     delta_tod;   /* Relative tod that io needs to start         */
                               /* WT makes this clock tod to start            */
  public long     cmd_lba;     /* lba within SD where i/o goes                */
  public long     cmd_xfersize;/* Number of bytes for read/write              */

  public SD_entry sd_ptr;      /* Instance of SD used for this io             */
  public SD_entry concat_sd;
  public long     concat_lba;

  public WG_entry cmd_wg;      /* WG_entry that is requesting this cmd        */
  public int      jni_index;   /* Where to accumulate statistics in JNI       */

  public boolean  cmd_read_flag; /* True == read                              */
  public boolean  cmd_hit;       /* Request must be a cache hitarea hit       */
  public boolean  cmd_rand;      /* This is a random io                       */

  public int      type_of_dv_read;   /* identifying read/pre-read/read_immed      */

  private static boolean preallocated = false;


  /**
   * Allocate enough space to avoid running out of memory too fast when
   * allocation Cmd_entries.
   * Used to have a preallocated pool of Cmd_entries, but using new was more efficient.
   * Now making sure that there is 128MB worth of free space available so
   * that GC does not get called too often.
   */
  public static void cmd_create_pool()
  {
    int MB        = 1024 * 1024;
    int memory    = 0;
    int WANTED_MB = 128;
    int STEP      = 16;

    if (!preallocated)
    {
      for (memory = WANTED_MB; memory > 1; memory -= STEP)
      {
        try
        {
          //common.ptod("trying to allocate: " + memory);
          byte temp[] = new byte[memory * MB];
          break;
        }

        catch (Throwable t)
        {
          String bad_class = t.getClass().getName();

          if (bad_class.compareTo("java.lang.OutOfMemoryError") != 0)
            common.failure("Unknown failure during cmd_create_pool()");
        }
      }
      /*
      long total = Runtime.getRuntime().totalMemory() / MB;
      if (total < 256 - 5)
      {
        common.ptod("It is recommended to run with a minimum Java heap size of 256m");
        common.ptod("Run with 'java -Xmx256m -Xms256m' (minimum) " + total);
      }

      common.plog(Format.f("Preallocated %dk bytes for Cmd_entries", memory));
      */
    }

    preallocated = true;
  }



  /**
   * Note: for sequential the real next LBA is determined in JNI code.
   */
  public void cmd_print(String txt)
  {
    long delta = Native.get_simple_tod() - SlaveWorker.first_tod;
    delta /= 1000000;
    delta = delta - delta_tod / 1000000;

    //common.ptod("%-16s %-5s %-5s lba: %9.1fm %10d xf=%7.1fk dlta: %12.6f %s %s conc: %6d ",
    common.ptod("%-16s %-5s %-5s lba: 0x%08x %10d xf=%7.1fk dlta: %12.6f %s %s conc: %6d ",
                txt,
                cmd_read_flag ? "read" : "write",
                cmd_rand ? "rand" : "seq",
                cmd_lba,
                //(double) cmd_lba / 1048576.,
                0, //  cmd_lba,
                //(cmd_xfersize == 0) ? 0 : (((double) cmd_lba) / cmd_xfersize),
                (double) cmd_xfersize / 1024,
                (double) (delta_tod ) / 1000000,
                //(double) (delta_tod - SlaveWorker.first_tod) / 1000000,
                sd_ptr.sd_name,
                cmd_wg.wd_name,
                delta); // concat_lba / 1048576);

    //common.ptod("%-16s %5s %5s block: %8.1f xf=%7d dlta: %12.6f %s %s",
    //            txt,
    //            cmd_read_flag ? "read" : "write",
    //            cmd_rand ? "rand" : "seq",
    //            (cmd_xfersize == 0) ? 0 : (((double) cmd_lba) / cmd_xfersize),
    //            cmd_xfersize,
    //            (double) (delta_tod - SlaveWorker.first_tod) / 1000000,
    //            sd.sd_name,
    //            cmd_wg.wd_name);
  }


  public void cmd_print2(SD_entry sd, String txt)
  {
    String rw = this.cmd_read_flag ? " read  " : " write ";

    long xfer = (cmd_xfersize == 0) ? 0 : (cmd_lba / cmd_xfersize);
    long now  = Native.get_simple_tod();
    common.ptod("%-16s %s rand: %5b block: %6d xfer: %6d dlta: %10.4f %s late: %6d %.6f %.6f " + Thread.currentThread(),
                txt, rw, cmd_rand, xfer, cmd_xfersize,
                (double) (delta_tod - SlaveWorker.first_tod) / 1000000,
                sd.sd_name, (now - delta_tod), delta_tod / 1000000., now / 1000000.);
  }


  static double GB = 1024. * 1024. * 1024.;
  public String toString()
  {
    return
    sd_ptr.sd_name +
    Format.f(" %10.6f", (double) delta_tod / 1000000.) +
    Format.f(" %10.3fg", (double) cmd_lba / GB);
  }
}


class DeltaCompare implements Comparator
{
  public int compare(Object o1, Object o2)
  {
    Cmd_entry cmd1 = (Cmd_entry) o1;
    Cmd_entry cmd2 = (Cmd_entry) o2;

    long rc = cmd1.delta_tod - cmd2.delta_tod;
    if (rc == 0)
      return 0;
    if (rc < 0)
      return -1;
    else
      return +1;
  }
}





