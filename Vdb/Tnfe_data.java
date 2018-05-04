package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import java.text.*;
import Utils.Fput;

/**
 * This class handles all data manipulation related to data transferred from
 * vdbench to TNFE (now Swat).
 *
 * It makes most sense to also convert this format from ascii to the new Bin
 * file format, but at this time that is just too much work. 3/14/06
 */
class Tnfe_data
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private static Fput fp_interval =  null;
  private static Fput fp_total    =  null;

  static DateFormat printdf = new SimpleDateFormat("MMddyyyy-HH:mm:ss.000" );


  /**
   * Create Swat file and store fixed configuration data
   */
  private static void openSwatFiles(Date ts)
  {
    fp_interval = new Fput(Vdbmain.output_dir, "swat_mon.txt");
    fp_total    = new Fput(Vdbmain.output_dir, "swat_mon_total.txt");

    fp_interval.println(":date " + printdf.format(ts));
    fp_interval.println(":fixed_data");
    fp_total.println(":date " + printdf.format(ts));
    fp_total.println(":fixed_data");

    /* Report only REAL SDs: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (!sd.concatenated_sd)
      {
        fp_interval.println(i + " " + i + " 0 0 " + "sd_" + sd.sd_name);
        fp_total.println(i + " " + i + " 0 0 " + "sd_" + sd.sd_name);
      }
    }
  }


  /**
   * Write header to start a new interval
   */
  public static void writeIntervalHeader(boolean total, Date begin_ts, Date end_ts)
  {
    /* If file not open yet, do so: */
    if (fp_interval == null)
      openSwatFiles(begin_ts);

    Fput fp = (total) ? fp_total : fp_interval;

    /* Write performance data for this interval: */
    fp.println(":");
    fp.println(":vdbench503_vdbench_data_for_swat303");
    fp.println(":date " + printdf.format(begin_ts) + " " + printdf.format(end_ts));
  }


  /**
   * Write data for one SD
   */
  public static void writeOneSd(boolean total, String sdname, SdStats stats)
  {
    /* Find relative position in the SD list: */
    int index = Vdbmain.sd_list.indexOf(SD_entry.findSD(sdname));

    long max  = Math.max(stats.r_max, stats.w_max);
    long resp = stats.r_resptime + stats.w_resptime;

    Fput fp = (total) ? fp_total : fp_interval;

    fp.println(index +                         //  0  index
               " " + "0" +                     //  1  this_second
               " " + stats.reads +             //  2  kio.reads
               " " + stats.writes +            //  3  kio.writes
               " " + resp        +             //  4  kio.rlentime
               " " + "0" +                     //  5  kio.rtime
               " " + stats.r_bytes / 512 +     //  6  kio.nread
               " " + stats.w_bytes / 512 +     //  7  kio.nwritten
               " " +         max        +      //  8  kio.resp_max
               " " + stats.r_max        +      //  9  kio.resp_max_r
               " " + stats.w_max        +      // 10  kio.resp_max_w
               " " + "0" +                     // 11  kio.q_max
               " " + "0" +                     // 12  kio.q_max_r
               " " + "0");                     // 13  kio.q_max_w
  }


  public static void close()
  {
    if (fp_interval != null)
    {
      fp_interval.close();
      fp_total.close();
      fp_interval = null;
      fp_total = null;
    }
  }

  public static void flush()
  {
    /* Removed to avoid reporting delays. */
    //if (fp_interval != null)
    //{
    //  fp_interval.flush();
    //  fp_total.flush();
    //}
  }
}

