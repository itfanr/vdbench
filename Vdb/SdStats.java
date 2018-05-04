package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Vector;

import Utils.Format;

/**
 * This class contains statistics maintained by vdbench code.
 */
public class SdStats implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  String sd_name;
  String wd_name;
  long   last_ts;
  long   elapsed;

  long   reads;
  long   r_resptime;             /* Duration in jni around rw calls           */
  long   r_resptime2;            /* Sum of squares of same                    */
  long   r_max;
  long   r_bytes;
  long   r_errors;

  long   writes;
  long   w_resptime;             /* Duration in jni around rw calls           */
  long   w_resptime2;            /* Sum of squares of same                    */
  long   w_max;
  long   w_bytes;
  long   w_errors;

  long   val_count;
  long   val_error;

  long   rtime;                  /* cumulative run (service) time             */
  long   rlentime;               /* cumulative run length*time product        */

  Histogram histogram  = new Histogram("default");
  Histogram read_hist  = new Histogram("default");
  Histogram write_hist = new Histogram("default");


  private static int negative_response_count = 0;

  /**
   * We really only want histograms at the end.
   * By clearing the by default allocated histgrams we prevent the info to be
   * send from the slave to the master
   */
  public void clearHistograms()
  {
    histogram  = null;
    read_hist  = null;
    write_hist = null;
  }

  void stats_accum(SdStats in, boolean add_elapsed)
  {
    last_ts     =  in.last_ts;

    reads       += in.reads;
    r_resptime  += in.r_resptime;
    r_resptime2 += in.r_resptime2;
    r_max        = Math.max(r_max, in.r_max);
    r_bytes     += in.r_bytes;
    r_errors    += in.r_errors;

    writes      += in.writes;
    w_resptime  += in.w_resptime;
    w_resptime2 += in.w_resptime2;
    w_max        = Math.max(w_max, in.w_max);
    w_bytes     += in.w_bytes;
    w_errors    += in.w_errors;

    val_count   += in.val_count;
    val_error   += in.val_error;
    rlentime    += in.rlentime;

    if (add_elapsed)
    {
      elapsed    += in.elapsed ;
      rtime      += in.rtime;
    }
    else if (in.elapsed != 0)
    {
      elapsed     = in.elapsed ;
      rtime       = in.rtime;
    }

    if (histogram != null)
    {
      histogram  .accumBuckets(in.histogram);
      read_hist  .accumBuckets(in.read_hist);
      write_hist .accumBuckets(in.write_hist);
    }
  }


  void stats_copy(SdStats in)
  {
    sd_name     = in.sd_name;
    last_ts     = in.last_ts;
    elapsed     = in.elapsed;

    reads       = in.reads;
    r_resptime  = in.r_resptime;
    r_resptime2 = in.r_resptime2;
    r_max       = in.r_max = 0;
    r_bytes     = in.r_bytes;
    r_errors    = in.r_errors;

    writes      = in.writes;
    w_resptime  = in.w_resptime;
    w_resptime2 = in.w_resptime2;
    w_max       = in.w_max = 0;
    w_bytes     = in.w_bytes;
    w_errors    = in.w_errors;

    val_count   = in.val_count;
    val_error   = in.val_error;

    rtime       = in.rtime;
    rlentime    = in.rlentime;

    if (in.histogram != null)
    {
      histogram   = (Histogram) in.histogram.clone();
      read_hist   = (Histogram) in.read_hist.clone();
      write_hist  = (Histogram) in.write_hist.clone();
    }
  }


  void stats_delta(SdStats nw, SdStats old)
  {
    elapsed     = nw.last_ts     - old.last_ts;

    reads       = nw.reads       - old.reads;
    r_resptime  = nw.r_resptime  - old.r_resptime;
    r_resptime2 = nw.r_resptime2 - old.r_resptime2;
    r_max       = nw.r_max;
    r_bytes     = nw.r_bytes     - old.r_bytes;
    r_errors    = nw.r_errors    - old.r_errors;

    writes      = nw.writes      - old.writes;
    w_resptime  = nw.w_resptime  - old.w_resptime;
    w_resptime2 = nw.w_resptime2 - old.w_resptime2;
    w_max       = nw.w_max;
    w_bytes     = nw.w_bytes     - old.w_bytes;
    w_errors    = nw.w_errors    - old.w_errors;

    val_count   = nw.val_count   - old.val_count;
    val_error   = nw.val_error   - old.val_error;

    rtime       = nw.rtime       - old.rtime;
    rlentime    = nw.rlentime    - old.rlentime;

    if (histogram != null && old.histogram != null && nw.histogram != null)
    {
      histogram  .deltaBuckets(nw.histogram,  old.histogram);
      read_hist  .deltaBuckets(nw.read_hist,  old.read_hist);
      write_hist .deltaBuckets(nw.write_hist, old.write_hist);
    }

    if (r_resptime < 0)
    {
      if (negative_response_count == 0)
      {
        common.ptod("Negative response time. Usually caused by out of sync CPU clocks .");
        common.ptod("Will be reported a maximum of 100 times and then Vdbench will continue.");
      }

      if (negative_response_count++ < 100)
        common.ptod("r_resptime (microseconds): " + r_resptime + " " + old.r_resptime + " " + nw.r_resptime);
    }

    if (w_resptime < 0)
    {
      if (negative_response_count == 0)
      {
        common.ptod("Negative response time. Usually caused by out of sync CPU clocks .");
        common.ptod("Will be reported a maximum of 100 times and then Vdbench will continue.");
      }

      if (negative_response_count++ < 100)
        common.ptod("w_resptime (microseconds): " + w_resptime + " " + old.w_resptime + " " + nw.w_resptime);
    }
  }

  /**
   * Increment count of number of data blocks validated
   */
  synchronized void add_validation_count()
  {
    val_count ++;
  }


  /**
   * Increment count of number of validation errors
   */
  synchronized void add_validation_error()
  {
    val_error ++;
  }


  public double rate()
  {
    return(elapsed > 0) ? (double) (reads + writes) * 1000000 / elapsed : 0;
  }

  public double megabytes()
  {
    long tbytes = r_bytes + w_bytes;
    return(elapsed > 0) ? (double) tbytes / (elapsed / 1000000.0) / Report.MB : 0;
  }

  public long bytes()
  {
    long tbytes = r_bytes + w_bytes;
    return(reads + writes > 0) ? (tbytes / (reads + writes)) : 0;
  }

  public double readpct()
  {
    return(reads + writes > 0) ? (double) reads / (reads + writes) * 100 : 0;
  }

  public double readResp()
  {
    double ret = (reads > 0) ? (double) r_resptime / reads / 1000 : 0;
    if (ret < 0)
    {
      common.where(8);
      common.ptod("readResp(): " + r_resptime + " " + reads);
    }

    return ret;
  }

  public double writeResp()
  {
    double ret = (writes > 0) ? (double) w_resptime / writes / 1000 : 0;
    if (ret < 0)
    {
      common.where(8);
      common.ptod("writeResp(): " + w_resptime + " " + writes);
    }

    return ret;
  }

  public double respTime()
  {
    long resp = r_resptime + w_resptime;
    double ret = (reads + writes > 0) ? (double) resp / (reads + writes) / 1000 : 0;
    if (ret < 0)
    {
      common.where(8);
      common.ptod("respTime(): " + r_resptime + " " + w_resptime + " " + reads + " " + writes);
    }

    return ret;
  }

  public double respMax()
  {
    long max = Math.max(r_max, w_max);
    return(double) max / 1000;
  }

  public double resptime_std()
  {
    double total = reads + writes;

    if (total <= 1 || (r_resptime + w_resptime) == 0)
      return 0;

    long resptime  = r_resptime  + w_resptime;
    long resptime2 = r_resptime2 + w_resptime2;

    return Math.sqrt( ( (total  * (double) resptime2) -
                        ( (double) resptime * (double) resptime) ) /
                      (total * (total - 1) ) ) / 1000.0 ;
  }

  public double busypct()
  {
    double busy = (elapsed == 0) ? 0 : (double) rtime * 100 / elapsed;
    //common.ptod("rtime: " + elapsed + " " + rtime + " " + busy);
    return busy;
  }
  public double qdepth()
  {
    double dbl = (elapsed == 0) ? 0 : (double) rlentime / elapsed;

    //common.ptod("dbl: " + (long) dbl + " " + (long) rlentime + " el: " + elapsed );

    return dbl;
  }

  public String toString()
  {
    String txt = "SdStats:";

    txt += Format.f(" sd: %-8s", (sd_name == null) ? "null" : sd_name);

    txt += Format.f(" rd: %4d",  reads   );
    txt += Format.f(" wrt: %4d", writes  );
    txt += Format.f(" rb: %8d",  r_bytes );
    txt += Format.f(" wb: %8d",  w_bytes );
    txt += Format.f(" el: %8d",  elapsed );

    return txt;
  }

  public Vector printHistograms()
  {
    Vector lines = new Vector(64);

    if (histogram.getTotals() == 0)
      lines.add("No statistics generated for this run.");
    else
    {
      if (histogram.getTotals() == read_hist.getTotals())
        lines.addAll(histogram.printHistogram("Only reads done for this workload:"));
      else if (histogram.getTotals() == write_hist.getTotals())
        lines.addAll(histogram.printHistogram("Only writes done for this workload:"));
      else
      {
        lines.addAll(histogram.printHistogram("Reads and writes:"));
        lines.addAll(read_hist.printHistogram("Reads:"));
        lines.addAll(write_hist.printHistogram("Writes:"));
      }
    }
    return lines;
  }

  public static void main(String[] args)
  {
    long value = Long.parseLong(args[0]);
    DecimalFormat df = new DecimalFormat("#,###");
    common.ptod("xx: " + df.format(value));
  }
}
