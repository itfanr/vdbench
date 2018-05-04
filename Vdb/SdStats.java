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
import Utils.Format;

/**
 * This class contains statistics maintained by vdbench code.
 */
public class SdStats extends VdbObject implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String sd_name;
  int    wg_number_for_slave;
  long   last_ts;
  long   reads;
  long   writes;
  long   rbytes;
  long   wbytes;
  long   respmax;
  long   resptime;               /* Duration in jni around rw calls           */
  long   resptime2;              /* Sum of squares of same                    */
  long   val_count;
  long   val_error;
  long   read_errors;
  long   write_errors;
  long   elapsed;

  long   wbytes_end_of_run;    /* Used by a possible following tape read run  */
                               /* to know how many bytes were written to make */
                               /* sure we don't read beyond that.             */
                               /* This avoids EOT issues.                     */
                               /* We never calculate a delta for this.        */
  private static int negative_response_count = 0;


  public SdStats()
  {
    //common.where(8);
  }

  void stats_accum(SdStats in, boolean add_elapsed)
  {
    last_ts        =  in.last_ts;
    reads          += in.reads;
    writes         += in.writes;
    rbytes         += in.rbytes;
    wbytes         += in.wbytes;
    resptime       += in.resptime;
    resptime2      += in.resptime2;
    val_count      += in.val_count;
    val_error      += in.val_error;
    read_errors    += in.read_errors;
    write_errors   += in.write_errors;

    if (in.respmax > respmax)
      respmax = in.respmax;

    if (add_elapsed)
      elapsed    += in.elapsed ;
    else if (in.elapsed != 0)
      elapsed     = in.elapsed ;
  }

  void stats_copy(SdStats in)
  {
    sd_name        = in.sd_name;
    last_ts        = in.last_ts;
    elapsed        = in.elapsed;
    reads          = in.reads;
    writes         = in.writes;
    rbytes         = in.rbytes;
    wbytes         = in.wbytes;
    resptime       = in.resptime;
    resptime2      = in.resptime2;
    val_count      = in.val_count;
    val_error      = in.val_error;
    read_errors    = in.read_errors;
    write_errors   = in.write_errors;
    respmax        = 0;
    in.respmax     = 0;
  }

  void stats_delta(SdStats nw, SdStats old)
  {
    elapsed        = nw.last_ts        - old.last_ts;
    reads          = nw.reads          - old.reads;
    writes         = nw.writes         - old.writes;
    rbytes         = nw.rbytes         - old.rbytes;
    wbytes         = nw.wbytes         - old.wbytes;
    resptime       = nw.resptime       - old.resptime;
    resptime2      = nw.resptime2      - old.resptime2;
    val_count      = nw.val_count      - old.val_count;
    val_error      = nw.val_error      - old.val_error;
    read_errors    = nw.read_errors    - old.read_errors;
    write_errors   = nw.write_errors   - old.write_errors;
    respmax        = nw.respmax;

    if (resptime < 0)
    {
      if (negative_response_count == 0)
      {
        common.ptod("Negative response time. Usually caused by out of sync CPU clocks .");
        common.ptod("Will be reported a maximum of 100 times and then Vdbench will continue.");
      }

      if (negative_response_count++ < 100)
        common.ptod("resptime (microseconds): " + resptime + " " + old.resptime + " " + nw.resptime);
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
    long tbytes = rbytes + wbytes;
    return(elapsed > 0) ? (double) tbytes / (elapsed / 1000000.0) / Report.MB : 0;
  }

  public long bytes()
  {
    long tbytes = rbytes + wbytes;
    return(reads + writes > 0) ? (tbytes / (reads + writes)) : 0;
  }

  public double readpct()
  {
    return(reads + writes > 0) ? (double) reads / (reads + writes) * 100 : 0;
  }

  public double respTime()
  {
    double resp = (reads + writes > 0) ? (double) resptime / (reads + writes) / 1000 : 0;
    if (resp < 0)
    {
      common.where(8);
      common.ptod("resptime: " + resptime + " " + reads + " " + writes);
    }

    return resp;
  }

  public double respmax()
  {
    return(double) respmax / 1000;
  }

  public double resptime_std()
  {
    double total = reads + writes;

    if (total <= 1 || resptime == 0 || resptime2 == 0)
      return 0;

    return Math.sqrt( ( (total  * (double) resptime2) -
                        ( (double) resptime * (double) resptime) ) /
                      (total * (total - 1) ) ) / 1000.0 ;
  }

  public String toString()
  {
    String txt = "SdStats:";

    txt += Format.f(" sd: %-8s", (sd_name == null) ? "null" : sd_name);

    txt += Format.f(" rd: %4d",   reads  );
    txt += Format.f(" wrt: %4d",  writes );
    txt += Format.f(" rb: %8d",  rbytes );
    txt += Format.f(" wb: %8d",  wbytes );
    txt += Format.f(" el: %8d", elapsed);

    return txt;
  }
}
