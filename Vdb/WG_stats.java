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

public class WG_stats
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  long   reads;
  long   writes;
  long   rbytes;
  long   wbytes;
  long   respmax;
  long   resptime;               /* Duration in jni around rw calls */
  long   resptime2;              /* Sum of squares of same          */
  long   read_errors;
  long   write_errors;



  /**
   * Get JNI statistics for each workload.
   * Delta statistics will be stored in wg.dlt_stats
   */
  public static void get_jni_statistics(boolean workload_done)
  {

    long tod = Native.get_simple_tod();
    WG_stats wg_stats = new WG_stats();
    SdStats  sd_stats = new SdStats();

    /* JNI statistics are maintained per workload.  */
    /* One workload is always only one SD           */
    for (int i = 0; i < SlaveWorker.work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) SlaveWorker.work.wgs_for_slave.elementAt(i);
      Native.get_one_set_statistics(wg_stats, i, workload_done);
      sd_stats.reads        = wg_stats.reads;
      sd_stats.writes       = wg_stats.writes;
      sd_stats.rbytes       = wg_stats.rbytes;
      sd_stats.wbytes       = wg_stats.wbytes;
      sd_stats.resptime     = wg_stats.resptime;
      sd_stats.resptime2    = wg_stats.resptime2;
      sd_stats.read_errors  = wg_stats.read_errors;
      sd_stats.write_errors = wg_stats.write_errors;
      sd_stats.respmax      = wg_stats.respmax;

      /* Create old_stats if needed: */
      if (wg.old_stats == null)
        wg.old_stats = new SdStats();

      /* Change those statistics to delta values: */
      sd_stats.last_ts = tod;
      wg.dlt_stats = new SdStats();
      wg.dlt_stats.stats_delta(sd_stats, wg.old_stats);
      wg.old_stats.stats_copy(sd_stats);

      /* Save the total wbytes count for tape: */
      wg.dlt_stats.wbytes_end_of_run = wg_stats.wbytes;
      //common.ptod("wg.dlt_stats.wbytes_end_of_run: " + wg.dlt_stats.wbytes_end_of_run);
    }

  }
}

