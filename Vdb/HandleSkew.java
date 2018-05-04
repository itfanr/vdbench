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

import Utils.Format;


/**
 * This class contains statistics maintained by vdbench code.
 */
public class HandleSkew
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



  /**
   * Report possible skew violations at the end of a run.
   *
   * When a skew is requested for a workload it should match that requested skew.
   * If not, the difference must be reported.
   *
   * When an uncontrolled curve run is done the skew that must be used in
   * the next curve runs will be determined here.
   */
  public static void endOfRunSkewCheck(RD_entry rd)
  {
    long   total = 0;
    double maxdeltapct = 0;

    /* Add up all the i/o done: */
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);
      total += wd.total_io_done;
    }

    /* Report ios and percentage: */
    int intervals = Report.getInterval() - Reporter.getWarmupIntervals();
    common.plog("");
    common.plog("Counts reported are for non-warmup intervals (%d).",
                intervals);

    common.plog(Format.f("I/O count for all WDs: %d", total));
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);
      if (total > 0)
      {
        double skew_observed = (double) wd.total_io_done * 100.0 / total;
        common.plog(Format.f("Calculated skew for WD %-12s: ", wd.wd_name) +
                    Format.f("%12d ", wd.total_io_done) +
                    Format.f("%12d/sec ", wd.total_io_done / intervals) +
                    Format.f(" (%6.2f%%)", skew_observed) +
                    Format.f(" Expected skew: %6.2f%%", wd.getSkew()));
        maxdeltapct = Math.max(maxdeltapct,
                               (Math.abs((double) wd.total_io_done * 100 / total - wd.getSkew())));

        /* At the end of an uncontrolled curve run, remember the skew: */
        if (rd.doing_curve_max)
        {
          wd.valid_skew_obs = false;
          if (!rd.use_waiter)
          {
            wd.valid_skew_obs = true;
            wd.skew_observed  = skew_observed;
          }
        }
      }
    }

    if (!Vdbmain.isReplay() && maxdeltapct > 1.0)
    {
      common.ptod("");
      common.ptod("Observed Workload skew difference greater than 1% (" +
                  Format.f("%.2f", maxdeltapct) + "). See logfile.html for details");
      common.ptod("");
    }
  }




  /**
   * Calculate skew and/or spread unrequested skew around if no skew defined.
   */
  public static void spreadWdSkew(RD_entry rd)
  {
    double tot_skew = 0;
    double remainder;
    int    no_skews = 0;

    if (rd.wds_for_rd.size() == 0)
      common.failure("No wds_for_rd for rd=" + rd.rd_name);

    /* Go through all used WDs: */
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);

      /* Use either the skew observed from 'iorate=curve' or use what's been requested: */
      if (rd.doing_curve_point && wd.valid_skew_obs)
        wd.skew = wd.skew_observed;
      else
        wd.skew = wd.skew_original;

      /* For those who have skew, determine the total skew: */
      if (wd.wd_iorate == 0 && wd.skew != 0)
        tot_skew += wd.skew;
      else if (wd.wd_iorate == 0)
        no_skews++;
    }

    /* If any WDs did not specify skew, spread the remainder around: */
    if (no_skews > 0)
    {
      remainder = (100.0 - tot_skew) / no_skews;
      for (int i = 0; i < rd.wds_for_rd.size(); i++)
      {
        WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);

        if (wd.wd_iorate == 0 && wd.skew == 0)
        {
          wd.skew   = remainder;
          tot_skew += wd.skew;
        }
      }
    }

    /* If all WDs specify an iorate, no need to check skew: */
    int no_iorates = 0;
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);
      if (wd.wd_iorate == 0)
        no_iorates++;
    }

    /* Skew must be close to 100% (floating point allows just a little off): */
    if (no_iorates > 0)
    {
      if ((tot_skew < 99.9999) || (tot_skew > 100.0001))
        common.failure("rd=%s: Total skew must add up to 100: %.2f", rd.rd_name, tot_skew);
    }
  }



  /**
   * Spread out the skew requested in the WDs.
   * Since the requested WD can be spread out over multiple slaves, each slave
   * needs to get a portion of the requested skew.
   */
  public static void spreadWgSkew(RD_entry rd)
  {
    /* Count how often each WD is used in this run: */
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);

      int slaves_running_this_wd = 0;
      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);
        if (wg.wd_used == wd)
          slaves_running_this_wd++;
      }

      if (slaves_running_this_wd == 999990)
      {
        common.ptod("");
        common.ptod("Unexpected zero slave count for rd=" + rd.rd_name + ",wd=" + wd.wd_name);
        common.ptod("Could this have been the result of multiple sequential workloads " +
                    "requested against the same SD_entry for multiple target hosts?");
        common.failure("Unexpected zero slave count for rd=" + rd.rd_name + ",wd=" + wd.wd_name);
      }


      /* Split up the originaly defined skew: */
      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);

        if (wg.wd_used == wd)
        {
          wg.skew = wd.skew / slaves_running_this_wd;
          if (wg.wg_iorate != 0)
          {
            //common.ptod("wg.wg_iorate: " + wg.wg_iorate + " " + slaves_running_this_wd);
            wg.wg_iorate = wd.wd_iorate / slaves_running_this_wd;
          }
        }

      }
    }
  }




  /**
   * Calculate skew and/or spread unrequested skew around if no skew defined
   */
  public static void calcLeftoverWgSkew(RD_entry rd)
  {
    double tot_skew = 0;
    double remainder;
    int    no_skews = 0;

    /* Go through all WG entries: */
    for (int i = 0; i < rd.wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(i);

      /* For those who have skew, determine the total skew: */
      if (wg.skew != 0)
        tot_skew += wg.skew;
      else
        no_skews++;
    }

    /* If any WDs did not specify skew, spread the remainder around: */
    if (no_skews > 0)
    {
      remainder = (100.0 - tot_skew) / no_skews;
      for (int i = 0; i < rd.wgs_for_rd.size(); i++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(i);

        if (wg.skew == 0)
        {
          wg.skew        = remainder;
          tot_skew      += wg.skew;
        }
      }
    }

    /* Skew must be close to 100% (floating point allows just a little off): */
    if (rd.wgs_for_rd.size() != 0)
    {
      if ((tot_skew < 99.9999) || (tot_skew > 100.0001))
        common.failure("Total skew must add up to 100: " + tot_skew);
    }
  }
}


