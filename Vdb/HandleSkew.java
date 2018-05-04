package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


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
      for (WG_entry wg : Host.getAllWorkloads())
      {
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
      for (WG_entry wg : Host.getAllWorkloads())
      {

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
   * Calculate skew and/or spread unrequested skew around if no skew defined.
   *
   * Note: the 100% error message below can also be the result of a
   * the same seek=seq workload being requested TWICE, with the second one,
   * obviously, not being used.
   */
  public static void calcLeftoverWgSkew(RD_entry rd)
  {
    double tot_skew = 0;
    double remainder;
    int    no_skews = 0;

    /* Go through all WG entries: */
    for (WG_entry wg : Host.getAllWorkloads())
    {
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
      for (WG_entry wg : Host.getAllWorkloads())
      {
        if (wg.skew == 0)
        {
          wg.skew        = remainder;
          tot_skew      += wg.skew;
        }
      }
    }

    /* Skew must be close to 100% (floating point allows just a little off): */
    if (Host.getAllWorkloads().size() != 0)
    {
      if ((tot_skew < 99.9999) || (tot_skew > 100.0001))
        common.failure("rd=%s Total skew must add up to 100: %.2f", rd.rd_name, tot_skew);
    }
  }
}


