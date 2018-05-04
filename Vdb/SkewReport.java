package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import Utils.Format;


/**
 * This class contains statistics maintained by vdbench code.
 */
public class SkewReport
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static int    label_length = 0;
  private static String type_mask    = null;
  private static boolean warning_done = false;


  private static void printRawHeaders(Object obj)
  {
    String[] headers = getRawHeaders(obj);
    ptodOrFile("");
    ptodOrFile(headers[0]);
    ptodOrFile(headers[1]);

    //PrintWriter pw = Report.getReport("skew").getWriter();
    //common.ptod("", pw);
    //common.ptod(headers[0], pw);
    //common.ptod(headers[1], pw);
  }

  private static void printRawHeadersExtra(Object obj, String extra1, String extra2)
  {
    String[] headers = getRawHeaders(obj);
    ptodOrFile("");
    ptodOrFile(headers[0] + " " + extra1);
    ptodOrFile(headers[1] + " " + extra2);
  }

  private static String[] getRawHeaders(Object obj)
  {
    if (!warning_done)
    {
      ptodOrFile("");
      ptodOrFile("Counts reported below are for non-warmup intervals.");
      //ptodOrFile("Note that for an Uncontrolled MAX run workload skew is irrelevant.");
      warning_done = true;
    }

    label_length = 0;
    label_length = Math.max(label_length, SD_entry.max_sd_name);
    label_length = Math.max(label_length, WD_entry.max_wd_name);
    label_length = Math.max(label_length, Slave.max_slave_name);
    label_length = Math.max(label_length, Host.max_host_name);
    String type = null;

    if (obj instanceof SD_entry)
      type = "SD:";
    else if (obj instanceof WD_entry)
      type = "WD:";
    else if (obj instanceof Slave)
      type = "Slave:";
    else if (obj instanceof Host)
      type = "Host:";
    else if (obj instanceof String)
      type = (String) obj;
    else
      common.failure("Unknown Object type: " + obj);

    type_mask = "%-" + label_length + "s ";

    String fmt1 = type_mask + "%10s %8s %7s %6s %8s %8s %8s %8s %8s %5s ";
    String hdr1 = String.format(fmt1,
                                "",
                                "i/o",       /* rate  10  */
                                "MB/sec",    /*        8  */
                                "bytes",     /* i/o    7  */
                                "read",      /* pct    6  */
                                "resp",      /* time   8  */
                                "read",      /* resp   8  */
                                "write",     /* resp   8  */
                                "resp",      /* max    8  */
                                "resp",      /* stddev 8  */
                                "queue");    /* depth  5  */

    String hdr2 = String.format(fmt1,
                                type,
                                /* i/o    */  "rate",
                                /* MB/sec */  "1024**2",
                                /* bytes  */  "i/o",
                                /* read   */  "pct",
                                /* resp   */  "time",
                                /* read   */  "resp",
                                /* write  */  "resp",
                                /* resp   */  "max",
                                /* resp   */  "stddev",
                                /* queue  */  "depth");

    String[] headers = new String[] { hdr1, hdr2};
    return headers;
  }

  private static void printFileHeaders(String label)
  {
    ptodOrFile("");
    ptodOrFile("%-12s %s", "",    FwdStats.getShortHeader1());
    ptodOrFile("%-12s %s", label, FwdStats.getShortHeader2());
  }

  public static void reportRaw(Object obj, SdStats st)
  {
    ptodOrFile(createRawLine(obj, st));
  }


  public static void reportExtra(Object obj, SdStats st, String format, Object ... args)
  {
    if (args.length == 0)
      ptodOrFile(createRawLine(obj, st) + " " + format);
    else
      ptodOrFile(createRawLine(obj, st) + " " + String.format(format, args));
  }

  private static String createRawLine(Object obj, SdStats st)
  {
    String label = null;

    if (obj instanceof SD_entry)
      label = ((SD_entry) obj).sd_name;
    else if (obj instanceof WD_entry)
      label = ((WD_entry) obj).wd_name;
    else if (obj instanceof Slave)
      label = ((Slave) obj).getLabel();
    else if (obj instanceof Host)
      label = ((Host) obj).getLabel();
    else if (obj instanceof String)
      label = (String) obj;
    else
      common.failure("Unknown Object type: " + obj);

    String fmt1 = type_mask + "%10.2f %8.2f %7d %6.2f %8.3f %8.3f %8.3f %8.3f %8.3f %5.1f ";
    String txt  = String.format(fmt1,
                                label,                      /*       */
                                st.rate(),                  /* 10.2f */
                                st.megabytes(),             /*  8.2f */
                                st.bytes(),                 /*    7d */
                                st.readpct(),               /*  6.2f */
                                st.respTime(),              /*  8.3f */
                                st.readResp(),              /*  8.3f */
                                st.writeResp(),             /*  8.3f */
                                st.respMax(),               /*  8.3f */
                                st.resptime_std(),          /*  8.3f */
                                st.qdepth());               /*  5.1f */
    return txt;
  }

  private static void printFileLine(Object obj, FwdStats fstat)
  {
    String label = null;

    if (obj instanceof FsdEntry)
      label = ((FsdEntry) obj).name;
    else if (obj instanceof WD_entry)
      label = ((FwdEntry) obj).fwd_name;
    else if (obj instanceof Slave)
      label = ((Slave) obj).getLabel();
    else if (obj instanceof Host)
      label = ((Host) obj).getLabel();
    else if (obj instanceof String)
      label = (String) obj;
    else
      common.failure("Unknown Object type: " + obj);

    /* I don't really need the 'avg' column. Remove it some day: */
    String avg  = Report.getAvgLabel();
    String line = fstat.printShortLine(null, null, avg);

    /* The line returned starts with the 'label'. Remove. */
    //common.ptod("line1: >>>%s<<<", line);
    //line = line.trim();
    //line = line.substring(line.indexOf(" "));
    //common.ptod("line2: >>>%s<<<", line);

    ptodOrFile("%-12s %s", label, line);
  }


  /**
   * Make sure that the requested workload skew ultimately has been honored.
   */
  public static void endOfRawRunSkewCheck(RD_entry rd)
  {
    long   total_io          = 0;
    double maxdeltapct       = 0;
    String maxdelta_workload = null;

    /* Add up all the i/o done: */
    for (WD_entry wd : rd.wds_for_rd)
      total_io += wd.total_io_done;

    if (rd.wds_for_rd.size() > 1)
    {
      ptodOrFile("");
      ptodOrFile("Calculated versus requested workload skew. "+
                 "(Delta only shown if > 0.10% absolute)");
      if (rd.areWeSharingThreads())
      {
        if (rd.current_override.threads_before_override == 0)
          ptodOrFile("Number of shared threads: %d", (int) rd.current_override.getThreads());
        else
          ptodOrFile("Shared thread count overridden from %d to %d",
                     (int) rd.current_override.threads_before_override,
                     (int) rd.current_override.getThreads());
      }

      ptodOrFile("Note that for an Uncontrolled MAX run workload skew is irrelevant.");
      if (!rd.use_waiter)
        ptodOrFile("This was an Uncontrolled MAX run.");
      String extra1 = String.format("%9s %8s %7s ", "skew",      "skew",     "skew");
      String extra2 = String.format("%9s %8s %7s ", "requested", "observed", "delta");
      printRawHeadersExtra(new WD_entry(), extra1, extra2);
    }

    /* Report ios and percentage: Remember, this is non-warmup i/o,           */
    /* so if during testing you're done within one second you'll get NOTHING! */
    int intervals = Report.getInterval() - Reporter.first_elapsed_interval + 1;
    String wd_mask = String.format("wd=%%-%ds ",  WD_entry.max_wd_name);
    double tot_skew_found = 0;
    if (total_io > 0)
    {
      for (int i = 0; i < rd.wds_for_rd.size(); i++)
      {
        WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);
        double skew_observed = (double) wd.total_io_done * 100.0 / total_io;
        double delta         = Math.abs((double) wd.total_io_done * 100 / total_io - wd.getSkew());

        tot_skew_found += wd.getSkew();

        if (rd.wds_for_rd.size() > 1)
        {
          SdStats stats = Report.getReport(wd.wd_name).getData().getTotalSdStats();
          double delta2 =  wd.getSkew() - skew_observed;
          reportExtra(wd, stats, "%8.2f%% %7.2f%% %s",
                      wd.getSkew(), skew_observed,
                      (Math.abs(delta2) < 0.1) ? "" : String.format("%6.2f%%", delta2));
        }

        if (delta > maxdeltapct)
        {
          maxdeltapct = delta;
          maxdelta_workload = wd.wd_name;
        }

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

      if (rd.wds_for_rd.size() > 1)
      {
        SdStats tot_stats = Report.getSummaryReport().getData().getTotalSdStats();
        reportExtra("Total", tot_stats, "%8.2f%% %7.2f%%", 100., tot_skew_found);
      }
    }

    /* Display a warning if skew failed WHEN using the Waiter Task: */
    if (!ReplayInfo.isReplay() && maxdeltapct > 1.0 && rd.use_waiter)
    {
      BoxPrint box = new BoxPrint();
      box.add("Observed Workload skew for wd=%s delta greater than 1%% (%.2f). " +
              "See skew.html for details", maxdelta_workload, maxdeltapct);
      box.printSumm();

      /* Give some reasons for the problem: */
      box.clear();
      box.add("Possible reasons for workload skew being out of sync:");
      box.add("");
      box.add("- Elapsed time too short.  ");
      box.add("  Each SD should have done at least 2000 i/o's. Adding 'warmup=nn' may help this.");
      box.add("  For SD concatenation this is the ONLY acceptable reason. "+
              "Contact me at the Oracle Vdbench Forum if the differences are too big.");
      box.add("");
      box.add("- A mix of seekpct=seq and random workloads. ");
      box.add("  Sequential workloads may run on only ONE slave, but random workloads");
      box.add("  may run on multiple slaves. Workload skew depends on ALL workloads");
      box.add("  running on all slaves. Force slave count to just one. (hd=default,jvms=1)");
      box.add("");
      box.add("- Not enough threads available to guarantee that all workloads can run");
      box.add("  on all slaves.");
      box.add("");
      box.add("- There may be more, can't think (of any) right now. :-)");
      box.add("");
      box.add("You may use 'abort_failed_skew=nn' for Vdbench to abort after skew failures. See documentation.");
      box.add("");
      box.print();
    }

    double limit = Validate.getSkewAbort();
    if (!ReplayInfo.isReplay() && maxdeltapct > limit)
    {
      common.failure("'abort_failed_skew=%.3f' parameter used. "+
                     "See file skew.html for more info.", limit);
    }

  }


  /**
   * Report all kinds of run totals.
   * Mainly for diagnostics so that I do not screw up again.....
   */
  public static void reportRawEndOfRunSkew(RD_entry rd)
  {
    try
    {

      /* Calculate total i/o done: */
      long total_io = 0;
      for (WD_entry wd : rd.wds_for_rd)
        total_io += wd.total_io_done;


      /* Print Slave level totals: */
      if (SlaveList.getSlaveCount() > 1)
      {
        for (Host host : Host.getDefinedHosts())
        {
          if (!host.anyWork())
            continue;

          printRawHeaders("Slave:");
          for (Slave slave : host.getSlaves())
            reportRaw(slave, slave.getSummaryReport().getData().getTotalSdStats());

          reportRaw("Total", host.getSummaryReport().getData().getTotalSdStats());
        }
      }


      /* Print Host level totals: */
      if (Host.getDefinedHosts().size() > 1)
      {
        if (CpuStats.isCpuReporting())
          printRawHeadersExtra("Host:", "  cpu", "sys+u");
        else
          printRawHeaders("Host:");

        for ( Host host : Host.getDefinedHosts())
        {
          if (!host.anyWork())
            continue;

          if (CpuStats.isCpuReporting())
            reportExtra(host, host.getSummaryReport().getData().getTotalSdStats(), "%5.1f",
                        host.getSummaryReport().getData().getTotalCpuStats().getBoth());
          else
            reportRaw(host, host.getSummaryReport().getData().getTotalSdStats());
        }

        if (CpuStats.isCpuReporting())
          reportExtra("Total", Report.getSummaryReport().getData().getTotalSdStats(),
                      "%5.1f", Report.getSummaryReport().getData().getTotalCpuStats().getBoth());
        else
          reportRaw("Total", Report.getSummaryReport().getData().getTotalSdStats());
      }


      /* Print SD level totals (it's OK to report unused SDs) */
      if (Report.sdDetailNeeded())
      {
        int real_sds = 0;
        for (SD_entry sd : Vdbmain.sd_list)
        {
          if (!sd.concatenated_sd)
            real_sds++;
        }
        if ( real_sds > 1)
        {
          printRawHeaders("SD:");
          for (SD_entry sd : Vdbmain.sd_list)
          {
            /* We don't have data based on concat SDs, so skip. We do report Real SDs: */
            if (sd.concatenated_sd)
              continue;

            if (sd.sd_is_referenced)
              reportRaw(sd,  Report.getReport(sd.sd_name).getData().getTotalSdStats());
          }

          reportRaw("Total", Report.getSummaryReport().getData().getTotalSdStats());
        }
      }
    }
    catch (Exception e)
    {
      common.ptod("Exception while doing skew reporting. Exception ignored, but please report it to me");
      common.ptod(e);
    }
  }



  public static void reportFileEndOfRunSkew(RD_entry rd)
  {
    try
    {
      /* Calculate operation counts: */
      long total_io = 0;
      for (FwdEntry fwd : rd.fwds_for_rd)
      {
        ReportData rs = Report.getReport(fwd.fwd_name).getData();
        total_io     += rs.getTotalFwdStats().getTotalRate();
      }



      /* Print Slave level totals: */
      if (SlaveList.getSlaveCount() > 1)
      {
        for (Host host : Host.getDefinedHosts())
        {
          if (!host.anyWork())
            continue;

          printFileHeaders("Slave:");
          for (Slave slave : host.getSlaves())
            printFileLine(slave, slave.getSummaryReport().getData().getTotalFwdStats());

          if (host.getSlaves().size() > 1)
            printFileLine("Total", host.getSummaryReport().getData().getTotalFwdStats());
        }
      }



      /* Print Host level totals: */
      printFileHeaders("Host:");
      for ( Host host : Host.getDefinedHosts())
      {
        if (!host.anyWork())
          continue;

        printFileLine(host, host.getSummaryReport().getData().getTotalFwdStats());
      }

      if (Host.getDefinedHosts().size() > 1)
        printFileLine("Total", Report.getSummaryReport().getData().getTotalFwdStats());



      /* Print FSD level totals (it's OK to report unused FSDs) */
      printFileHeaders("FSD:");
      for (FsdEntry fsd : FsdEntry.getFsdList())
      {
        printFileLine(fsd.name, Report.getReport(fsd.name).getData().getTotalFwdStats());
      }

      if (FsdEntry.getFsdList().size() > 1)
        printFileLine("Total", Report.getSummaryReport().getData().getTotalFwdStats());



      /* Print FWD level totals */
      if (!rd.fwds_for_rd.get(0).fwd_name.equals("format"))
      {
        printFileHeaders("FWD:");
        for (FwdEntry fwd : rd.fwds_for_rd)
          printFileLine(fwd.fwd_name, Report.getReport(fwd.fwd_name).getData().getTotalFwdStats());

        if (rd.fwds_for_rd.size() > 1)
          printFileLine("Total", Report.getSummaryReport().getData().getTotalFwdStats());
      }




      /* See how many workloads had no skew so that I can divvy up the remainder: */
      int    skew_yes     = 0;
      int    skew_no      = 0;
      double total_skew   = 0;
      double left_per_fwd = 0;
      for (FwdEntry fwd : rd.fwds_for_rd)
      {
        total_skew += fwd.skew;
        if (fwd.skew == 0)
          skew_no++;
        else
          skew_yes++;
      }
      left_per_fwd = (100 - total_skew) / skew_no;

      /* We do not report format, nor tests without skew: */
      if (skew_yes == 0 || rd.fwds_for_rd.get(0).fwd_name.equals("format"))
        return;


      /* Report the skew results: */
      if (rd.fwds_for_rd.size() > 1)
      {
        double max_delta = 0;
        FwdEntry max_fwd = null;

        ptodOrFile("");
        ptodOrFile("FWD skew requested vs. actual (Use 'abort_failed_skew=n.n' to abort when delta > n.n)");
        ptodOrFile("%-12s %7s %10s %7s %7s", "FWD", "ops/sec", "requested", "actual", "delta");
        for (FwdEntry fwd : rd.fwds_for_rd)
        {
          double fwd_ops   = Report.getReport(fwd.fwd_name).getData().getTotalFwdStats().getTotalRate();
          double requested = (fwd.skew != 0) ? fwd.skew : left_per_fwd;
          double actual    = fwd_ops * 100 / total_io;
          double delta     = actual- requested;
          if (Math.abs(delta) >= max_delta)
          {
            max_fwd = fwd;
            max_delta = Math.abs(delta);
          }
          ptodOrFile("%-12s %7.0f %9.2f%% %6.2f%% %+6.2f%%", fwd.fwd_name,
                     fwd_ops, requested, actual, delta);
        }

        if (max_delta > 1)
        {
          BoxPrint box = new BoxPrint();
          box.add("Observed Workload skew for fwd=%s delta greater than 1%% (%.2f). " +
                  "See skew.html for details", max_fwd.fwd_name, max_delta);
          box.printSumm();

          /* Give some reasons for the problem: */
          box.clear();
          box.add("Possible reasons for workload skew being out of sync:");
          box.add("");
          box.add("- Elapsed time too short.  ");
          box.add("  Each FSD should have done at least 2000 operations. Adding 'warmup=nn' may help this.");
          box.add("");
          box.add("- Not enough threads available to guarantee that all workloads can run on all slaves.");
          box.add("");
          box.add("- There may be more, can't think (of any) right now. :-)");
          box.add("  Contact me at the Oracle Vdbench Forum if the differences are out of line.");
          box.add("");
          box.add("You may use 'abort_failed_skew=nn' for Vdbench to abort after skew failures. See documentation.");
          box.add("");
          box.print();

          double limit = Validate.getSkewAbort();
          if (max_delta > limit)
          {
            common.failure("'abort_failed_skew=%.3f' parameter used. "+
                           "See file skew.html for more info.", limit);
          }
        }
      }
    }
    catch (Exception e)
    {
      common.ptod("Exception while doing skew reporting. Exception ignored, but please report it to me");
      common.ptod(e);
    }

  }



  public static void ptodOrFile(String format, Object ... args)
  {
    PrintWriter pw = Report.getReport("skew").getWriter();
    String txt = (args.length == 0) ? format : String.format(format, args);
    if (common.get_debug(common.SKEW_ON_CONSOLE))
    {
      common.ptod(txt);
      common.ptod(txt, pw);
    }
    else
      common.ptod(txt, pw);
  }
}
