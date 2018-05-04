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

import java.util.*;
import java.io.*;
import Utils.Fput;


/**
 * This class contains the Reporter task and related methods.
 */
public class Reporter extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public static Date interval_start_time;
  public static Date interval_end_time;

  private static long last_pause       = 0;
  private static int  warmup_intervals = 0;

  /**
   * Reporter task.
   *
   * The Reporter task not only does the reporting, it also is responsible
   * for the starting of each RD.
   * Once it has sent the new work to the slaves, it asks them every
   * 'interval' seconds for their latest set of statistics.
   *
   * These statistics then will be reported asynchronously once the last
   * slave returns the requested statistics.
   */
  public void run()
  {
    RD_entry rd;
    SdStats avg_stats;

    setName("Reporter");

    try
    {
      /* Reporter must run higher than the rest: */
      Thread.currentThread().setPriority( Thread.MAX_PRIORITY );

      /* See if user want's something extra done: */
      Debug_cmds.starting_command.run_command();


      RD_entry.next_rd = null;
      while (true)
      {
        if ((rd = RD_entry.getNextWorkload()) == null)
        {
          if (Vdbmain.loop_all_runs)
          {
            RD_entry.next_rd = null;
            if (Validate.isJournalRecovery())
              Validate.setJournalRecovered(false);
            continue;
          }
          break;
        }

        prepareNextRun(rd);

        int reporting_interval = 1;
        int intervals_needed   = (int) ((rd.getElapsed() + rd.getWarmup()) / rd.getInterval());

        /* Save the start time of this interval: */
        interval_start_time = interval_end_time = new Date();


        /* This is the 'interval for elapsed time' loop: */
        while (true)
        {
          /* If someone else told us to terminate the run, we can simply do it */
          /* now because we just came back from reporting the last interval:   */
          if (Vdbmain.isWorkloadDone())
            break;

          /* Terminate because of DV errors if needed: */
          DV_map.checkDVStatus();

          /* Wait until end of interval: */
          common.wait_interval(rd.getInterval());

          /* Run is complete if we did the last interval (not for format): */
          boolean last_call = (reporting_interval + 1 > intervals_needed ) &&
                              !rd.isThisFormatRun();
          if (Vdbmain.isWorkloadDone())
            last_call = true;

          /* Ask slaves for all statistics. Reporting will be done asynchronously: */
          interval_start_time = interval_end_time;
          interval_end_time = new Date();
          CollectSlaveStats css = new CollectSlaveStats(reporting_interval++,
                                                        rd.getInterval(), last_call);

          /* If this was the last interval, wait for statistics and break: */
          if (last_call)
          {
            css.waitForLast();
            break;
          }

          if (common.get_debug(common.PRINT_MEMORY))
          {
            System.gc();
            common.memory_usage();
            Native.printMemoryUsage();
            VdbCount.listCounters("Counters at end of interval");
          }
        }   /* while (true) waiting for interval count */


        /* This RD workload is now done: */
        Vdbmain.setWorkloadDone(true);
        SlaveList.sendWorkloadDone();
        SlaveList.waitForSlaveWorkCompletion();
        Blocked.printAndResetCounters();

        /* Report SD vs WG i/o counts: */
        if (Vdbmain.isWdWorkload()) // && rd.wgs_for_rd.size() > 0)
          HandleSkew.endOfRunSkewCheck(rd);
        //WG_entry.report_wd_iocount(rd);

        last_pause = rd.pause;

        Report.flushAllReports();

        common.memory_usage();
        System.gc();

        /* If we want 'recovery only', clean all existing RDs: */
        if (Validate.isRecoveryOnly())
        {
          common.ptod("");
          common.ptod("'journal=(recover,only)' requested. Terminating execution.");
          common.ptod("");
          break;
        }
      }

      /* See if user want's something extra done: */
      Debug_cmds.ending_command.run_command();
      SwatCharts.createCharts();


      common.plog("Ending Reporter");

    }
    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  private static void prepareNextRun(RD_entry rd)
  {
    /* During a run, AllWork() only knows about current run: */
    AllWork.clearWork();

    /* Issue pause request if needed: */
    if (last_pause != 0)
    {
      common.ptod("Waiting " + last_pause + " seconds; requested by 'pause' parameter");
      common.sleep_some(last_pause * 1000);
    }

    /* Final iorate is set as late as possible so that we can */
    /* pick up possible %% and/or curve rates:                */
    rd.set_iorate();

    /* Clear all initial statisics: */
    Blocked.resetCounters();
    initWarmupIntervals(rd);
    Report.setIntervalDuration((int) rd.getInterval());
    ReportData.clearAllTotalStats();

    /* Send information to all slaves: */
    Work.prepareWorkForSlaves(rd, true);
    SlaveList.sendWorkToSlaves(rd);
    SlaveList.printWorkForSlaves(rd);

    Vdbmain.setWorkloadDone(false);

    /* Wait for all slaves to be ready to start running: */
    common.plog("Waiting for synchronization of all slaves");
    SlaveList.waitForSlavesReadyToGo();
    common.plog("Synchronization of all slaves complete");

    /* Now that they are ready to go, lets wait until the next */
    /* rounded one second and then take off:                   */
    common.set_interval_start();
    rd.display_run();

    SlaveList.tellSlavesToGo();

  }

  /**
   * End of run reporting.
   * Report averages of all intervals but the first one.
   */
  public static void reportEndOfRun()
  {
    SdStats stats = null;
    SdStats run_totals = null;
    FwdStats fwd_totals = null;

    if (Vdbmain.isWdWorkload())
    {
      WdReport.reportWdTotalStats();
      run_totals = SdReport.reportSdTotalStats();
    }
    else
      fwd_totals = FwdReport.reportRunTotals();

    if (Report.isKstatReporting())
      Report.reportKstatTotals();

    Flat.printInterval();


    /* Set last observed rate: */
    if (Vdbmain.isWdWorkload())
    {
      Vdbmain.observed_iorate = Math.round(run_totals.rate());
      if (RD_entry.next_rd.doing_curve_max)
      {
        Vdbmain.last_curve_max = Vdbmain.observed_iorate;
        if (Vdbmain.last_curve_max == 0)
        {
          common.ptod("iorate=curve. No i/o rate observed. Was run too short?");
          common.ptod("Setting to 10,000 iops.");
          Vdbmain.last_curve_max = 10000;
        }
      }

      Report.writeFlat(run_totals, Report.getAvgLabel());
    }

    else
    {
      Vdbmain.observed_iorate = Math.round(fwd_totals.getTotalRate());
      if (RD_entry.next_rd.doing_curve_max)
        Vdbmain.last_curve_max = Vdbmain.observed_iorate;
    }

    /* If we're still alive with Data Validation errors, die now: */
    if (ErrorLog.getErrorCount() > 0)
    {
      if (Vdbmain.isFwdWorkload())
        Blocked.printAndResetCounters();

      common.ptod("*");
      common.ptod("Total Data Validation or I/O error count: " + ErrorLog.getErrorCount());
      common.ptod("*");
      common.failure("Vdbench terminating due to Data Validation or I/O errors. See errorlog.html.");
    }
  }


  private static void initWarmupIntervals(RD_entry rd)
  {
    warmup_intervals = (int) (rd.getWarmup() / rd.getInterval());

    if (warmup_intervals == 0)
      warmup_intervals = 1;
  }

  public static int getWarmupIntervals()
  {
    return warmup_intervals;
  }



  public static boolean needHeaders()
  {
    return Report.getInterval() % 30 == 1;
  }
}


