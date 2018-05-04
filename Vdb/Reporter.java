package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.ClassPath;
import Utils.Fget;
import Utils.Fput;


/**
 * This class contains the Reporter task and related methods.
 */
public class Reporter extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public static Date interval_start_time;
  public static Date interval_end_time;
  public static Date run_start_time;
  public static Date run_end_time;

  private static long    last_pause       = 0;
  private static int     warmup_intervals = 0;
  private static boolean warmup_done      = false;
  public  static int     first_elapsed_interval = 1;

  public  static String  monitor_file  = null;
  public  static boolean monitor_final = false;


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
    long first_start_tod = System.currentTimeMillis();

    setName("Reporter");

    clearShutdownFile(true);

    try
    {
      /* Reporter must run higher than the rest: */
      Thread.currentThread().setPriority( Thread.MAX_PRIORITY );

      /* See if user want's something extra done: */
      Debug_cmds.starting_command.run_command();

      /* If we OS goes down before the first run is complete we won't have much: */
      Report.flushAllReports();

      RD_entry.next_rd = null;
      MiscParms.printLoopStart();
      while (true)
      {
        if (monitor_final)
          break;

        if ((rd = RD_entry.getNextWorkload()) == null)
        {
          if (!MiscParms.shutDownAfterLoops(first_start_tod))
          {
            RD_entry.next_rd = null;
            if (Validate.isJournalRecovery())
              Validate.setJournalRecovered(false);

            MiscParms.printLoopStart();
            continue;
          }
          break;
        }

        /* Once more into the breach: */
        if (Vdbmain.isWdWorkload())
        {
          RD_entry.createWgListForOneRd(rd, true);
          rd.finalizeWgEntry();
        }

        /* Execute 'startcmd' on master if requested: */
        if (rd.start_cmd != null && rd.start_cmd.masterOnly())
          rd.start_cmd.run_command();

        prepareNextRun(rd);
        ErrorLog.clearCount();

        int current_interval         = 1;
        int elapsed_intervals_needed = (int) (rd.getElapsed() / rd.getInterval());
        int elapsed_intervals_done   = 0;

        /* If no warmup is requested, one interval is implied, */
        /* but Replay does not do warmup:                      */
        if (rd.getWarmup() == 0 && !ReplayInfo.isReplay())
          elapsed_intervals_needed--;

        /* Save the start time of this interval: */
        interval_start_time = interval_end_time = new Date();

        /* Some times end-of-run is so fast that we do not have a value */
        /* set at the end of the warmup:                                */
        run_start_time = new Date();

        /* This is the 'interval for elapsed time' loop: */
        while (true)
        {
          /* If someone else told us to terminate the run, we can simply do it */
          /* now because we just came back from reporting the last interval:   */
          if (Vdbmain.isWorkloadDone())
          {
            //common.where();
            break;
          }

          /* Terminate because of DV errors if needed: */
          DV_map.checkDVStatus();

          /* Wait until end of interval: */
          Interval.wait_interval();

          /* All slaves reported seekpct=eof? */
          if (SlaveList.allSequentialDone())
            Vdbmain.setWorkloadDone(true);

          /* We're outside of the warmup, so count this within the elapsed time: */
          if (isWarmupDone())
            elapsed_intervals_done++;

          //common.ptod("elapsed_intervals_done: " + warmup_done + " " +
          //            elapsed_intervals_done + " " + elapsed_intervals_needed);

          /* Run is complete if we did the last interval (not for format): */
          boolean last_call = (elapsed_intervals_done >= elapsed_intervals_needed);
          if (Vdbmain.isWorkloadDone())
          {
            //common.where();
            last_call = true;
          }

          /* We may want to shut down due to the shutdown file: */
          if (checkMonitorFile())
            last_call = true;
          if (Report.getAuxReport() != null && Report.getAuxReport().isShutdown())
            last_call = true;


          /* Ask slaves for all statistics. Reporting will be done asynchronously: */
          interval_start_time = interval_end_time;
          if (current_interval == getWarmupIntervals())
            run_start_time = new Date();
          run_end_time          =
          interval_end_time     = new Date();
          CollectSlaveStats css = new CollectSlaveStats(current_interval++,
                                                        rd.getInterval(), last_call);


          /* GC debugging: report GC usage: */
          GcTracker.report();

          /* Figure out if we're reaching the end of warmup or the end of the run: */
          if (!isWarmupDone())
          {
            if (current_interval > Reporter.getWarmupIntervals())
            {
              setWarmupDone();
              Status.printStatus("Warmup done", rd);
              first_elapsed_interval = current_interval;
            }

            else if (Report.getAuxReport() != null && Report.getAuxReport().isWarmupComplete())
            {
              setWarmupDone();
              Status.printStatus("Warmup done", rd);
              Report.getAuxReport().setWarmupComplete(false);
              first_elapsed_interval = current_interval;
            }
          }

          /* If this was the last interval, wait for statistics and break: */
          if (last_call)
          {
            css.waitForLast();
            reportEndOfRun(rd);

            if (rd.doing_curve_point && !rd.doing_curve_max && Vdbmain.observed_resp > rd.curve_end)
            {
              common.ptod("Vdbench terminating because of requested 'stopcurve=%.3f' "+
                          "with observed response time of %.3f ms", rd.curve_end, Vdbmain.observed_resp);
              monitor_final = true;
            }

            if (Report.getAuxReport() != null)
              Report.getAuxReport().setShutdown(false);
            //common.where();
            break;
          }

          if (common.get_debug(common.PRINT_MEMORY))
          {
            System.gc();
            common.memory_usage();
            Native.printMemoryUsage();
          }
        }   /* while (true) waiting for interval count */


        /* Notify user that the format may not have completed (as requested): */
        if (rd.isThisFormatRun() && rd.format.format_limited)
          common.pboth("Format run terminated because of 'format=(only,limited)' request");

        /* This RD workload is now done: */
        Vdbmain.setWorkloadDone(true);
        Status.printStatus("Workload done", rd);
        SlaveList.sendWorkloadDone();
        SlaveList.waitForSlaveWorkCompletion();
        Status.printStatus("Slaves done", rd);
        Blocked.printAndResetCounters();

        if (ThreadMonitor.active())
          Host.reportMonTotals();

        /* Report SD vs WG i/o counts: */
        //if (Vdbmain.isWdWorkload()) // && rd.wgs_for_rd.size() > 0)
        //  HandleSkew.endOfRunSkewCheck(rd);
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

        /* Execute 'endcmd' on master if requested: */
        if (rd.end_cmd != null && rd.end_cmd.masterOnly())
          rd.end_cmd.run_command();
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

    /* Doing this for each RD is not really needed */
    if (Dedup.isDedup() && Vdbmain.isWdWorkload())
      SdDedup.adjustSdDedupValues();

    /* Send information to all slaves: */
    Work.prepareWorkForSlaves(rd, true);
    SlaveList.sendWorkToSlaves(rd);
    WhereWhatWork.printWorkForSlaves("prepareNextRun", rd);


    /* For debugging: help verify where-what-goes: */
    if (common.get_debug(common.GENERATE_WORK_INFO))
    {
      System.out.println();
      for (String d : CompareWorkInfo.generateDebugInfo(rd))
        System.out.println(d);
      System.out.println();
    }

    if (CompareWorkInfo.debugOutputNeeded())
    {
      for (String d : CompareWorkInfo.generateDebugInfo(rd))
        System.out.println(d);
    }
    if (CompareWorkInfo.debugCompareNeeded())
      CompareWorkInfo.compareWorkInfo(rd);


    Vdbmain.setWorkloadDone(false);

    /* Wait for all slaves to be ready to start running: */
    common.plog("Waiting for synchronization of all slaves");
    SlaveList.waitForSlavesReadyToGo();
    common.plog("Synchronization of all slaves complete");

    /* Allow for optional extern synchronization: */
    SlaveList.externalSynchronize();

    /* Now that they are ready to go, lets wait until the next */
    /* rounded one second and then take off:                   */
    Interval.set_interval_start(rd.getInterval());
    rd.display_run();

    SlaveList.tellSlavesToGo();

    /* Collect some aux stats if needed to set a baseline for the end of interval: */
    // Removed till further notice
    //if (Report.getAuxReport() != null)
    //  Report.getAuxReport().collectIntervalData();

  }

  /**
   * End of run reporting.
   * Report averages of all intervals but the first one.
   */
  public static void reportEndOfRun(RD_entry rd)
  {
    SdStats stats = null;
    SdStats run_totals = null;
    FwdStats fwd_totals = null;

    if (Vdbmain.isWdWorkload())
    {
      WdReport.reportWdTotalStats();
      run_totals = SdReport.reportSdTotalStats();
      SkewReport.reportRawEndOfRunSkew(rd);
      SkewReport.endOfRawRunSkewCheck(rd);
      Report.getReport("histogram").println(run_totals.printHistograms());
    }
    else
    {
      fwd_totals = FwdReport.reportRunTotals();
      SkewReport.reportFileEndOfRunSkew(rd);
    }

    if (Report.isKstatReporting())
      Report.reportKstatTotals();

    Flat.printInterval();



    /* Set last observed rate: */
    if (Vdbmain.isWdWorkload())
    {
      Vdbmain.observed_iorate = Math.round(run_totals.rate());
      Vdbmain.observed_resp   = run_totals.respTime();
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

    else // if (Report.sdDetailNeeded())
    {
      Vdbmain.observed_iorate = Math.round(fwd_totals.getTotalRate());
      Vdbmain.observed_resp   = fwd_totals.getTotalResp();
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

      ErrorLog.plog("*");
      ErrorLog.plog("Total Data Validation or I/O error count: " + ErrorLog.getErrorCount());
      ErrorLog.plog("*");
      if (!common.get_debug(common.NO_ERROR_ABORT))
        common.failure("Vdbench terminating due to Data Validation or I/O errors. See errorlog.html.");
    }

    CpuStats.cpu_shortage(rd);
  }


  private static void initWarmupIntervals(RD_entry rd)
  {
    warmup_done = false;
    warmup_intervals = (int) (rd.getWarmup() / rd.getInterval());

    if (warmup_intervals == 0)
      warmup_intervals = 1;

    /* For replay we have no warmup: */
    if (ReplayInfo.isReplay())
      warmup_done = true;
  }

  private static int getWarmupIntervals()
  {
    return warmup_intervals;
  }
  public static boolean isWarmupDone()
  {
    return warmup_done;
  }
  private static void setWarmupDone()
  {
    warmup_done = true;
  }



  public static boolean needHeaders()
  {
    return Report.getInterval() % 30 == 1;
  }

  /**
   * If the file below has been requested using the monitor=filename parameter,
   * create/clear it first and then monitor it at the end of each interval and
   * cause the current RD to terminate normally if the file contains 'shutdown'.
   */
  private static String tmpdir       = (common.onWindows()) ? Fput.getTmpDir() : "/tmp";
  private static int    process_id   = common.getProcessId();
  private static String tmp_shutdown = new File(tmpdir, "vdbench.shutdown." + process_id).getAbsolutePath();
  private static void clearShutdownFile(boolean report)
  {
    /* Delete a possible old shutdown file: */
    new File(tmp_shutdown).delete();

    if (monitor_file == null)
      return;

    Fput fp = new Fput(monitor_file);
    fp.close();

    if (report)
      common.pboth("User requesting monitoring of this run using file '%s'.", monitor_file);
  }


  /**
   * Check for /tmp/vdbench.shutdown.12345
   * If that file exists, request a shutdown.
   *
   * Otherwise, use normal monitor= processing.
   */
  private static boolean checkMonitorFile()
  {
    if (Fget.file_exists(tmp_shutdown))
    {
      common.pboth("User requested early Vdbench termination using file '%s'.", tmp_shutdown);
      Status.printStatus("User requested early Vdbench termination.", null);
      clearShutdownFile(false);
      monitor_final = true;
      return true;
    }


    if (monitor_file == null)
      return false;

    String[] lines = Fget.readFileToArray(monitor_file);
    if (lines.length == 0)
      return false;

    String line = lines[0].trim().toLowerCase();
    if (line.equals("shutdown") || line.equals("end_rd"))
    {
      common.pboth("User requested early shutdown of this run.");
      Status.printStatus("User requested early shutdown of this run.", null);
      clearShutdownFile(false);
      return true;
    }

    else if (line.equals("terminate") || line.equals("end_vdbench"))
    {
      common.pboth("User requested early Vdbench termination.");
      Status.printStatus("User requested early Vdbench termination.", null);
      clearShutdownFile(false);
      monitor_final = true;
      return true;
    }

    return false;
  }
}


