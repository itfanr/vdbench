package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.SimpleDateFormat;
import java.util.*;

import User.ControlUsers;
import User.UserData;

import Utils.*;


/**
 * This class handles Vdbench statistics collection from the slaves.
 *
 * The Reporter thread creates an instance each 'n' seconds, and statistics
 * requests are sent to each slave.
 * When the (asynchronous) response comes in we keep track to make sure
 * that each slave has responded to this specific request.
 * Once we find out that all slaves have responded for this interval, only then
 * will we print the statistics.
 *
 * These statistics will be reported asynchronously here in run().
 *
 * It can happen in theory that some slave responses are so slow in coming in that
 * the reporting period covering the data for this slave does not match the period
 * that we intended this to cover.
 * We will still use the original period.
 *
 */
public class CollectSlaveStats extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static Reporter reporter = null;

  private long         request_sequence_number;

  private int          requested_interval;
  private long         interval_duration;
  private boolean      last_interval;

  private Semaphore    wait_for_last = null;

  private boolean      some_data_returned = false;

  private SlaveStats[] data_from_slaves;

  private static long  static_seqno = 0;


  /* This queue holds CollectSlaveStats instances that have been requested. */
  private static Vector <CollectSlaveStats> stats_pending_queue = new Vector(64, 0);

  /* This list holds CollectSlaveStats instances that are ready for reporting */
  /* because all the slaves have responded. Acrtual reporting is done async. */
  private static Vector <CollectSlaveStats> reporting_queue = new Vector(8);

  /* Semaphore to trigger async reporting: */
  private static Semaphore async_sema = new Semaphore(0);



  public long   expected_interval_end   = 0;
  public long   expected_next_interval  = 0;
  public long   start_send              = 0;
  public long   last_receive            = 0;
  public long   start_reporting         = 0;
  public long   end_reporting           = 0;

  //private static Fput report_report = null;


  /**
   * Async thread instance.
   */
  public CollectSlaveStats(Reporter rep)
  {
    reporter = rep;
  }

  /**
   * New instance: handle statistis for the just finished reporting interval
   */
  public CollectSlaveStats(int intv, long duration, boolean last_call)
  {
    interval_duration       = duration;
    request_sequence_number = static_seqno++;
    requested_interval      = intv;
    last_interval           = last_call;
    data_from_slaves        = new SlaveStats[SlaveList.getSlaveCount()];

    expected_interval_end   = Interval.expected_interval_end;
    expected_next_interval  = Interval.expected_next_interval;

    /* When we reach this, the Inter inputs are already set for the following interval: */
    expected_interval_end  -= Interval.getSeconds() * 1000;
    expected_next_interval -= Interval.getSeconds() * 1000;

    requestStatistics(last_call);
  }


  public void requestStatistics(boolean last_call)
  {

    /* At the start of a new run, clear all ReportData statistics: */
    if (requested_interval == 1)
      ReportData.clearAllTotalStats();

    /* If this is last call, create a semaphore that we can wait for: */
    if (last_call)
      wait_for_last = new Semaphore(0);

    /* Add this to the list so that we can find it later:                  */
    /* This must be done BEFORE we send out the message because it can     */
    /* happen that the response comes back BEFORE we get the chance to add */
    /* it to the list.                                                     */
    stats_pending_queue.addElement(this);

    /* Prepare statistics request to each slave: */
    for (int i = 0; i < SlaveList.getSlaveCount(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* If the slave right now has no work, create dummy stats and skip: */
      if (slave.getCurrentWork() == null)
      {
        data_from_slaves[i] = new SlaveStats(request_sequence_number);
        data_from_slaves[i].owning_slave = slave;
        data_from_slaves[i].setSdStats(new SdStats[0]);

        /* new SlaveStats() above copies static counters (don't ask why). */
        /* Clear them.                                                    */
        long[] empties = data_from_slaves[i].getBlockCounters();
        for (int e = 0; e < empties.length; e++)
          empties[e] = 0;

        continue;
      }
    }

    /* Send statistics request to each slave: */
    start_send = System.currentTimeMillis();
    for (int i = 0; i < SlaveList.getSlaveCount(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* If the slave right now has no work, skip: */
      if (slave.getCurrentWork() == null)
        continue;

      /* Make seq# negative to identify 'last call': */
      SocketMessage sm = new SocketMessage(SocketMessage.REQUEST_SLAVE_STATISTICS);
      if (last_interval)
        sm.setInfo(request_sequence_number * -1);
      else
        sm.setInfo(request_sequence_number);
      slave.getSocket().putMessage(sm);
    }

    /* If needed, pick up some AUX statistcs.                                */
    /* In theory this can run so darned fast that we have all the statistics */
    /* back from all slaves and already have those statistics therefore      */
    /* reported using OLD aux data before we are even finished here.         */
    /* However, I do not want to delay the normal reporting.                 */
    if (Report.getAuxReport() != null)
      Report.getAuxReport().collectIntervalData();
  }


  public SlaveStats[] getDataFromSlaves()
  {
    return data_from_slaves;
  }

  /**
   * Send statistics to the master.
   */
  public static SlaveStats getStatsForMaster(long stat_number)
  {
    /* A negatuve statistics sequence number identifies 'last call': */
    boolean last_interval = (stat_number < 0);
    stat_number = Math.abs(stat_number);


    SlaveStats sts = new SlaveStats(stat_number);

    /* CPU and Kstat data if needed: */
    if ((common.onSolaris() || common.onWindows() || common.onLinux()) &&
        SlaveJvm.isFirstSlaveOnHost())
    {
      sts.setCpuStats(CpuStats.getNativeCpuStats());
      //common.ptod("sts.getCpuStats: " + sts.getCpuStats());
      if (common.onSolaris() || common.onWindows() )
        sts.setKstatData(getAllKstatData());

      /* Obtain all delta NFS statistics: */
      if (common.onSolaris())
      {
        NfsStats.getAllNfsDeltasFromKstat();
        sts.setNfsData(NfsStats.getNfs3(), NfsStats.getNfs4());
      }
    }

    if (SlaveWorker.work.wgs_for_slave != null)
      getSdStatistics(sts, last_interval);

    else
      getFileSystemStatistics(sts, last_interval);

    return sts;
  }


  /**
   * 'Regular' statistics: statistics related to the original non-file
   *  system version of vdbench.
   */
  private static void getSdStatistics(SlaveStats sts, boolean last_interval)
  {
    /* Get the raw statistics (it's stored in WG_entry): */
    WG_stats.get_jni_statistics(last_interval);

    /* Pick up all SdStats data from the WG_entry instances: */
    ArrayList <SdStats> sd_stats = new ArrayList(64);
    for (int i = 0; i < SlaveWorker.work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) SlaveWorker.work.wgs_for_slave.get(i);

      for (int s = 0; s < wg.jni_index_list.size(); s++)
      {
        JniIndex jni  = wg.jni_index_list.get(s);
        sd_stats.add(jni.dlt_stats);
        jni.dlt_stats.sd_name = jni.sd_name;
        jni.dlt_stats.wd_name = jni.wd_name;
      }
    }

    /* Send to master: */
    sts.setSdStats((SdStats[]) sd_stats.toArray(new SdStats[0]));
    sts.setThreadMonData(ThreadMonitor.getAllDeltas());
  }


  /**
   * Deliver file system statistics. Gather stats from all threads,
   * accumulate them, and return.
   */
  private static void getFileSystemStatistics(SlaveStats sts, boolean last_interval)
  {
    FwdStats total_stats = new FwdStats();
    FwdStats delta_fwd   = new FwdStats();
    HashMap fsd_map      = new HashMap(32);
    HashMap fwg_map      = new HashMap(32);

    Vector threads = FwgRun.getThreads();
    for (int i = 0; i < threads.size(); i++)
    {
      FwgThread thread = (FwgThread) threads.elementAt(i);

      /* Accumulate slave totals: */
      delta_fwd.delta(thread.per_thread_stats, thread.old_stats);
      total_stats.accum(delta_fwd, false);

      /* Count each fsd and fwd: */
      FwdStats fsd_stat = (FwdStats) fsd_map.get(thread.fwg.fsd_name);
      if (fsd_stat == null)
        fsd_map.put(thread.fwg.fsd_name, fsd_stat = new FwdStats());
      fsd_stat.accum(delta_fwd, false);

      FwdStats fwg_stat = (FwdStats) fwg_map.get(thread.fwg.getName());
      if (fwg_stat == null)
        fwg_map.put(thread.fwg.getName(), fwg_stat = new FwdStats());
      fwg_stat.accum(delta_fwd, false);


      /* Prepare for future delta: */
      thread.old_stats.copyStats(thread.per_thread_stats);
    }


    // This is not complete. If we do this below we report ONLY the
    // last interval, not all intervals

    /* We only want to send over the histogram data at the end: */
    //if (!last_interval)
    //  total_stats.clearHistogram();


    /* Send to master: */
    sts.setSlaveIntervalStats(total_stats, fsd_map, fwg_map);
  }


  /**
   * Match statistics received from slave with a CollectSlaveStats instance that
   * is waiting for data.
   */
  private static Object just_one_test = new Object();
  public static void receiveStats(Slave slave, SlaveStats sts)
  {
    /* This lock must be here to make sure that while I am reporting ONE */
    /* interval, I do not start reporting the next (late) interval:      */
    /* (Though a static synchronized lock should do the trick also)      */
    /* There is a risk though: what if the actual REPORTING takes longer than an interval? */
    synchronized(just_one_test)
    {
      CollectSlaveStats css = null;

      /* First look for the correct CollectSlaveStats instance: */
      boolean found = false;
      for (int i = 0; i < stats_pending_queue.size(); i++)
      {
        css = (CollectSlaveStats) stats_pending_queue.elementAt(i);
        if (css.request_sequence_number == sts.getNumber())
        {
          found = true;
          css.last_receive = System.currentTimeMillis();
          break;
        }
      }

      if (!found)
        common.failure("CollectSlaveStats sequence number not found: " + sts.getNumber());


      /* Remember which slave this belongs to: */
      sts.owning_slave = slave;

      /* Make sure only one thread is scanning the list for completeness: */
      synchronized(css)
      {
        int slaveno = slave.getSlaveNumber();

        if (css.data_from_slaves[slaveno] != null)
          common.failure("CollectSlaveStats returned twice");

        /* We have received statistics for THIS request for THIS slave: */
        css.data_from_slaves[slaveno] = sts;

        /* If all the data is not here yet, then we can not report: */
        for (int j = 0; j < css.data_from_slaves.length; j++)
        {
          if (css.data_from_slaves[j] == null)
            return;
        }


        /* We have all data for this interval, now do reporting: */
        css.start_reporting = System.currentTimeMillis();

        /* We have all the data for this interval. Remove this instance. */
        stats_pending_queue.remove(css);

        /* Tell our run() thread to go ahead: */
        reporting_queue.add(css);
        async_sema.release();
      }
    }
  }


  private void addAllSlaves(Slave slave, SlaveStats sts)
  {

    /* SD statistics: */
    if (Vdbmain.isWdWorkload())
    {
      storeSdStats(slave, sts);
      slave.accumMonData(sts.tmonitor_deltas);
    }

    /* or FSD statistics: */
    else
    {
      Blocked.accumCounters(sts.getBlockCounters());

      /* Add slave totals for this interval: */
      if (sts.getSlaveIntervalStats() != null)
      {
        ReportData rs = Report.getReport(slave).getData();
        rs.accumIntervalFwdStats(sts.getSlaveIntervalStats());

        /* Add slave totals for this interval to host: */
        rs = Report.getReport(slave.getHost()).getData();
        rs.accumIntervalFwdStats(sts.getSlaveIntervalStats());

        /* Add slave totals for this interval to system total: */
        rs = Report.getSummaryReport().getData();
        rs.accumIntervalFwdStats(sts.getSlaveIntervalStats());

        /* Add FSD and FWD specific statistics to run totals: */
        ReportData.accumMappedIntervalStats(slave, sts.getFsdMap());
        ReportData.accumMappedIntervalStats(slave, sts.getFwdMap());
      }
    }


    /* The first slave on a host gives us cpu stats: */
    if (sts.getCpuStats() != null)
    {
      slave.getHost().getSummaryReport().getData().accumIntervalCpuStats(sts.getCpuStats());
      Report.getSummaryReport().getData().accumIntervalCpuStats(sts.getCpuStats());
    }

    /* Pick up Kstat data if there is some available: */
    InfoFromHost info = slave.getHost().getHostInfo();
    if (!info.anyKstatErrors() && info != null && info.isSolaris() && sts.getKstatData() != null)
    //if (info != null && info.isSolaris() && sts.getKstatData() != null)
    {
      if (sts.getKstatData().size() != info.getInstancePointers().size())
        common.failure("receiveStats() host: " + slave.getHost().getLabel() +
                       " unmatched Kstat data count: " + sts.getKstatData().size() +
                       "/" + info.getInstancePointers().size());

      /* Add Kstat data to this slave's host: */
      storeKstats(slave, sts);
    }
  }


  /**
   * We have received all the statistics data from all slaves on all hosts.
   * I bet we'll now do some reporting.
   *
   * This is run asynchronous.
   */
  private void doDetailedReporting(boolean catching_up)
  {
    SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss.SSS" );

    /* We have all data for this interval: */
    Report.setInterval(requested_interval);
    if (Vdbmain.isWdWorkload())
    {
      if (!catching_up)
        Report.reportWdInterval();

      if (Reporter.isWarmupDone())
      {
        ReportData.addSdIntervalToTotals();
        ReportData.addCpuIntervalToTotals();
        ReportData.addKstatIntervalToTotals();
        ReportData.addNfsIntervalToTotals();
      }

      /* Shutdown if maxdata= requested: */
      SdStats run_totals = Report.getSummaryReport().getData().getTotalSdStats();
      checkMaxData(run_totals.r_bytes, run_totals.w_bytes);
    }

    else
    {
      if (!catching_up)
        FwdReport.reportFwdInterval();

      if (Reporter.isWarmupDone())
      {
        ReportData.addFwdIntervalToTotals();
        ReportData.addCpuIntervalToTotals();
        ReportData.addKstatIntervalToTotals();
        ReportData.addNfsIntervalToTotals();
      }

      /* Shutdown if maxdata= requested: */
      FwdStats fwd_total = Report.getSummaryReport().getData().getTotalFwdStats();
      checkMaxData(fwd_total.getTotalBytesRead(), fwd_total.getTotalBytesWritten());
    }


    /* Call user classes with the info we received and returned what they give back: */
    /* To be honest here: this is not used, which is good because I don't remember   */
    /* much of what/why. Now that we do reporting async this may need some work      */
    UserData[] ret = ControlUsers.receivedIntervalDataFromSlaves(this);
    ControlUsers.sendUserDataToSlaves(ret);

    end_reporting = System.currentTimeMillis();

    /* Report only if the completion time of the reporting is more than */
    /* one second later than the expected end of the interval:          */
    // Header to copy into output:
    // Reporting delay:                req   pend   rep  interval      sent   recv    rep strt rep end rep
    // Reporting delay:               intv  queue  q    end           late   late    late     late    elapsd
    if (catching_up || end_reporting - expected_interval_end > 1000)
    {
      common.plog("Reporting delay: %5d (%3d) (%3d) %s %7.3f %7.3f %7.3f %7.3f %7.3f",
                  requested_interval,
                  stats_pending_queue.size(),
                  reporting_queue.size(),
                  df.format(new Date(expected_interval_end)),
                  (start_send      - expected_interval_end) / 1000.,
                  (last_receive    - expected_interval_end) / 1000.,
                  (start_reporting - expected_interval_end) / 1000.,
                  (end_reporting   - expected_interval_end) / 1000.,
                  (end_reporting   - start_reporting)       / 1000.);
    }

  }


  /**
   * Shut down this RD after maxdata= bytes.
   * If maxdata < 100, then it is a multiplier of all active SDs.
   */
  private static void checkMaxData(long bytes_read, long bytes_written)
  {
    long total_bytes = bytes_read + bytes_written;

    RD_entry rd = RD_entry.next_rd;
    if (rd.max_data   == Double.MAX_VALUE &&
        rd.max_data_r == Double.MAX_VALUE &&
        rd.max_data_w == Double.MAX_VALUE)
      return;

    /* If we are already shutting down, don't bother again: */
    if (Vdbmain.isWorkloadDone())
      return;

    /* Calculate total active SD size: */
    double limit_t = getLimit(rd.max_data);
    double limit_r = getLimit(rd.max_data_r);
    double limit_w = getLimit(rd.max_data_w);
    //common.ptod("limit_t: %,d", (long) limit_t);
    //common.ptod("limit_r: %,d", (long) limit_r);
    //common.ptod("limit_w: %,d", (long) limit_w);

    /* Shutdown if maxdata= requested: */
    if (rd.max_data != Double.MAX_VALUE && total_bytes > limit_t)
    {
      common.pboth("Reached maxdata=%s. rd=%s shutting down after next interval. ",
                   FileAnchor.whatSize(limit_t), rd.rd_name);
      Vdbmain.setWorkloadDone(true);
    }

    else if (rd.max_data_r != Double.MAX_VALUE && bytes_read > limit_r)
    {
      common.pboth("Reached maxdataread=%s. rd=%s shutting down after next interval. ",
                   FileAnchor.whatSize(limit_r), rd.rd_name);
      Vdbmain.setWorkloadDone(true);
    }

    else if (rd.max_data_w != Double.MAX_VALUE && bytes_written > limit_w)
    {
      common.pboth("Reached maxdatawritten=%s. rd=%s shutting down after next interval. ",
                   FileAnchor.whatSize(limit_w), rd.rd_name);
      Vdbmain.setWorkloadDone(true);
    }
  }

  private static double getLimit(double max)
  {
    double limit = max;
    if (max <= 100)
    {
      limit = 0;
      for (SD_entry sd : Vdbmain.sd_list)
      {
        if (sd.isActive())
          limit += sd.end_lba;
      }
      limit *= max;
    }
    return limit;
  }

  /**
   * Wait for the last set of statistics to arrive from the slaves
   */
  public void waitForLast()
  {
    try
    {
      long signaltod = 0;
      while (!wait_for_last.attempt(500))
      {
        if ( (signaltod = common.signal_caller(signaltod, 2000)) == 0)
        {
          common.ptod("Waiting for the last interval's statistics to be reported");

          /* If we only have one slave don't bother reporting it: */
          for (int i = 1; i < data_from_slaves.length; i++)
          {
            if (data_from_slaves[i] == null)
              common.ptod("Waiting for slave: " + ((Slave) SlaveList.getSlaveList().elementAt(i)).getLabel());
          }
        }
      }
    }
    catch (InterruptedException e)
    {
      common.where();
    }

    if (!Vdbmain.isWdWorkload())
      return;

    /* Total i/o for all slaves: */
    //long total = 0;
    //int  max_width    = 0;
    //for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    //{
    //  Host host = (Host) Host.getDefinedHosts().elementAt(i);
    //  for (int j = 0; j < host.getSlaves().size(); j++)
    //  {
    //    Slave slave = (Slave) host.getSlaves().elementAt(j);
    //    max_width   = Math.max(max_width, slave.getLabel().length());
    //    total      += slave.reads + slave.writes;
    //  }
    //}
    //
    //common.plog("");
    //long total_reads  = 0;
    //long total_writes = 0;
    //for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    //{
    //  Host host = (Host) Host.getDefinedHosts().elementAt(i);
    //  for (int j = 0; j < host.getSlaves().size(); j++)
    //  {
    //    Slave slave = (Slave) host.getSlaves().elementAt(j);
    //    if (slave.reads + slave.writes == 0)
    //      continue;
    //    total_reads  += slave.reads;
    //    total_writes += slave.writes;
    //    common.plog("Total i/o for slave=%-" + max_width + "s: reads: %8d writes: %8d total: %8d skew: %7.2f%%",
    //                slave.getLabel(),
    //                slave.reads, slave.writes, (slave.reads + slave.writes),
    //                (double)(slave.reads + slave.writes) * 100 / total);
    //  }
    //}
    //String filler = String.format("%" + (max_width+5) + "s", " ");
    //common.plog("Total i/o done: %s reads: %8d writes: %8d total: %8d",
    //            filler,
    //            total_reads, total_writes, (total_reads + total_writes));
  }


  /**
   * Store Vdbench statistics.
   */
  private void storeSdStats(Slave slave, SlaveStats ss)
  {
    /* Statistics arrive here as an array for each slave with statistics  */
    /* collected per WG_entry. Since there can be multiple WG_entry's     */
    /* for any SD, we need to accumulate the totals for these SDs before  */
    /* we can store them with Data.addSdStats().                          */

    /* Create a list of SD names: */
    SdStats[] sd_stats = ss.getSdStats();
    HashMap sd_map = new HashMap(64);
    for (int i = 0; i < sd_stats.length; i++)
    {
      sd_map.put(sd_stats[i].sd_name, null);
      sd_stats[i].elapsed = interval_duration * 1000000;
    }

    String[]  sds       = (String[]) sd_map.keySet().toArray(new String[0]);
    SdStats[] new_stats = new SdStats[sds.length];

    /* Accumulate per SD: */
    for (int i = 0; i < sds.length; i++)
    {
      new_stats[i]         = new SdStats();
      new_stats[i].sd_name = sds[i];

      for (int j = 0; j < sd_stats.length; j++)
      {
        if (sd_stats[j].sd_name.equals(sds[i]))
        {
          new_stats[i].stats_accum(sd_stats[j], false);
          new_stats[i].elapsed = interval_duration * 1000000;
        }
      }
    }


    /* Now store the stats that we just calculated for each SD: */
    for (int i = 0; i < new_stats.length; i++)
    {
      SdStats stats = new_stats[i];

      /* Add SD totals for this interval: */
      Report.getReport(stats.sd_name).getData().accumIntervalSdStats(stats);

      /* Add slave totals for this interval: */
      Report.getSummaryReport().getData().accumIntervalSdStats(stats);
      Report.getReport(slave).getData().accumIntervalSdStats(stats);
      if (Report.slave_detail)
        slave.getReport(stats.sd_name).getData().accumIntervalSdStats(stats);
      if (Report.host_detail)
        slave.getHost().getReport(stats.sd_name).getData().accumIntervalSdStats(stats);

      /* Add slave totals for this interval to host: */
      Report.getReport(slave.getHost()).getData().accumIntervalSdStats(stats);

      slave.reads  += stats.reads;
      slave.writes += stats.writes;
    }


    /* Accumulate WG_entry specific statistics: */
    for (int i = 0; i < sd_stats.length; i++)
    {
      String wdname = sd_stats[i].wd_name;

      /* Find the proper WD_entry to use: */
      WD_entry wd = null;
      for (int w = 0; w < Vdbmain.wd_list.size(); w++)
      {
        wd = (WD_entry) Vdbmain.wd_list.get(w);
        if (wd.wd_name.equals(wdname))
          break;
      }
      if (wd == null)
        common.failure("Unable to find wd=" + wdname);

      synchronized (wd)
      {
        /* This is used for WD reports: */
        if (Vdbmain.wd_list.size() > 1)
          Report.getReport(wdname).getData().accumIntervalSdStats(sd_stats[i]);

        /* Count total i/o for each WD (used for setting curve skew): */
        if (Reporter.isWarmupDone())
          wd.total_io_done += sd_stats[i].reads + sd_stats[i].writes;
      }
    }
  }


  /**
   * Store the Kstat data received from the first slave on a host.
   *
   * We store the InstancePointer here to later on make it possible to connect
   * Kstat_data to the original device.
   */
  private void storeKstats(Slave slave, SlaveStats sts)
  {
    Vector list = sts.getKstatData();
    for (int i = 0; i < list.size(); i++)
    {
      Kstat_data kd = (Kstat_data) list.elementAt(i);
      kd.pointer = slave.getHost().getInstancePointer(i);

      /* Add this to the Kstat summary report: */
      ReportData rs = Report.getReport("kstat").getData();
      rs.accumIntervalKstats(kd);

      /* Add this to the Host Kstat summary report: */
      rs = slave.getHost().getReport("kstat").getData();
      rs.accumIntervalKstats(kd);

      /* Add this to the individual Host device report: */
      rs = slave.getHost().getReport(kd.pointer.getID()).getData();
      rs.accumIntervalKstats(kd);

      if (NfsStats.areNfsReportsNeeded())
      {
        slave.getHost().getReport("nfsstat3").getData().accumIntervalNfs(sts.getNfs3());
        slave.getHost().getReport("nfsstat4").getData().accumIntervalNfs(sts.getNfs4());
      }
    }
  }


  public static Vector getAllKstatData()
  {
    Vector pointers = SlaveWorker.work.instance_pointers;
    if (pointers == null)
      return null;

    //OS_cmd ocmd = OS_cmd.executeCmd("iostat -xd");
    //ocmd.printStdout();
    //ocmd = OS_cmd.executeCmd("iostat -xdn");
    //ocmd.printStdout();
    //ocmd = OS_cmd.executeCmd("mount");
    //ocmd.printStdout();

    Vector kd = new Vector(pointers.size());
    long tod = Native.get_simple_tod();
    for (int i = 0; i < pointers.size(); i++)
    {
      InstancePointer ip = (InstancePointer) pointers.elementAt(i);
      kd.add(ip.getDeltaKstatData(tod));
    }

    return kd;
  }

  public void run()
  {
    /* Reporter must run higher than the rest: */
    Thread.currentThread().setPriority( Thread.MAX_PRIORITY );


    try
    {
      while (true)
      {
        boolean catching_up = false;



        /* Wait for a signal, but with timeout to recognize 'all done': */
        async_sema.attempt(100);


        /* Handle all outstanding reporting intervals.                               */
        /* When everything goes OK there should only be one, but with some times     */
        /* seeing 50 second response times writing to the reports, don't count on it */
        while (reporting_queue.size() > 0)
        {
          CollectSlaveStats css = reporting_queue.get(0);

          // Force delays:
          //if (css.requested_interval %20 == 3)
          //{
          //  //common.where();
          //  common.sleep_some(15 * 1000);
          //}

          /* Clear all old interval accumulators: */
          Blocked.resetCounters();
          ReportData.clearAllIntervalStats(css.interval_duration);

          /* Add all slave statistics to interval accumulators: */
          for (int i = 0; i < css.data_from_slaves.length; i++)
          {
            SlaveStats sts = css.data_from_slaves[i];
            css.addAllSlaves(sts.owning_slave, sts);
          }

          /* Some intervals may not be behind but we'll skip ALL anyway: */
          boolean toofar_behind = (reporting_queue.size() > 10);
          if (!catching_up && toofar_behind)
          {
            catching_up = true;
            common.pboth("Detailed reporting is running behind; reporting of intervals %d-%d has been skipped.",
                         reporting_queue.get(0).requested_interval,
                         reporting_queue.get(reporting_queue.size() - 1).requested_interval);;
          }

          /* Now do all the hard work: */
          css.doDetailedReporting(catching_up);
          reporting_queue.remove(css);


          /* If we did the last interval, wake up the code so that it */
          /* can finish up this run and start the next one:                 */
          if (css.last_interval)
          {
            //Reporter.reportEndOfRun();
            css.wait_for_last.release();

            /* Since the first run, if needed, is always the Journal recovery run */
            /* we are now sure that journal recovery for ALL SDs is complete      */
            Validate.setJournalRecovered(true);
            for (SD_entry sd : Vdbmain.sd_list)
              sd.journal_recovery_complete = true;
            break;
          }
        }

        /* Nothing queued, so if the Reporter is done, so are we. */
        /* (Reporter can be gone earlier, but we still may have had to finish) */
        if (!reporter.isAlive())
          return;

        if (Thread.currentThread().isInterrupted())
          break;
      }
    }

    catch (Exception e)
    {
      common.ptod("CollectSlaveStats died unexpectedly. Run terminated.");
      common.memory_usage();
      common.failure(e);
    }
  }
}


