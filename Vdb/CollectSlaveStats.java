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

import Utils.Format;
import Utils.NfsV3;
import Utils.Semaphore;


/**
 * This class handles vdbench statistics collection from the slaves.
 *
 * The Reporter thread creates an instance each 'n' seconds, and statistics
 * requests are sent to each slave.
 * When the (asynchronous) response comes in we keep track to make sure
 * that each slave has responded to this specific request.
 * Once we find out that all slaves have responded for this interval, only then
 * will we print the statistics.
 *
 * It can happen in theory that some slave responses are so slow in coming in that
 * the reporting period covering the data for this slave does not match the period
 * that we intended this to cover.
 * We will still use the original period.
 *
 */
class CollectSlaveStats extends VdbObject
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private long         request_sequence_number;

  private int          requested_interval;
  private long         interval_duration;
  private boolean      last_interval;

  private Semaphore    wait_for_last = null;

  private boolean      some_data_returned = false;

  private SlaveStats[] data_from_slaves;

  private static long  static_seqno = 0;


  /* This queue holds CollectSlaveStats instances that have been requested. */
  /* At this time entries are never removed, but that may change?    */
  private static Vector stats_pending_queue = new Vector(64, 0);



  public CollectSlaveStats(int intv, long duration, boolean last_call)
  {
    interval_duration       = duration;
    request_sequence_number = static_seqno++;
    requested_interval      = intv;
    last_interval           = last_call;
    data_from_slaves        = new SlaveStats[SlaveList.getSlaveCount()];

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

      /* If the slave has already finished, don't bother him again: */
      //if (slave.isCompleted())
      //  continue;

      /* If the slave right now has no work, create dummy stats and skip: */
      if (slave.getCurrentWork() == null)
      {
        data_from_slaves[i] = new SlaveStats(request_sequence_number);
        data_from_slaves[i].setSdStats(new SdStats[0]);
        //SlaveWorker.work.wgs_for_slave);
        //common.ptod("fake stats: " + i);
        continue;
      }
    }

    /* Send statistics request to each slave: */
    for (int i = 0; i < SlaveList.getSlaveCount(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* If the slave has already finished, don't bother him again: */
      //if (slave.isCompleted())
      //  continue;

      /* If the slave right now has no work, skip: */
      if (slave.getCurrentWork() == null)
        continue;

      SocketMessage sm = new SocketMessage(SocketMessage.REQUEST_SLAVE_STATISTICS);
      sm.setInfo(request_sequence_number);
      slave.getSocket().putMessage(sm);
    }
  }


  /**
   * Send statistics to the master.
   */
  public static void sendStatsToMaster(SlaveSocket socket, long stat_number)
  {
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
      getSdStatistics(sts);

    else
      getFileSystemStatistics(sts);

    SocketMessage sm = new SocketMessage(SocketMessage.SLAVE_STATISTICS, sts);
    socket.putMessage(sm);
  }


  /**
   * 'Regular' statistics: statistics related to the original non-file
   *  system version of vdbench.
   */
  private static void getSdStatistics(SlaveStats sts)
  {
    /* Get the raw statistics (it's stored in WG_entry): */
    WG_stats.get_jni_statistics(SlaveJvm.isWorkloadDone());

    /* Pick up all SdStats data from the WG_entry instances: */
    SdStats[] sd_stats = new SdStats[SlaveWorker.work.wgs_for_slave.size()];
    for (int i = 0; i < SlaveWorker.work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) SlaveWorker.work.wgs_for_slave.elementAt(i);
      sd_stats[i] = wg.dlt_stats;
      sd_stats[i].sd_name = wg.sd_used.sd_name;
      sd_stats[i].wg_number_for_slave = i;
      //common.ptod("wg.dlt_stats: " + wg.dlt_stats.wbytes_end_of_run);
      //common.ptod("sd_stats[i]: " + sd_stats[i].reads + " " + wg.sd_used.sd_name);
    }


    /* Send to master: */
    sts.setSdStats(sd_stats);
  }


  /**
   * Deliver file system statistics. Gather stats from all threads,
   * accumulate them, and return.
   */
  private static void getFileSystemStatistics(SlaveStats sts)
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
    ReportData rs;

    synchronized(just_one_test)  // did not help, so remove.
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
          break;
        }
      }

      if (!found)
        common.failure("CollectSlaveStats sequence number not found: " + sts.getNumber());


      /* Make sure only one thread is scanning the list for completeness: */
      synchronized(css)
      {
        int slaveno = slave.getSlaveNumber();

        if (css.data_from_slaves[slaveno] != null)
          common.failure("CollectSlaveStats returned twice");

        /* See if this is the first time data is returned for this instance. */
        /* If so, we must clear counters so that we can accumulate           */
        /* multiple possible slaves into it:                                 */
        css.clearStatsIfNeeded();

        /* We have received statistics for THIS request for THIS slave: */
        css.data_from_slaves[slaveno] = sts;
        if (sts.getSdStats() != null)
          css.storeSdStats(slave, sts);

        else
        {
          Blocked.accumCounters(sts.getBlockCounters());

          /* Add slave totals for this interval: */
          rs = Report.getReport(slave).getData();
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

        /* The first slave on a host gives us cpu stats: */
        if (sts.getCpuStats() != null)
        {
          slave.getHost().getSummaryReport().getData().accumIntervalCpuStats(sts.getCpuStats());
          Report.getSummaryReport().getData().accumIntervalCpuStats(sts.getCpuStats());
        }

        /* Pick up Kstat data if there is some available: */
        if (slave.getHost().getHostInfo().isSolaris() && sts.getKstatData() != null)
        {
          if (sts.getKstatData().size() != slave.getHost().getHostInfo().getInstancePointers().size())
            common.failure("receiveStats() host: " + slave.getHost().getLabel() +
                           " unmatched Kstat data count: " + sts.getKstatData().size() +
                           "/" + slave.getHost().getHostInfo().getInstancePointers().size());

          /* Add Kstat data to this slave's host: */
          css.storeKstats(slave, sts);
        }

        /* If all the data is not here yet, then we can not report: */
        for (int j = 0; j < css.data_from_slaves.length; j++)
        {
          if (css.data_from_slaves[j] == null)
            return;
        }

        /* We have all data for this interval: */
        allDataReceivedFromAllHosts(css, sts);

        /* We have all the data for this interval. Remove this instance. */
        stats_pending_queue.remove(css);
      }
    }
  }


  /**
   * We have received all the statistics data from all slaves on all hosts.
   * I bet we'll now do some reporting.
   */
  private static void allDataReceivedFromAllHosts(CollectSlaveStats css,
                                                  SlaveStats sts)
  {
    /* We have all data for this interval: */
    Report.setInterval(css.requested_interval);
    if (sts.getSdStats() != null)
    {
      Report.reportWdInterval();
      if (css.requested_interval > Reporter.getWarmupIntervals())
      {
        ReportData.addSdIntervalToTotals();
        ReportData.addCpuIntervalToTotals();
        ReportData.addKstatIntervalToTotals();
        ReportData.addNfsIntervalToTotals();
      }
    }

    else
    {
      FwdReport.reportFwdInterval();
      if (css.requested_interval > Reporter.getWarmupIntervals())
      {
        ReportData.addFwdIntervalToTotals();
        ReportData.addCpuIntervalToTotals();
        ReportData.addKstatIntervalToTotals();
        ReportData.addNfsIntervalToTotals();
      }
    }

    /* Now that we did the last interval, wake up the code so that it */
    /* can finish up this run and start the next one:                 */
    if (css.last_interval)
    {
      Reporter.reportEndOfRun();
      css.wait_for_last.release();

      /* Since the first run, if needed, is always the Journal recovery run */
      /* we are now sure that journal recovery for the SDs is complete      */
      Validate.setJournalRecovered(true);
      SdStats[] stats = sts.getSdStats();
      for (int i = 0; stats != null && i < stats.length; i++)
      {
        SD_entry sd = SD_entry.findSD(stats[i].sd_name);
        sd.journal_recovery_complete = true;
      }
    }
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
          for (int i = 0; i < data_from_slaves.length; i++)
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
    long total = 0;
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(j);
        total += slave.reads + slave.writes;
      }
    }

    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(j);
        common.plog("Total i/o for slave " + slave.getLabel() +
                    Format.f(":%12d", (slave.reads + slave.writes)) +
                    Format.f("%7.2f%%", (double)(slave.reads + slave.writes) * 100 / total));
      }
    }

  }


  /**
   * See if this is the first time data is returned for this instance.
   * If so, we must clear counters so that we can accumulate multiple possible
   * slaves into it.
   *
   * (We can not do this earlier because we must be sure that the previous
   * interval has been handled correctly.
   */
  public void clearStatsIfNeeded()
  {
    if (!some_data_returned)
    {
      some_data_returned = true;
      Blocked.resetCounters();
      ReportData.clearAllIntervalStats(interval_duration);
    }
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
          new_stats[i].wbytes_end_of_run = sd_stats[j].wbytes_end_of_run;

          // There should be a better way; we should get the elapsed from the slave?
          if (common.get_debug(common.SHORT_INTERVALS1))
            new_stats[i].elapsed = interval_duration * 250000;
          else if (common.get_debug(common.SHORT_INTERVALS2))
            new_stats[i].elapsed = interval_duration * 100000;
          else
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

      /* Store the write byte count to be used for the next tape read: */
      if (stats.wbytes_end_of_run > 0)
        SD_entry.findSD(stats.sd_name).bytes_written = stats.wbytes_end_of_run;

      slave.reads  += stats.reads;
      slave.writes += stats.writes;
    }


    /* Accumulate WG_entry specific statistics: */
    for (int i = 0; i < sd_stats.length; i++)
    {
      WG_entry wg = (WG_entry) slave.wgs_for_slave.elementAt(sd_stats[i].wg_number_for_slave);
      synchronized (wg.wd_used)
      {
        /* This is used for WD reports: */
        if (Vdbmain.wd_list.size() > 1)
          Report.getReport(wg.wd_name).getData().accumIntervalSdStats(sd_stats[i]);

        /* Count total i/o for each WD (used for setting curve skew): */
        if (requested_interval > Reporter.getWarmupIntervals())
          wg.wd_used.total_io_done += sd_stats[i].reads + sd_stats[i].writes;
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

    Vector kd = new Vector(pointers.size());
    long tod = Native.get_simple_tod();
    for (int i = 0; i < pointers.size(); i++)
    {
      InstancePointer ip = (InstancePointer) pointers.elementAt(i);
      kd.add(ip.getDeltaKstatData(tod));
    }

    return kd;
  }
}


