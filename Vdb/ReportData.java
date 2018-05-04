package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.Format;
import Utils.NfsV3;
import Utils.NfsV4;


/**
 * This class contains common code for statistics gathering and
 * reporting.
 * Current uses: FwdEntry and FsdEntry.
 *
 * Now that I am finally smart enough to realize that the best place to store
 * the performance data is the Report where it gets reported it makes 100% sense
 * to start doing the same for other data, e.g. CPU and SD data.
 * This will then replace the Data() class which I never really liked anyway.
 */
public class ReportData
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Report   owner;

  private FwdStats   total_fwdstats;       /* Contains stats AFTER warmup     */
  private FwdStats   interval_fwdstats;    /* Statistics for current interval */

  private SdStats    total_sdstats;
  private SdStats    interval_sdstats;

  private Kstat_cpu  total_cpustats;
  private Kstat_cpu  interval_cpustats;

  private Kstat_data total_kstats;
  private Kstat_data interval_kstats;

  private NfsV3      total_nfs3;
  private NfsV3      interval_nfs3;

  private NfsV4      total_nfs4;
  private NfsV4      interval_nfs4;

  public ReportData(Report owning_report)
  {
    owner = owning_report;
  }


  /**
   * At the beginning of each interval, previous stats must be cleared.
   */
  public static void clearAllIntervalStats(long interval_duration)
  {
    Report[] reps = Report.getReports();
    for (int i = 0; i < reps.length; i++)
    {
      if (Vdbmain.isFwdWorkload())
      {
        reps[i].getData().interval_fwdstats = new FwdStats();
        reps[i].getData().interval_fwdstats.setElapsed(interval_duration * 1000000);
      }

      else
      {
        reps[i].getData().interval_sdstats = new SdStats();
        reps[i].getData().interval_sdstats.elapsed = interval_duration * 1000000;
      }

      reps[i].getData().interval_cpustats = new Kstat_cpu();

      reps[i].getData().interval_kstats   = new Kstat_data();
      reps[i].getData().interval_kstats.elapsed = interval_duration * 1000000;

      if (NfsStats.areNfsReportsNeeded())
      {
        reps[i].getData().interval_nfs3 = new NfsV3();
        reps[i].getData().interval_nfs4 = new NfsV4();
      }
    }
  }

  /**
   * At the beginning of each run, previous stats must be cleared.
   */
  public static void clearAllTotalStats()
  {
    Report[] reps = Report.getReports();
    for (int i = 0; i < reps.length; i++)
    {
      reps[i].getData().total_fwdstats = new FwdStats();
      reps[i].getData().total_sdstats  = new SdStats();
      reps[i].getData().total_cpustats = new Kstat_cpu();
      reps[i].getData().total_kstats   = new Kstat_data();
      reps[i].getData().total_nfs3     = new NfsV3();
      reps[i].getData().total_nfs4     = new NfsV4();
    }
  }

  public FwdStats getIntervalFwdStats()
  {
    return interval_fwdstats;
  }
  public SdStats getIntervalSdStats()
  {
    return interval_sdstats;
  }
  public Kstat_data getIntervalKstats()
  {
    return interval_kstats;
  }
  public Object getIntervalNfsStats(Object type)
  {
    if (type instanceof NfsV3)
      return interval_nfs3;
    else
      return interval_nfs4;
  }
  public Object getTotalNfsStats(Object type)
  {
    if (type instanceof NfsV3)
      return total_nfs3;
    else
      return total_nfs4;
  }
  public Kstat_cpu getIntervalCpuStats()
  {
    if (CpuStats.isCpuReporting())
      return interval_cpustats;
    else
      return null;
  }


  public FwdStats getTotalFwdStats()
  {
    return total_fwdstats;
  }
  public SdStats getTotalSdStats()
  {
    return total_sdstats;
  }
  public Kstat_data getTotalKstats()
  {
    return total_kstats;
  }
  public Kstat_cpu getTotalCpuStats()
  {
    if (CpuStats.isCpuReporting())
      return total_cpustats;
    else
      return null;
  }

  public void accumIntervalFwdStats(FwdStats stats)
  {
    interval_fwdstats.accum(stats, false);
  }

  public void accumIntervalSdStats(SdStats stats)
  {
    interval_sdstats.stats_accum(stats, false);
  }

  public void accumIntervalCpuStats(Kstat_cpu stats)
  {
    interval_cpustats.cpu_accum(stats);
  }
  public void accumIntervalKstats(Kstat_data stats)
  {
    interval_kstats.kstat_accum(stats, false);

    //if (interval_kstats.kstat_busy() < 0)
    //  common.ptod("report1: %-20s busy: %6.2f %6d %6.2f",
    //              owner.getFileName(),
    //              interval_kstats.kstat_busy(),
    //              interval_kstats.devices,
    //              stats.kstat_busy());
  }

  public void accumIntervalNfs(Object stats)
  {
    if (stats instanceof NfsV3)
      interval_nfs3.accum((NfsV3) stats);
    else
      interval_nfs4.accum((NfsV4) stats);
  }

  public static void accumMappedIntervalStats(Slave slave, HashMap rs_map)
  {
    String[]   names = (String[])   rs_map.keySet().toArray(new String[0]);
    FwdStats[] stats = (FwdStats[]) rs_map.values().toArray(new FwdStats[0]);

    /* First add statistics to run totals: */
    for (int i = 0; i < names.length; i++)
    {
      ReportData rs = Report.getReport(names[i]).getData();
      rs.interval_fwdstats.accum(stats[i], false);

      rs = Report.getReport(names[i], "histogram").getData();
      rs.interval_fwdstats.accum(stats[i], false);
    }
  }


  /**
   * At the end of an interval these stats must be added to the run totals.
   * Note: this is only called for the second interval, or after the warmup=.
   */
  public static void addFwdIntervalToTotals()
  {
    Report[] reports = Report.getReports();
    for (int i = 0; i < reports.length; i++)
    {
      ReportData rs = reports[i].getData();
      rs.total_fwdstats.accum(rs.interval_fwdstats, true);
    }
  }

  public static void addSdIntervalToTotals()
  {
    Report[] reports = Report.getReports();
    for (int i = 0; i < reports.length; i++)
    {
      ReportData rs = reports[i].getData();
      rs.total_sdstats.stats_accum(rs.interval_sdstats, true);
    }
  }

  public static void addCpuIntervalToTotals()
  {
    Report[] reports = Report.getReports();
    for (int i = 0; i < reports.length; i++)
    {
      ReportData rs = reports[i].getData();
      rs.total_cpustats.cpu_accum(rs.interval_cpustats);
    }
  }

  public static void addKstatIntervalToTotals()
  {
    Report[] reports = Report.getReports();
    for (int i = 0; i < reports.length; i++)
    {
      ReportData rs = reports[i].getData();
      rs.total_kstats.kstat_accum(rs.interval_kstats, true);

      //if (rs.interval_kstats.kstat_busy() < 0)
      //  common.ptod("reports: %-20s busy: %6.2f %6.2f",
      //              reports[i].getFileName(),
      //              rs.interval_kstats.kstat_busy(),
      //              rs.total_kstats.kstat_busy());
    }
  }

  public static void addNfsIntervalToTotals()
  {
    if (!NfsStats.areNfsReportsNeeded())
      return;
    Report[] reports = Report.getReports();
    for (int i = 0; i < reports.length; i++)
    {
      ReportData rs = reports[i].getData();
      rs.total_nfs3.accum(rs.interval_nfs3);
      rs.total_nfs4.accum(rs.interval_nfs4);
    }
  }

  public static void addHistogramToTotal(String[] names)
  {
    for (int i = 0; i < names.length; i++)
    {
      ReportData rs = Report.getReport(names[i], "histogram").getData();
      rs.total_fwdstats.accum(rs.interval_fwdstats, true);
    }
  }


  /**
   * Print the current interval's data on the owning Report.
   */
  public void reportInterval(Kstat_cpu kstat_cpu)
  {
    interval_fwdstats.printLine(owner, kstat_cpu);
  }


  /**
   * Report the current totals.
   */
  public void reportFwdTotal(Kstat_cpu kstat_cpu, String label)
  {
    total_fwdstats.printLine(owner, kstat_cpu, label);
  }

  public static void reportFwdTotals(String[] names, Kstat_cpu kstat_cpu, String label)
  {
    for (int i = 0; i < names.length; i++)
    {
      ReportData rs = Report.getReport(names[i]).getData();
      rs.reportFwdTotal(kstat_cpu, label);
    }
  }
}


