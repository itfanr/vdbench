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
import Utils.Fput;


/**
 * This class handles File system reports
 */
public class FwdReport extends Report
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static int total_header_lines = 1;

  /**
   * Report FWD statistics for this interval for all hosts and slaves.
   */
  public static void reportFwdInterval()
  {
    /* Report on Host reports: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      FwdStats host_fwd = new FwdStats();

      Kstat_cpu kstat_cpu = host.getSummaryReport().getData().getIntervalCpuStats();

      /* Report on Slave reports: */
      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(j);
        if (slave.getCurrentWork() == null)
          continue;

        Report.getReport(slave).getData().reportInterval(kstat_cpu);
      }

      Report.getReport(host).getData().reportInterval(kstat_cpu);
    }

    Kstat_cpu kc_total = Report.getSummaryReport().getData().getIntervalCpuStats();
    FwdStats total_fwd = Report.getSummaryReport().getData().getIntervalFwdStats();

    total_fwd.printLine(Report.getSummaryReport(), kc_total);
    if (!Vdbmain.kstat_console)
      total_fwd.printLine(Report.getStdoutReport(), kc_total);

    /* Report all FSDs: */
    for (int i = 0; i < FsdEntry.getFsdList().size(); i++)
    {
      FsdEntry fsd  = (FsdEntry) FsdEntry.getFsdList().elementAt(i);
      Report.getReport(fsd.name).getData().reportInterval(kc_total);
    }

    /* Report all Fwds: */
    for (int i = 0; i < FwdEntry.getFwdList().size(); i++)
    {
      FwdEntry fwd  = (FwdEntry) FwdEntry.getFwdList().elementAt(i);
      Report.getReport(fwd.fwd_name).getData().reportInterval(kc_total);

      /* There is a bug: when running multiple formats there are multiple    */
      /* format FWDs and the code is writing a line for each possible format */
      /* on the same file. Just end it after the first.                      */
      if (fwd.fwd_name.equals("format"))
        break;
    }

    if (isKstatReporting())
      Report.reportKstat();

    if (common.get_debug(common.PRINT_FS_COUNTERS))
      Blocked.printCountersToLog();

    if (CpuStats.isCpuReporting())
      writeFlatCpu(kc_total);
    total_fwd.writeFlat("" + Report.getInterval(), kc_total);
    Flat.printInterval();

    total_fwd.writeBinFile();
  }


  /**
   * Report run-level totals.
   */
  public static FwdStats reportRunTotals()
  {
    String avg = Report.getAvgLabel();

    /* Look at all hosts: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host           = (Host) Host.getDefinedHosts().elementAt(i);
      FwdStats fwd_host   = new FwdStats();
      Kstat_cpu kstat_cpu = host.getSummaryReport().getData().getTotalCpuStats();

      /* Look at all slaves: */
      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave  = (Slave) host.getSlaves().elementAt(j);
        if (slave.getCurrentWork() == null)
          continue;

        Report.getReport(slave).getData().reportFwdTotal(kstat_cpu, avg);
      }

      Report.getReport(host).getData().reportFwdTotal(kstat_cpu, avg);

      if (NfsStats.areNfsReportsNeeded())
        host.PrintNfsstatTotals(avg);
    }

    Kstat_cpu kstat_cpu = Report.getSummaryReport().getData().getTotalCpuStats();
    FwdStats fwd_total  = Report.getSummaryReport().getData().getTotalFwdStats();

    /* Now report these numbers: */
    fwd_total.printLine(Report.getSummaryReport(), kstat_cpu, avg);
    if (!Vdbmain.kstat_console)
      fwd_total.printLine(Report.getStdoutReport(), kstat_cpu, avg);

    /* Write the run total also in the totals file: */
    if (total_header_lines++ % 10 == 1)
      fwd_total.printHeaders(Report.getTotalReport());
    fwd_total.printLine(Report.getTotalReport(), kstat_cpu, avg);

    fwd_total.writeFlat(avg, kstat_cpu);
    if (CpuStats.isCpuReporting())
      Report.writeFlatCpu(kstat_cpu);

    /* Report FSD and FWD statistics: */
    ReportData.reportFwdTotals(FsdEntry.getFsdNames(), kstat_cpu, avg);
    ReportData.reportFwdTotals(FwdEntry.getFwdNames(), kstat_cpu, avg);

    /* Report summary histogram: */
    Report report = Report.getReport("histogram");
    String title = "Total of all requested operations: ";
    report.println(fwd_total.getTotalHistogram().printHistogram(title));

    for (int i = 0; i < FsdEntry.getFsdList().size(); i++)
    {
      FsdEntry fsd = (FsdEntry) FsdEntry.getFsdList().elementAt(i);

      /* Report summary histogram: */
      report        = Report.getReport(fsd, "histogram");
      ReportData rs = report.getData();
      report.println(rs.getTotalFwdStats().getTotalHistogram().printHistogram(title));
    }

    for (int i = 0; i < FwdEntry.getFwdList().size(); i++)
    {
      FwdEntry fwd = (FwdEntry) FwdEntry.getFwdList().elementAt(i);

      /* Report summary histogram: */
      report        = Report.getReport(fwd, "histogram");
      ReportData rs = report.getData();
      report.println(rs.getTotalFwdStats().getTotalHistogram().printHistogram(title));

      /* There is a bug: when running multiple formats there are multiple    */
      /* format FWDs and the code is writing a line for each possible format */
      /* on the same file. Just end it after the first.                      */
      if (fwd.fwd_name.equals("format"))
        break;
    }

    return fwd_total;
  }
}


