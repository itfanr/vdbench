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

import java.io.*;
import java.util.*;

import Utils.Format;
import Utils.Fput;


/**
 * This class handles File system reports
 */
public class FwdReport extends Report
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


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
    }

    Kstat_cpu kstat_cpu = Report.getSummaryReport().getData().getTotalCpuStats();
    FwdStats fwd_total  = Report.getSummaryReport().getData().getTotalFwdStats();

    /* Now report these numbers: */
    fwd_total.printLine(Report.getSummaryReport(), kstat_cpu, avg);
    if (!Vdbmain.kstat_console)
      fwd_total.printLine(Report.getStdoutReport(), kstat_cpu, avg);

    fwd_total.writeFlat(avg, kstat_cpu);
    if (CpuStats.isCpuReporting())
      Report.writeFlatCpu(kstat_cpu);

    /* Report FSD and FWD statistics: */
    ReportData.reportFwdTotals(FsdEntry.getFsdNames(), kstat_cpu, avg);
    ReportData.reportFwdTotals(FwdEntry.getFwdNames(), kstat_cpu, avg);

    /* Report summary histogram: */
    Report report = Report.getReport("histogram");
    report.println("Total of all requested operations: ");
    report.println(fwd_total.getTotalHistogram().printit());

    for (int i = 0; i < FsdEntry.getFsdList().size(); i++)
    {
      FsdEntry fsd = (FsdEntry) FsdEntry.getFsdList().elementAt(i);

      /* Report summary histogram: */
      report        = Report.getReport(fsd, "histogram");
      ReportData rs = report.getData();
      report.println(rs.getTotalFwdStats().getTotalHistogram().printit());
    }

    for (int i = 0; i < FwdEntry.getFwdList().size(); i++)
    {
      FwdEntry fwd = (FwdEntry) FwdEntry.getFwdList().elementAt(i);

      /* Report summary histogram: */
      report        = Report.getReport(fwd, "histogram");
      ReportData rs = report.getData();
      report.println(rs.getTotalFwdStats().getTotalHistogram().printit());
    }

    CpuStats.cpu_shortage();

    return fwd_total;
  }
}


