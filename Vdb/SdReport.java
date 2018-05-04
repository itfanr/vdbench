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
import Utils.printf;
import Utils.OS_cmd;

/**
 */
public class SdReport extends Report
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /**
   *  Create SD reports showing the the run numbers.
   */
  public static void createRunSdReports()
  {
    Vector report_list  = new Vector(8);
    Vector name_list    = new Vector(8);

    /* Create all SD reports: */
    String[] all_sds = AllWork.getSdList();
    Arrays.sort(all_sds, new SdSort());

    for (int k = 0; k < all_sds.length; k++)
    {
      String sdname = all_sds[k];
      Report report = new Report(sdname, "SD report for sd=" + sdname);
      report_list.add(report);
      name_list.add(sdname);
    }

    getSummaryReport().printMultipleLinks(report_list, name_list, "sd");
  }

  /**
   * Create Slave SD reports for a specific host
   */
  public static void createSlaveSdReports(Host host)
  {
    /* Create all slave SD reports: */
    for (int j = 0; j < host.getSlaves().size(); j++)
    {
      Slave slave = (Slave) host.getSlaves().elementAt(j);
      String[] slave_sds = AllWork.getSdList(host);
      Arrays.sort(slave_sds, new SdSort());

      for (int k = 0; k < slave_sds.length; k++)
      {
        String sdname = slave_sds[k];
        Report report = new Report(slave.getLabel() + "." + sdname,
                                   "Slave SD report for sd=" + sdname);
        slave.addReport(sdname, report);

        String txt = "sd=" + sdname + ",host=" + host.getLabel() +
                     ",lun=" + host.getLunNameForSd(sdname);

        if (k == 0)
          slave.getSummaryReport().printHtmlLink("Links to slave SD reports",
                                                 report.getFileName(), txt);
        else
          slave.getSummaryReport().printHtmlLink(null, report.getFileName(), txt);
      }
    }
  }


  /**
   * Create SD reports for all hosts.
   * (Only done when there is more than one host. Avoids duplication of reports)
   */
  public static void createHostSdReports()
  {
    /* Note: If we have only ONE host, don't create host level SD reports */
    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);

      String[] sds = AllWork.getSdList(host);
      Arrays.sort(sds, new SdSort());

      /* Create all host SD reports: */
      if (host_detail)
      {
        for (int k = 0; k < sds.length; k++)
        {
          String sdname = sds[k];
          String txt = "Host SD report for sd=" + sdname +
                       ",host=" + host.getLabel() +
                       ",lun=" + host.getLunNameForSd(sdname);
          Report report = new Report(host.getLabel() + "." + sdname, txt);
          host.addReport(sdname, report);

          txt = "sd=" + sdname +
                ",host=" + host.getLabel() +
                ",lun=" + host.getLunNameForSd(sdname);

          if (k == 0)
            host.getSummaryReport().printHtmlLink("Link to host SD reports",
                                                  report.getFileName(), txt);
          else
            host.getSummaryReport().printHtmlLink(null, report.getFileName(), txt);
        }
      }

      if (slave_detail)
        SdReport.createSlaveSdReports(host);
    }
  }


  /**
   * Report SD statistics on:
   * - Slave SD report
   * - Slave SD summary report
   * - Host SD report
   * - Host summary report
   * - SD report
   * - Summary report
   */
  public static void reportSdStats()
  {
    /* Tell the Swat file that we have a new interval: */
    Tnfe_data.writeIntervalHeader(Reporter.interval_start_time,
                                  Reporter.interval_end_time);

    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (!host.anyWork())
        continue;

      Kstat_cpu kc_host = host.getSummaryReport().getData().getIntervalCpuStats();

      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(j);
        Work work = slave.getCurrentWork();
        if (work == null)
          continue;

        /* Report on the slave SD report: */
        if (slave_detail)
        {
          String[] sds = slave.getCurrentWork().getSdList();
          for (int k = 0; k < sds.length; k++)
          {
            SdStats stats = slave.getReport(sds[k]).getData().getIntervalSdStats();
            slave.getReport(sds[k]).reportDetail(stats, kc_host);
          }
        }

        /* Report on the slave summary report: */
        SdStats stats = slave.getSummaryReport().getData().getIntervalSdStats();
        slave.getSummaryReport().reportDetail(stats, kc_host);
      }


      /* Report on the host SD report: */
      if (host_detail)
      {
        String[] sds = AllWork.getSdList(host);
        for (int j = 0; j < sds.length; j++)
        {
          String sdname = sds[j];
          SdStats stats = host.getReport(sdname).getData().getIntervalSdStats();
          host.getReport(sdname).reportDetail(stats, kc_host);
        }
      }

      /* Report on the host summary report: */
      SdStats stats = host.getSummaryReport().getData().getIntervalSdStats();
      host.getSummaryReport().reportDetail(stats, kc_host);
    }

    /* Get total cpu info: */
    Kstat_cpu kc_total = Report.getSummaryReport().getData().getIntervalCpuStats();

    /* Report on the SD report: */
    String[] sds = AllWork.getSdList(RD_entry.next_rd);
    for (int j = 0; j < sds.length; j++)
    {
      String sdname = sds[j];
      SdStats stats = Report.getReport(sdname).getData().getIntervalSdStats();
      getReport(sdname).reportDetail(stats, kc_total);
      Tnfe_data.writeOneSd(sdname, stats);
    }

    /* Report on the summary report: */
    SdStats stats = getSummaryReport().getData().getIntervalSdStats();
    if (stats != null)
    {
      getSummaryReport().reportDetail(stats, kc_total);

      FinalTotals.add(stats);

      if (!Vdbmain.kstat_console)
      {
        if (!common.get_debug(common.ORACLE_NO_CONSOLE))
          getStdoutReport().reportDetail(stats, kc_total);
      }
    }

    /* Send (optional) data to Gui: */
    if (Vdbmain.gui_server != null)
      Vdbmain.gui_server.send_interval_data(stats);

    writeFlat(stats, "" + getInterval());
  }



  /**
   * Report SD total statistics on:
   * - Slave SD report
   * - Slave SD summary report
   * - Host SD report
   * - Host summary report
   * - SD report
   * - Summary report
   */
  public static SdStats reportSdTotalStats()
  {
    String avg   = Report.getAvgLabel();
    String[] sds = Work.getSdsForRun();

    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (!host.anyWork())
        continue;

      Kstat_cpu kc = host.getSummaryReport().getData().getTotalCpuStats();

      /* Report on slave SD reports: */
      for (int j = 0; j < host.getSlaves().size(); j++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(j);
        Work work   = slave.getCurrentWork();
        if (work == null)
          continue;

        /* Report on the slave SD reports: */
        if (slave_detail)
        {
          sds = slave.getCurrentWork().getSdList();
          for (int k = 0; k < sds.length; k++)
          {
            SdStats stats = slave.getReport(sds[k]).getData().getTotalSdStats();
            slave.getReport(sds[k]).reportDetail(stats, kc, avg);
          }
        }

        /* Report on the slave summary report: */
        SdStats slave_sum = slave.getSummaryReport().getData().getTotalSdStats();
        slave.getSummaryReport().reportDetail(slave_sum, kc, avg);
      }


      /* Report on the host SD report: */
      if (host_detail)
      {
        sds = Work.getSdsForRun(host);
        for (int j = 0; j < sds.length; j++)
        {
          ReportData rs = host.getReport(sds[j]).getData();
          SdStats stats = rs.getTotalSdStats();
          host.getReport(sds[j]).reportDetail(stats, kc, avg);
        }
      }

      /* Report on the host summary report: */
      SdStats host_sum = host.getSummaryReport().getData().getTotalSdStats();
      host.getSummaryReport().reportDetail(host_sum, kc, avg);
    }

    /* Get cpu totals: */
    Kstat_cpu kc_total = Report.getSummaryReport().getData().getTotalCpuStats();

    /* Report SD totals: */
    for (int i = 0; i < sds.length; i++)
    {
      SdStats stats = getReport(sds[i]).getData().getTotalSdStats();
      getReport(sds[i]).reportDetail(stats, kc_total, avg);
    }

    /* Report on the summary report: */
    SdStats run_totals = getSummaryReport().getData().getTotalSdStats();
    getSummaryReport().reportDetail(run_totals, kc_total, avg);
    if (!Vdbmain.kstat_console)
    {
      if (!common.get_debug(common.ORACLE_NO_CONSOLE))
        getStdoutReport().reportDetail(run_totals, kc_total, avg);
    }

    Report.writeFlat(run_totals, avg);
    if (CpuStats.isCpuReporting())
      Report.writeFlatCpu(getSummaryReport().getData().getTotalCpuStats());

    CpuStats.cpu_shortage();

    return run_totals;
  }
}
