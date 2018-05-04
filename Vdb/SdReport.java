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
import Utils.printf;
import Utils.OS_cmd;

/**
 */
public class SdReport extends Report
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static int total_header_calls = 1;

  /**
   *  Create SD reports showing the total run statistics.
   */
  public static void createRunSdReports()
  {
    Vector report_list  = new Vector(8);
    Vector name_list    = new Vector(8);

    /* Create all SD reports: */
    String[] all_sds = RD_entry.getSdNamesUsed();
    Arrays.sort(all_sds, new SdSort());

    for (String sdname : all_sds)
    {
      Report report = new Report(sdname, "SD report for sd=" + sdname, sdDetailNeeded());
      report_list.add(report);
      name_list.add(sdname);

      if (sdDetailNeeded())
      {
        Report hist = new Report(sdname, "histogram", "SD response time histogram.");
        report.printHtmlLink("Link to response time histogram",
                             hist.getFileName(), "histogram");
      }
    }

    if (sdDetailNeeded())
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
      String[] slave_sds = RD_entry.getSdsUsedForHost(host);
      Arrays.sort(slave_sds, new SdSort());

      for (int k = 0; k < slave_sds.length; k++)
      {
        String sdname = slave_sds[k];
        Report report = new Report(slave.getLabel() + "." + sdname,
                                   "Slave SD report for sd=" + sdname);
        slave.addReport(sdname, report);

        String txt = "sd=" + sdname + ",host=" + host.getLabel() +
                     ",lun=" + host.getLunNameForSd(SD_entry.findSD(sdname));

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

      String[] sds = RD_entry.getSdsUsedForHost(host);
      Arrays.sort(sds, new SdSort());

      /* Create all host SD reports: */
      if (host_detail)
      {
        for (int k = 0; k < sds.length; k++)
        {
          String sdname = sds[k];
          String txt = "Host SD report for sd=" + sdname +
                       ",host=" + host.getLabel() +
                       ",lun=" + host.getLunNameForSd(SD_entry.findSD(sdname));
          Report report = new Report(host.getLabel() + "." + sdname, txt);
          host.addReport(sdname, report);

          txt = "sd=" + sdname +
                ",host=" + host.getLabel() +
                ",lun=" + host.getLunNameForSd(SD_entry.findSD(sdname));

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
   *
   * Host and slave reports are by default suppressed, see reports=(slave,host)
   */
  public static void reportSdStats()
  {
    RD_entry rd = RD_entry.next_rd;

    /* Tell the Swat file that we have a new interval: */
    Tnfe_data.writeIntervalHeader(false, Reporter.interval_start_time,
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
          SD_entry[] sds = slave.getCurrentWork().getSdList();
          for (int k = 0; k < sds.length; k++)
          {
            SdStats stats = slave.getReport(sds[k].sd_name).getData().getIntervalSdStats();
            slave.getReport(sds[k].sd_name).reportDetail(stats, kc_host);
          }
        }

        /* Report on the slave summary report: */
        SdStats stats = slave.getSummaryReport().getData().getIntervalSdStats();
        slave.getSummaryReport().reportDetail(stats, kc_host);
      }


      /* Report on the host SD report: */
      if (host_detail)
      {
        String[] sds = rd.getSdsUsedForHostThisRd(host);
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
    String[] sds = RD_entry.next_rd.getSdNamesUsed();
    for (int j = 0; j < sds.length; j++)
    {
      String sdname = sds[j];
      SdStats stats = Report.getReport(sdname).getData().getIntervalSdStats();
      getReport(sdname).reportDetail(stats, kc_total);
      Tnfe_data.writeOneSd(false, sdname, stats);
    }
    Tnfe_data.flush();

    /* Report on the summary report: */
    SdStats stats = getSummaryReport().getData().getIntervalSdStats();
    if (stats != null)
    {
      getSummaryReport().reportDetail(stats, kc_total);

      FinalTotals.add(stats);

      if (!Vdbmain.kstat_console)
        getStdoutReport().reportDetail(stats, kc_total);
    }

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
    String     avg = Report.getAvgLabel();
    SD_entry[] sds = Work.getSdsForRun();

    Tnfe_data.writeIntervalHeader(true, Reporter.run_start_time,
                                  Reporter.run_end_time);

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
            SdStats stats = slave.getReport(sds[k].sd_name).getData().getTotalSdStats();
            slave.getReport(sds[k].sd_name).reportDetail(stats, kc, avg);
          }
        }

        /* Report on the slave summary report: */
        SdStats slave_sum = slave.getSummaryReport().getData().getTotalSdStats();
        slave.getSummaryReport().reportDetail(slave_sum, kc, avg);

        if (host.getSlaves().size() > 1)
          getReport(slave.getLabel(), "histogram").println(slave_sum.printHistograms());
      }


      /* Report on the host SD report: */
      if (host_detail)
      {
        sds = Work.getHostSdsForRun(host);
        for (int j = 0; j < sds.length; j++)
        {
          ReportData rs = host.getReport(sds[j].sd_name).getData();
          SdStats stats = rs.getTotalSdStats();
          host.getReport(sds[j].sd_name).reportDetail(stats, kc, avg);
        }
      }

      /* Report on the host summary report: */
      SdStats host_sum = host.getSummaryReport().getData().getTotalSdStats();
      host.getSummaryReport().reportDetail(host_sum, kc, avg);

      if (hosts.size() > 1)
        getReport(host.getLabel(), "histogram").println(host_sum.printHistograms());
    }

    /* Get cpu totals: */
    Kstat_cpu kc_total = Report.getSummaryReport().getData().getTotalCpuStats();

    /* Report SD totals: */
    for (int i = 0; Report.sdDetailNeeded() && i < sds.length; i++)
    {
      SD_entry sd = sds[i];
      SdStats stats = getReport(sd.sd_name).getData().getTotalSdStats();
      getReport(sd.sd_name).reportDetail(stats, kc_total, avg);
      getReport(sd.sd_name, "histogram").println(stats.printHistograms());
      Tnfe_data.writeOneSd(true, sd.sd_name, stats);
    }
    Tnfe_data.flush();

    /* Report on the summary report: */
    SdStats run_totals = getSummaryReport().getData().getTotalSdStats();
    getSummaryReport().reportDetail(run_totals, kc_total, avg);

    getTotalReport().printHeaders(getTotalReport().getWriter(), kc_total, total_header_calls);
    total_header_calls+=2;
    getTotalReport().reportDetail(run_totals, kc_total, avg);
    if (!Vdbmain.kstat_console)
      getStdoutReport().reportDetail(run_totals, kc_total, avg);

    Report.writeFlat(run_totals, avg);
    if (CpuStats.isCpuReporting())
      Report.writeFlatCpu(getSummaryReport().getData().getTotalCpuStats());

    return run_totals;
  }
}
