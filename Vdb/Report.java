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
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import Utils.Format;
import Utils.Fput;
import Utils.printf;
import Utils.OS_cmd;

/**
 * This handles all the performance reporting.
 *
 * Note: The files below are created BEFORE we start the slaves so we don't know
 * anything about the hosts/slaves/luns/files during the report creation
 * process.
 */
public class Report
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String      fname;
  private PrintWriter pw;
  private String      headers       = null;
  private String      kstat_headers = null;

  private ReportData  data = null;

  protected static boolean slave_detail = false;
  protected static boolean host_detail  = false;

  private static HashMap report_map    = new HashMap(8);
  private static Report logfile_report = null;
  private static Report summary_report = null;
  private static Report kstat_summary  = null;
  private static Report stdout_report  = null;

  private static int interval_duration;
  private static int current_interval;  /* These are stored at the same time. */
  private static int highest_interval;  /* it just reads better.              */

  private static Vector all_writers      = new Vector(64, 0);
  private static Vector writer_filenames = new Vector(64, 0);
  private static Vector all_reports      = new Vector(64, 0);

  private static boolean binary_or_decimal = true;
  public static long KB = 1024;       /* values changed by setDecimal() below */
  public static long MB = 1024 * 1024;
  public static long GB = 1024 * 1024 * 1024;
  public static long TB = 1024 * 1024 * 1024 * 1024;

  public Report()
  {
    common.failure("This instantiation only to prevent 'extends Report' compiler message");
  }

  public Report(Object l1, String fname_in, String title)
  {
    this(xlate(l1) + "." + fname_in, title);
  }

  public Report(String fname_in, String title)
  {
   //if (fname_in.indexOf("localhost.kstat") != -1)
   //  common.where(8);

    fname = fname_in;
    if (fname.endsWith(".html"))
      common.failure("At this point, file name may not include '.html' yet: " + fname);
    pw    = createHmtlFile(fname, title);
    all_writers.add(this.pw);
    all_reports.add(this);
    writer_filenames.add(fname);
    data = new ReportData(this);
    addReport();
  }

  public Report(PrintWriter pw_in, String fnam, String title)
  {
    pw    = pw_in;
    fname = fnam;
    all_writers.add(pw);
    all_reports.add(this);
    writer_filenames.add("Directly assigned pw: " + title);

    /* For some of these reports this data is not used, but it is easier to */
    /* just give them these fields to avoid null exceptions:                */
    data = new ReportData(this);
  }


  /**
   * All reports are stored in a static HashMap here.
   */
  private void addReport()
  {
    if (fname.indexOf(" ") != -1)
      common.failure("Blank embedded Report name: " + fname);

    if (fname.equals("localhost-0.fsd1"))
      common.failure("xx");

    if (report_map.put(fname, this) != null)
    {
      common.ptod("Trying to add report twice: " + fname);
      common.ptod("Are there duplicate names between HD/SD/WD/FSD/FWD/RD parameters?");
      printMap();
      common.failure("Trying to add report name twice: " + fname);
    }
  }


  public ReportData getData()
  {
    return data;
  }

  /**
   * Any output to stdout will be also sent to logfile.html
   *
   * When you see something printing directly on common.summ_html within this
   * class it is mainly because we don't want to show that line on stdout and
   * logfile, e.g. html href= links.
   */
  public void println(Vector lines)
  {
    for (int i = 0; i < lines.size(); i++)
      println((String) lines.elementAt(i));
  }
  public void println(String txt)
  {
    //prtDebug(txt);
    pw.println(txt);

    if (fname == null)
      common.failure("null file name");

    if (pw.equals(common.stdout) && txt.toLowerCase().indexOf("href=") == -1)
    {
      if (txt.trim().length() != 0)
        common.log_html.println(txt);
    }
  }
  public void print(String txt)
  {
    //prtDebug(txt);
    pw.print(txt);

    if (pw.equals(common.stdout) && txt.toLowerCase().indexOf("href=") == -1)
    {
      if (txt.trim().length() != 0)
        common.log_html.print(txt);
    }
  }

  private void prtDebug(String txt)
  {
    if (fname.equals("sd1"))
    {
      common.ptod("txt: " + fname + " " + txt);
      common.where(5);
    }
  }


  public static void setSummaryReport(Report report)
  {
    summary_report = report;
    report.addReport();
  }
  public static Report getSummaryReport()
  {
    return summary_report;
  }
  public static void setLogReport(Report report)
  {
    logfile_report = report;
    report.addReport();
  }
  public static Report getLogReport()
  {
    return logfile_report;
  }
  public static Report getStdoutReport()
  {
    return stdout_report;
  }


  /**
   * Create summary and SD level reports.
   * - summary.html (Created elsewhere)
   *   - sd1.html
   *   - sd2.html
   *   - host-a.html
   *     - slave-1.html
   *       - sd1.html
   *       - sd2.html
   *     - slave-n.html
   *   - host-b.html
   *     - slave-1.html
   *   etc.
   *
   * Since not all slaves/hosts/sds are used everywhere, prevent a report from
   * being created that would end up being empty.
   *
   * Each host and each slave will have one summary report and an SD report for
   * each SD that is actually used.
   *
   * The PrintWriter instances for each SD report are stored in the respective
   * Host and Slave instances in a HashMap to be retrieved using the SD name.
   *
  */
  public static void createHostSummaryFiles()
  {
    Report summ = getSummaryReport();
    Vector report_list = new Vector(8);
    Vector name_list = new Vector(8);

    /* These host summary reports all need to be created BEFORE we start */
    /* slaves (A chicken and egg thingy)                                 */
    Vector hosts = Host.getDefinedHosts();
    summ.println("");
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);

      Report report = new Report(host.getLabel(),
                                 "Host summary report(s) for host=" + host.getLabel());
      host.setSummaryReport(report);
      report_list.add(report);
      name_list.add(host.getLabel());
    }

    getSummaryReport().printMultipleLinks(report_list,name_list, "host");

    /* Create FWD and FSD detail reports: */
    Report.createNamedReports(FsdEntry.getFsdNames(), "fsd");
    Report.createNamedReports(FwdEntry.getFwdNames(), "fwd");
  }

  public static void createSlaveSummaryFiles()
  {
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      slave.createSummaryFile();
    }
  }

  public static void createSlaveFsdFiles()
  {
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      for (int j = 0; j < FsdEntry.getFsdList().size(); j++)
      {
        FsdEntry fsd = (FsdEntry) FsdEntry.getFsdList().elementAt(j);
        Report report = new Report(slave.getLabel(), fsd.name, "abcxyz");
        slave.getSummaryReport().printHtmlLink("Links to FSD report",
                                               report.getFileName(), fsd.name);
      }
    }
  }

  public static void createSummaryHistogramFile()
  {
    Report report = new Report("histogram", "Total Performance histogram.");
    getSummaryReport().printHtmlLink("Link to response time histogram",
                                     report.getFileName(), "histogram");
  }

  public static void tbd_createHostHistogramFiles()
  {
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      Report report = new Report(host, "histogram", "Host Performance histogram.");
      host.getSummaryReport().printHtmlLink("Link to response time histogram",
                                            report.getFileName(), "histogram");
    }
  }

  public static void tbd_createSlaveHistogramFiles()
  {
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      Report report = new Report(slave, "histogram", "Host Performance histogram.");
      slave.getSummaryReport().printHtmlLink("Link to response time histogram",
                                             report.getFileName(), "histogram");
    }
  }


  /**
   * Create more report files.
   */
  public static void createOtherReportFiles()
  {
    /* Create a 'fake' Report() for stdout: */
    stdout_report = new Report(common.stdout, "stdout", "common.stdout");

    /* Scan through all RDs. At the end we will know for each */
    /* slave and each host which SDs are used anywhere.       */
    /* (Information is stored by Work.prepareWgWork()         */

    /* During a report creation, AllWork() should know about ALL runs: */
    AllWork.clearWork();

    RD_entry.next_rd = null;
    RD_entry rd;
    int runs = 0;
    while (true)
    {
      if ((rd = RD_entry.getNextWorkload()) == null)
        break;

      /* Prepare work for slaves. This one does not get send to the slaves yet. */
      /* This code so nicely determines which slaves get which SDs, so we might */
      /* as well reuse this code to find out which files to create:             */
      Work.prepareWorkForSlaves(rd, false);
    }

    createKstatReports();

    /* Open Anchor report if needed: */
    if (Vdbmain.isFwdWorkload())
      AnchorReport.create();

    /* If more than one host, each host gets his own SD reports: */
    SdReport.createHostSdReports();

    /* Create SD reports that contains run totals of all hosts and slaves: */
    SdReport.createRunSdReports();
    WdReport.createRunWdReports();

    if (MiscParms.maintain_run_totals)
      Report.getSummaryReport().printHtmlLink("Link to run totals", "totals", "totals");

    Flat.define_column_headers_cpu();

    /* Now that we have all reports, write RD html links on them: */
    printRdLinks();

    /* Make sure everyone can read these reports: */
    chModAllReports();

    /* For debugging: */
    showReports();
  }


  /**
   *  Create Fsd or Fwd reports showing their detail statistics
   */
  public static void createNamedReports(String names[], String label)
  {
    Arrays.sort(names);
    Vector report_list = new Vector(8);
    Vector name_list   = new Vector(8);

    for (int i = 0; i < names.length; i++)
    {
      Report report = new Report(names[i], "Report for " + label + " " + names[i]);
      report_list.add(report);
      name_list.add(names[i]);

      Report histreport = new Report(names[i] + ".histogram",
                                     "Performance histogram for " +
                                     label.toUpperCase() + "=" + names[i]);

      /* For now only create histograms for FSD: */
      //if (label.equalsIgnoreCase("fsd"))
        report.printHtmlLink("Link to Performance histogram",
                             histreport.getFileName(), "histogram");
    }

    getSummaryReport().printMultipleLinks(report_list, name_list, label);
  }


  public void printMultipleLinks(Vector report_list, Vector name_list, String type)
  {
    for (int i = 0; i < report_list.size(); i++)
    {
      Report report = (Report) report_list.elementAt(i);
      String name   = (String) name_list.elementAt(i);
      if (i == 0)
        print(Format.f("%-32s", "Link to " + type.toUpperCase() + " reports:"));

      else if (i % 8 == 0)
      {
        println("");
        print(Format.f("%-32s", ""));
      }

      String blanks = Format.f("%80s", " ");
      if (name.length() < 8)
        blanks = blanks.substring(0, 8 - name.length());
      else
        blanks = "";
      print(" <A HREF=\"" + report.getFileName() + ".html\">" +
            name + "</A>" + blanks);
    }

    /* Blank line separator: */
    if (report_list.size() > 0)
      println("");
  }


  /**
   * Create an html link directly pointing to the start of an RD
   */
  private static void printRdLinks()
  {
    Report[] reps = Report.getReports();
    for (int i = 0; i < reps.length; i++)
    {
      if (!reps[i].fname.equals("stdout"))
        reps[i].printRdLink();
    }
  }
  private void printRdLink()
  {
    println("");
    RD_entry rd = RD_entry.next_rd = null;
    int runs = 0;
    while (true)
    {
      if ((rd = RD_entry.getNextWorkload()) == null)
        break;

      /* Create an html link to the start of each RD: */
      /* Since 'format' ignores the WSS, do not include any loops: */
      String label;
      if (rd.rd_name.startsWith(RD_entry.FORMAT_RUN))
        label = rd.rd_name;
      else
        label = rd.rd_name + " " + rd.current_override.getText();

      /* Create an html link to the start of each RD: */
      if (runs++ == 0)
      {
        String txt = Format.f("%-32s", "Link to Run Definitions:");
        println(txt + " <A HREF=\"#_" + rd.hashCode() + "\">" +
                label + "</A>");
      }
      else
      {
        String txt = Format.f("%-32s", " ");
        println(txt + " <A HREF=\"#_" + rd.hashCode() + "\">" +
                label + "</A>");
      }
    }

    println("");
  }


  /**
   * Create Kstat reports for those hosts that provide us with Kstat info.
   */
  private static void createKstatReports()
  {
    HashMap hosts_used = new HashMap(6);
    HashMap host_luns  = new HashMap(6);

    /* Create list of unique host+lun combinations: */
    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (host.getHostInfo() == null)
        continue;
      Vector pointers = host.getHostInfo().getInstancePointers();
      if (pointers == null)
        continue;

      for (int j = 0; j < pointers.size(); j++)
      {
        InstancePointer ip = (InstancePointer) pointers.elementAt(j);
        hosts_used.put(host.getLabel(), null);
        host_luns.put(host.getLabel() + " " + ip.getLun(), null);
      }
    }

    /* If there's no kstat at all, just exit: */
    String[] host_names = (String[]) hosts_used.keySet().toArray(new String[0]);
    if (host_names.length == 0)
      return;


    /* Create run-level kstat reports: */
    kstat_summary = new Report("kstat", "Kstat summary report");
    getSummaryReport().printHtmlLink("Link to Kstat summary report",
                                     kstat_summary.getFileName(), "Kstat");


    /* Create host-level kstat reports: */
    for (int i = 0; i < host_names.length; i++)
    {
      Host host       = Host.findHost(host_names[i]);
      Report report   = new Report(host.getLabel() + ".kstat",
                                   "Host Kstat summary report for host=" + host.getLabel());
      host.setKstatReport(report);
      kstat_summary.printHtmlLink("Host Kstat summary report",
                                  report.getFileName(), host_names[i]);
      host.addReport("kstat", report);

      /* Get error messages if any: */
      Vector luns = host.getHostInfo().getLuns();
      for (int j = 0; j < luns.size(); j++)
      {
        LunInfoFromHost linfo = (LunInfoFromHost) luns.elementAt(j);
        for (int k = 0; k < linfo.kstat_error_messages.size(); k++)
        {
          report.println("host=" + host_names[i] + ",lun=" + linfo.lun +
                         ": " +
                         (String) linfo.kstat_error_messages.elementAt(k));
          kstat_summary.println("host=" + host_names[i] + ",lun=" + linfo.lun +
                                ": " +
                                (String) linfo.kstat_error_messages.elementAt(k));
        }
        if (linfo.kstat_error_messages.size() > 0)
          report.println("");
      }


      /* Create host level instance reports: */
      Vector ptrs = host.getHostInfo().getInstancePointers();
      InstancePointer[] pointers = (InstancePointer[]) ptrs.toArray(new InstancePointer[0]);
      Arrays.sort(pointers);

      for (int j = 0; j < pointers.length; j++)
      {
        InstancePointer ip = pointers[j];
        report = new Report(host.getLabel() + "." + ip.getID(),
                            "Host Kstat instance report for host=" + host.getLabel());
        host.addReport(ip.getID(), report);

        host.getKstatReport().printHtmlLink(ip.getLun(), report.getFileName(),
                                            ip.getInstance());

        if (ip.getInstance().startsWith("nfs"))
          NfsStats.setNfsReportsNeeded(true);

        report.println("Device:   " + ip.getLun());
        report.println("Instance: " + ip.getInstance());
      }
    }


    Flat.define_column_headers_kstat();

    if (NfsStats.areNfsReportsNeeded())
      NfsStats.createNfsReports();
  }


  /**
   * Print HTML link.
   * Use a minimum 32 byte width for the label to allow for easier alignment.
   */
  public void printHtmlLink(String label,
                            String fname,
                            String click)
  {
    String txt;
    if (label != null)
      txt = Format.f("%-32s", label + ":");
    else
      txt = Format.f("%-32s", "");

    println (txt + " <A HREF=\"" + fname + ".html\">" + click + "</A>");
  }


  public static Report getHistReport(String name)
  {
    return getReport(name + ".histogram");
  }
  public static Report getHistReport(Host host, String name)
  {
    return getReport(host.getLabel() + "." + name + ".histogram");
  }
  public static Report getHistReport(Slave slave, String name)
  {
    return getReport(slave.getLabel() + "." + name + ".histogram");
  }
  public static Report[] getReports()
  {
    return(Report[]) report_map.values().toArray(new Report[0]);
  }

  public static Report getReport(Object l1)
  {
    return getReport(xlate(l1));
  }
  public static Report getReport(Object l1, Object l2)
  {
    return getReport(xlate(l1, l2));
  }
  public static Report getReport(Object l1, Object l2, Object l3)
  {
    return getReport(xlate(l1, l2, l3));
  }


  private static String xlate(Object l1)
  {
    return xlate1(l1);
  }
  private static String xlate(Object l1, Object l2)
  {
    return xlate1(l1) + "." + xlate1(l2);
  }
  private static String xlate(Object l1, Object l2, Object l3)
  {
    return xlate1(l1) + "." + xlate1(l2) + "." + xlate1(l3);
  }


  private static String xlate1(Object obj)
  {
    String s1 = null;
    if (obj instanceof Host)
      s1 = ((Host) obj).getLabel();
    else if (obj instanceof Slave)
      s1 = ((Slave) obj).getLabel();
    else if (obj instanceof FwdEntry)
      s1 = ((FwdEntry) obj).fwd_name;
    else if (obj instanceof FsdEntry)
      s1 = ((FsdEntry) obj).name;
    else if (obj instanceof String)
      s1 = ((String) obj);
    else
      common.failure("Invalid object: " + obj.getClass().getName());

    return s1;
  }


  public static Report getReport(String name)
  {
    Report report = (Report) report_map.get(name);
    if (report == null)
    {
      printMap();
      common.failure("Requesting an unknown report file: " + name);
    }

    return report;
  }

  private static void printMap()
  {
    String[] reps = (String[]) report_map.keySet().toArray(new String[0]);
    Arrays.sort(reps);
    for (int i = 0; i < reps.length; i++)
      common.ptod("getReport(): " + reps[i]);
  }

  public static String[] getReportedKstats()
  {
    return Report.filterSds(report_map, false);
  }

  public String getFileName()
  {
    return fname;
  }


  /**
   * Print one line of simple statistics.
   */
  protected void reportDetail(SdStats st)
  {
    /* Print headers if needed: */
    printHeaders(getWriter(), null, getInterval());

    reportDetail(st, null, "" + getInterval());
  }
  protected void reportDetail(SdStats st, Kstat_cpu kc)
  {
    /* Print headers if needed: */
    printHeaders(getWriter(), kc, getInterval());

    reportDetail(st, kc, "" + getInterval());
  }
  protected void reportDetail(SdStats st, Kstat_cpu kc, String title)
  {
    printf mask;
    if (kc != null)
      mask = new printf("%10s %10.2f %8.2f %7d %6.2f %8.3f %8.3f %8.3f %7.1f %5.1f");
    else
      mask = new printf("%10s %10.2f %8.2f %7d %6.2f %8.3f %8.3f %8.3f");

    mask.add(title            );
    mask.add(st.rate()        );
    mask.add(st.megabytes()   );
    mask.add(st.bytes()       );
    mask.add(st.readpct()     );
    mask.add(st.respTime()    );
    mask.add(st.respmax()     );
    mask.add(st.resptime_std());

    if (kc != null)
    {
      mask.add(kc.user_pct() + kc.kernel_pct());
      mask.add(kc.kernel_pct());
    }

    String txt = common.tod() + mask.print();
    println(txt);
  }




  /**
   * Display a 'Starting RD=' message on most reports
   */
  public static void displayRunStart(String txt, RD_entry rd)
  {
    Report[] reports = getReports();
    for (int i = 0; i < reports.length; i++)
    {
      if (!reports[i].equals("stdout"))
        reports[i].printStart(txt, rd);
    }
  }


  private void printStart(String txt, RD_entry rd)
  {
    String label = rd.rd_name + " " + rd.current_override.getText();

    println("");
    printRdLink(common.tod() + " " + txt,
                "<a name=\"_" + rd.hashCode() + "\"></a><i><b>" +
                common.tod() + " " + txt + "</b></i>");
    println("");
  }

  public void printRdLink(String txt, String ref)
  {
    pw.println(ref);

    if (pw.equals(common.summ_html))
    {
      common.log_html.println(txt);
      common.stdout.println(txt);
    }
  }


  public void reportKstatDetail(Kstat_data kd, Kstat_cpu kc)
  {
    /* Print headers if needed: */
    printKstatHeaders(pw, current_interval);

    reportKstatDetail(kd, kc, "" + current_interval);

  }
  public void reportKstatDetail(Kstat_data kd, Kstat_cpu kc, String title)
  {

    printf mask = new printf("%9s %10.2f %8.2f %8.2f %8.2f %7.2f %6.2f %5.1f %7.2f %7.2f %7d %7.1f %5.1f");

    mask.add(title);
    mask.add(kd.kstat_rate() );
    mask.add(kd.kstat_wait() + kd.kstat_svctm());
    mask.add(kd.kstat_wait());
    mask.add(kd.kstat_svctm());
    mask.add(kd.kstat_megabytes()) ;
    mask.add(kd.kstat_readpct());
    mask.add(kd.kstat_busy());
    mask.add(kd.kstat_ioswaiting() );
    mask.add(kd.kstat_iosrunning() );
    mask.add((int) kd.kstat_bytes());
    mask.add(kc.user_pct() + kc.kernel_pct());
    mask.add(kc.kernel_pct());

    String txt = common.tod() + " " + mask.print();
    println(txt);
  }



  public String toString()
  {
    return "Report fname: " + fname; // + " cpu: " + cpu;
  }


  /**
   * End of interval reporting.
   * Report all the numbers obtained from the slaves.
   */
  public static synchronized void reportWdInterval()
  {
    /* Report all SD statistics */
    SdReport.reportSdStats();
    WdReport.reportWdStats();

    /* Report all Kstat statistics: */
    if (isKstatReporting())
      reportKstat();

    if (MiscParms.maintain_run_totals)
      FinalTotals.printTotals(highest_interval);

    Flat.printInterval();
  }


  /**
   * Report on the Kstat summary report and on the run-level Kstat reports
   */
  public static void reportKstat()
  {
    /* Accumulate all data from all hosts: */
    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (!host.anyWork())
        continue;

      Kstat_cpu kc_host = host.getSummaryReport().getData().getIntervalCpuStats();

      if (host.getHostInfo().isSolaris())
      {
        /* This will tell us which devices we have data for: */
        Vector pointers = host.getHostInfo().getInstancePointers();

        /* Get one device at the time: */
        for (int j = 0; j < pointers.size(); j++)
        {
          InstancePointer ip = (InstancePointer) pointers.elementAt(j);

          /* Pick the last interval for this device: */
          Kstat_data ks = host.getReport(ip.getID()).getData().getIntervalKstats();

          /* Report this on this host's kstat device report: */
          Report report = host.getReport(ip.getID());
          report.reportKstatDetail(ks, kc_host);
        }

        /* Report this on the host's kstat summary report: */
        Kstat_data ks = host.getKstatReport().getData().getIntervalKstats();
        host.getKstatReport().reportKstatDetail(ks, kc_host);
      }
    }

    /* Now report the total for all of this: */
    Kstat_cpu  kc_total = getSummaryReport().getData().getIntervalCpuStats();
    Kstat_data ks_total = kstat_summary.getData().getIntervalKstats();

    kstat_summary.reportKstatDetail(ks_total, kc_total);
    FinalTotals.add(ks_total);

    if (Vdbmain.kstat_console)
      getStdoutReport().reportKstatDetail(ks_total, kc_total);

    if (CpuStats.isCpuReporting())
      writeFlatCpu(kc_total);
    writeFlatKstat(ks_total);

    if (NfsStats.areNfsReportsNeeded())
      NfsStats.PrintAllNfs("" + current_interval);
  }


  /**
   * Report on the Kstat summary report and on the run-level Kstat reports
   */
  public static void reportKstatTotals()
  {
    String avg = getAvgLabel();

    /* Accumulate all interval cpu statistics: */
    Kstat_cpu kc_total = getSummaryReport().getData().getTotalCpuStats();

    /* Accumulate all data from all hosts: */
    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (!host.anyWork())
        continue;

      Kstat_cpu  kc_host  = host.getSummaryReport().getData().getTotalCpuStats();

      if (host.getHostInfo().isSolaris())
      {
        /* This will tell us which devices we have data for: */
        Vector pointers = host.getHostInfo().getInstancePointers();

        /* Get one device at the time: */
        for (int j = 0; j < pointers.size(); j++)
        {
          InstancePointer ip = (InstancePointer) pointers.elementAt(j);

          /* Report this as total for this host, this device: */
          Report report    = host.getReport(ip.getID());
          Kstat_data ks_ip = report.getData().getTotalKstats();
          report.reportKstatDetail(ks_ip, kc_host, avg);
        }
      }

      if (host.getKstatReport() != null)
      {
        Kstat_data host_sum = host.getKstatReport().getData().getTotalKstats();
        host.getKstatReport().reportKstatDetail(host_sum, kc_host, avg);
      }
    }

    /* Now report the total for all of this: */
    Kstat_data run_sum = kstat_summary.getData().getTotalKstats();
    kstat_summary.reportKstatDetail(run_sum, kc_total, avg);
    if (Vdbmain.kstat_console)
      getStdoutReport().reportKstatDetail(run_sum, kc_total, avg);


    Report.writeFlatKstat(run_sum);
  }


  /**
   * Print headers if needed.
   */
  public void printHeaders(PrintWriter pw, Kstat_cpu kc, int interval)
  {
    headers = createHeaders(kc);

    int count = (common.get_debug(common.FAST_HEADERS)) ? 5 : 30;
    if (interval % count == 1)
      println(headers);
  }

  public void printKstatHeaders(PrintWriter pw, int interval)
  {
    if (isKstatReporting())
      kstat_headers = createKstatHeaders();

    int count = (common.get_debug(common.FAST_HEADERS)) ? 5 : 30;
    if (interval % count == 1)
      println(kstat_headers);
  }

  private String createHeaders(Kstat_cpu kc)
  {
    printf hdr1;
    if (kc != null)
      hdr1 = new printf("%10s %10s %8s %7s %6s %8s %8s %8s %7s %5s");
    else
      hdr1 = new printf("%10s %10s %8s %7s %6s %8s %8s %8s");
    hdr1.add("interval");
    hdr1.add("i/o");
    hdr1.add("MB/sec");
    hdr1.add("bytes");
    hdr1.add("read");
    hdr1.add("resp");
    hdr1.add("resp");
    hdr1.add("resp");
    if (kc != null)
    {
      hdr1.add("cpu%");
      hdr1.add("cpu%");
    }

    printf hdr2;
    if (kc != null)
      hdr2 = new printf("%10s %10s %8s %7s %6s %8s %8s %8s %7s %5s");
    else
      hdr2 = new printf("%10s %10s %8s %7s %6s %8s %8s %8s");
    hdr2.add("");
    hdr2.add("rate");
    hdr2.add((binary_or_decimal) ? "1024**2" : "1000**2");
    hdr2.add("i/o");
    hdr2.add("pct");
    hdr2.add("time");
    hdr2.add("max");
    hdr2.add("stddev");
    if (kc != null)
    {
      hdr2.add("sys+usr");
      hdr2.add("sys");
    }

    DateFormat df = new SimpleDateFormat( "MMM dd, yyyy" );
    String line1 = "\n" + df.format(new Date());
    String line2 = "\n            ";
    String txt = line1 + hdr1.print() + line2 + hdr2.print();

    return txt;
  }


  private String createKstatHeaders()
  {
    printf hdr1 = new printf(" %9s %10s %8s %8s %8s %7s %6s %5s %7s %7s %7s %7s %5s");
    hdr1.add("interval");
    hdr1.add("KSTAT_i/o");
    hdr1.add("resp");
    hdr1.add("wait");
    hdr1.add("service");
    hdr1.add("MB/sec");
    hdr1.add("read");
    hdr1.add("busy");
    hdr1.add("avg_i/o");
    hdr1.add("avg_i/o");
    hdr1.add("bytes");
    hdr1.add("cpu%");
    hdr1.add("cpu%");

    printf hdr2 = new printf(" %9s %10s %8s %8s %8s %7s %6s %5s %7s %7s %7s %7s %5s");
    hdr2.add("");
    hdr2.add("rate");
    hdr2.add("time");
    hdr2.add("time");
    hdr2.add("time");
    hdr2.add((binary_or_decimal) ? "1024**2" : "1000**2");
    hdr2.add("pct");
    hdr2.add("pct");
    hdr2.add("waiting");
    hdr2.add("active");
    hdr2.add("per_io");
    hdr2.add("sys+usr");
    hdr2.add("sys");

    DateFormat df = new SimpleDateFormat( "MMM dd, yyyy" );
    String line1 = "\n" + df.format(new Date());
    String line2 = "\n            ";
    String txt = line1 + hdr1.print() + line2 + hdr2.print();

    return txt;
  }

  public static void closeAllReports()
  {
    for (int i = 0; i < all_writers.size(); i++)
    {
      PrintWriter pw = (PrintWriter) all_writers.elementAt(i);

      /* We can't close stdout. That causes problems some times: */
      if (pw == common.stdout)
        continue;

      pw.close();
    }
  }

  public static void flushAllReports()
  {
    common.plog("Flushing all reports");
    for (int i = 0; i < all_writers.size(); i++)
    {
      PrintWriter pw = (PrintWriter) all_writers.elementAt(i);
      pw.flush();
    }
  }


  /**
   * Create a PrintWriter output file
   */
  public static PrintWriter createHmtlFile(String fname_in)
  {
    return createHmtlFile(fname_in, null);
  }

  /**
   * Create an output file, adding a title at the top of the file.
   */
  public static PrintWriter createHmtlFile(String filename,
                                           String title)
  {
    /* If '.html' has not been added, do so: */
    if (!filename.endsWith(".html"))
      filename += ".html";

    File fptr      = null;
    PrintWriter pw = null;

    try
    {
      fptr = new File(Vdbmain.output_dir, filename);

      BufferedOutputStream bout;
      FileOutputStream ofile = new FileOutputStream(fptr);
      bout = new BufferedOutputStream(ofile);

      /* Don't flush for some files: */
      /* No flush at all starting 502. Rescinded: stdout and logfile */
      if (!common.get_debug(common.NO_PRINT_FLUSH) &&
          (filename.startsWith("logfile")  ||
           filename.startsWith("kstat")    ||
           filename.startsWith("parmfile") ||
           filename.indexOf("stdout")  != -1))
        pw = new PrintWriter(bout, true);
      else
        pw = new PrintWriter(bout);
    }

    catch (Exception e)
    {
      common.failure(e);
    }

    /* An HTML file gets a little extra: */
    if (filename.indexOf(".html") != -1)
    {
      common.println("<pre>", pw);
      if (title != null)
      {
        pw.println(title);
        pw.println();
      }
    }

    /* Keep track of file names and writers for reporting in common.checkerror(): */
    writer_filenames.add(fptr.getAbsolutePath());
    all_writers.add(pw);

    return pw;
  }

  public static void chModAllReports()
  {
    if (!common.onWindows())
      OS_cmd.executeCmd("chmod -R 777 " + Vdbmain.output_dir);
  }

  public PrintWriter getWriter()
  {
    return pw;
  }
  public static Vector getAllWriters()
  {
    return all_writers;
  }
  public static String getWriterName(PrintWriter pw)
  {
    int index = all_writers.indexOf(pw);
    if (index != -1)
      return(String) writer_filenames.elementAt(index);

    return "Unknown PrintWriter name";
  }

  public static void showReports()
  {
    if (!common.get_debug(common.SHOW_REPORTS))
      return;

    for (int i = 0; i < all_reports.size(); i++)
    {
      Report report = (Report) all_reports.elementAt(i);
      common.ptod("report: " + report);
    }
  }


  /**
   * Split a list of reports between SD reports or Kstat reports
   * This is done because there can be a mix of the two but they need to be
   * treated separately.
   */
  public static String[] filterSds(HashMap map, boolean sds)
  {
    Vector list = new Vector(map.keySet());

    for (int i = 0; i < list.size(); i++)
    {
      String name = (String) list.elementAt(i);
      if (sds)
      {
        if (name.startsWith(InstancePointer.getKstatPrefix()))
          list.setElementAt(null, i);
      }
      else
      {
        if (!name.startsWith(InstancePointer.getKstatPrefix()))
          list.setElementAt(null, i);
      }
    }

    while (list.removeElement(null));

    return(String[]) list.toArray(new String[0]);
  }

  public static boolean isKstatReporting()
  {
    return kstat_summary != null;
  }

  public static void setDecimal()
  {
    binary_or_decimal = false;
    KB = 1000;
    MB = 1000 * 1000;
    GB = 1000 * 1000 * 1000;
    TB = 1000 * 1000 * 1000 * 1000;
  }

  public static String getAvgLabel()
  {
    return "avg_" + (Reporter.getWarmupIntervals() + 1) + "-" + highest_interval;
  }
  public static int getInterval()
  {
    return current_interval;
  }

  public static void setIntervalDuration(int dur)
  {
    interval_duration = dur;
  }
  public static int getIntervalDuration()
  {
    return interval_duration;
  }
  public static void setInterval(int interval)
  {
    current_interval =
    highest_interval = interval;
  }


  public static void writeFlat(SdStats stats, String title)
  {
    Flat.put_col("interval", title);
    Flat.put_col("rate",     stats.rate());
    Flat.put_col("mb/sec",   stats.megabytes());
    Flat.put_col("bytes/io", stats.bytes());
    Flat.put_col("read%",    stats.readpct());
    Flat.put_col("resp",     stats.respTime());
    Flat.put_col("resp_max", stats.respmax());
    Flat.put_col("resp_std", stats.resptime_std());
  }

  protected static void writeFlatCpu(Kstat_cpu kc)
  {
    double total = kc.cpu_total;
    Flat.put_col("cpu_idle",   kc.cpu_idle   * 100 / total);
    Flat.put_col("cpu_user",   kc.cpu_user   * 100 / total);
    Flat.put_col("cpu_kernel", kc.cpu_kernel * 100 / total);
    Flat.put_col("cpu_wait",   kc.cpu_wait   * 100 / total);
    Flat.put_col("cpu_used",   kc.cpu_user   * 100 / total + kc.cpu_kernel * 100 / total);
  }

  private static void writeFlatKstat(Kstat_data kd)
  {

    Flat.put_col("ks_rate",   kd.kstat_rate());
    Flat.put_col("ks_resp",   kd.kstat_wait() + kd.kstat_svctm());
    Flat.put_col("ks_wait",   kd.kstat_wait());
    Flat.put_col("ks_svct",   kd.kstat_svctm());
    Flat.put_col("ks_mb",     kd.kstat_megabytes());
    Flat.put_col("ks_read%",  kd.kstat_readpct());
    Flat.put_col("ks_busy%",  kd.kstat_busy());
    Flat.put_col("ks_avwait", kd.kstat_ioswaiting());
    Flat.put_col("ks_avact",  kd.kstat_iosrunning());
    Flat.put_col("ks_bytes",  (long) kd.kstat_bytes());
  }

  public static void parseParameters(String[] parms)
  {
    for (int i = 0; i < parms.length; i++)
    {
      if ("slave_detail".startsWith(parms[i]))
        slave_detail = true;
      else if ("host_detail".startsWith(parms[i]))
        host_detail = true;
      else
        common.failure("Invalid 'reports=' parameter: " + parms[i]);
    }
  }
}

