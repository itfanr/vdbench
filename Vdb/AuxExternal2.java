package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.StringTokenizer;

import Utils.OS_cmd;


/**
 * Auxilary reporting add-on for Vdbench.
 * Usage: auxreport=(Vdb.AuxExternal2,xxxxxxx.sh)
 *
 * 'xxxxx.sh 'header1' is called ONCE, just for the header1 text.
 * 'xxxxx.sh 'header2' is called ONCE, just for the header2 text.
 * 'xxxxx.sh 'data'    is called every reporting interval.
 *
 * The header1 and header2 information returned is free format text.
 * The data returned is also free format text.
 * This of course makes YOU responsible for getting nice column layouts.
 *
 * 'xxxxx.sh 'data' is called at the end of each interval, though, depending on
 * how long this call takes, the reporting for this interval may already have
 * been done. In other words: This script will NOT hold up reporting!!!!
 *
 * The script may return:
 * - 'shutdown', and the current RD will be completed at the end of the next
 *   interval.
 * - TBD: 'msg=xxx', and this message will be displayed both on the console and
 *   in summary.html.
 *
 * Note that this class returns arrays of Strings, though we'll only put ONE
 * string in the array.
 *
 */
public class AuxExternal2 extends AuxReport
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private String[] parms    = null;

  private boolean disabled  = false;

  private OS_cmd  ocmd      = null;

  private String[]  blank_header    = { "n/a",      "n/a"};
  private String[]  disabled_header = { "disabled", "disabled"};

  private String    data    = null;
  private String    head1   = null;
  private String    head2   = null;




  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms)
  {
    this.parms = parms;
    if (parms.length < 2)
      common.failure("'auxreport=' must consist of a minimum of two parameters.");

    /* Get the column headers right away, one time only: */
    obtainSummaryHeaders();
  }

  /**
   * Receive run time information.
   */
  public void storeRunInfo(int warmup, int elapsed, int interval)
  {
    common.where();
  }

  /**
   * Receive a String array that needs to be used as report header in
   * summary.html
   */
  public String[] getSummaryHeaders()
  {
    if (disabled)
      return disabled_header;

    return new String[] { head1, head2 };
  }



  /**
   * Call the requested script asking for column headers.
   */
  private void obtainSummaryHeaders()
  {
    if (disabled)
      return;

    head1 = " | " + callScript("header1");
    head2 = " | " + callScript("header2");
  }


  /**
   * Call the user's script and return its one-line output.
   * Line may be prefixed with 'shutdown' etc, which will be removed.
   */
  private String callScript(String command)
  {
    try
    {
      /* Get the headers: */
      ocmd = new OS_cmd();
      ocmd.addQuot(parms[1]);
      ocmd.addText(command);
      for (int i = 2; i < parms.length; i++)
        ocmd.addQuot(parms[i]);

      if (Vdbmain.isWdWorkload())
      {
        SdStats   stats = Report.getSummaryReport().getData().getIntervalSdStats();
        Kstat_cpu kc    = Report.getSummaryReport().getData().getIntervalCpuStats();
        if (stats != null)
        {
          ocmd.addText(String.format("rd    %s",   RD_entry.next_rd.rd_name));
          ocmd.addText(String.format("iops  %.3f", stats.rate()));
          ocmd.addText(String.format("resp  %.3f", stats.respTime()));
          ocmd.addText(String.format("rresp %.3f", stats.readResp()));
          ocmd.addText(String.format("wresp %.3f", stats.writeResp()));
          ocmd.addText(String.format("max   %.3f", stats.respMax()));
          ocmd.addText(String.format("std   %.3f", stats.resptime_std()));

          if (kc != null)
          {
            ocmd.addText(String.format("cpu     %.3f", kc.kernel_pct() + kc.user_pct()));
            ocmd.addText(String.format("cpu_sys %.3f", kc.kernel_pct()));
            ocmd.addText(String.format("cpu_usr %.3f", kc.user_pct()));
          }

        }
      }

      /* If it failed, give up: */
      if (!ocmd.execute(false))
      {
        ocmd.printStderr();
        ocmd.printStdout();
        common.ptod("'auxreport=' failure. Disabled.");
        disabled = true;
        return null;
      }

      /* Make sure I get the right data: */
      String[] stdout = ocmd.getStdout();
      if (stdout.length != 1)
      {
        ocmd.printStderr();
        ocmd.printStdout();
        common.ptod("'auxreport=' expecting one line containing two blank separated headers. Disabled.");
        disabled = true;
        return null;
      }

      // debugging
      ocmd.printStderr();

      ocmd = null;

      return stdout[0];
    }
    catch (Exception e)
    {
      common.ptod("'auxreport=' failure. Disabled.");
      common.ptod(e);
      disabled = true;
      return null;
    }
  }


  /**
   * Receive a String that contains data to be reported in summary.html
   */
  public String getSummaryData()
  {
    if (disabled)
      return " disabled";

    if (isShutdown())
      return " shutdown";

    if (data == null)
      return " no data yet";

    return data;
  }


  /**
   * Receive a String that needs to be used as report header in report.html
   */
  public String[] getReportHeader()
  {
    common.where();
    return null;
  }

  /**
   * Receive a String that contains data to be reported in report.html
   */
  public String getReportData()
  {
    common.where();
    return null;
  }

  /**
   * Work is starting. Prepare for data collection
   */
  public void runStart()
  {
    common.where();
  }

  /**
   * Collect all data that you will need for a reporting interval.
   * Note: it is technically possible for this method to not be finished until
   * AFTER the statistics have been reported.
   * This means that the statistics as reported by a call to getSummaryData()
   * can be stale.
   *
   * The statistics reporting will NOT be delayed by Aux statistics.
   */
  private static double free_bytes = -1;
  public void collectIntervalData()
  {
    if (disabled)
      return;

    if (isShutdown())
      return;

    try
    {
      /* If any OS_cmd is running we are behind. Skip: */
      if (ocmd != null)
        return;

      /* Get the data: */
      String line = callScript("data 777.777");
      if (line == null)
        return;

      String[] split = line.split(" +");
      if (split[0].equals("shutdown_rd"))
      {
        if (!isShutdown())
        {
          setShutdown(true);
          common.pboth("'auxreport=' requested shutdown of the current RD.");
        }
      }

      else if (split[0].equals("shutdown"))
      {
        Reporter.monitor_final = true;
        if (!isShutdown())
        {
          setShutdown(true);
          common.pboth("'auxreport=' requested shutdown of Vdbench.");
        }
      }

      else if (split[0].equals("end_warmup"))
      {
        if (!isWarmupComplete())
        {
          setWarmupComplete(true);
          common.pboth("'auxreport=' requested end to warmup.");
        }
      }

      else
        data = " | " + line;

    }

    catch (Exception e)
    {
      common.ptod("'auxreport=' auxiliarly reporting is being disabled.");
      common.ptod(e);
      disabled = true;
    }
  }
}

