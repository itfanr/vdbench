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
 * Usage: auxreport=(Vdb.AuxExternal,xxxxxxx.sh)
 * or:
 *        auxreport=(Vdb.AuxExternal,xxxxxxx.sh,no_output)
 *
 * 'xxxxx.sh 'header1'  is called ONCE, just for the header1 text.
 * 'xxxxx.sh 'header2'  is called ONCE, just for the header2 text.
 * 'xxxxx.sh 'masks'    is called ONCE, just for the printf masks.
 * 'xxxxx.sh 'interval' is called every reporting interval.
 *
 * 'no_output' if you just want the 'interval' calls.
 *
 *
 * 'header1/header2/masks/interval' after this is called a request 'type'.
 *
 * Information to be returned, including the 'type':
 * - header1  hdr1_col1 hdr1_col2 . . . . .
 * - header2  hdr2_col1 hdr2_col2 . . . . .
 * - masks    10.2f     10.2f     . . . . .
 * - interval 123.45    678.90    . . . . .
 *
 * The column width of the reported columns will be set to the longer of the
 * two column headers given for each column.
 *
 * The 'header' and 'masks' calls are made at the start of each Run Definition.
 *
 * The 'interval' call will be made immediately after statistcs have been
 * requested from all slaves, but Vdbench will NOT wait for this 'interval' call
 * to complete. If after the statistics have been returned from all slaves the
 * 'interval' call has not completed, the next interval call will be skipped.
 * This is done to make sure that anything done during the interval call will
 * NOT hold up Vdbench reporting.
 *
 * All 'interval' calls, except for the first one, will contain statistics
 * available from the previous interval, e.g. 'rd rd2 iops 74.000 resp 0.219',
 * identifying the currenr RD and iops + response times. That information then
 * can be used inside of the script.
 *
 *
 * All output returned must start with the above mentioned 'type', any output
 * that does  not start with 'type' will be sent to logfile.html, unless it
 * starts with 'stdout', then it will be reported on stdout. The latter can be
 * useful while debugging.
 *
 * The script may also return anywhere in the output:
 * - 'shutdown_rd': the current RD will be completed at the end of the next
 *   interval.
 * - 'shutdown':    Vdbench will shut down at the end of the next interval'
 * - 'end_warmup':  This will end the current warmup period.
 *
 */
public class AuxExternal extends AuxReport
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private String[] parms    = null;

  private boolean disabled  = false;
  private boolean no_output = false;

  private OS_cmd  ocmd      = null;

  private String[] blank_header    = { "n/a",      "n/a"};
  private String[] disabled_header = { "disabled", "disabled"};

  private String[]  header1 = null;
  private String[]  header2 = null;
  private String[]  masks   = null;
  private double[]  data    = null;
  private int[]     widths  = null;
  private String              head1         = null;
  private String              head2         = null;




  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms)
  {
    this.parms = parms;
    if (parms.length < 2)
      common.failure("'auxreport=' must consist of a minimum of two parameters.");

    if (parms.length > 2 && parms[2].equals("no_output"))
      no_output = true;

    /* Get the column headers right away, one time only: */
    if (!no_output)
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
    /* If we're not interested in adding columns, return two empty headers: */
    /* (yeah, this was easier than suppressing) */
    if (no_output)
      return new String[] {"", ""};

    if (disabled)
      return disabled_header;

    return new String[] { head1, head2};
  }



  /**
   * Call the requested script asking for column headers.
   */
  private void obtainSummaryHeaders()
  {
    if (disabled)
      return;

    /* Get the headers: */
    header1 = null;
    header2 = null;
    masks   = null;

    String h1 = callScript("header1");
    String h2 = callScript("header2");
    String mk = callScript("masks");

    if (h1 != null) header1 = h1.split(" +");
    if (h2 != null) header2 = h2.split(" +");
    if (mk != null) masks   = mk.split(" +");

    if (header1 == null || header2 == null || masks == null)
    {
      disabled = true;
      return;
    }

    if (header1.length != header2.length)
    {
      common.ptod("'auxreport=' failure. header1 and header2 field count not equal (%d/%d)",
                  header1.length, header2.length);
      disabled = true;
      return;
    }

    if (header1.length != masks.length)
    {
      common.ptod("'auxreport=' failure. header and mask count not equal (%d/%d)",
                  header1.length, masks.length);
      disabled = true;
      return;
    }



    /* Determine the maximum width of each column: */
    widths = new int[header1.length];
    for (int i = 0; i < header1.length; i++)
      widths[i] = Math.max(header1[i].length(), header2[i].length());


    /* Include the masks' length if specified: */
    for (int i = 0; i < masks.length; i++)
    {
      StringTokenizer st = new StringTokenizer(masks[i], ".");
      if (st.countTokens() == 1)
        common.failure("AuxExternal: you must specify full field width in printf mask: " + masks[i]);

      widths[i] = Math.max(widths[i], Integer.parseInt(st.nextToken()));
    }

    /* Prefix masks with '%': have problems getting that from a script (windows?) */
    /* Also suffix with one blank for separation:                                 */
    for (int i = 0; i < masks.length; i++)
    {
      if (!masks[i].startsWith("%"))
        masks[i] = "%" + masks[i];
    }

    /* Now create the REAL column headers: */
    head1 = "";
    for (int i = 0; i < header1.length; i++)
    {
      String mask  = "%" + widths[i] + "s ";
      head1 += String.format(mask, header1[i]);
    }

    head2 = "";
    for (int i = 0; i < header2.length; i++)
    {
      String mask  = "%" + widths[i] + "s ";
      head2 += String.format(mask, header2[i]);
    }
  }


  /**
   * Call the user's script and return its one-line output.
   * Line may be prefixed with 'shutdown' etc, which will be removed.
   */
  private String callScript(String command)
  {
    //common.where(command);
    try
    {
      /* Get the headers: */
      ocmd = new OS_cmd();
      ocmd.addQuot(parms[1]);
      ocmd.addText(command);
      for (int i = (no_output) ? 3 : 2; i < parms.length; i++)
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

      else
      {
        FwdStats  stats = Report.getSummaryReport().getData().getIntervalFwdStats();
        Kstat_cpu kc    = Report.getSummaryReport().getData().getIntervalCpuStats();
        if (stats != null)
        {
          ocmd.addText(String.format("rd    %s",   RD_entry.next_rd.rd_name));
          ocmd.addText(String.format("iops  %.3f", stats.getTotalRate()));
          ocmd.addText(String.format("resp  %.3f", stats.getTotalResp()));

          if (kc != null)
          {
            ocmd.addText(String.format("cpu     %.3f", kc.kernel_pct() + kc.user_pct()));
            ocmd.addText(String.format("cpu_sys %.3f", kc.kernel_pct()));
            ocmd.addText(String.format("cpu_usr %.3f", kc.user_pct()));
          }

        }
      }

      //auxreport.bat data rd    rd1 iops  84.000 resp  1.480 rresp 1.157 wresp 1.852 max   16.711
      //             std   3.313 cpu     4.103 cpu_sys 0.772 cpu_usr 3.332

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
      String data_line = null;
      for (String line : ocmd.getStdout())
      {
        line = line.trim();
        //common.ptod("line: >>>%s<<<", line);
        if (line.startsWith(command))
          data_line = line.substring(line.indexOf(" ")).trim();
        else
        {
          if (line.startsWith("stdout"))
            common.ptod("Info from %s: %s", parms[1], line);
          else
            common.plog("Info from %s: %s", parms[1], line);
        }
      }

      // debugging
      ocmd.printStderr();

      ocmd = null;

      if (data_line == null && !no_output)
        common.failure("No response received from last AuxExternal call: " + command);

      return data_line;
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
    if (no_output)
      return "";

    if (disabled)
      return " disabled";

    if (isShutdown())
      return " shutdown";

    if (data == null)
      return " no data yet";

    try
    {
      String tmp = "";
      for (int i = 0; i < data.length; i++)
      {
        //String mask = "%" + widths[i] + "s ";
        tmp += String.format(masks[i], data[i]) + " ";
      }
      return tmp;
    }
    catch (Exception e)
    {
      common.ptod("'auxreport=' error. Disabled.");
      common.ptod(e);
    }
    return null;
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
      String line = callScript("data");
      if (line == null)
        return;

      String[] split      = line.split(" +");
      String   first_word = split[0];
      if (first_word.equals("shutdown_rd"))
      {
        if (!isShutdown())
        {
          setShutdown(true);
          common.pboth("'auxreport=' requested shutdown of the current RD.");
        }
        split = line.substring(line.indexOf(" ")).trim().split(" +");
      }

      else if (first_word.equals("shutdown"))
      {
        Reporter.monitor_final = true;
        if (!isShutdown())
        {
          setShutdown(true);
          common.pboth("'auxreport=' requested shutdown of Vdbench.");
        }
        split = line.substring(line.indexOf(" ")).trim().split(" +");
      }

      else if (first_word.equals("end_warmup"))
      {
        if (!isWarmupComplete())
        {
          setWarmupComplete(true);
          common.pboth("'auxreport=' requested end to warmup.");
        }
        split = line.substring(line.indexOf(" ")).trim().split(" +");
      }

      /* If there is no data to be report, done: */
      if (no_output)
        return;

      if (split.length != header1.length )
      {
        common.ptod("split.length: " + split.length);
        common.ptod("header1.size() : " + header1.length );
        common.pboth("'auxreport=' The header column count and data count do not match.");
        disabled = true;
        return;
      }

      data = new double[split.length];
      for (int i = 0; i < split.length; i++)
        data[i] = Double.parseDouble(split[i]);
    }

    catch (Exception e)
    {
      common.ptod("'auxreport=' auxiliarly reporting is being disabled.");
      common.ptod(e);
      disabled = true;
    }
  }
}

