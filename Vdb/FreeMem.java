package Vdb;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;

import Utils.Fget;
import Utils.OS_cmd;

import Vdb.common;


/**
 * Auxilary reporting add-on for Vdbench.
 * On Linux this reports current memory usage as received from /proc/meminfo.
 * And of course, only on the system where the vdbench Master is running.
 */
public class FreeMem extends AuxReport
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  private String[] parms    = null;

  private boolean disabled  = false;

  private OS_cmd  ocmd      = null;


  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms)
  {
    this.parms = parms;
  }

  /**
   * Receive run time information.
   */
  public void storeRunInfo(int warmup, int elapsed, int interval)
  {
    common.where();
  }

  /**
   * Receive a String that needs to be used as report header in summary.html
   */
  public String[] getSummaryHeaders()
  {
    String[] headers = new String[2];
    headers[0] = String.format("%8s ", "MemFree" );
    headers[1] = String.format("%8s ", "gb"      );
    return headers;
  }

  /**
   * Receive a String that contains data to be reported in summary.html
   */
  public String getSummaryData()
  {
    if (disabled)
      return "disabled";

    if (free_bytes >= 0)
    {
      String txt = String.format("%8.3f ", free_bytes / (1024*1024*1024.));
      return txt;
    }

    return String.format("%8s", "n/a");
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

    /* Do NOT clear stats: this way when we run behind we just reuse PREVIOUS stats. */
    //free_bytes = -1;

    try
    {
      if (common.onLinux())
      {
        String[] lines = Fget.readFileToArray("/proc/meminfo");
        for (String line : lines)
        {
          if (!line.startsWith("MemFree"))
            continue;

          String[] split = line.split(" +");
          free_bytes = Double.parseDouble(split[1]) * 1024;
          return;
        }
      }

      if (common.onSolaris())
      {
        /* Are we running behind? ignore request: */
        if (ocmd != null)
        {
          common.ptod("FreeMem auxiliarly reporting is running behind. Request ignored.");
          return;
        }

        ocmd = new OS_cmd();
        ocmd.addText("kstat -m unix -n system_pages -s freemem -p");
        ocmd.execute(false);
        if (!ocmd.getRC())
          return;

        String[] lines = ocmd.getStdout();
        ocmd = null;
        for (String line : lines)
        {
          //common.ptod("line: " + line);
          if (!line.startsWith("unix:0:system_pages:freemem"))
            continue;

          line = line.replace("\t", " ");
          String[] split = line.split(" +");
          double pages = Double.parseDouble(split[1]);

          String arch = System.getProperty("os.arch");
          if (arch.equals("amd64") || arch.equals("x86"))
            free_bytes = pages * 4096;
          else
            free_bytes = pages * 8192;

          return;
        }
      }
      return;
    }

    catch (Exception e)
    {
      common.ptod("FreeMem auxiliarly reporting is being disabled.");
      common.ptod(e);
      disabled = true;
    }
  }
}

