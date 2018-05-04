package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import Vdb.common;

public class AuxReport implements AuxInterface
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private boolean shutdown    = false;
  private boolean warmup_done = false;

  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms)
  {
  }

  /**
   * Receive run time information.
   */
  public void storeRunInfo(int warmup, int elapsed, int interval)
  {
  }

  /**
   * Receive a String that needs to be used as report header in summary.html
   */
  public String[] getSummaryHeaders()
  {
    return null;
  }

  /**
   * Receive a String that contains data to be reported in summary.html
   */
  public String getSummaryData()
  {
    return null;
  }


  /**
   * Receive a String that needs to be used as report header in report.html
   */
  public String[] getReportHeader()
  {
    return null;
  }

  /**
   * Receive a String that contains data to be reported in report.html
   */
  public String getReportData()
  {
    return null;
  }

  /**
   * Work is starting. Prepare for data collection
   */
  public void runStart()
  {
  }

  /**
   * Collect all data that you will need for a reporting interval.
   */
  public void collectIntervalData()
  {
  }

  /**
   * Check whether the warmup should complete.
   */
  public boolean isWarmupComplete()
  {
    return warmup_done;
  }
  public void setWarmupComplete(boolean bool)
  {
    warmup_done = bool;
  }

  public static void main(String[] args)
  {
    try
    {
      AuxReport arcstat = (AuxReport) Class.forName("Vdb.AuxReport").newInstance();
      common.ptod("auxreport: " + arcstat);
    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public void setShutdown(boolean bool)
  {
    //common.ptod("bool: " + bool);
    //common.where(8);
    shutdown = bool;
  }
  public boolean isShutdown()
  {
    return shutdown;
  }
}

