package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Vdb.common;

/**
 * The objective of this interface is in its first phase to allow any
 * reportClass to decide whether the Vdbench warmup should complete.
 * With that, user can return some String values to be printed on file
 * summary.html at the end of the line.
 */
public interface AuxInterface
{

  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms);

  /**
   * Receive run time information.
   */
  public void storeRunInfo(int warmup, int elapsed, int interval);

  /**
   * Receive a String that needs to be used as report header in summary.html
   */
  public String[] getSummaryHeaders();

  /**
   * Receive a String that contains data to be reported in summary.html
   */
  public String getSummaryData();

  /**
   * Receive a String that needs to be used as report header in report.html
   */
  //public String[] getReportHeader();

  /**
   * Receive a String that contains data to be reported in report.html
   */
  //public String getReportData();

  /**
   * Work is starting. Prepare for data collection
   */
  public void runStart();

  /**
   * Collect all data that you will need for a reporting interval.
   */
  public void collectIntervalData();

  /**
   * Check whether the warmup should complete.
   */
  public boolean isWarmupComplete();
}
