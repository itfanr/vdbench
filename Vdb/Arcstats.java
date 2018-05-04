package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.ArrayList;

import Utils.Fget;

import Vdb.common;

public class Arcstats extends AuxReport
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String[] parms    = null;
  private String   arc_file = null;
  private double   requested_rate = 0;
  private int      duration       = 0;
  private boolean  warmup_done    = false;

  private ArcstatsData old_data = null;
  private ArcstatsData new_data = null;

  private ArrayList <ArcstatsData> history = new ArrayList(16);

  /**
   * Parse parameters provided by Vdbench.
   */
  public void parseParameters(String[] parms)
  {
    this.parms = parms;
    if (parms.length < 4)
      common.failure("Arcstats.parseParameters(): Requiring a minimum of four parameters");
    arc_file = parms[1];

    if (!Fget.file_exists(arc_file))
      common.failure("Arcstats.parseParameters(): file %s does not exist", arc_file);

    for (int i = 2; i < parms.length; i++)
    {
      String[] split = parms[i].split("=");
      if (split[0].equals("hitrate"))
        requested_rate = Double.parseDouble(split[1]);
      else if (split[0].equals("duration"))
        duration       = Integer.parseInt(split[1]) * 1000;
      else
        common.failure("Arcstats.parseParameters(): invalid parameter:", split[0]);
    }
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
    String[] lines = new String[2];
    lines[0] = String.format("%7s %7s %7s", "l1arc",  "l1arc", "l1arc");
    lines[1] = String.format("%7s %7s %7s", "hitpct", "hits",  "misses");
    return lines;
  }

  /**
   * Receive a String that contains data to be reported in summary.html
   */
  public String getSummaryData()
  {
    if (old_data == null)
      return "*No old arcstats data available";
    if (new_data == null)
      return "*No new arcstats data available";
    if (old_data.snap_time == new_data.snap_time)
      return "*arcstats data did not change";

    long hits     = new_data.demand_data_hits   - old_data.demand_data_hits;
    long misses   = new_data.demand_data_misses - old_data.demand_data_misses;
    long elapsed  = new_data.snap_time          - old_data.snap_time;
    long total    = hits + misses;
    double hitpct = (total > 0) ? hits * 100. / total : 0;

    String txt = String.format("%7.2f %7d %7d", hitpct, hits, misses);

    return txt;
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
   *
   * There is a major problem when testing this on windows using Samba: Samba
   * just does not update often enough, retaining old file contents.
   * For simplicity I now just put some data in the fields.
   *
   * zfs:0:arcstats:hits	13067384
   * zfs:0:arcstats:misses	565066
   */
  private static long hits   = 0;
  private static long misses = 0;
  public void collectIntervalData()
  {
    ArcstatsData interval_data = new ArcstatsData();

    if (common.onWindows())
    {
      hits                   += 100;
      misses                 += 200;
      interval_data.demand_data_hits      = hits;
      interval_data.demand_data_misses    = misses;
      interval_data.snap_time = System.currentTimeMillis();
      old_data                = new_data;
      new_data                = interval_data;
      return;
    }

    String[] lines = Fget.readFileToArray(arc_file);
    for (int i = 0; i < lines.length; i++)
    {
      String[] split = lines[i].substring(15).split("\t+");
      if (split[0].equals("snaptime"))
        interval_data.snap_time = (long) (Double.parseDouble(split[1]) * 1000l);
      else if (split[0].equals("demand_data_hits"))
        interval_data.demand_data_hits = Long.parseLong(split[1]);
      else if (split[0].equals("demand_data_misses"))
        interval_data.demand_data_misses = Long.parseLong(split[1]);
    }

    /* We have not received 'snaptime'. This means that the file currently */
    /* is not complete; it may be in the middle of an update:              */
    if (interval_data.snap_time == 0)
      return;

    /* Preserve 'n' seconds of history for end-of-warmup detection: */
    if (!warmup_done)
      saveHistory(interval_data);

    /* If snaptime did not change it means that we don't have new data. */
    /* We'll just use the previous data. */
    if (old_data != null && old_data.snap_time == interval_data.snap_time)
      return;

    /* Preserve the previous and new contents of the data: */
    old_data = new_data;
    new_data = interval_data;
  }

  /**
   * Check whether the warmup should complete.
   */
  public boolean isWarmupComplete()
  {
    if (warmup_done)
      return true;

    boolean debug = common.get_debug(common.DEBUG_AUX_REPORT);

    if (debug) common.ptod("isWarmupComplete history.size(): " + history.size());
    /* Wait until we have at least 'duration' worth of data: */
    if (history.size() < 2)
      return false;

    ArcstatsData beg = history.get(0);
    ArcstatsData end = history.get(history.size() - 1);

    if (debug) common.ptod("isWarmupComplete delta: " + (end.snap_time - beg.snap_time));
    if (end.snap_time - beg.snap_time < duration)
      return false;

    /* Calculate the hitrate now: */
    long total_hits   = end.demand_data_hits   - beg.demand_data_hits;
    long total_misses = end.demand_data_misses - beg.demand_data_misses;
    long total        = total_hits + total_misses;
    if (total == 0)
      return false;

    double rate = (double) (total_hits * 100. / total);
    if (debug)  common.ptod("isWarmupComplete: %.2f total: %d hits: %d misses: %d", rate, total, total_hits, total_misses);
    if (rate > requested_rate)
    {
      common.ptod("Reporting end of warmup: %.2f%% total: %d hits: %d misses: %d", rate, total, total_hits, total_misses);
      warmup_done = true;
      return true;
    }

    return false;
  }

  public static void main(String[] args)
  {
    try
    {
      Arcstats arcstat = (Arcstats) Class.forName("Vdb.Arcstats").newInstance();
      common.ptod("arcstat: " + arcstat);
    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }


  /**
   * Preserve 'n' seconds of history for end-of-warmup detection.
   */
  private void saveHistory(ArcstatsData data)
  {
    history.add(data);

    /* Remove anything that is older than what we need: */
    for (int i = 0; i < history.size(); i++)
    {
      if (history.get(i).snap_time < data.snap_time - duration - 1000)
        history.set(i, null);
    }

    //common.ptod("history1: " + history.size());
    while (history.remove(null));
    //common.ptod("history2: " + history.size());
  }

  class ArcstatsData
  {
    long demand_data_hits;
    long demand_data_misses;
    long snap_time;
  }
}

