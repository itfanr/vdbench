package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Utils.Format;


/**
 * This class handles vdbench performance measurements for debugging
 *
 * This code does not work anymore.
 */
public class Timers
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private long    tod_before;
  private long    tod_total;
  private long    tod_counts;
  private long    first_tod;
  private long    last_tod;
  private String  label;

  private final   static boolean active = common.get_debug(common.TIMERS);;

  public Timers(String label_in)
  {
    tod_total  = 0;
    tod_counts = 0;
    label      = label_in;
  }

  public long simpleTime()
  {
    long rc = System.nanoTime();
    if (rc == 0)
      common.failure("Timers(): zero nanoTime");
    return rc;
  }

  public void before()
  {
    if (active)
    {
      synchronized (this)
      {
        if (tod_counts == 0)
          first_tod = simpleTime();
        tod_before = simpleTime();
      }
    }
  }


  public void after()
  {
    if (active)
    {
      synchronized (this)
      {
        if (tod_before == 0)
          common.failure("Timers.after(): previous tod equals zero");
        last_tod = simpleTime();
        tod_total += last_tod - tod_before;
        tod_before = 0;
        tod_counts++;
      }
    }
  }

  public void xadd(long time)
  {
    if (active)
    {
      synchronized (this)
      {
        if (tod_counts == 0)
          first_tod  = simpleTime();
        last_tod   = simpleTime();
        tod_total += time;
        tod_counts++;
      }
    }
  }


  /**
   * Report:
   * - what percentage of the total elapsed time we were somewhere in the Fifo
   * code.
   * - how long for each call were we in the Fifo code.
   */
  public void print()
  {
    if (!active)
      return;

    try
    {
      common.ptod("+Timers: %-20s calls: %8d elapsed%%: %6.2f; avg time per set: %10d ns; %10d us",
                  label,
                  tod_counts,
                  (tod_counts == 0) ? 0 : (double) tod_total * 100 / (last_tod - first_tod),
                  (tod_counts == 0) ? 0 : tod_total / tod_counts,
                  (tod_counts == 0) ? 0 : tod_total / tod_counts / 1000);
    }
    catch (Exception e)
    {
      common.ptod("Exception in Timers.print(): " + e.toString());
    }
  }
}


