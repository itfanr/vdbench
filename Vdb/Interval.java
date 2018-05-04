package Vdb;
import java.util.Date;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */


/**
 * This class handles the waiting and synchronizing of reporting intervals
 */
public class Interval
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";


  public static long interval_seconds        = 0;
  public static long expected_interval_start = 0;
  public static long expected_interval_end   = 0;
  public static long expected_next_interval  = 0;



  /**
   * Set the intervalnumber to 1, return at next one second boundary
   */
  public static void set_interval_start(long seconds)
  {
    interval_seconds        = seconds;
    long now                = System.currentTimeMillis();
    expected_interval_start = ownmath.round_lower(now + 1000, 1000);
    expected_interval_end   = expected_interval_start + (interval_seconds * 1000);
    expected_next_interval  = expected_interval_end   + (interval_seconds * 1000);
    //common.ptod("expected_interval_start: " + new Date(expected_interval_start));
    //common.ptod("expected_interval_end:   " + new Date(expected_interval_end));
    //common.ptod("expected_next_interval:  " + new Date(expected_next_interval));

    long wait               = expected_interval_start - now;
    if (wait > 0)
      common.sleep_some(wait);
    return;
  }


  /**
   * Wait until a specific synchronized amount of seconds.
   * The target tod of the previous call is taken, and the new interval time
   * is added.
   * Routine returns at that tod.
   */
  public static void wait_interval()
  {
    expected_interval_start += interval_seconds * 1000;
    expected_interval_end   += interval_seconds * 1000;
    expected_next_interval  += interval_seconds * 1000;

    long now  = System.currentTimeMillis();
    long wait = expected_interval_start - now;

    while (wait > 0)
    {
      now = System.currentTimeMillis();
      wait = expected_interval_start - now;
      if (wait <= 0)
        break;
      common.sleep_some(Math.min(1000, wait));
      if (Vdbmain.isWorkloadDone())
        break;
    }
  }

  public static long getSeconds()
  {
    if (interval_seconds == 0)
      common.failure("premature call to getSeconds()");
    return interval_seconds;
  }
}
