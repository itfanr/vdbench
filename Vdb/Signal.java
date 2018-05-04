package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;

public class Signal implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private long base  = 0;
  private long msecs = 0;
  private int  secs  = 0;
  private int  signals_given = 0;
  private long signal_started = 0;

  public Signal(int secs)
  {
    this.secs      = secs;
    this.msecs     = secs * 1000;
    signal_started = System.currentTimeMillis();
  }

  /**
   * Signal caller after n milliseconds.
   * Returns true if more than 'msecs' time elapsed since the first call.
   */
  public boolean go()
  {
    long tod = System.currentTimeMillis();

    /* First call, just set base tod: */
    if (base == 0)
    {
      base = tod;
      return false;
    }

    /* If tod expired, return true which is the signal that time expired */
    //common.ptod("base: " + tod + " " + base + " " + (tod - base));
    if (base + msecs < tod)
    {
      base = tod;
      signals_given++;
      return true;
    }

    return false;
  }
  public boolean anySignals()
  {
    return signals_given != 0;
  }
  public int getDuration()
  {
    return secs;
  }

  public int getAge()
  {
    return (int) (System.currentTimeMillis() - signal_started) / 1000;
  }

  public static void main(String[] args)
  {
    Signal signal = new Signal(5);
    for (int i = 0; i < 20; i++)
    {
      common.sleep_some_usecs(1000000);
      if (signal.go())
        common.where();
    }
  }
}


