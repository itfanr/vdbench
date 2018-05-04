package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */


import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Monitor elapsed time of specific pieces of code.
 */
public class Elapsed
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private double start;
  private long   count     = 0;
  private long   frequency = 0;
  private String label     = null;
  private long   last_track_report = 0;


  /**
   * Simple elapsed time reporting
   */
  public Elapsed(String lbl)
  {
    label = lbl;
    start = System.currentTimeMillis();
  }


  /**
   * Elapsed time reporting, also
   */
  public Elapsed(String lbl, long freq)
  {
    label             = lbl;
    start             = System.currentTimeMillis();
    last_track_report = System.currentTimeMillis();
    frequency         = freq;
  }


  public void track()
  {
    track(1);
  }

  /**
   * Count once, but for eporting multiply the numbers with something like "each
   * call to track() represents X iterations".
   */
  public void track(long extra)
  {
    if (frequency == 0)
      common.failure("Elapsed.track(): requesting tracking without a specified frequence");
    count++;

    if (count % frequency == 0 && count != 1)
    {
      long   now     = System.currentTimeMillis();
      double seconds = (now - last_track_report) / 1000.;
      common.ptod("Elapsed.track: '%s' tracking has been called %,16d times, at %,10.0f per second",
                  label, count * extra, ((double) frequency * extra / seconds));
      last_track_report = now;
    }
  }

  public void end()
  {
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss" );
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    double end          = System.currentTimeMillis();

    // maybe, if tracking, also report FULL 'tracks pewr second'???/

    common.ptod("Elapsed.end: %s took %s", label, df.format(end - start));
  }

  /**
   * 'end' reporting, don't report if we ran shorter than 'ignore' seconds.
   */
  public void end(int ignore)
  {
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss" );
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    double end          = System.currentTimeMillis();
    double seconds      = (end - start) / 1000;
    if (seconds < ignore)
      return;

    common.ptod("Elapsed.end: %s took %s", label, df.format(end - start));
  }

  public static void main(String[] args)
  {

    Elapsed elapsed = new Elapsed("testing", 10000);

    for (int i = 0; i < 100000; i++)
      elapsed.track();

    elapsed.end();
  }
}
