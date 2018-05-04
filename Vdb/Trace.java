package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import Utils.Format;

/**
 *
 */
public class Trace
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private long   timestamp;
  private String txt;
  private long   data1;
  private long   data2;

  private static DateFormat df = new SimpleDateFormat( "HH:mm:ss.SSS" );

  private static Trace[] table = new Trace[4096];
  private static long index    = 0;
  private static long base_simple   = 0;
  private static long base_local    = 0;

  static
  {
    for (int i = 0; i < table.length; i++)
      table[i] = new Trace();

    base_simple = Native.get_simple_tod();
    base_local  = System.currentTimeMillis();
  };


  public static void trace(String txt, long data1, long data2)
  {
    synchronized (table)
    {
      Trace tr     = table[(int) (index++ % table.length)];
      tr.timestamp = Native.get_simple_tod() - base_simple;
      tr.txt       = txt;
      tr.data1     = data1;
      tr.data2     = data2;
    }
  }


  /**
   * Print the trace table.
   *
   * Note: there will be a minor difference between the TOD that is reported here,
   * and the real local tod. However, relatively to each other the timestamps on
   * these trace entries are correct, and that is what it is all about.
   *
   * (This is caused because I don't get a microsecond count from
   * System.currentTimeMillis()).
   */
  public static void print()
  {
    common.where(8);
    synchronized (table)
    {
      int ix = (int) index;
      for (int i = 0; i < table.length; i++)
      {
        Trace tr = table[ix++ % table.length];
        long  micros = (tr.timestamp + base_simple);
        long  millis = tr.timestamp / 1000;

        String tod = df.format( new Date(base_local + millis) );

        common.ptod("trace: " +
                    Format.f("%4d ", i) +
                    tod +
                    Format.f("%03d ", (micros % 1000)) +
                    ((tr.txt == null) ? "null" : Format.f(" %-40s ", tr.txt)) +
                    Format.f(" %12d", tr.data1) +
                    Format.f(" %12d", tr.data2) );
      }
    }
  }

  public String toString()
  {
    String tod = df.format( new Date(timestamp) );
    return tod + " " + txt;
  }
}


