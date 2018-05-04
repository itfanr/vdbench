package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

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


