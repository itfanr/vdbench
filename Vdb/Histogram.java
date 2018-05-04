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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;

import Utils.Format;


/**
  */
public class Histogram extends VdbObject implements java.io.Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public  static int CHART_HEIGHT = 25;

  private long[] counters = null;
  private double highest_pct = 0;
  private BucketLine[] bucket_lines;

  private BucketRanges ranges;
  private BucketRanges sorted_ranges;

  private ArrayList print_lines;

  private long total_operations;    /* Calculated by getprintableData() */



  public Histogram(String type)
  {
    ranges   = BucketRanges.getRanges(type);
    counters = new long[ ranges.getBucketCount() ];

    /* Create a clone of the ranges. That list may be sorted to help find */
    /* those bucket ranges that are used most quicker, saving cycles:     */
    sorted_ranges = (BucketRanges) ranges.clone();

  }

  public Object clone()
  {
    try
    {
      Histogram hist = (Histogram) super.clone();
      hist.counters  = (long[]) counters.clone();

      return hist;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  /**
   * Look through the ranges finding in what bucket to count this value.
   */
  public int findBucket(long value)
  {
    return sorted_ranges.findBucket(value);
  }

  public void addToBucket(long value)
  {
    int bucket = sorted_ranges.findBucket(value);
    counters[ bucket ] ++;
    //common.ptod("buckets[ bucket ]: " + bucket + " " + buckets[ bucket ]);
  }

  public void deltaBuckets(Histogram nw, Histogram old)
  {
    for (int i = 0; i < counters.length; i++)
    {
      counters[i] = nw.counters[i] - old.counters[i];
    }
  }

  public void accumBuckets(Histogram hist)
  {
    if (hist == null)
      return;

    if (counters.length != hist.counters.length)
      common.failure("Unmatch counter sizes: " + counters.length + "/" + hist.counters.length);
    for (int i = 0; i < counters.length; i++)
    {
      counters[i] += hist.counters[i];
    }
  }

  public void printBucket(String label)
  {
    String txt = Format.f("%-8s: ", label);
    for (int i = 0; i < counters.length; i++)
    {
      txt += counters[i] + " ";
    }
    common.ptod(txt);
  }


  /**
   * Return information about the bucket contents, ready to be printed.
   */
  public void getPrintableData()
  {
    bucket_lines = new BucketLine[ranges.getBucketCount()];

    /* Calculate total of buckets: */
    total_operations = 0;
    for (int i = 0; i < counters.length; i++)
      total_operations += counters[i];

    /* Calculate the highest %% to maybe establish the chart height: */
    //long max_pct = 0;
    //for (int i = 0; i < counters.length; i++)
    //  max_pct = Math.max(counters[i] * 100 / total, max_pct);

    /* Now Calculate each bucket's percentage: */
    for (int i = 0; i < counters.length; i++)
    {
      double pct = (total_operations == 0) ? 0 : counters[i] * 100 / total_operations;
      bucket_lines[i] = new BucketLine(pct, i, counters[i], ranges);
      highest_pct = Math.max(highest_pct, pct);
    }


    /* If we have too many buckets, cut them down by removing the vertical */
    /* lines representing zero counts: */
    if (bucket_lines.length > 50)
    {
      Vector tmp = new Vector(100);
      for (int i = 0; i < bucket_lines.length; i++)
      {
        if (i == 0 || i == bucket_lines.length - 1)
          tmp.add(bucket_lines[i]);
        else if (bucket_lines[i].count > 0)
          tmp.add(bucket_lines[i]);
      }

      bucket_lines = (BucketLine[]) tmp.toArray(new BucketLine[0]);
    }
  }


  public long[] getCounters()
  {
    return counters;
  }

  public void showBuckets()
  {
    long usecs = 1;
    for (int i = 0; i < ranges.getBucketCount(); i++)
    {
      int bit = findBucket(usecs);

      common.ptod(Format.f("secs %20.6f", usecs / 1000000.) +
                  Format.f(" %12X", usecs) + " " + bit );
      usecs *=2;
    }
  }

  public Vector printit()
  {
    Vector lines = new Vector(64);
    getPrintableData();

    if (total_operations == 0)
    {
      lines.add("There are no 'requested operations' to report for this run.");
      return lines;
    }

    lines.add("");

    if (counters.length != bucket_lines.length)
      lines.add("Histogram has been shrunk to only include those buckets " +
                "that have a non-zero value.\n");

    /* Print all the bucket maxes: */
    printAny(lines, "max", "Bucket max:");

    /* Print all the stars: */
    printStars(lines);
    //printAny(lines, "stars", "xxx");

    /* Print all the bucket pcts: */
    printAny(lines, "pct", "Percentage:");

    /* Print all the bucket counts: */
    printAny(lines, "cnt", "Raw count:");

    //for (int i = 0; i < lines.size(); i++)
    //  System.out.println(lines.elementAt(i));

    lines.add("Total operations: " + total_operations);

    return lines;
  }

  private void printStars(Vector lines)
  {
    /* Print all the stars: */
    int offset = 0;
    boolean firstline = true;
    while (offset < bucket_lines[0].star_string.length())
    {
      String line = "";
      for (int i = 0; i < bucket_lines.length; i++)
        line += bucket_lines[i].star_string.charAt(offset) + " ";

      if (line.trim().length() > 0)
      {
        String pct_label = "           ";
        if (firstline)
          pct_label = "Histogram: ";
        pct_label += Format.f("%3d%% ", (CHART_HEIGHT-offset) * (100 / Histogram.CHART_HEIGHT));

        lines.add(pct_label + line);
        firstline = false;
      }
      offset++;
    }

    String stripe = "               ";
    for (int i = 0; i < bucket_lines.length; i++)
      stripe += "__";
    lines.add(stripe);

    lines.add("");
  }

  /**
  * Print out the printable information created by getPrintableData().
  *
  * You can envision getPrintableData() creating a horizontal representation of
  * what you will see vertically in the histogram report.
  * This code will 'rotate' everything left at a 90 degrees angle.
  */
  private void printAny(Vector lines, String which, String prefix)
  {
    String PREFIX = "%-16s";

    /* Print all the bucket counts: */
    int offset = 0;
    boolean first_line = true;
    while (true)
    {
      /* If we are far enough in the 'counts' strings to  */
      /* now only have blanks, stop here:                 */
      boolean finished = true;
      for (int i = 0; i < bucket_lines.length; i++)
      {
        String value = bucket_lines[i].getWhich(which);
        if (value.substring(offset).trim().length() != 0)
        {
          finished = false;
          break;
        }
      }
      if (finished)
        break;

      /* Pick up the next character from each 'horizontal' line: */
      String line = "";
      for (int i = 0; i < bucket_lines.length; i++)
      {
        String value = bucket_lines[i].getWhich(which);
        line += value.charAt(offset) + " ";
      }

      /* If all we're printing is blanks, ignore this line: */
      if (line.trim().length() == 0)
      {
        offset++;
        continue;
      }

      /* The first line in the output gets an extra prefix: */
      if (first_line)
        line = Format.f(PREFIX, prefix) + line;
      else
        line = Format.f(PREFIX, "") + line;
      first_line = false;

      /* Skip to the next character to be printed: */
      offset++;
      lines.add(line);
    }

    lines.add("");
  }



  public static void main(String args[]) throws Exception
  {
    //showBuckets();

    /* Fill a test bucket: */
    Random rand = new Random(0);
    new BucketRanges("test", "1-30,d,33-45,1");
    Histogram hist = new Histogram("test");
    for (int i = 0; i < 10000; i++)
      hist.addToBucket(Math.abs(rand.nextLong() % 31) + 10);

    for (int i = 9990; i < hist.counters.length; i++)
    {
      if (hist.counters[i] != 0)
        common.ptod("bucket: " + i + " " + hist.counters[i]);
    }

    Vector lines = hist.printit();
    for (int i = 0; i < lines.size(); i++)
      common.ptod(lines.elementAt(i));
  }
}


class BucketLine extends VdbObject
{
  int    pct;
  long   count;
  String max_string;
  String pct_string;
  String cnt_string;
  String star_string;

  /* Note: must be a minimum of CHART_HEIGHT bytes: */
  static String STARS = "";
  static String BLNKS = "";
  {
    for (int i = 0; i < Histogram.CHART_HEIGHT; i++) STARS += "*";
    for (int i = 0; i < Histogram.CHART_HEIGHT; i++) BLNKS += " ";

  }

  /**
   * Create an instance with in there all data we need:
   * - %%
   * - %% printable
   * - number of stars to print, right adjusted in a 32-byte string.
   */
  public BucketLine(double pct_in, int bucket_no, long cnt, BucketRanges br)
  {
    pct = (int) pct_in;
    count = cnt;
    max_string = br.getMax(bucket_no) + BLNKS;

    pct_string = (pct_in != 0) ? (Format.f("%3d%%", (int) pct_in) + BLNKS) : BLNKS;

    cnt_string = (count != 0) ? Format.f("%12d", count) + BLNKS : BLNKS;

    int scale = pct / (100 / Histogram.CHART_HEIGHT);
    star_string  = BLNKS.substring(0, (Histogram.CHART_HEIGHT - scale));
    star_string += STARS.substring(0, scale);

  }


  public String getWhich(String which)
  {
    if (which.equals("cnt"))
      return cnt_string;
    else if (which.equals("pct"))
      return pct_string;
    else if (which.equals("max"))
      return max_string;
    else if (which.equals("stars"))
      return star_string;
    else
      common.failure("Illegal which hunt: " + which);

    return null;
  }
}
