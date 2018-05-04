package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;

import Utils.Format;


/**
 * Code to maintain response time histogram statistics.
 *
 * There is one thing still to do: run a separate thread that every 'n' seconds
 * sorts the bucket list, or instead, create an MRU list for the buckets.
 * May also want to remember the --last-- bucket used to possibly find a quick
 * match. However, if the list is sorted frequently that is implied.
 */
public class Histogram implements java.io.Serializable, Cloneable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private long[] counters = null;

  private BucketRanges ranges;
  private BucketRanges sorted_ranges;

  private boolean header_printed = false;

  private static DecimalFormat df = new DecimalFormat("#,###");
  private static String STARS = "--------------------------------------------------";
  private static String PLUS  = "++++++++++++++++++++++++++++++++++++++++++++++++++";



  public Histogram(String type)
  {
    ranges   = BucketRanges.getRangesFromType(type);
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


  /**
   * Get a long[] array to be used for Jni code.
   * This array contains a pair of longs for each bucket low and high.
   */
  public long[] getJniBucketArray()
  {
    return ranges.getJniBucketArray();
  }
  public void storeJniBucketArray(long[] array)
  {
    for (int i = 0; i < array.length / 3; i++)
      counters[i] = array [i * 3 + 2];
  }


  public long[] getCounters()
  {
    return counters;
  }
  public long getTotals()
  {
    long total = 0;
    for (int i = 0; i < counters.length; i++)
      total += counters[i];
    return total;
  }


  public static void main(String args[]) throws Exception
  {
    double value = Double.parseDouble(args[0]);
    common.ptod("xx: ===>" + getShort(value, 7) + "<===");
  }

  public ArrayList printHistogram(String title)
  {
    long total;
    ArrayList output = new ArrayList(64);
    //if (!header_printed)
    //  output.add(printHeader());

    /* If we have nothing to print, don't bother: */
    if ((total = getTotals()) == 0)
      return output;

    output.add(title);
    String line = String.format(" %8s <  %10s   %10s %8s %8s  ",
                                "min(ms)", "max(ms)", "count", "%%", "cum%%");
    output.add(line + "'+': Individual%; '+-': Cumulative%" );
    output.add("");

    /* Then calculate maximum percentage: */
    double max_pct = 0;
    BucketRange[] branges = ranges.getRanges();
    for (int i = 0; i < branges.length; i++)
      max_pct = Math.max((double) counters[i] * 100. / total, max_pct);

    /* Fixed value: how many stars per percentage point: */
    double plus_per_pct = 0.5;

    /* Skip beginning and ending zeroes, but print zeros in the middle: */
    /* However only use those when requested: */
    int first = 0;
    int last  = branges.length -1;
    if (ranges.suppress())
    {
      for (; first < branges.length && counters[first] == 0; first++);
      for (; last >= 0 && counters[last] == 0; last--);
    }

    /* Print the last empty at the beginning and first empty at the end though: */
    if (first > 0)
      first--;
    if (last < branges.length -1)
      last++;

    long cumulative = 0;
    for (int i = first; i <= last; i++)
    {
      long counter     = counters[ branges[i].which];
      cumulative      += counter;
      double pct       = (double) counter * 100. / total;
      double cum_pct   = (double) cumulative * 100. / total;
      int plus_needed  = (int) (pct * plus_per_pct);
      int stars_needed = (int) ((cum_pct - pct) * plus_per_pct);
      String plusses   = (plus_needed > 0) ? PLUS.substring(0, plus_needed) : "";
      String stars     = (stars_needed > 0)  ? STARS.substring(0, stars_needed) : "";


      String xmax = (branges[i].max == Long.MAX_VALUE) ? "max" : getShort(branges[i].max / 1000., 8);
      line = String.format(" %8s < %11s %12s %8.4f %8.4f  %s%s",
                           getShort(branges[i].min / 1000., 8),
                           xmax,
                           df.format(counter),
                           pct, cum_pct, plusses, stars);
      output.add(line);
    }

    output.add("");

    return output;
  }

  private static String getShort(double num, int max)
  {
    String txt = String.format("%.3f", num);
    while (txt.length() > max)
    {
      if (txt.endsWith("."))
        break;
      txt = txt.substring(0, txt.length() -1);
    }

    if (txt.endsWith("."))
      return txt.substring(0, txt.length() -1);
    return txt;
  }

  // I currently have no way to stop this from being printed only once for each Report()!!!
  private String printHeader()
  {
    header_printed = true;
    String hdr = "All but one of the empty buckets at the beginning \n";
    hdr       += "and all but one empty buckets at the end will will be skipped.\n";
    hdr       += "Empty buckets in the middle though will be printed.\n";
    return hdr;
  }
}

