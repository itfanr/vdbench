package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import Utils.ClassPath;
import Utils.Fget;
import Utils.Format;


/**
  */
public class BucketRanges implements java.io.Serializable, Cloneable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String        type;
  private BucketRange[] ranges;
  private String[]      maxes;
  private boolean       suppress = false;

  private static        HashMap <String, BucketRanges> type_map = setDefaultHistogram();


  public BucketRanges(String[] parms)
  {
    this.type = parms[0];

    parseList(findOptions(parms));

    type_map.put(type, this);
    createMaxText();
  }

  public boolean suppress()
  {
    return suppress;
  }

  public static HashMap getBucketTypes()
  {
    return type_map;
  }
  public static void setBucketTypes(HashMap tps)
  {
    type_map = tps;
  }
  public static void setOption(String type, String option)
  {
    BucketRanges br = type_map.get(type);
    if (br == null)
      common.failure("BucketRanges.setOption(): bucket %s not found", type);
    if ("suppress".startsWith(option))
      br.suppress = true;
    else
      common.failure("BucketRanges.setOption(): option %s not found", option);
  }

  public Object clone()
  {
    try
    {
      BucketRanges br = (BucketRanges) super.clone();
      br.ranges       = (BucketRange[]) ranges.clone();

      return br;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  private void createMaxText()
  {
    maxes = new String[ ranges.length ];
    String last_max = null;
    for (int i = 0; i < ranges.length; i++)
    {
      BucketRange br = ranges[i];
      if (br.max < 10000)
        last_max = Format.f("%12dus", br.max);

      else if (br.max < 1000000)
        last_max = Format.f("%12.3fms", br.max / 1000.);

      else if (i != ranges.length -1)
        last_max = Format.f(" %12.3fs", br.max / 1000000.);

      else
      {
        last_max = ">" + last_max.trim();
        last_max = Format.f("  %12s", last_max);
      }

      maxes[i] = last_max;

      //if (type.equals("default"))
      //  common.ptod("MAXES[i] for type + " + i + " " + type + ": " + maxes[i]);
    }
  }

  public String getMax(int m)
  {
    return maxes[m];
  }


  public int getBucketCount()
  {
    return ranges.length;
  }

  public static BucketRanges getRangesFromType(String type)
  {
    BucketRanges hb = type_map.get(type);
    if (hb == null)
      common.failure("Unable to find BucketRanges type: " + type);
    return hb;
  }

  public BucketRange[] getRanges()
  {
    return ranges;
  }

  // This should be in Histogram where we then can use an MRU list.
  public int findBucket(long resp)
  {
    for (int i = 0; i < ranges.length; i++)
    {
      BucketRange br = ranges[i];
      if (resp >= br.min && resp < br.max)
        return br.which;
    }

    common.ptod("Requesting bucket for: " + resp);
    for (int i = 0; i < ranges.length; i++)
      common.ptod("" + ranges[i]);
    common.failure("Unable to find bucket. Are we having a Harry Belafonte moment here? " + resp);
    return -1;
  }

  /**
   * Parse the input parameter list creating a long[] array containing each
   * pairs of bucket ranges.
   */
  private void parseList(String[] parms)
  {
    ArrayList array = new ArrayList(64);

    /* First in the input is the name, skip. */
    long last_value = -1;
    long increment  = 0;
    for (int i = 1; i < parms.length; i++)
    {
      String value = parms[i];
      long usecs = 1;
      if (value.endsWith("u"))
      {
        usecs = 1;
        value = value.substring(0, value.length() - 1);
      }
      else if (value.endsWith("m"))
      {
        usecs = 1000;
        value = value.substring(0, value.length() - 1);
      }
      else if (value.endsWith("s"))
      {
        usecs = 1000000;
        value = value.substring(0, value.length() - 1);
      }

      /* If the new number is less or equal, treat it as an increment: */
      long number = Long.parseLong(value) * usecs;
      if (number <= last_value)
      {
        increment = number;
        continue;
      }

      /* If we have a pending increment, do a 'for (x through y): */
      if (increment > 0)
      {
        for (long next = last_value + increment; next < number; next += increment)
          array.add(new Long(next));
        increment = 0;
      }

      /* Just a regular new number, use it: */
      array.add(new Long(number));
      last_value = number;
    }

    /* Check some value: */
    if (array.size() == 0)
      common.failure("Histogram bucket list is empty. ");
    if (((Long) array.get(0)).longValue() == 0)
      common.failure("First bucket may not be zero: " + array.get(0));

    /* Add the default last bucket: */
    array.add(new Long(Long.MAX_VALUE));

    if (array.size() > 64)
    {
      for (int i = 0; i < array.size() && i < 64; i++)
        common.ptod("bucket: " + i + " " + array.get(i));
      common.failure("Histogram bucket list may not contain more than 64 buckets");
    }

    //for (int i = 0; i < array.size() && i < 64; i++)
    //  common.ptod("buckets: " + i + " " + array.get(i));

    /* Now translate this list to pairs of longs showing a bucket's range: */
    ranges = new BucketRange[ array.size() ];
    long last_max = 0;
    for (int i = 0; i < array.size(); i++)
    {
      long this_max = ((Long) array.get(i)).longValue();
      ranges[i] = new BucketRange(last_max, this_max, i);
      last_max = this_max;
    }

    //for (int i = 0; common.get_debug(common.PRINT_BUCKETS) && i < ranges.length; i++)
    //  common.ptod(ranges[i]);
  }


  /**
   * Identify, use, and remove possible options that have been coded.
   */
  private String[] findOptions(String[] parms)
  {
    while (true)
    {
      /* Look for an option: */
      boolean found = false;
      for (int i = 1; i < parms.length; i++)
      {
        if ("suppress".startsWith(parms[i]))
          found = true;
      }

      if (!found)
        return parms;

      /* Now remove and use it: */
      Vector <String> next = new Vector(parms.length);
      for (int i = 1; i < parms.length; i++)
      {
        if ("suppress".startsWith(parms[i]))
          suppress = true;
        else
          next.add(parms[i]);
      }

      /* Replace array and check again for (possible) others: */
      parms = (String[]) next.toArray(new String[0]);
    }
  }


  /**
   * Get a long[] array to be used for Jni code.
   * This array contains a pair of longs for each bucket low and high, and a
   * long for the actual counter.
   */
  public long[] getJniBucketArray()
  {
    long[] array = new long[ ranges.length * 3];
    for (int i = 0; i < array.length / 3; i++)
    {
      array[i*3 + 0] = ranges[i].min;
      array[i*3 + 1] = ranges[i].max;
      array[i*3 + 2] = 0;
    }
    return array;
  }

  /**
  * Set the hardcoded default bucket sizes.
  * This default can be overridden with 'histogram.txt'
  */
  private static HashMap setDefaultHistogram()
  {
    type_map = new HashMap(8);
    String[] dflt = new String[]
    {
      "default", "20",  "40",   "60",   "80",   "100",  "200",  "400", "600",
      "800",     "1m",  "2m",   "4m",   "6m",   "8m",   "10m",  "20m", "40m",
      "60m",     "80m", "100m", "200m", "400m", "600m", "800m", "1s",  "2s"
    };

    /* Read 'histogram.txt to pick up default override: */
    if (Fget.file_exists(ClassPath.classPath("histogram.txt")))
    {
      String[] lines = Fget.readFileToArray(ClassPath.classPath("histogram.txt"));
      for (int i = 0; i < lines.length; i++)
      {
        if (lines[i].startsWith("*"))
          continue;
        if (!lines[i].startsWith("histogram="))
          common.failure("Reading file %s; expecting 'histogram='",
                         ClassPath.classPath("histogram.txt"));
        StringTokenizer st = new StringTokenizer(lines[i], "=,()");
        String[] tokens = new String[ st.countTokens() - 1 ];
        st.nextToken();
        for (int j = 0; j < tokens.length; j++)
          tokens[j] = st.nextToken();

        /* If coding only two parameters, there is an extra option: */
        if (tokens.length == 2)
        {
          /* If a user specifies an option we better have the default first: */
          if (type_map.size() == 0)
            new BucketRanges(dflt);
          setOption(tokens[0], tokens[1]);
        }
        else
          new BucketRanges(tokens);
      }
      return type_map;
    }

    new BucketRanges(dflt);
    return type_map;
  }

  public static void main(String[] args)
  {
    String type = args[0];
    String list = args[1];

    //BucketRanges hb = new BucketRanges(type, list);
    BucketRanges hb = BucketRanges.getRangesFromType("default");
    common.ptod("hb 0       : " + hb.findBucket(0        ));
    common.ptod("hb 1       : " + hb.findBucket(1        ));
    common.ptod("hb 2       : " + hb.findBucket(2        ));
    common.ptod("hb 8388608 : " + hb.findBucket(8388608  ));
    common.ptod("hb 16777215: " + hb.findBucket(16777215 ));
    common.ptod("hb 16777216: " + hb.findBucket(16777216 ));
  }
}


/**
 * BucketRange contains the min (inclusive) and max (exclusive) range of a bucket.
 * It also contains the bucket number where this range is being counted.
 * This allows an MRU sort to be done on a copy of this list so that when we
 * search to find which bucket to use we can start with those buckets that will
 * likely be found first.
 */
class BucketRange implements java.io.Serializable
{
  public long min;
  public long max;
  public int  which;

  public BucketRange(long min, long max, int which)
  {
    this.min   = min;
    this.max   = max;
    this.which = which;
  }


  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
  }

  public String toString()
  {
    if (max == Long.MAX_VALUE)
      return String.format("BucketRange for bucket %4d: %10d - %-10s ", which, min, "max");
    else
      return String.format("BucketRange for bucket %4d: %10d - %-10d ", which, min, max);
  }
}
