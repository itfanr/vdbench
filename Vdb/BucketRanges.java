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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import Utils.Format;


/**
  */
public class BucketRanges extends VdbObject implements java.io.Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String type;
  private BucketRange[] ranges;
  private String[] maxes;

  private static HashMap types = null;

  static
  {
    types = new HashMap(8);
    new BucketRanges("default", "1-2147483647,d");
  }


  public static HashMap getBucketTypes()
  {
    return types;
  }
  public static void setBucketTypes(HashMap tps)
  {
    types = tps;
  }

  public BucketRanges(String type, String list)
  {
    this.type = type;
    parseList(list);

    types.put(type, this);
    createMaxText();
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

  public static BucketRanges getRanges(String type)
  {
    BucketRanges hb = (BucketRanges) types.get(type);
    if (hb == null)
      common.failure("Unable to find BucketRanges type: " + type);
    return hb;
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
      common.ptod(ranges[i]);
    common.failure("Unable to find bucket. Are we having a Harry Belafonte moment here?");
    return -1;
  }

  /**
   * Parse the input parameter list creating a long[] array containing each
   * pairs of bucket ranges.
   */
  private void parseList(String list)
  {
    ArrayList array    = new ArrayList(64);

    /* Use tokenizer to split the list: */
    StringTokenizer st = new StringTokenizer(list, "-,", true);

    /* Put the tokens then into an array: */
    String[] tokens = new String[ st.countTokens() ];
    for (int i = 0; i < tokens.length; i++)
      tokens[i] = st.nextToken();

    /* Further parse the tokens: */
    boolean pending_range = false;
    long     last_value = -1;
    for (int i = 0; i < tokens.length; i++)
    {
      String tmp = tokens[i];
      if (tmp.equals("-"))
      {
        /* We have a pending range. Pick the next token to find out what to do: */
        pending_range = true;
        if (i+3 >= tokens.length)
          common.failure("Requesting histogram bucket range, but no end value specified: " + list);

        String end_range = tokens[i+1];
        String option    = tokens[i+2];
        String increment = tokens[i+3];

        //common.ptod("end_range: " + end_range);
        //common.ptod("option:    " + option);
        //common.ptod("increment: " + increment);

        if (!isNumeric(end_range))
          common.failure("Histogram bucket list: parsed value not numeric: (" +
                         end_range + ") " + list);

        if (!option.equals(","))
          common.failure("A histogram bucket range ('-x') must always be followed " +
                         "by a comma: (" + option + ") " + list);

        long end = Long.parseLong(end_range);
        if (increment.equalsIgnoreCase("d"))
        {
          for (last_value *= 2; last_value <= end; last_value *= 2)
            array.add(new Long(last_value));
        }
        else
        {
          long plus = Long.parseLong(increment);
          for (last_value += plus; last_value < end; last_value += plus)
            array.add(new Long(last_value));
          last_value--;
        }

        /* Skip past the tokens we used here: */
        i +=3 ;
        continue;
      }

      if (tmp.equals(","))
        continue;

      if (!isNumeric(tmp))
        common.failure("Histogram bucket list: parsed value not numeric: (" +
                       tmp + ") " + list);

      long number = Long.parseLong(tmp);
      if (number <= last_value)
      {
        common.ptod("Histogram bucket list parameters: " + list);
        common.failure("Histogram bucket list values must be incrementing. Last: " +
                       last_value + "; New: " + number);
      }

      array.add(new Long(number));
      last_value = number;
    }

    /* Check some value: */
    if (array.size() == 0)
      common.failure("Histogram bucket list is empty: " + list);
    if (((Long) array.get(0)).longValue() == 0)
      common.failure("First bucket may not be zero: " + list);

    /* Add the default last bucket: */
    array.add(new Long(Long.MAX_VALUE));

    if (array.size() > 1024)
    {
      for (int i = 0; i < array.size() && i < 1024; i++)
        common.ptod("bucket: " + i + " " + array.get(i));
      common.failure("Histogram bucket list may not contain more than 1024 buckets");
    }

    /* Now translate this list to pairs of longs showing a bucket's range: */
    ranges = new BucketRange[ array.size() ];
    long last_max = 0;
    for (int i = 0; i < array.size(); i++)
    {
      long this_max = ((Long) array.get(i)).longValue();
      ranges[i] = new BucketRange(last_max, this_max, i);
      last_max = this_max;
    }

   for (int i = 0; common.get_debug(common.PRINT_BUCKETS) && i < ranges.length; i++)
     common.ptod(ranges[i]);
  }

  private static boolean isNumeric(String txt)
  {
    try
    {
      long number = Long.parseLong(txt);
    }
    catch (NumberFormatException e)
    {
      return false;
    }
    return true;
  }

  public static void main(String[] args)
  {
    String type = args[0];
    String list = args[1];

    //BucketRanges hb = new BucketRanges(type, list);
    BucketRanges hb = BucketRanges.getRanges("default");
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
 * likely be found firstElement.
 */
class BucketRange extends VdbObject implements java.io.Serializable
{
  public long min;
  public long max;
  public int  which;

  public BucketRange(long min, long max, int which)
  {
    //Instances.count(this);
    this.min   = min;
    this.max   = max;
    this.which = which;
  }


  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    //Instances.count(this);
  }

  public String toString()
  {
    if (max == Long.MAX_VALUE)
      return String.format("BucketRange for bucket %4d: %10d - %-10s ", which, min, "max");
    else
      return String.format("BucketRange for bucket %4d: %10d - %-10d ", which, min, max);
  }
}
