package Utils;

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

import java.util.*;


/**
 * Kstat named data manipulation.
 */
public class NamedData extends Vdb.VdbObject implements java.io.Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private LookupAnchor instance_anchor = null;
  private long   last_ts  = 0;   /* in Microseconds */
  private long   elapsed  = 0;   /* in Microseconds */
  private long[] counters = null;

  // this can give a problem when compiling Swat. Deal with it separately
  //public String[] getTitles();


  private        int seqno    = tcounter++;
  private static int tcounter = 0;

  private static String current_new_label = null;


  public NamedData(LookupAnchor anchor)
  {
    instance_anchor = anchor;
    counters        = new long[ anchor.getFieldCount() ];
  }


  public synchronized static NamedData newInstance(String iname)
  {
    String name = iname;

    //common.ptod("newInstance: " + name);
    if (iname.indexOf("/") != -1)
    {
      name              = iname.substring(0, iname.indexOf("/"));
      current_new_label = iname.substring(iname.indexOf("/") + 1);
      //common.ptod("current_new_label: " + current_new_label);
    }

    try
    {
      return(NamedData) Class.forName(name).newInstance();
    }

    catch (ClassNotFoundException e)
    {
      common.failure(e);
    }
    catch (InstantiationException e)
    {
      common.ptod("This error can happen if the Class being instantiated ");
      common.ptod("does not have a 'blank' instantiation defined, ");
      common.ptod("e.getCause. public Vmstatdata() { };");
      common.failure(e);
    }
    catch (IllegalAccessException e)
    {
      common.failure(e);
    }

    return null;
  }

  public static String getCurrentNewLabel()
  {
    if (current_new_label == null)
      common.failure("current_new_label may not be null");
    return current_new_label;
  }
  public static void clearCurrentNewLabel()
  {
    current_new_label = null;
  }

  public String getAnchorName()
  {
    return instance_anchor.getAnchorName();
  }

  public void setAnchor(LookupAnchor anc)
  {
    instance_anchor = anc;
  }
  public void setCounters(long[] c)
  {
    counters = c;
  }


  /**
   * Check the length of the counter array.
   * The anchor for this instance can have increased its amount of fields that it
   * supports. If a change occurred make sure the counters[] array gets the proper
   * size.
   */
  private void validateCounterLength()
  {
    if (instance_anchor.getFieldCount() > counters.length)
    {
      synchronized(this)
      {
        /* First make sure no other thread already changed it.   */
        /* I am not sure if there can be any, but what the heck: */
        if (instance_anchor.getFieldCount() > counters.length)
        {
          long[] array = new long[ instance_anchor.getFieldCount() ];
          System.arraycopy(counters, 0, array, 0, counters.length);
          counters = array;
        }
      }
    }
  }


  /**
   * Get the total of all counters.
   */
  public double getTotal()
  {
    double total = 0;
    for (int i = 0; i < counters.length; i++)
    {
      //Lookup look = instance_anchor.getLookupForIndex(i);
      //common.ptod("look.getScale(): " + look.getScale());
      total += counters[i]; // / look.getScale();
    }
    return total;
  }

  /**
   * Get a counter addressed by a Lookup entry.
   *
   * Beware: the value of the counter is adjusted both by Lookup.scale and by
   * LookupAnchor.useDoubles().
   */
  public double getCounter(Lookup look)
  {
    double ret = 0;
    try
    {
      validateCounterLength();

      ret = (double) counters[ look.getIndex() ] / look.getScale();
    }

    catch (Exception e)
    {
      common.ptod("look: " + look);
      if (look != null)
        common.ptod("title: " + look.getTitle());
      common.failure(e);
    }

    return ret;
  }
  public void setCounter(Lookup look, long cnt)
  {
    validateCounterLength();
    counters[ look.getIndex() ] = cnt;
  }

  /**
   * Get all counters
   */
  public long[] getCounters()
  {
    validateCounterLength();
    return counters;
  }


  /**
   * Store elapsed time
   */
  public void setElapsed(long el)
  {
    elapsed = el;
  }

  /**
   * Return elapsed time
   */
  public long getElapsed()
  {
    return elapsed;
  }


  /**
   * Store timestamp
   */
  public void setTime(long time)
  {
    last_ts = time;
  }


  /**
   * Return timestamp
   */
  public long getTime()
  {
    return last_ts;
  }



  public double getRate(int relative)
  {
    validateCounterLength();
    return counters[ relative ] * 1000000. / getElapsed();
  }



  /**
   * Calculate delta between current and an older set of counters.
   */
  public void delta(NamedData nw, NamedData old)
  {
    validateCounterLength();
    nw.validateCounterLength();
    old.validateCounterLength();


    if (nw.counters.length != old.counters.length)
      common.failure("Unequal array lengths: " + nw.counters.length + "/" + old.counters.length);

    counters = new long[nw.counters.length];

    /* Make sure you handle overflow! */
    for (int i = 0; i < counters.length; i++)
    {
      if (nw.counters[i] >= old.counters[i])
        counters[i] = nw.counters[i] - old.counters[i];
      else
      {
        counters[i] = old.counters[i] - nw.counters[i];
        common.ptod("NamedData.delta() overflow handled for " +
                    instance_anchor.getFieldNames()[i]);
      }
    }

    elapsed = nw.last_ts - old.last_ts;
    last_ts = nw.last_ts;
  }


  /**
   * Accumulate one source set of counters.
   */
  public void accum(NamedData src)
  {
    validateCounterLength();
    src.validateCounterLength();


    /* if we have not allocated the counters yet, do so now: */
    if (counters == null)
      counters = new long[ src.counters.length ];

    /* Fill up the buckets: */
    for (int i = 0; i < counters.length; i++)
    {
      counters[i] += src.counters[i];
    }
    elapsed += src.elapsed;
    last_ts  = src.last_ts;
  }


  /**
   * Translate instance to a long array.
   *
   * The first two elements in the new array will contain the last_ts and elapsed
   * values.
   */
  public long[] export()
  {
    long[] array = new long[ counters.length+2 ];
    array[0] = last_ts;
    array[1] = elapsed;
    System.arraycopy(counters, 0, array, 2, counters.length);

    //for (int i = 0; i < array.length; i++)
    //  common.ptod("arrayx: " + array[i]);

    return array;
  }


  /**
   * Translate long[] array into instance, reversing export()
   */
  public void emport(long[] array)
  {
    counters = new long[ array.length-2 ];
    last_ts = array[0];
    elapsed = array[1];
    System.arraycopy(array, 2, counters, 0, counters.length);
  }

  public String[] getFieldTitles()
  {
    return instance_anchor.getFieldTitles();
  }
  public String[] getFieldNames()
  {
    return instance_anchor.getFieldNames();
  }

  public String toString()
  {
    String txt = this.getAnchorName() + " " +
                 seqno + " " +
                 new Date(last_ts) + " " +
                 elapsed;
    for (int i = 0; counters != null && i < counters.length; i++)
      txt += " " + counters[i];

    return txt;
  }


  /**
   * Find the Lookup instance matching the requested field name.
   */
  public Lookup getLookupForField(String field)
  {
    return instance_anchor.getLookupForField(field);
  }

  /**
   * Find the relative index for a specific field name
   */
  public int obsolete_getIndexForField(String field)
  {
    return instance_anchor.getLookupForField(field).getIndex();
  }


  /**
   * Validate the lookup table.
   */
  public void validateLookupTable(String[] new_list)
  {
    instance_anchor.validateLookupTable(new_list);
  }


  public LookupAnchor getAnchor()
  {
    return instance_anchor;
  }


  /**
   * Parse data received from Jni into a new NamedData instance.
   * Input data starts with all labels and ends with all counters, with
   * an asterix in between.
   */
  public void parseNamedData(String data)
  {
    if (data.indexOf("*") == -1)
      common.failure("Invalid data syntax: " + data);

    /* Get all the field names: */
    String   field_string = data.substring(0, data.indexOf("*")).trim();
    String[] fields;
    if (field_string.indexOf("$") == -1)
      fields = field_string.split(" +");
    else
      fields = field_string.split("\\$");

    /* Get all the counters: */
    String   counter_string = data.substring(data.indexOf("*") + 1).trim();
    String[] counters       = counter_string.split(" +");

    if (fields.length != counters.length)
    {
      common.ptod("fields.length:   " + fields.length);
      common.ptod("field_string:    " + field_string);
      common.ptod("counters.length: " + counters.length);
      common.ptod("counter_string:  " + counter_string);
      common.failure("Unequal token count. Receiving more labels than expected");
    }

    /* Some times some math error results in garbage:
    for (int i = 0; i < counters.length; i++)
    {
      try
      {
        double tmp = Double.parseDouble(counters[i]);
      }
      catch (Exception e)
      {
        counters[i] = "0";
      }
    } */

    /* If any new fields came in add them: */
    validateLookupTable(fields);

    /* Store the counters in the proper place decided by the label: */
    for (int i = 0; i < fields.length; i++)
    {
      Lookup look = getLookupForField(fields[i]);

      /* If we can't find this field we're in trouble: */
      if (look == null)
        common.ptod("Lookup missing for field: " + fields[i]);

      if (!instance_anchor.useDoubles())
      {
        if (counters[i].indexOf(".") == -1)
          setCounter(look, Long.parseLong(counters[i]));
        else
          setCounter(look, (long) Double.parseDouble(counters[i]));
      }
      else
      {
        setCounter(look, (long) (Double.parseDouble(counters[i]) * 1000.) );
      }
    }
  }

}



