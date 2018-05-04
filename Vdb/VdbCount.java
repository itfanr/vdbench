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

import java.util.*;


/**
 *
 */
public class VdbCount
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private String      name      = null;
  private long        counts    = 0;
  private long        subs      = 0;

  private static HashMap list_of_counters = new HashMap(64);

  private static boolean count_active = common.get_debug(common.COUNT_INSTANCES);


  public static void count(Object obj)
  {
    if (!count_active)
      return;

    /* See if a counter exists for this Class, if not, create one: */
    VdbCount vdbc = (VdbCount) list_of_counters.get(obj.getClass().getName());
    if (vdbc != null)
    {
      vdbc.counts++;
      return;
    }

    /* We did the 'normal' one without locking to speed things up. */
    /* We now allocate a new entry if still needed:                */
    synchronized (list_of_counters)
    {
      /* See if a counter exists for this Class, if not, create one: */
      vdbc = (VdbCount) list_of_counters.get(obj.getClass().getName());
      if (vdbc == null)
      {
        vdbc      = new VdbCount();
        vdbc.name = obj.getClass().getName();
        list_of_counters.put(obj.getClass().getName(), vdbc);
      }

      vdbc.counts++;
    }
  }


  public static void sub(Object obj)
  {
    if (!count_active)
      return;

    /* See if a counter exists for this Class, if not, create one: */
    VdbCount vdbc = (VdbCount) list_of_counters.get(obj.getClass().getName());
    if (vdbc == null)
      common.failure("Doing a subtract for an unknown class: " + obj.getClass().getName());

    vdbc.subs++;
  }



  public static void listCounters(String txt)
  {
    double free  = (double) Runtime.getRuntime().freeMemory()  / 1048576.0;
    double total = (double) Runtime.getRuntime().totalMemory() / 1048576.0;

    common.ptod("VdbCount statistics for " + txt);
    common.ptod("Java heap: %8.3f / %8.3f / %8.3f",
                total, free, total - free);

    String[] keys = (String[]) list_of_counters.keySet().toArray(new String[0]);
    Arrays.sort(keys);

    for (int i = 0; i < keys.length; i++)
    {
      VdbCount vdbc = (VdbCount) list_of_counters.get(keys[i]);
      if (vdbc.counts - vdbc.subs > 10)
        common.ptod("VdbCount : %-26s %6d %6d %6d", vdbc.name,
                    vdbc.counts - vdbc.subs, vdbc.counts, vdbc.subs);
    }
  }
}

