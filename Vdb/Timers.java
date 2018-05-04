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

import Utils.Format;


/**
 * This class handles vdbench performance measurements for debugging
 */
public class Timers
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  long   tod_before;
  long   tod_total;
  long   tod_counts;
  long   first_tod;
  long   last_tod;
  boolean  active;
  String label;

  public Timers(String label_in)
  {
    tod_total  = 0;
    tod_counts = 0;
    label      = label_in;
    active     = common.get_debug(common.TIMERS);
  }


  public void before()
  {
    if (active)
    {
      if (tod_counts == 0)
        first_tod  = Native.get_simple_tod();
      tod_before = Native.get_simple_tod();
    }
  }


  public void after()
  {
    if (active)
    {
      if (tod_before == 0)
        common.failure("Timers.after(): previous tod equals zero");
      last_tod = Native.get_simple_tod();
      tod_total += last_tod - tod_before;
      tod_before = 0;
      tod_counts++;
    }
  }

  public void add(long time)
  {
    if (active)
    {
      if (tod_counts == 0)
        first_tod  = Native.get_simple_tod();
      last_tod   = Native.get_simple_tod();
      tod_total += time;
      tod_counts++;
    }
  }


  public void print()
  {
    if (active)
    {
      try
      {
      common.ptod(Format.f("++Timers: %-20s ", label) +
                  Format.f("%6.2f%%;", (double) tod_total * 100 / (last_tod - first_tod)) +
                  Format.f(" avg %6d us", tod_counts > 0 ? tod_total / tod_counts : 0));
      }
      catch (Exception e)
      {
        common.ptod("Exception in Timers.print(): " + e.toString());
      }
    }
  }
}


