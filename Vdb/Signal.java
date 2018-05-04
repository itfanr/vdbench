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

public class Signal
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private long base  = 0;
  private long msecs = 0;
  private int  signals_given = 0;

  public Signal(int secs)
  {
    msecs = secs * 1000;
  }

  /**
   * Signal caller after n milliseconds.
   * Returns true if more than 'msecs' time elapsed since the first call.
   */
  public boolean go()
  {
    long tod = System.currentTimeMillis();

    /* First call, just set base tod: */
    if (base == 0)
    {
      base = tod;
      return false;
    }

    /* If tod expired, return true which is the signal that time expired */
    //common.ptod("base: " + tod + " " + base + " " + (tod - base));
    if (base + msecs < tod)
    {
      base = tod;
      signals_given++;
      return true;
    }

    return false;
  }
  public boolean anySignals()
  {
    return signals_given != 0;
  }

  public static void main(String[] args)
  {
    Signal signal = new Signal(5);
    for (int i = 0; i < 20; i++)
    {
      common.sleep_some_usecs(1000000);
      if (signal.go())
        common.where();
    }
  }
}


