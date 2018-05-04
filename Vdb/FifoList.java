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
 * Maintain a list of Fifos to allow for i/o priorities.
 * Priority 0 is highest.
 */
public class FifoList
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private Fifo[] fifos = null;
  private int    fifo_length;

  public FifoList(String name, int size, int prios)
  {
    fifo_length = size;
    fifos = new Fifo[ prios ];
    for (int i = 0; i < prios; i++)
      fifos[i] = new Fifo(name, size);
  }

  /**
   * Count and validate the workload priorities used.
   * The default priority (Integer.MAX_VALUE) will be set to the next available
   * priority
   */
  public static int countPriorities(Vector wg_list)
  {
    boolean any_default = false;
    HashMap prios = new HashMap(32);
    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);
      if (wg.hasPriority())
        prios.put(new Integer(wg.getpriority()), null);
      else
        any_default = true;
    }

    Integer[] ties = (Integer[]) prios.keySet().toArray(new Integer[0]);
    Arrays.sort(ties);

    int next_prio = 0;
    for (int i = 0; i < ties.length; i++)
    {
      if (ties[i].intValue() != next_prio++)
      {
        for (i = 0; i < ties.length; i++)
          common.ptod("Priority: " + ties[i] + 1);
        common.failure("Workload priorities must be defined in numeric sequence starting at 1");
      }
    }

    /* If any default priority is used, set it to the next value: */
    if (any_default)
    {
      for (int i = 0; i < wg_list.size(); i++)
      {
        WG_entry wg = (WG_entry) wg_list.elementAt(i);
        if (!wg.hasPriority())
        {
          wg.setPriority(ties.length);
          prios.put(new Integer(ties.length), null);
        }
      }
    }

    return prios.size();
  }


  /**
   * Look for work in the fifos. If we can't find any work, sleep.
   */
  public int getArray(Object[] array) throws InterruptedException
  {
    while (!SlaveJvm.isWorkloadDone())
    {
      /* Look for the first Fifo who has some work: */
      for (int i = 0; i < fifos.length; i++)
      {
        int burst = fifos[i].getArray(array, false);
        if (burst != 0)
          return burst;
      }

      /* We did not find any work, sleep a bit: */
      common.sleep_some(1);
    }

    /* End of the road for us: */
    return 0;
  }

  public void put(Object obj, int prio) throws InterruptedException
  {
    /* if this queue is too full, throw it away: */
    //if (prio > 0 && fifos[prio].getQueueDepth() > 1800)
    //  common.ptod("fifos[prio].getQueueDepth(): " + fifos[prio].getQueueDepth());

    fifos[prio].put(obj);
  }


  public void setThreadCount(int t)
  {
    for (int i = 0; i < fifos.length; i++)
      fifos[i].setThreadCount(t);
  }


  /**
   * When the fifo gets to x% full, wait until it goes back to y%
   */
  public void waitForRoom(int p) throws InterruptedException
  {
    fifos[p].waitForRoom();
  }

  public int getQueueDepth(int p)
  {
    return fifos[p].getQueueDepth();
  }

  public boolean isGettingFull(int p)
  {
    return fifos[p].isGettingFull();
  }
}
