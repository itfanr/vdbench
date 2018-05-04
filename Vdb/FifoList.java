package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Fifo[] fifos = null;
  private int    fifo_length;

  long sleeps = 0;

  private final static boolean spin = common.get_debug(common.SPIN);

  public FifoList(String name, int size, int prios)
  {
    fifo_length = size;
    fifos       = new Fifo[ prios ];
    for (int i = 0; i < prios; i++)
      fifos[i] = new Fifo(name + "_prio_" + i, size);
  }

  /**
   * Count and validate the workload priorities used.
   * The default priority (Integer.MAX_VALUE) will be set to the next available
   * priority
   */
  public static int countPriorities(ArrayList <WG_entry> wg_list)
  {
    boolean any_default = false;
    HashMap prios = new HashMap(32);
    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.get(i);
      //common.ptod("wg: " + wg.wd_name + " " + wg.getpriority());
      if (wg.hasPriority())
        prios.put(new Integer(wg.getpriority()), null);
      else
        any_default = true;
    }

    Integer[] ties = (Integer[]) prios.keySet().toArray(new Integer[0]);
    Arrays.sort(ties);

    /* This check needs to be done on the Master not the Slave, because */
    /* the workloads and their priorities may not all run on each slave: */
    if (!SlaveJvm.isThisSlave())
    {
      int next_prio = 0;
      for (int i = 0; i < ties.length; i++)
      {
        if (ties[i].intValue() != next_prio++)
        {
          for (i = 0; i < ties.length; i++)
            common.ptod("Priority: " + ties[i] + " " + 1);
          common.failure("Workload priorities must be defined in numeric sequence starting at 1");
        }
      }
    }

    /* This is the slave, we assign priorities in order, they they may not */
    /* match the order they were specified, since some priorities may be   */
    /* missing due to some workloads not running on all slaves:            */
    else
    {
      int next_prio = 0;
      for (int i = 0; i < ties.length; i++, next_prio++)
      {
        for (int w = 0; w < wg_list.size(); w++)
        {
          WG_entry wg = (WG_entry) wg_list.get(w);
          if (wg.hasPriority())
          {
            if (wg.getpriority() == ties[i])
              wg.setPriority(next_prio);
          }
        }
      }
    }

    if (any_default && SlaveJvm.isThisSlave())
      common.failure("FifoList.countPriorities(wg_list): not expecting default");

    /* If any default priority is used, set it to the next value. */
    /* This will be done once on the Master.                      */
    if (any_default)
    {
      for (int i = 0; i < wg_list.size(); i++)
      {
        WG_entry wg = (WG_entry) wg_list.get(i);
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
   * Note that 99.999% of the workloads will not use priorities, so will have only
   * one Fifo in the list.
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

      /* There's no work. If we have only one Fifo, just get/wait: */
      /* We won't get an array, but since there is no work anyway right now */
      /* it is highly unlikely that there will be more than one entry. */
      if (fifos.length == 1)
      {
        array[0] = fifos[0].get();
        return 1;
      }

      /* We did not find any work, sleep a bit: */
      if (spin)
      {
        Thread.currentThread().yield();
        sleeps++;
      }
      else
      {
        common.sleep_some(1);
        sleeps++;
      }
    }

    /* End of the road for us: */
    return 0;
  }

  public void printStats(SD_entry sd)
  {
    synchronized (common.ptod_lock)
    {
      common.ptod("printStats for %s: getArray sleeps: %6d", sd.sd_name, sleeps);
    }
  }

  public void put(Object obj, int prio) throws InterruptedException
  {
    fifos[prio].put(obj);
    //common.ptod("qdepth: %s %2d %2d", fifos[prio].getLabel(),
    //            fifos[prio].getQueueDepth(),
    //            fifos[prio].entries_high  );
  }
  public void putQ(ArrayList queue, int prio) throws InterruptedException
  {
    fifos[prio].putQ(queue);
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
  public void waitAndPut(Object obj, int p) throws InterruptedException
  {
    waitForRoom(p);
    put(obj, p);
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
