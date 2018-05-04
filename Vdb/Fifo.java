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

import java.util.ArrayList;
import java.util.concurrent.*;

import Vdb.common;



public class Fifo
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private long      entries;
  private long      entries_low;
  private long      entries_high;
  private String    label;

  /* Only access the next fields while locking Fifo */
  private Object[]  list;
  private long      last_get_index;
  private long      last_put_index;
  private int       threads = 1;

  private ArrayList  waiting_list = new ArrayList (64);

  private Semaphore free_entries;
  private Semaphore avail_entries;
  private Semaphore wait_for_room = new Semaphore(0);

  private int    gets = 0;
  private int    puts = 0;


  public static int WT_TO_IOT_SIZE       = 2000;
  public static int WT_TO_IOT_SIZE_SHORT = 50;
  public static int WG_TO_WT_SIZE        = 2000;


  public Fifo(String lbl, int count)
  {
    label         = lbl;
    list          = new Object[count];
    entries       = count;
    entries_low   = count * 30 / 100;
    entries_high  = count * 90 / 100;
    free_entries  = new Semaphore(count);
    avail_entries = new Semaphore(0);
  }

  public void setThreadCount(int t)
  {
    threads = t;
  }

  public void put(Object obj) throws InterruptedException
  {
    /* Wait for a free entry: */
    getUntilDone(free_entries);

    /* We can now store this entry: */
    synchronized (this)
    {
      puts++;
      /* If someone's waiting, don't bother to store: */
      int size = waiting_list.size();
      if (size > 0)
      {
        /* Pick up the LIFO waiter and tell him to go ahead: */
        FifoGetWaiter waiter = (FifoGetWaiter) waiting_list.get(size - 1);
        waiting_list.remove(size - 1);

        /* Place the new object there for the waiter to pick up: */
        waiter.obj = obj;
        waiter.sema_waiting.release();

        /* I do need to give the 'free_entry' back however: */
        free_entries.release(1);

        return;
      }

      /* Now add the entry into the fifo: */
      list[ (int) (last_put_index++ % entries) ] = obj;
      avail_entries.release(1);
    }
  }


  public Object get() throws InterruptedException
  {
    FifoGetWaiter waiter = null;

    synchronized (this)
    {
      /* If we have Objects available: */
      if (avail_entries.availablePermits() > 0)
      {
        /* This call is guaranteed since we already know we'll get one: */
        avail_entries.acquireUninterruptibly();

        /* Pick up the oldest object available: */
        Object obj = list[ (int) (last_get_index++ % entries) ];
        free_entries.release(1);

        if (free_entries.availablePermits() > entries)
          common.failure("too many free entries: " + free_entries.availablePermits()
                         + " " + entries);

        /* If we now dropped down to 80% of the list, notify anyone who */
        /* was waiting because they reached 90% of the list: */
        if (avail_entries.availablePermits() == entries_low)
        {
          int waiters = wait_for_room.getQueueLength();
          if (waiters > 0)
            wait_for_room.release(waiters);
        }

        gets++;
        return obj;
      }

      /* We have no Objects available. Put me in the LIFO waiter queue: */
      waiter = new FifoGetWaiter();
      waiting_list.add(waiter);
    }

    /* When I am in the LIFO queue, wait: */
    if (waiter != null)
    {
      try
      {
        getUntilDone(waiter.sema_waiting);
      }
      /* Only called during shutdown: */
      catch (InterruptedException e)
      {
        return null;
      }
    }

    return waiter.obj;
  }


  public int getArray(Object[] array) throws InterruptedException
  {
    return getArray(array, true);
  }
  public int getArray(Object[] array, boolean wait) throws InterruptedException
  {
    FifoGetWaiter waiter = null;
    int count = array.length;

    synchronized (this)
    {
      /* If we have one or more Objects available: */
      int avail = avail_entries.availablePermits();
      if (avail > 0)
      {
        /* We want to make sure that there are enough entries available */
        /* so that when an other thread does a get() that he does       */
        /* not have to wait:  */
        int max = (avail + threads - 1) / threads;
        if (max > count) max = count;

        /* Get as many as you can get: */
        int howmany = 0;
        for (howmany = 0; howmany < max && howmany < avail; howmany++)
          array[ howmany ] = get();

        //common.ptod("avail: " + avail + " " + max + " " + howmany + " " + threads);

        //free_entries.release(howmany);

        if (free_entries.availablePermits() > entries)
        {
          common.ptod("howmany: " + howmany);
          common.failure("too many free entries: " + free_entries.availablePermits()
                         + " " + entries);
        }

        return howmany;
      }
    }

    /* We have none. This makes this a single request: */
    if (!wait)
      return 0;

    array[0] = get();
    return 1;
  }


  /**
   * Acquire a Semaphore permit.
   * This method is written to allow the SlaveJvm.isWorkloadDone() to be recognized
   * so that it can throw an InterruptedException, which then in turn will
   * be recognized by the caller to stop processing.
   */
  private static void getUntilDone(Semaphore sema_waiting) throws InterruptedException
  {
    while (true)
    {
      boolean rc = sema_waiting.tryAcquire(1, 100, TimeUnit.MILLISECONDS);
      if (rc)
        return;

      if (SlaveJvm.isWorkloadDone())
        throw new InterruptedException();
    }
  }

  public int getQueueDepth()
  {
    return avail_entries.availablePermits();
  }


  public boolean isGettingFull()
  {
    //common.ptod("avail_entries.availablePermits(): " + avail_entries.availablePermits());
    return avail_entries.availablePermits() > entries_high;
  }


  /**
   * When the fifo gets to x% full, wait until it goes back to y%
   */
  public void waitForRoom() throws InterruptedException
  {
    //common.ptod("avail_entries.availablePermits(): " + avail_entries.availablePermits() + " " + entries_high);
    if (avail_entries.availablePermits() > entries_high)
    {
      //common.ptod("availablePermits " + avail_entries.availablePermits());
      //common.where();
      wait_for_room.acquire();
    }
  }

  public static void main(String[] args)
  {
    /*
    Fifo fifo = new Fifo("test", 1000);
    int COUNT = 100;

    for (int i = 0; i < COUNT; i++)
    {
      fifo.put(new Integer(i));
    }


    for (int i = 0; i < COUNT;)
    {
      Object[] array = new Object[5];

      int burst = fifo.getArray(array);

      for (int j = 0; j < array.length; j++,i++)
      {
        Integer obj = (Integer) array[j];
        common.ptod("obj: " + obj);
      }
    }
    */
  }

}

class FifoGetWaiter
{
  public Object obj;
  public Semaphore sema_waiting;

  public FifoGetWaiter()
  {
    sema_waiting = new Semaphore(0);
  }
}
