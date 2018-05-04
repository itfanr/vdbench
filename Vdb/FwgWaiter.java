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
import java.util.concurrent.*;


/**
 * File System waiter task.
 *
 * This thread controls the workload skew for file system workloads.
 *
 * A semaphore is maintained for each FwgEntry.
 * The proper FwgThread will wait for this semaphore.
 *
 * The active FwgWaiter thread will release an FwgEntry's semaphore as soon as
 * the proper FwgEntry start time is reached, allowing the FwgThread to execute
 * its operation.
 *
 * Once the amount of available semaphore permits reaches a value higher than
 * 'xxx', no new semaphore releases will be done, forcing all other FwgEntry's
 * to wait. I imagine that 'xxx' will be much lower than the value 2000 used for
 * the Fifo() queues in WT_task().
 *
 * When no skew is needed for an Uncontrolled Max workload this code of course
 * will just involve a no-op.
 *
 * Note:
 * It can and will happen that an FwgThread will have to wait for a bit, for
 * instance to find a new file name to use. That is normal operation.
 *
 */
class FwgWaiter extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Task_num tn;
  private static HashMap  queues_map = null;
  private static FwgQueue[] queues   = null;
  private static int distribution    = 0;
  private static boolean format;


  public FwgWaiter(Task_num t, Vector fwgs, double fwd_rate, int dist)
  {
    VdbCount.count(this);
    setName("FwgWaiter");
    tn           = t;
    distribution = dist;
    format       = SlaveWorker.work.format_run;

    /* Create one queue entry per FWG: */
    queues_map = new HashMap(64);
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg   = (FwgEntry) fwgs.elementAt(i);
      FwgQueue queue = new FwgQueue(fwg, fwd_rate, distribution, queues_map.size());
      if (queues_map.put(fwg, queue) != null)
        common.failure("Duplicate FwgEntry");
    }

    /* Translate this map into an array for easier handling: */
    queues = (FwgQueue[]) queues_map.values().toArray(new FwgQueue[0]);

    tn.task_set_start_pending();
  }


  /**
   * Initialize queues.
   * This may not happen until we get the GO signal from the master
   */
  public static void initialize()
  {
    for (int i = 0; i < queues.length; i++)
      queues[i].initialize();
  }

  public static void clearStuff()
  {
    queues     = null;
    queues_map = null;
  }

  public void run()
  {
    try
    {
      tn.task_set_start_complete();
      tn.task_set_running();
      initialize();

      long tod = Native.get_simple_tod();

      int ctr = 0;
      while (!SlaveJvm.isWorkloadDone())
      {
        /* Look for the lowest starting time: */
        FwgQueue lowq = queues[0];
        for (int i = 1; i < queues.length; i++)
        {
          if (queues[i].next_arrival < lowq.next_arrival)
            lowq = queues[i];
        }

        while (true)
        {
          /* Not getting the tod here saves a get_simple_tod() if we are way too late already! */
          if (tod >= lowq.next_arrival / 1000)
            break;

          /* Wait util we get there: */
          common.sleep_some_usecs(Math.min(100000, (lowq.next_arrival / 1000) - tod));

          tod = Native.get_simple_tod();
        }

        /* Now release the semaphore, but only if we are not running too far behind: */
        try
        {
          //common.ptod("bwaiting for lowq" + lowq.fwg.fsd_name);
          if (!lowq.isSuspended())
            getUntilDone(lowq.max_queue_sema, "max_queue_sema " + lowq.fwg.fsd_name );
          //if (lowq.max_queue_sema.availablePermits() < 100)
          //  common.ptod("ewaiting for lowq" + lowq.fwg.fsd_name + " " + lowq.max_queue_sema.availablePermits());
          lowq.work_avail_sema.release();
          lowq.releases++;
          //common.ptod("releases: " + lowq.fwg.fsd_name + " " + lowq.fwg.getOperation() + " " + lowq.releases);
        }
        catch (InterruptedException e)
        {
          break;
        }

        /* Increment to the next start time: */
        lowq.calculateNextStartTime();
      }

      tn.task_set_terminating(0);
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * Get an FwgEntry's proper FwgQueue element
   */
  public static FwgQueue getMyQueue(FwgEntry fwg)
  {
    FwgQueue queue = (FwgQueue) queues_map.get(fwg);
    if (queue == null)
      common.failure("Unknown FwgEntry");
    return queue;
  }

  public static void printQueues()
  {
    for (int i = 0; i < queues.length; i++)
    {
      common.ptod("printQueues: " + queues[i].fwg.fsd_name + " max " +
                  queues[i].max_queue_sema.availablePermits() + " work " +
                  queues[i].work_avail_sema.availablePermits());
    }
  }


  /**
   * Acquire a Semaphore permit.
   * This method is written to allow the SlaveJvm.isWorkloadDone() to be recognized
   * so that it can throw an InterruptedException, which then in turn will
   * be recognized by the caller to stop processing.
   */
  public static void getUntilDone(Semaphore sema_waiting, String txt) throws InterruptedException
  {
    while (true)
    {
      boolean rc = sema_waiting.tryAcquire(1, 100, TimeUnit.MILLISECONDS);
      if (rc)
        return;

      //common.ptod("blocking for " + txt);
      //if (txt.startsWith("max"))
      //  common.where(8);

      if (SlaveJvm.isWorkloadDone())
        throw new InterruptedException();
    }
  }

}


/**
 * Skew control mechanism for File System workloads.
 * Timestamps are calculated in nano seconds, though when we get to comparing
 * it with the current TOD (in microseconds) we divide it again by 1000.
 * This gives us the best granularity as far as timestamp comparisons done
 * above.
 *
 */
class FwgQueue
{
  long      inter_arrival;     /* Nano seconds */
  long      next_arrival;      /* Nano seconds */
  double    fwd_rate;
  int       seq;
  int       distribution;
  boolean   suspended = false;
  FwgEntry  fwg;
  boolean   max_rate_requested = false;
  Semaphore work_avail_sema = new Semaphore(0);
  Semaphore max_queue_sema  = new Semaphore(2000);
  int       releases = 0;

  public FwgQueue(FwgEntry f, double rate, int dist, int seq_in)
  {
    fwg          = f;
    distribution = dist;
    fwd_rate     = rate;
    seq          = seq_in;
  }

  public void initialize()
  {

    /* With Rd_entry.MAX_RATE and CURVE_RATE being (on 04/14/09)  999988 and 999977  */
    /* these rate values in theory can not go higher than 1 million iops per         */
    /* workload. To play it safe I adjust the values used here to a higher value.    */
    if (fwd_rate == RD_entry.MAX_RATE || fwd_rate == RD_entry.CURVE_RATE)
      fwd_rate = 99999999;

    inter_arrival = (long) (1000000000. * 100. / fwg.skew / fwd_rate);
    //common.ptod("inter_arrival: " + inter_arrival + " skew: " + fwg.skew + " rate: " + fwd_rate);

    /* Set first start time: */
    if (distribution == 0)
      next_arrival = (long) ownmath.exponential(inter_arrival);

    else if (distribution == 1)
      next_arrival = (long) ownmath.uniform(0, inter_arrival * 2);

    else
    {
      /* Make sure not every thread starts at the same time: */
      Random rand = new Random(10000 * seq);
      next_arrival = (long) (inter_arrival * rand.nextDouble());
    }

    /* Adjust the start times relative to the high performance clock: */
    next_arrival += Native.get_simple_tod() * 1000;

    /* Though for maxrate we don't care: */
    max_rate_requested = (fwd_rate == RD_entry.MAX_RATE);
  }

  public synchronized boolean isSuspended()
  {
    return suspended;
  }
  public synchronized void suspend()
  {
    suspended = true;
    //max_queue_sema.release(work_avail_sema.drainPermits());
    //
    /* Just in case our run() method is blocked for this: */
    max_queue_sema.release(1);
  }

  public synchronized void restart()
  {
    //max_queue_sema.release(work_avail_sema.drainPermits());
    //common.ptod("max_queue_sema:  " + max_queue_sema.availablePermits());
    //common.ptod("work_avail_sema: " + work_avail_sema.availablePermits());

    /* Clean and restart the semaphores: */
    work_avail_sema.drainPermits();
    max_queue_sema.release(2000);
    suspended = false;
  }

  /**
   * Calculate the next arrival time for the next operation
   */
  public void calculateNextStartTime()
  {
    long tmp = next_arrival;
    if (distribution == 0)
    {
      long delta = (long) ownmath.exponential(inter_arrival);
      next_arrival += delta;
    }

    else if (distribution == 1)
    {
      long delta = (long) ownmath.uniform(0, inter_arrival * 2);
      next_arrival += delta;
    }

    else
      next_arrival += inter_arrival;

    //long now = Native.get_simple_tod() * 1000;
    //common.ptod("next_arrival1: " + next_arrival + " " +
    //            tmp + " " + now + " " + (now - next_arrival));
  }


  /**
   * Wait for a semaphore so that I can get some work done.
   */
  public int getPermit() throws InterruptedException
  {
    //if (suspended)
    //  return 5678;
    int depth = max_queue_sema.availablePermits();
    FwgWaiter.getUntilDone(work_avail_sema, "work_avail_sema " + fwg.fsd_name);
    max_queue_sema.release();
    return depth;
  }

}


