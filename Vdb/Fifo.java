package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.*;

import Vdb.common;



public class Fifo
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private long      entries;
  private long      entries_low;
  public  long      entries_high;
  private String    label;

  /* Only access the next fields while locking Fifo */
  private Object[]  list;
  private long      last_get_index;
  private long      last_put_index;
  private int       threads = 1;

  private ArrayList  waiting_list = new ArrayList (64);

  public  Semaphore free_entries;
  private Semaphore avail_entries;
  public  Semaphore wait_for_room = new Semaphore(0);

  private long    getw = 0;
  private long    gets = 0;
  private long    puts = 0;
  private long    xfers = 0;  // put that never went in the queue but were give to waiters
  private long    try1 = 0;
  private long    try2 = 0;
  private long    get_acq = 0;
  private long    acq2 = 0;
  private long    putw = 0;
  private long    roomacq = 0;
  private long    nowaits = 0;

  private long    put_queue_sum  = 0;
  private long    put_queue_max  = 0;
  private long    room_waits     = 0;
  private long    room_release   = 0;

  private static final boolean debug = false;

  private static Vector <Fifo> active_fifos = new Vector(64);


  private static int WT_TO_IOT_SIZE            = 2000;
  private static int WT_TO_IOT_SIZE_PRIORITIES = 2000;    // was 50
  private static int WT_TO_IOT_SIZE_DV         = 2000;
  private static int WG_TO_WT_SIZE             = 2000;


  /**
   * Command Fifo. Used to communicate i/o requests for raw i/o workloads across
   * WG_task(), WT_task(), and IO_task().
   *
   * To prevent excessive context switching waitForRoom() is called before a
   * put(). If the fifo is 90% full the put will be held off until it reaches
   * 30%, after which puts again are honored until 90% full.
   */
  public Fifo(String lbl, int count)
  {
    label         = lbl;
    list          = new Object[count];
    entries       = count;
    entries_low   = count * 30 / 100;
    entries_high  = count * 90 / 100;
    free_entries  = new Semaphore(count);
    avail_entries = new Semaphore(0);
    active_fifos.add(this);
  }

  public static void clearFifoVector()
  {
    active_fifos.removeAllElements();
  }

  public void setThreadCount(int t)
  {
    threads = t;
  }

  public void put(Object obj) throws InterruptedException
  {
    if (debug) common.ptod("Fifo.put1 %8s ", label);

    while (true)
    {
      boolean rc = free_entries.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
      if (rc)
        break;
      if (SlaveJvm.isWorkloadDone())
        throw new InterruptedException();
    }

    /* We can now store this entry: */
    synchronized (this)
    {
      /* If someone's waiting, don't bother to store: */
      int size = waiting_list.size();
      if (size > 0)
      {
        /* Pick up the LIFO waiter and tell him to go ahead: */
        FifoGetWaiter waiter = (FifoGetWaiter) waiting_list.get(size - 1);
        waiting_list.remove(size - 1);
        nowaits++;

        /* Place the new object there for the waiter to pick up: */
        waiter.obj = obj;
        waiter.sema_waiting.release();

        /* I do need to give the 'free_entry' back however: */
        free_entries.release(1);

        if (debug) common.ptod("Fifo.put2 %8s ", label);

        //putw += System.nanoTime() - begin;
        xfers++;
        return;
      }

      /* Now add the entry into the fifo: */
      list[ (int) (last_put_index++ % entries) ] = obj;
      avail_entries.release(1);
      int avail = avail_entries.availablePermits();
      puts++;
      put_queue_sum += avail;
      put_queue_max  = Math.max(put_queue_max, avail);
    }

    if (debug) common.ptod("Fifo.put3 %8s ", label);

    //putw += System.nanoTime() - begin;
  }


  public void putQ(ArrayList queue) throws InterruptedException
  {
    /* We can now store this entry: */
    //synchronized (this)
    {
      for (int i = 0; i < queue.size(); i++)
        put(queue.get(i));
    }
  }


  public Object get() throws InterruptedException
  {
    if (debug) common.ptod("Fifo.get1 %8s ", label);

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

        gets++;
        if (debug) common.ptod("Fifo.get2 %8s ", label);

        /* If we now dropped down to 30% of the list, notify anyone who */
        /* was waiting because they reached 90% of the list: */
        if (avail_entries.availablePermits() == entries_low)
        {
          int waiters = wait_for_room.getQueueLength();
          if (waiters > 0)
          {
            wait_for_room.release(waiters);
            room_release++;
          }
        }

        return obj;
      }                //vdbench -t -e28000 -o out.bg2 -v +io=max

      // is this really needed?

      /* Fifo is now empty. See if anyone is waiting: */
      if (avail_entries.availablePermits() == 0)
      {
        synchronized(avail_entries)
        {
          avail_entries.notifyAll();
        }
      }

      /* Fifo is empty. Is someone by chance waiting for room and above check */
      /* for 'entries_low' did not make it? */
      //int waiters = wait_for_room.getQueueLength();
      //if (waiters > 0)
      //{
      //  wait_for_room.release(waiters);
      //  room_release++;
      //}

      /* We have no Objects available. Put me in the LIFO waiter queue: */
      waiter = new FifoGetWaiter();
      waiting_list.add(waiter);
    }


    /* I am now in the LIFO queue, wait: */
    try
    {
      while (true)
      {
        boolean rc = waiter.sema_waiting.tryAcquire(1, 100, TimeUnit.MILLISECONDS);
        if (rc)
          break;
        if (SlaveJvm.isWorkloadDone())
          throw new InterruptedException();
      }
      getw++;
    }
    /* Only called during shutdown: */
    catch (InterruptedException e)
    {
      return null;
    }

    if (debug) common.ptod("Fifo.get3 %8s ", label);
    return waiter.obj;
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

        /* Experiment: get one less than the max: */
        //max = Math.max(1, max - 1);

        /* This call is guaranteed since we already know we'll get them: */
        //long start = System.nanoTime();
        avail_entries.acquireUninterruptibly(max);
        //acq2 += System.nanoTime() - start;

        for (int i = 0; i < max; i++)
          array[i] = list[ (int) (last_get_index++ % entries) ];

        free_entries.release(max);

        int howmany = max;
        gets += max;

        /* If Fifo is now empty, see if anyone is waiting: */
        if (avail_entries.availablePermits() == 0)
        {
          synchronized(avail_entries)
          {
            avail_entries.notifyAll();
          }
        }


        if (free_entries.availablePermits() > entries)
        {
          common.ptod("howmany: " + howmany);
          common.failure("too many free entries: " + free_entries.availablePermits()
                         + " " + entries);
        }

        /* If we now dropped down to 30% of the list, notify anyone who */
        /* was waiting because they reached 90% of the list: */
        if (avail_entries.availablePermits() == entries_low)
        {
          int waiters = wait_for_room.getQueueLength();
          if (waiters > 0)
          {
            wait_for_room.release(waiters);
            room_release++;
          }
        }

        //if (avail_entries.availablePermits() == entries_low)
        //  common.where();

        return howmany;
      }
    }

    /* We have none. This makes this a single request: */
    if (!wait)
      return 0;

    //long start = System.nanoTime();
    array[0]   = get();
    //getw      += System.nanoTime() - start;
    //common.ptod("add single : %12d", ((Cmd_entry) array[ 0 ]).cmd_lba);
    return 1;
  }


  /**
   * Acquire a Semaphore permit.
   * This method is written to allow the SlaveJvm.isWorkloadDone() to be recognized
   * so that it can throw an InterruptedException, which then in turn will
   * be recognized by the caller to stop processing.
   */
  private static void obsolete_getUntilDone(String label, Semaphore sema_waiting) throws InterruptedException
  {
    while (true)
    {
      boolean rc = sema_waiting.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
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
   *
   * I ran into intermittent problems with this, and no matter what I could not
   * figure out what was wrong.
   * The objective was to avoid too many thread context switches, for instance
   * having the put() and get() methods alternate each time an entry became
   * available.
   * However, for some unknown reason I kept getting intermittent hangs where
   * get() was stuck in getUntilDone(), and put() was stuck in waitForRoom().
   * Gave up, maybe some day I'll have the braincells to try again.
   *
   * 10/21/10: looks as if things are finally fine.
   * 05/09/13: added an extra wait_for_room.release() in getArray, though
   * getArray will be obsolete after the removal of multi_io
   */
  public void waitForRoom() throws InterruptedException
  {
    //common.ptod("avail_entries.availablePermits(): " + avail_entries.availablePermits() + " " + entries_high);

    if (avail_entries.availablePermits() > entries_high)
    {
      synchronized (this)
      {
        room_waits++;
      }

      /* Wait for notification that there is room again, but not too long.                 */
      /* Will we however be able to fill up a fifo between low and high in one cpu tick?   */
      /* If we get woken up after the fifo is empty, we're too late.                       */
      /* However, the wait_for_room.release() calls should have called us far BEFORE that. */
      /* This 'try' vs 'acquire' is mainly there in case, because of timings,              */
      /* the get() methods fail to do the release.                                         */
      boolean rc = wait_for_room.tryAcquire(10, 1, TimeUnit.MILLISECONDS);
      //while (!wait_for_room.tryAcquire(10, 1, TimeUnit.MILLISECONDS))
      //{
      //}
      //wait_for_room.acquire();
    }
  }

  public void waitAndPut(Object obj) throws InterruptedException
  {
    waitForRoom();
    put(obj);
  }

  /**
   * Wait until this Fifo has nothing left in it.
   */
  public void waitUntilEmpty()
  {
    synchronized (avail_entries)
    {
      while (avail_entries.availablePermits() > 0)
      {
        //common.ptod("avail_entries.availablePermits(): " + avail_entries.availablePermits());
        try
        {
          avail_entries.wait();
        }
        catch (InterruptedException e)
        {
          return;
        }
      }
    }
  }


  /**
   * Drain all fifos.
   * Realize that someone could be stuffing them again the moment you're done!
   */
  public static void drainAllFifos()
  {
    for (int i = 0; i < active_fifos.size(); i++)
    {
      active_fifos.get(i).drainFifo();
    }
  }


  /**
   * Drain the fifo.
   * Realize that someone could be stuffing it again the moment you're done!
   */
  public void drainFifo()
  {
    synchronized (this)
    {
      common.ptod("avail_entries.availablePermits(): " + avail_entries.availablePermits());
      synchronized (avail_entries)
      {
        while (avail_entries.availablePermits() > 0)
        {
          try
          {
            get();
          }
          catch (InterruptedException e)
          {
            break;
          }
        }
      }
    }
  }


  public long getPutCount()
  {
    return puts;
  }

  public static void printQueues()
  {
    if (common.get_debug(common.FIFO_STATS))
    {
      synchronized (common.ptod_lock)
      {
        printQueueLengths("to_iot");
        printQueueLengths("to_wait");
      }
    }
  }

  private static void printQueueLengths(String prefix)
  {
    long puts = 0;
    long xfers = 0;
    long gets = 0;
    long getw = 0;
    long nwt  = 0;
    long pqs  = 0;
    long avq  = 0;
    long room = 0;
    for (int i = 0; i < active_fifos.size(); i++)
    {
      Fifo fifo = (Fifo) active_fifos.elementAt(i);
      if (fifo.label.startsWith(prefix))
      {
        puts += fifo.puts;
        xfers += fifo.xfers;
        gets += fifo.gets;
        getw += fifo.getw;
        nwt  += fifo.nowaits;
        pqs  += fifo.put_queue_sum;
        room += fifo.room_release;
        avq  += (fifo.puts > 0) ?  (fifo.put_queue_sum / fifo.puts) : 0;

        common.ptod("fifo: %-18s p: %8d x: %8d g: %8d gw: %8d nwt: %8d avq: %4d max: %4d rr: %3d",
                    fifo.label, fifo.puts, fifo.xfers, fifo.gets, fifo.getw, fifo.nowaits,
                    (fifo.puts > 0) ?  (fifo.put_queue_sum / fifo.puts) : 0,
                    fifo.put_queue_max,
                    fifo.room_release);
      }
    }

    common.ptod("fifo: %-18s p: %8d x: %8d g: %8d gw: %8d nwt: %8d avq: %4d rr: %3d",
                prefix, puts, xfers, gets, getw, nwt,
                (puts > 0) ?  (pqs / puts) : 0, room);
  }


  public static void printFifoStatuses()
  {
    if (!common.get_debug(common.FIFO_STATS))
      return;

    /* This lock is here to keep all the printing together: */
    synchronized (common.ptod_lock)
    {
      printStatus("to_wait");
      printStatus("to_iot");
    }
  }


  private long old_puts         = 0;
  private long old_xfers        = 0;
  private long old_gets         = 0;
  private long old_getw         = 0;
  private long old_nowaits      = 0;
  private long old_room_waits   = 0;
  private long old_room_release = 0;

  private static void printStatus(String prefix)
  {
    int     sleeping    = 0;
    long    sleep_count = 0;
    long    sleep_last  = 0;
    synchronized (WT_task.sleep_lock)
    {
      sleeping    = WT_task.sleeping;
      sleep_count = WT_task.sleep_count;
      sleep_last  = WT_task.sleep_last;
      //common.ptod("sleep: %d %8d %8d", WT_task.sleeping, WT_task.sleep_count, WT_task.last_diff );
    }

    for (int i = 0; i < active_fifos.size(); i++)
    {
      Fifo fifo = (Fifo) active_fifos.elementAt(i);
      synchronized (fifo)
      {
        if (fifo.label.startsWith(prefix))
        {
          common.ptod("fifo: %-20s p: %6d x: %6d g: %6d w: %6d nw: %6d "+
                      "f: %4d a: %4d wfrp: %3d wfrq: %2d "+
                      "rwt: %4d rrl: %4d wl: %4d "+
                      "sl: %d sc: %6d sl: %d",
                      fifo.label,
                      fifo.puts    - fifo.old_puts,         // p
                      fifo.xfers   - fifo.old_xfers,        // x
                      fifo.gets    - fifo.old_gets,         // g
                      fifo.getw    - fifo.old_getw,         // w
                      fifo.nowaits - fifo.old_nowaits,      // nw

                      fifo.free_entries.availablePermits(),
                      fifo.avail_entries.availablePermits(),
                      fifo.wait_for_room.availablePermits(),
                      fifo.wait_for_room.getQueueLength(),

                      fifo.room_waits   - fifo.old_room_waits,    // rwt
                      fifo.room_release - fifo.old_room_release,  // rwl
                      fifo.waiting_list.size(),

                      sleeping,
                      sleep_count,
                      sleep_last);

          fifo.old_puts         = fifo.puts;
          fifo.old_xfers        = fifo.xfers;
          fifo.old_gets         = fifo.gets;
          fifo.old_getw         = fifo.getw;
          fifo.old_nowaits      = fifo.nowaits;
          fifo.old_room_waits   = fifo.room_waits;
          fifo.old_room_release = fifo.room_release;
        }
      }
    }

  }

  /**
   * When running with specific priorities, the fifo can not be too long. With a
   * low iops the low priority workloads can run too far ahead with the standard
   * 2000 ios. Cutting it down avoids that.
   *
   * Reset this back to 2000 after changing the relative long sleep in WT_task
   * to the minimum of 1 microsecond.
   */
  public static int getSizeNeeded(int priorities)
  {
    int override = findOverride();
    if (override != 0)
    {
      //common.ptod("Set FIFO size to %d", override);
      return override;
    }

    if (Validate.isRealValidate())
      return WT_TO_IOT_SIZE_DV;

    else if (priorities == 1)
      return WT_TO_IOT_SIZE;

    else
      return WT_TO_IOT_SIZE_PRIORITIES;
  }

  public String getLabel()
  {
    return label;
  }


  /**
   * Allow user override of the fifo depth.
   * This override has been published.
   */
  private static int findOverride()
  {
    ArrayList <String[]> misc = MiscParms.getMiscellaneous();

    for (String[] array : misc)
    {
      for (String parm : array)
      {
        String[] split = parm.trim().split("=");
        if (split[0].equalsIgnoreCase("fifo"))
          return Integer.parseInt(split[1]);
      }
    }
    return 0;
  }

  public static void main(String[] args)  throws Exception
  {
    int loop = Integer.parseInt(args[0]) * 1000000;
    Fifo fifo = new Fifo("test", 2000);
    Random seek_randomizer = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++)
    {
      double rand = seek_randomizer.nextDouble();
      //Cmd_entry cmd = new Cmd_entry();
      //fifo.put(cmd);
      //fifo.get();
    }
    long end      = System.currentTimeMillis();
    double seconds = (end - start) / 1000.;
    common.ptod("loop: %,d; per second: %,.0f; seconds: %.6f",
                loop,
                (double) (loop) / seconds,
                seconds);
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

