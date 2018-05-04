package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;


/**
 * This task waits for the correct time of day and sends i/o to proper IO_task
 */
public class WT_task extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static ArrayList <WG_entry> wgs_to_scan;
  private Task_num tn;



  private static int  priorities;
  private static long rr_index = 0;   /* Round robin index for getLowest() search */


  private static ThreadMonitor tmonitor = null;

  private final static boolean spin = common.get_debug(common.SPIN);

  private static boolean sd_concatenation;

  public static Object sleep_lock = new Object();
  public static int  sleeping = 1;
  public static long sleep_count = 0;
  public static long sleep_last = 0;


  private static boolean list_sort = common.get_debug(common.WT_TASK_LIST_SORT);

  /**
   * Setup of Waiter task
   */
  WT_task(Task_num tn_in)
  {
    priorities    = FifoList.countPriorities(SlaveWorker.work.wgs_for_slave);
    tn            = tn_in;
    tn.task_set_start_pending();
    sd_concatenation = Validate.sdConcatenation();
    setName("WT_task");
  }



  /**
   * Waiter task.
   * <pre>
   * The Cmd_entry with the lowest i/o start time is picked from all the FIFO
   * lists. The FIFO lists come from all Workload Generator task (WG).
   * The Waiter task waits for the proper time of day, and then sends the
   * Cmd_entry to the proper IO_task.
   *
   * Warning: If one of the output FIFOs is full, the Waiter task comes to a
   * halt. With a FIFO list size of 1000 it means that ONE SD is already 1000 i/o's
   * behind, so we are already in an overload situation.
   * If you make the FIFO lists too short however you can create problems too early.
   *
   */
  public void run()
  {
    Cmd_entry lowcmd = null;
    wgs_to_scan      = new ArrayList(SlaveWorker.work.wgs_for_slave);
    sorted_list.clear();

    Thread.currentThread().setPriority( Thread.MAX_PRIORITY-1 );

    try
    {
      tmonitor = new ThreadMonitor("WT_task", null, null);
      tn.task_set_start_complete();
      tn.waitForMasterGo();


      /* Problem: The moment we arrive here all WG_task instances may not have     */
      /* had enough time yet to put stuff in ALL the fifos.                        */
      /* The result is that getLowest() below will not be able to find the REAL    */
      /* lowest yet. That normally is not a big problem, code will soon catch up.  */
      /* However, if this is a Replay then the LOWEST we find may possily be an io */
      /* that is not scheduled to run for seconds or minutes. Ouch!                */
      /* I'll therefore will wait here for ALL fifos to have at least one request. */
      if (ReplayInfo.isReplay())
        waitForAllFifosActive();

      //common.plog("Starting WT_task");

      buildFifoSearchList();
      long tod              = Native.get_simple_tod();
      SlaveWorker.first_tod = tod;

      long tsleep = 0;

      long last_warning = tod;

      top:
      while (!SlaveJvm.isWorkloadDone())
      {
        /* Look for the lowest entry : */
        if ((lowcmd = getLowest(tod)) == null)
          break;

        //common.ptod("lowcmd.delta_tod: %s %16d", lowcmd.sd_ptr.sd_name, lowcmd.delta_tod);

        /* Wait until correct timestamp arrives: */
        long next_tod = lowcmd.delta_tod + SlaveWorker.first_tod;

        while (true)
        {
          /* This trick saves a get_tod if we are way too late already! */
          if (tod >= next_tod)
            break;
          tod = Native.get_simple_tod();
          if (tod >= next_tod)
            break;

          /* Wait a bit. set hires_tick=1 works just as well.     */
          /* hires_tick=0: .13 ms late; hires_tick=1: .03 ms late */
          if (!spin)
          {
            /* With priority workloads we may not sleep here because */
            /* we may have bypassed some SDs whose wt_to_iot fifos   */
            /* are (almost) full.                                    */
            if (priorities > 1)
            {
              common.sleep_some_usecs(1);
              continue top;
            }

            long diff = next_tod - tod;

            if (common.get_debug(common.NATIVE_SLEEP))   // Solaris only
              Native.nativeSleep(diff * 1000);
            else
            {
              common.sleep_some_usecs(Math.min(1000000, diff));
              tsleep += Math.min(1000000, diff);
              tmonitor.add2();
            }

            if (Thread.interrupted())
            {
              common.plog("WT Task interrupted at sleep");
              break;
            }
          }
        }


        // If this pipe is full, all of WT waits! See getLowest()
        /* Send to fifo: */
        try
        {
          if (lowcmd == null)
            common.failure("WT_task error1");

          if (lowcmd.sd_ptr == null)
            common.failure("WT_task error2");

          if (!sd_concatenation && lowcmd.sd_ptr.fifo_to_iot == null)
            common.failure("WT_task error3 " + lowcmd.sd_ptr.sd_name);

          if (lowcmd.cmd_wg == null)
            common.failure("WT_task error4");

          /* Using waitAndPut is a huge improvement over just using put(), */
          /* allowing the IO_task fifo to use its high water marks.        */
          /* I am not sure though if this will have an impact on priority  */
          /* workloads, so for now allow both.                             */
          if (priorities == 1)
            lowcmd.sd_ptr.fifo_to_iot.waitAndPut(lowcmd, lowcmd.cmd_wg.getpriority());
          else
            lowcmd.sd_ptr.fifo_to_iot.put(lowcmd, lowcmd.cmd_wg.getpriority());

          tmonitor.add1();
        }
        catch (InterruptedException e)
        {
          common.plog("WT Task interrupted at put");
          break;
        }


        /* Get the following entry for this lowest (for the next loop): */
        /* If this reaches end-of-fifo remove it from the 'scan fifos' list */
        try
        {
          if (!list_sort)
          {
            lowcmd.cmd_wg.pending_cmd = (Cmd_entry) lowcmd.cmd_wg.fifo_to_wait.get();
            if (lowcmd.cmd_wg.pending_cmd == null)
              break;
            if (lowcmd.cmd_wg.pending_cmd.delta_tod == Long.MAX_VALUE)
              wgs_to_scan.remove(lowcmd.cmd_wg);
          }
        }

        catch (InterruptedException e)
        {
          common.plog("WT Task interrupted at get");
          break;
        }

        //if (lowwg.pending_cmd == null)
        //  break;
      }

      //common.ptod("sleeps: " + sleeps);
      //if (sleeps > 0)
      //  common.ptod("tsleep: " + (tsleep / sleeps));
      //
      //common.ptod("passes: " + passes);
      //common.ptod("attempts: " + attempts);
      //if (passes > 0)
      //  common.ptod("average: " + (attempts/passes));

      common.plog("Ended WT_task");

      //Fifo.printStatuses();

      tn.task_set_terminating(0);

    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * GetLowest needs to run synchronized to allow buildFifoSearchList() to be
   * run from User.WorkloadInfo without breaking the scan of wgs_to_scan.
   */
  static long passes = 0;
  static long attempts = 0;
  private static ArrayList sorted_list = new ArrayList(1024);
  private static int index = 0;
  private synchronized static Cmd_entry getLowest(long tod)
  {
    if (!list_sort)
      return getLowestOne(tod);
    else
      return getLowestList(tod);
  }


  private synchronized static Cmd_entry getLowestOne(long tod)
  {
    passes++;
    WG_entry lowwg = null;

    /* If we have no WG_entries to scan anymore we're all done: */
    if (wgs_to_scan.size() == 0)
      return null;

    // This try can be removed once stable.
    try
    {
      /* Because of the priority scheme we don't want to keep storing       */
      /* commands in the SD fifos until it is 100% full and forces this     */
      /* thread to block, preventing higher priority i/o from being done.   */

      /* Find the fifo targeting an SD with a Fifo that is not almost full: */
      boolean found = false;
      do
      {
        /* A round-robin scan for the lowest prevents an input fifo from */
        /* being overused when an other fifo has the same timestamp.     */
        found      = false;
        int index  = (int) (rr_index++ % wgs_to_scan.size());
        for (int i = 0; i < wgs_to_scan.size(); i++, index++)
        {
          lowwg = wgs_to_scan.get(index % wgs_to_scan.size());
          if (priorities == 1)
          {
            found = true;
            break;
          }

          if (lowwg.pending_cmd == null)
            common.failure("oops1");
          if (lowwg.pending_cmd.sd_ptr == null)
            common.failure("oops2");
          if (lowwg.pending_cmd.sd_ptr.fifo_to_iot == null)
            common.failure("oops3");

          /* Look for a target SD fifo that has room left: */
          if (!lowwg.pending_cmd.sd_ptr.fifo_to_iot.isGettingFull(lowwg.getpriority()))
          {
            found = true;
            break;
          }
        }

        /* All target SD fifos are almost full. Try again later: */
        if (!found && !spin)
        {
          common.sleep_some_usecs(1);
        }

        if (SlaveJvm.isWorkloadDone())
          return null;

      } while (!found);


      /* Look for the lowest entry, skipping those fifos whose */
      /* target fifos are getting full:                        */
      Cmd_entry lowcmd = lowwg.pending_cmd;
      for (int i = 0; i < wgs_to_scan.size(); i++)
      {
        attempts++;
        WG_entry wg = wgs_to_scan.get(i);

        /* Since we're scanning the whole list again, ignore the first one: */
        if (wg == lowwg)
          continue;

        /* Bypass those whose SD fifo is getting full: */
        if (priorities != 1 && wg.pending_cmd.sd_ptr.fifo_to_iot.isGettingFull(wg.getpriority()))
        {
          //common.ptod("wg.pending_cmd.sd_ptr: " + wg.pending_cmd.sd_ptr.sd_name);
          /* Don't bypass when the current delta marks EOF. If we did, and this one */
          /* would be the lowest, we would return NULL below and signal endofrun */
          // this is obsolete, just being paranoid.
          if (lowwg.pending_cmd.delta_tod != Long.MAX_VALUE)
            continue;
        }

        if (lowwg.pending_cmd.delta_tod > wg.pending_cmd.delta_tod)
        {
          lowwg  = wg;
          lowcmd = lowwg.pending_cmd;
        }
      }

      // it appears I can get to this when not all the fifos have been scanned!
      //if (lowcmd.delta_tod == Long.MAX_VALUE)
      //{
      //  common.where();
      //  return null;
      //}

      return lowcmd;
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("lowwg: " + lowwg);
      common.ptod("lowwg.pending_cmd: " + lowwg.pending_cmd);
      common.ptod("lowwg.pending_cmd.sd_ptr: " + lowwg.pending_cmd.sd_ptr);
      common.ptod("lowwg.pending_cmd.sd_ptr.wt_to_iot: " + lowwg.pending_cmd.sd_ptr.fifo_to_iot);
      common.failure(e);
    }
    return null;
  }

  /**
   * Experiment to lower cpu needs scanning huge lists of fifos, for instance
   * when doing a 1000+ replay.
   * There are some bugs left here:
   *
   * GetLowest needs to run synchronized to allow buildFifoSearchList() to be
   * run from User.WorkloadInfo without breaking the scan of wgs_to_scan.
   *
   * Also, code does  not always appear to end immediately after the last i/o,
   * though it could be that was because of a network hang?
   *
   *
   *
   */
  private synchronized static Cmd_entry getLowestList(long tod)
  {
    WG_entry lowwg = null;

    /* If we have no WG_entries to scan anymore we're all done: */
    if (wgs_to_scan.size() == 0)
      return null;

    /* If we have any pending, return one: */
    if (sorted_list.size() != 0 && index < sorted_list.size())
    {
      Cmd_entry cmd = (Cmd_entry) sorted_list.get(index++);
      //common.ptod("return1: %8d %4d", cmd.delta_tod, cmd.cmd_lba / 131072);
      return cmd;
    }

    long now = System.currentTimeMillis();

    /* Select at least 'n' commands that are due soon: */
    sorted_list.clear();
    long soon = 1000;
    passes = 0;
    int removes = 0;
    do
    {
      passes++;
      /* If none found, increase 'soon' by a millisecond: */
      soon += 1000;
      top_wgscan:
      for (int i = 0; i < wgs_to_scan.size(); i++)
      {
        WG_entry wg = wgs_to_scan.get(i);
        if (wg == null)
          continue;

        /* Pick up as many of this WG as we can get: */
        // that is too many.
        int picked = 0;
        while (picked < 2)
        {
          Cmd_entry cmd = wg.pending_cmd;

          /* If i/o is due later, stop scanning this WG: */
          if (cmd.delta_tod + SlaveWorker.first_tod > tod + soon)
            break;

          /* Get the next entry from this fifo: */
          /* If this reaches end-of-fifo remove it from the 'scan fifos' list */
          try
          {
            //if (wg.fifo_to_wait.getQueueDepth() > 0)
            {
              sorted_list.add(cmd);
              picked++;
              wg.pending_cmd = (Cmd_entry) wg.fifo_to_wait.get();
              if (wg.pending_cmd == null)
                return null;

              if (wg.pending_cmd.delta_tod == Long.MAX_VALUE)
              {
                wgs_to_scan.set(i, null);
                removes++;
                continue top_wgscan;
              }
            }
          }
          catch (InterruptedException e)
          {
            common.plog("WT Task interrupted at get");
            return null;
          }
        }
      }
      //Thread.currentThread().yield();
    } while (sorted_list.size() < wgs_to_scan.size());

    /* If we had eof on any fifo, remove them now: */
    if (removes > 0)
      while (wgs_to_scan.remove(null));

    //
    //for (int i = 0; i < sorted_list.size(); i++)
    //{
    //  Cmd_entry cmd = (Cmd_entry) sorted_list.get(i);
    //  common.ptod("sorted_list1: " + cmd.delta_tod);
    //}

    /* We now have a list of commands, sort them: */
    //common.where();
    Collections.sort(sorted_list, new DeltaCompare());
    //common.where();

    //common.ptod("sorted_list: %8d %8d %8d %8d ", sorted_list.size(), wgs_to_scan.size(),
    //            passes, (System.currentTimeMillis() - now));
    //for (int i = 0; i < sorted_list.size(); i++)
    //{
    //  Cmd_entry cmd = (Cmd_entry) sorted_list.get(i);
    //  common.ptod("sorted_list2: " + cmd.delta_tod);
    //}

    Cmd_entry cmd = (Cmd_entry) sorted_list.get(0);
    index = 1;
    //common.ptod("return2: %8d %4d", cmd.delta_tod, cmd.cmd_lba / 131072);
    return cmd;
  }



  /**
   * (Re)build the WG_entry fifo search list.
   *
   * This is done so that in the User API users can temporarily not send any
   * commands to the fifo and then later on change their mind and start using
   * it.
   */
  public synchronized static void buildFifoSearchList()
  {
    Signal signal     = new Signal(1);
    ArrayList <WG_entry>  all_wgs = SlaveWorker.work.wgs_for_slave;
    ArrayList use_wgs = new ArrayList(all_wgs.size());

    while (true)
    {
      for (int i = 0; i < all_wgs.size(); i++)
      {
        WG_entry wg = all_wgs.get(i);
        if (!wg.suspend_fifo_use)
        {
          use_wgs.add(wg);
          if (wg.pending_cmd == null)
          {
            try
            {
              wg.pending_cmd = (Cmd_entry) wg.fifo_to_wait.get();
            }
            catch (InterruptedException e)
            {
              common.ptod("Interrupt during first get");
              common.interruptThread();
              return;
            }
          }
        }
      }

      if (use_wgs.size() > 0)
        break ;

      if (signal.go())
        common.ptod("WT_task.buildFifoSearchList(): waiting for work from at "+
                    "least one workload generator thread");
      common.sleep_some(100);
    }

    wgs_to_scan = use_wgs;
  }

  private void waitForAllFifosActive()
  {
    Signal signal = new Signal(10);
    double start = System.currentTimeMillis();

    while (!signal.go())
    {
      int actives = 0;
      for (WG_entry wg : SlaveWorker.work.wgs_for_slave)
      {
        if (wg.fifo_to_wait.getQueueDepth() > 0)
          actives++;
      }

      if (actives >= SlaveWorker.work.wgs_for_slave.size())
      {
        double end = System.currentTimeMillis();
        common.ptod("waitForAllFifosActive: %.2f seconds ", (end - start) / 1000.);
        return;
      }

      common.sleep_some(10);
    }

    for (WG_entry wg : SlaveWorker.work.wgs_for_slave)
    {
      //if (wg.fifo_to_wait.getQueueDepth() == 0)
      common.ptod("waitForAllFifosActive: waiting for " + wg.fifo_to_wait.getLabel() + " " + (wg.fifo_to_wait.getQueueDepth()));
    }

    common.failure("waitForAllFifosActive: unable to obtain stable fifo count "+
                   "after %d seconds", signal.getDuration());
  }
}
