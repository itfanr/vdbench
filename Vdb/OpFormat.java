package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;

import Utils.Format;


/**
 * Format all directories and/or all files.
 */
class OpFormat extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private OpMkdir   mkdir  = null;
  private OpCreate  create = null;

  private static Signal signal;

  private int    format_thread_number;


  public OpFormat(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
    signal = new Signal(15);


    /* If 'format' is requested, we just fake it by creating                   */
    /* some separate threads (threads won't run).                              */
    /* These thread instances then are used to call doOperation() to do the work. */
    mkdir  = new OpMkdir  (null, fwg);
    create = new OpCreate (null, fwg);
  }


  public void setFormatThreadNumber(int no)
  {
    format_thread_number = no;
  }


  /**
   * Format all directories and files
   */
  protected boolean doOperation() throws InterruptedException
  {
    /* If we only did deletes, exit now: */
    if (SlaveWorker.work.format_flags.format_clean)
      return false;

    // Format no longer uses FwgWaiter for all of its operations.
    // This has been done because some FSDs were just much faster than others.
    // We still have the mkdir/create synchronization.
    // Because of tis we should do the stuff below as a 'while true' loop, and
    // remove the 'format' check in FwgWaiter.
    // With this we then no longer will need (I think) the FWG suspension!
    //  TBD.

    /* Do 'mkdir' as long as needed: */
    if (fwg.anchor.mkdir_threads_running.notZero())
    {
      /* Don't create directories if we have more threads than width: */

      // We were suspending this FwgEntry (in waitFor) even BEFORE any work was done.
      // While suspended, we never allowed the suspended threads to say that they were done.
      //
      //
      //if (format_thread_number >= fwg.anchor.width)
      //  waitForAllOtherThreads(fwg, fwg.anchor.mkdir_threads_running, "mkdir");
      //
      //else
      {
        if (!mkdir.doOperation() || !fwg.anchor.moreDirsToFormat())
          waitForAllOtherThreads(fwg, fwg.anchor.mkdir_threads_running, "mkdir");
      }

      return true;
    }

    /* If we only format directories, stop now: */
    if (SlaveWorker.work.format_flags.format_dirs_requested)
      return false;


    /* Do 'create' as long as needed: */
    if (fwg.anchor.create_threads_running.notZero())
    {
      /* (This reports info for every X:) */
      reportStuff("Created", fwg.anchor.getFileCount(), fwg.anchor.getExistingFileCount());
      if (!create.doOperation() || !fwg.anchor.anyFilesToFormat())
      {
        waitForAllOtherThreads(fwg, fwg.anchor.create_threads_running, "create");

        return false;
      }
      return true;
    }

    //else
    {
      //common.ptod("fwg.anchor.getFullFileCount(): " + fwg.anchor.getFullFileCount());
      reportStuff("Formatted", fwg.anchor.getFileCount(), fwg.anchor.getFullFileCount());
    }

    return true;
  }

  /**
   * This is a little primitive, but it works somewhat.
   * Want to rewrite this some day.
   */
  private static Object duplicate_lock = new Object();
  private void reportStuff(String task, long total, long count)
  {
    /* Prevent multiple threads from giving the same message: */
    synchronized (duplicate_lock)
    {
      int one_pct = (int) total / 100;
      if (one_pct != 0 && count % one_pct == 0 && signal.go())
      {
        double pct = count * 100. / total;

        /* Prevent multiple threads reporting the same threshold: */
        if (fwg.anchor.last_format_pct <= one_pct)
        {
          SlaveJvm.sendMessageToConsole("anchor=%s: %s %d of %d files (%.2f%%)",
                                        fwg.anchor.getAnchorName(),
                                        task, count, total, pct);
          fwg.anchor.last_format_pct = one_pct;
        }
      }
    }
  }


  /**
   * Wait for either 'mkdir', 'create' or 'write' to complete for all threads.
   *
   * The end result is that if you have multiple anchors and therefore multiple
   * Fwg's and multiple OpFormat threads running, they all wait for each other
   * for any of the three mkdir/create/write steps, regardles of any anchor
   * having less files than others and therefore finishing earlier.
   */
  private void waitForAllOtherThreads(FwgEntry      fwg,
                                      FormatCounter counter,
                                      String        txt) throws InterruptedException
  {
    /* Suspend FwgWaiter logic for this FWG: */
    FwgWaiter.getMyQueue(fwg).suspendFwg();


    synchronized (counter)
    {
      /* Lower 'count of threads': */
      counter.counter--;
      //common.plog("Format: One thread '" + txt + "' complete for anchor=" + fwg.anchor.getAnchorName());

      /* Make sure round robin starts at the beginning of the file list: */
      //fwg.anchor.startRoundRobin();

      /* If all threads are done: */
      if (counter.counter == 0)
      {
        SlaveJvm.sendMessageToConsole("anchor=" + fwg.anchor.getAnchorName() +
                                      " " + txt + " complete.");
        //Blocked.printAndResetCounters();

        /* Sleep a bit. This allows one second interval reporting to */
        /* complete its last interval. This is for debugging only */
        //common.sleep_some(1000);

        /* Wake up everybody else: */
        counter.notifyAll();

        /* Make sure round robin starts at the beginning of the file list: */
        fwg.anchor.startRoundRobin();

        /* Tell FwgWaiter to start using this FWG again: */
        FwgWaiter.getMyQueue(fwg).restartFwg();

        return;
      }

      /* Wait until all threads are done: */
      while (counter.counter > 0)
      {
        counter.wait(100); // without the wait time it hung again???
      }

      /* When we exit here, the threads pick up the next operation, */
      /* either 'create' or 'write'. */

      //common.ptod("waitForAllOtherThreads2: %-12s %-15s %d", txt, fwg.anchor.getAnchorName(), counter.counter);
    }
    //common.plog("exit: '" + txt + "' complete for anchor=" + fwg.anchor.getAnchorName());
  }
}


class FormatCounter
{
  int counter;

  public FormatCounter(int count)
  {
    counter = count;
  }

  public boolean notZero()
  {
    synchronized (this)
    {
      return counter > 0;
    }
  }
}
