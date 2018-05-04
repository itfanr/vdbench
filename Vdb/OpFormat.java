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

import java.util.Vector;

import Utils.Format;

/*
 * Author: Henk Vandenbergh.
 */

/**
 * Format all directories and/or all files.
 */
class OpFormat extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private OpMkdir   mkdir  = null;
  private OpCreate  create = null;

  private static Signal signal;

  private int    format_thread_number;


  public OpFormat(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
    signal = new Signal(30);


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

    /* Do 'mkdir' as long as needed: */
    if (fwg.anchor.mkdir_threads_running.notZero())
    {
      /* Don't create directories if we have more threads than width: */
      if (format_thread_number >= fwg.anchor.width)
      {
        FwgWaiter.getMyQueue(fwg).suspend();
        waitForAllOtherThreads(fwg.anchor.mkdir_threads_running, "mkdir");
        FwgWaiter.getMyQueue(fwg).restart();
      }

      else
      {
        if (!mkdir.doOperation() || !fwg.anchor.moreDirsToFormat())
        {
          FwgWaiter.getMyQueue(fwg).suspend();
          waitForAllOtherThreads(fwg.anchor.mkdir_threads_running, "mkdir");
          FwgWaiter.getMyQueue(fwg).restart();
        }
      }
    }

    /* If we only format directories, stop now: */
    else if (SlaveWorker.work.format_flags.format_dirs_requested)
      return false;

    /* Do 'create' as long as needed: */
    else if (fwg.anchor.create_threads_running.notZero())
    {
      reportStuff("Created", fwg.anchor.getFileCount(), fwg.anchor.getExistingFileCount());
      if (!create.doOperation() || !fwg.anchor.anyFilesToFormat())
      {
        FwgWaiter.getMyQueue(fwg).suspend();
        waitForAllOtherThreads(fwg.anchor.create_threads_running, "create");
        FwgWaiter.getMyQueue(fwg).restart();

        return false;
      }
    }

    else
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
  private static int    last_pct = -1;
  private void reportStuff(String task, double total, double done)
  {
    /* Prevent multiple threads from giving the same message: */
    synchronized (duplicate_lock)
    {
      int one_pct = (int) total / 100;
      if (done % one_pct == 0 && signal.go())
      {
        double pct = done * 100 / total;

        /* Prevent multiple threads reporting the same threshold: */
        if (last_pct <= one_pct)
        {
          SlaveJvm.sendMessageToConsole("anchor=" + fwg.anchor.getAnchorName() +
                                        ": " + task + " " + (long) done + " of " +
                                        (long) total +
                                        Format.f(" files (%.2f%%)", pct));
          last_pct = one_pct;
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
  private void waitForAllOtherThreads(FormatCounter counter, String txt) throws InterruptedException
  {
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
        common.sleep_some(1000);

        /* Wake up everybody else: */
        counter.notifyAll();

        /* Make sure round robin starts at the beginning of the file list: */
        fwg.anchor.startRoundRobin();
      }

      /* Wait until all threads are done: */
      while (counter.counter > 0)
      {
        counter.wait(1000); // without the wait time it hung again???
      }

      /* When we exit here, the threads pick up the next operation, */
      /* either 'create' or 'write'. */
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
