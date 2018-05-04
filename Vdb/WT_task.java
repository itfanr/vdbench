package Vdb;
import java.util.ArrayList;

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



/**
 * This task waits for the correct time of day and sends i/o to proper IO_task
 */
public class WT_task extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private ArrayList wgs_for_slave;
  private int wg_count;
  private Task_num tn;
  private int priorities;


  /**
   * Setup of Waiter task
   */
  WT_task(Task_num tn_in)
  {
    priorities    = FifoList.countPriorities(SlaveWorker.work.wgs_for_slave);
    tn            = tn_in;
    tn.task_set_start_pending();
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
    long tod           = Native.get_simple_tod();
    Cmd_entry lowcmd   = null;
    wgs_for_slave      = new ArrayList(SlaveWorker.work.wgs_for_slave);
    wg_count           = wgs_for_slave.size();
    boolean spin = common.get_debug(common.SPIN);

    Thread.currentThread().setPriority( Thread.MAX_PRIORITY );

    try
    {

      tn.task_set_start_complete();
      tn.task_set_running();

      //common.plog("Starting WT_task");

      SlaveWorker.first_tod = Native.get_simple_tod();


      /* Get the first cmd entry from each WG: */
      for (int i = 0; i < wgs_for_slave.size(); i++)
      {
        WG_entry wg = (WG_entry) wgs_for_slave.get(i);

        try
        {
          wg.pending_cmd = (Cmd_entry) wg.wg_to_wt.get();
        }
        catch (InterruptedException e)
        {
          common.ptod("WT Task interrupted at FIRST get?????");
          Thread.currentThread().interrupt();
          break;
        }
        if (wg.pending_cmd == null)
          common.failure("Early eof from fifo");
      }


      while (!SlaveJvm.isWorkloadDone())
      {
        /* Look for the lowest entry : */
        if ((lowcmd = getLowest()) == null)
          break;


        /* Wait until correct timestamp arrives: */
        lowcmd.delta_tod += SlaveWorker.first_tod;
        while (true)
        {
          /* This trick saves a get_tod if we are way too late already! */
          if (tod >= lowcmd.delta_tod)
            break;
          tod = Native.get_simple_tod();
          if (tod >= lowcmd.delta_tod)
            break;

          /* Wait a bit. set hires_tick=1 works just as well.     */
          /* hires_tick=0: .13 ms late; hires_tick=1: .03 ms late */
          if (!spin)
          {
            common.sleep_some_usecs(lowcmd.delta_tod - tod);
            if (Thread.currentThread().interrupted())
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
          lowcmd.sd_ptr.wt_to_iot.put(lowcmd, lowcmd.cmd_wg.getpriority());
        }
        catch (InterruptedException e)
        {
          common.plog("WT Task interrupted at put");
          break;
        }


        /* Get the following entry for this lowest (for the next loop): */
        try
        {
          lowcmd.cmd_wg.pending_cmd = (Cmd_entry) lowcmd.cmd_wg.wg_to_wt.get();
        }

        catch (InterruptedException e)
        {
          common.plog("WT Task interrupted at get");
          break;
        }

        //if (lowwg.pending_cmd == null)
        //  break;
      }

      tn.task_set_terminating(0);
      common.plog("Ended WT_task");

    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }

  private long rr_index = 0;
  private Cmd_entry getLowest()
  {
    WG_entry lowwg = null;
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
        int index  = (int) rr_index++ % wgs_for_slave.size();
        for (int i = 0; i < wgs_for_slave.size(); i++, index++)
        {
          lowwg = (WG_entry) wgs_for_slave.get(index % wgs_for_slave.size());

          if (lowwg.pending_cmd == null)
            common.failure("oops1");
          if (lowwg.pending_cmd.sd_ptr == null)
            common.failure("oops2");
          if (lowwg.pending_cmd.sd_ptr.wt_to_iot == null)
            common.failure("oops3");

          /* Look for a target SD fifo that has room left: */
          if (!lowwg.pending_cmd.sd_ptr.wt_to_iot.isGettingFull(lowwg.getpriority()))
          {
            found = true;
            break;
          }
        }

        /* All target SD fifos are almost full. Try again later: */
        if (!found)
          common.sleep_some_usecs(1000);

        if (SlaveJvm.isWorkloadDone())
          return null;

      } while (!found);


      /* Look for the lowest entry, skipping those fifos whose */
      /* target fifos are getting full:                        */
      Cmd_entry lowcmd = lowwg.pending_cmd;
      for (int i = 0; i < wgs_for_slave.size(); i++)
      {
        WG_entry wg = (WG_entry) wgs_for_slave.get(i);
        if (wg.pending_cmd.delta_tod == Long.MAX_VALUE)
        {
          wgs_for_slave.remove(wg);
          continue;
        }

        /* Since we're scanning the whole list again, ignore the first one: */
        if (wg == lowwg)
          continue;

        /* Bypass those whose SD fifo is getting full: */
        if (wg.pending_cmd.sd_ptr.wt_to_iot.isGettingFull(wg.getpriority()))
        {
          //common.ptod("wg.pending_cmd.sd_ptr: " + wg.pending_cmd.sd_ptr.sd_name);
          /* Don't bypass when the current delta marks EOF. If we did, and this one */
          /* would be the lowest, we would return NULL below and signal endofrun */
          if (lowwg.pending_cmd.delta_tod != Long.MAX_VALUE)
            continue;
        }

        if (lowwg.pending_cmd.delta_tod > wg.pending_cmd.delta_tod)
        {
          lowwg  = wg;
          lowcmd = lowwg.pending_cmd;
        }
      }

      if (wgs_for_slave.size() == 0)
        return null;

      if (lowcmd.delta_tod == Long.MAX_VALUE)
        return null;

      return lowcmd;
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("lowwg: " + lowwg);
      common.ptod("lowwg.pending_cmd: " + lowwg.pending_cmd);
      common.ptod("lowwg.pending_cmd.sd_ptr: " + lowwg.pending_cmd.sd_ptr);
      common.ptod("lowwg.pending_cmd.sd_ptr.wt_to_iot: " + lowwg.pending_cmd.sd_ptr.wt_to_iot);
      common.failure(e);
    }
    return null;

  }
}
