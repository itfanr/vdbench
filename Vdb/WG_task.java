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
import Utils.Format;


/**
 * This is the Workload Generator task
 */
public class WG_task extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  WG_entry wg;
  int      distribution;
  Task_num tn;

  private Cmd_entry cmd;
  private double start_ts;
  private boolean read_only;
  private boolean use_waiter;

  private GenerateBack gb = null;
  private Generate     gen = null;


  /**
   * Initialization of the Workload Generator task.
   */
  public WG_task(Task_num tn_in, WG_entry wg_in)
  {
    wg           = wg_in;
    distribution = SlaveWorker.work.distribution;
    tn           = tn_in;
    tn.task_set_start_pending();

    read_only = (wg.readpct == 100) ? true : false;

    use_waiter = SlaveWorker.work.use_waiter;

    setName("WG_task " + wg.wg_name);

    /* Generate must be initialized before the IO_tasks are started.     */
    /* This is needed so that we can set the maximum xfersize needed for */
    /* memory buffer allocation.                                         */
    /* It does means that startup can be delayed a little.               */
    /* This will only be noticable if the file containing the generate() */
    /* parameters is very large, since all Generate instances read it.   */
    if (wg.wd_used.generate_parameters != null)
    {
      gb  = new GenerateBack(wg.sd_used, wg);
      gen = new Generate(gb, wg.wd_used.generate_parameters,
                         wg.sd_used.sd_name);
    }
  }



  /**
   * Workload Generator task.
   * This task creates all the i/o streams and sends them to the Waiter task
   */
  public void run()
  {
    int priorities = FifoList.countPriorities(SlaveWorker.work.wgs_for_slave);
    Timers puttime = new Timers("WG_task put()");

    common.ptod("wg: " + wg.wd_name + " " +  wg.sd_used.sd_name + " " + wg.arrival);
    try
    {
      Thread.currentThread().setPriority( Thread.NORM_PRIORITY);

      tn.task_set_start_complete();
      tn.task_set_running();


      if (wg.wd_used.generate_parameters != null)
      {
        gen.generate();

        /* All done. Tell IO_tasks that he can terminate after last i/o: */
        wg.seq_eof = true;

        tn.task_set_terminating(0);
        sendEOF();
        return;
      }

      //common.ptod("Starting WG_task");

      /* Set first start time: */
      calculateFirstStartTime();

      if (wg.bursts != null)
        start_ts = wg.bursts.getArrivalTime(start_ts, distribution);

      /* New cmd entry: */
      cmd = new Cmd_entry();

      //common.ptod("ios_on_the_way: " + wg.ios_on_the_way);

      while (true)
      {
        if (!createNormalIO())
          break;

        /* Keep track of number of generated ios to handle EOF later: */
        wg.add_io(cmd);

        /* Put this io in the proper fifo. If the ultimate target fifo */
        /* is close to being full, wait for it to free up some space.  */
        /* This greatly eliminates context switches.                   */
        try
        {
          puttime.before();
          if (use_waiter)
          {
            wg.wg_to_wt.waitForRoom();
            wg.wg_to_wt.put(cmd);
          }
          else
          {
            cmd.sd_ptr.wt_to_iot.waitForRoom(wg.getpriority());
            cmd.sd_ptr.wt_to_iot.put(cmd, wg.getpriority());
          }
          puttime.after();

        }
        catch (InterruptedException e)
        {
          //common.plog("WG Task interrupted 2");
          break;
        }

        /* Calculate the arrival time for the next i/o: */
        start_ts = calculateNextStartTime(start_ts);

        /* New cmd entry: */
        cmd = new Cmd_entry();
      }

      /* Send an EOF cmd entry: */
      sendEOF();

      puttime.print();

      //tn.task_set_terminating(0);

    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

    tn.task_set_terminating(0);
    //common.plog("Ended WG_task " + tn.task_number + tn.task_name);
  }


  /**
   * The first start time must be properly set
   */
  private void calculateFirstStartTime()
  {
    if (distribution == 0)
      start_ts = ownmath.exponential(wg.arrival);

    else if (distribution == 1)
      start_ts = ownmath.uniform(0, wg.arrival * 2);

    else
      start_ts = wg.ts_offset;
  }



  /**
   * Calculate the next arrival time for the next i/o
   */
  private double calculateNextStartTime(double start_time)
  {
    if (wg.bursts != null)
    {
      start_time = wg.bursts.getArrivalTime(start_ts, distribution);
      return start_time;
    }

    if (distribution == 0)
    {
      double delta = ownmath.exponential(wg.arrival);
      if (delta > 180 * 1000000)
        delta = 180 * 1000000;
      start_time += delta;
    }

    else if (distribution == 1)
    {
      double delta = ownmath.uniform(0, wg.arrival * 2);
      start_time += delta;
      if (delta > 1000000)
        delta = 1000000;
    }

    else
      start_time += wg.arrival;

    return start_time;
  }

  /**
   * Create an I/O command for normal processing.
   *
   * Return: false = EOF.
   */
  private boolean createNormalIO()
  {
    while (true)
    {
      cmd.cmd_wg      = wg;
      cmd.dv_pre_read = false;

      /* Set read or write: */
      cmd.cmd_read_flag = read_only ? true : wg.wg_read_or_write();

      /* Set relative starting time: */
      cmd.delta_tod = (long) start_ts;

      /* Set transfer size: */
      if (wg.xfersize != -1)
        cmd.cmd_xfersize = wg.xfersize;
      else
        cmd.cmd_xfersize = wg.wg_dist_xfersize(cmd);

      /* Hit or Miss? */
      cmd.cmd_hit = wg.wg_hit_or_miss(cmd);

      /* What type of i/o? */
      cmd.cmd_rand = wg.wg_random_or_seq(cmd);

      /* Calculate seek address, sequential EOF means STOP: */
      /* (For sequential the next lba is determined in JNI code) */
      if (wg.wg_calc_seek(cmd))
      {
        /* Sequential EOF, write record to show eof: */
        wg.seq_eof = true;
        common.ptod("Reached eof in wg_task " + wg.sd_used.sd_name);

        /* If we reach EOF here while all outstanding ios have completed */
        /* then we must lower the 'sequential luns active' count:        */
        if (wg.ios_on_the_way == 0 && (SlaveJvm.isReplay() || Validate.isValidate()))
        {
          common.ptod("calling seq lower for: " + wg.wg_name);
          WG_entry.sequentials_lower();
        }

        return false;
      }

      //common.ptod( Format.f("lba %12d ", cmd.cmd_lba) + cmd.cmd_rand);

      /* Did an interrupt show up? */
      if (Thread.interrupted())
        return false;


      /* Data validation: ignore if lba already busy: */
      try
      {
        if (Validate.isValidate() && !cmd.sd_ptr.getDvMap().dv_new_cmd(cmd))
          continue;
      }
      catch (Exception e)
      {
        common.ptod("cmd.sd_ptr.getDvMap(): " + cmd.sd_ptr);
        common.ptod("cmd.sd_ptr.getDvMap(): " + cmd.sd_ptr.getDvMap());
        common.failure(e);
      }

      return true;
    }
  }

  private void sendEOF()
  {
    /* Send an EOF cmd entry: */
    try
    {
      Cmd_entry cmd = new Cmd_entry();
      cmd.sd_ptr = wg.sd_used;
      cmd.delta_tod = Long.MAX_VALUE;
      if (use_waiter)
      {
        wg.wg_to_wt.waitForRoom();
        wg.wg_to_wt.put(cmd);
      }
      else
      {
        cmd.sd_ptr.wt_to_iot.waitForRoom(wg.getpriority());
        wg.sd_used.wt_to_iot.put(cmd, 0);
      }
    }
    catch (InterruptedException e)
    {
      //common.plog("WG Task interrupted 3");
    }
  }

}


