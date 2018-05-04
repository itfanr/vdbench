package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */


import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

import Utils.Format;

/*
 *
 * This file contains functionality that makes sure that we know what state
 * they are in.
 * It also allows us to make sure that all threads are ready before any i/o is
 * generated. We don't want some threads to take off before others are ready.
 *
 */

/**
 * Methods in this class allow monitoring of the state of each task.
 */
public class Task_num
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  static Vector <Task_num> task_list = new Vector(32, 0) ;  /* One entry per task                      */
  static int task_next = 1;    /* next task number to give out                */

  String task_name;            /* Optional name of task                       */
  int    task_number;          /* Number given to task, starting with 1       */
  int    task_status;          /*                                             */
  final static int INIT       = 0; /* Not started                             */
  final static int ST_PENDING = 1; /* Startup pending                         */
  final static int ST_COMP    = 2; /* Startup complete                        */
  final static int GO_RUN     = 3; /* Permission to run                       */
  final static int RUNNING    = 4; /* Running                                 */
  final static int TERM       = 5; /* Terminating                             */
  double task_return;          /* returned value from task_set_terminating()  */

  Thread task_thread;

  /**
   * Allocate a new Task_num instance.
   * If this is the first request, allocate the Vector.
   */
  Task_num(String tname)
  {
    this.task_get(tname);
  }
  public Task_num(Object owner)
  {
    this.task_get(owner.getClass().getName());
  }


  /**
   * Clear the task list.
   */
  public static synchronized void task_list_clear()
  {
    task_list.removeAllElements();
    task_next = 1;
  }


  /**
   * Initialize a new Task_num instance.
   */
  public synchronized void task_get(String tname)
  {

    task_name = tname;
    task_number = task_next++;
    task_status = INIT;
    task_thread = null;
    task_list.add(this);
  }


  /**
   * Task has just been started.
   */
  public synchronized void task_set_start_pending()
  {
    if (this.task_status != INIT)
      common.failure("task status out of sync: " + this.task_name + " " +
                     this.task_status + "/" + ST_PENDING);
    else
      this.task_status = ST_PENDING;
  }


  /**
   * Task has completed setup
   */
  public synchronized void task_set_start_complete()
  {
    task_thread = Thread.currentThread();

    if (this.task_status != ST_PENDING)
      common.failure("task status out of sync: " + this.task_name + " " +
                     this.task_status + "/" + ST_COMP);
    else
      this.task_status = ST_COMP;
  }


  /**
   * The task is waiting for permission to start running.
   */
  public synchronized void waitForMasterGo()
  {
    while (this.task_status != Task_num.GO_RUN)
    {
      try
      {
        wait(500);
      }
      catch (InterruptedException e)
      {
        common.plog("catch (InterruptedException) tasknum " + this.task_name);
      }
    }
    this.task_status = RUNNING;
  }


  /**
   * This task is now terminating.
   * A 'return code' is stored as a double value.
   */
  public synchronized void task_set_terminating(double ret_in)
  {
    if (this.task_status != RUNNING)
      common.failure("task status out of sync: " + this.task_name + " " +
                     this.task_status + "/" + TERM);
    else
      this.task_status = TERM;

    this.task_return = ret_in;
    task_thread = null;
  }


  /**
   * Set all tasks to RUN
   */
  public static void task_run_all()
  {
    Task_num tn;

    for (int i = 0; i < task_list.size(); i++)
    {
      tn = (Task_num) task_list.elementAt(i);

      synchronized (tn)
      {
        if (tn.task_status != ST_COMP)
          common.failure("task status out of sync: " + tn.task_name + " " +
                         tn.task_status + "/" + GO_RUN);
        else
          tn.task_status = GO_RUN;
      }
    }

    common.plog(Format.f("task_run_all(): %d tasks", task_list.size()));
  }


  /**
   * Wait for all tasks to be 'startup complete'
   */
  public static void task_wait_start_complete()
  {
    Task_num tn;
    int i, max;
    long signaltod = 0;

    common.plog("Waiting for task synchronization");

    /* Wait for all tasks to be 'startup complete': */
    while (true)
    {
      for (i = 0; i < task_list.size(); i++)
      {
        tn = (Task_num) task_list.elementAt(i);

        if (tn.task_status != Task_num.ST_COMP)
        {
          common.plog("task_wait_start_complete: " + tn.task_name + " " + tn.task_status);
          break;
        }
      }
      if (i == task_list.size())
        break;

      /* Report 'pending' every x seconds: */
      if ( (signaltod = common.signal_caller(signaltod, 10000)) == 0)
      {
        common.ptod("Waiting in task_wait_start_complete() ");
        for (max = i = 0; i < task_list.size(); i++)
        {
          tn = (Task_num) task_list.elementAt(i);

          if (tn.task_status != Task_num.ST_COMP)
          {
            common.ptod("for " + tn.task_name + " to start. Status " + tn.task_status + " " + i);
            if (max++ > 10)
            {
              common.ptod("Maximum 10 tasks are reported each 10 seconds");
              break;
            }
          }
        }
      }

      //common.ptod("still waiting? : " + signaltod);
      common.sleep_some(200);
      //common.ptod("after waiting? : " + signaltod);

    }


    /* Now that all tasks are ready to go, do a GC.                    */
    /* This will remove some overhead once we are really running,      */
    /* especially since the first GC apparently is the most expensive. */
    GcTracker.gc();
    //GcTracker.reportAlways();
    //GcTracker.gc();
    //GcTracker.gc();

    if (false)
    {

      long current = Runtime.getRuntime().totalMemory();
      long maxmem  = Runtime.getRuntime().maxMemory()  ;
      long spare = maxmem - current;
      common.ptod("spare: %,d", spare);
      //int loops = (int) (spare / (500*1024*1024));
      int loops = 0;

      Vector dump = new Vector(1024);
      while (true)
      {
        try
        {
          loops ++;
          common.ptod("loops1: " + loops);
          GcTracker.reportAlways();
          dump.add(new byte[ 500*1024*1024]);
        }

        catch (OutOfMemoryError e)
        {
          common.memory_usage();
          common.where();
          break;
        }
      }

      while (true)
      {
        try
        {
          loops ++;
          common.ptod("loops2: " + loops);
          GcTracker.reportAlways();
          dump.add(new byte[ 100*1024*1024]);
        }

        catch (OutOfMemoryError e)
        {
          common.memory_usage();
          common.where();
          break;
        }
      }


      dump = null;

      System.exit(777);
    }

    common.plog("task_wait_start_complete() end");
  }


  /**
   * Wait for all tasks to complete
   */
  public static int task_wait_all()
  {
    int i;
    long signaltod = 0;
    long start_wait = Native.get_simple_tod();

    while (true)
    {
      for (i = 0; i < task_list.size(); i++)
      {
        Task_num tn = (Task_num) task_list.elementAt(i);
        if (tn.task_status != Task_num.TERM)
          break;
      }

      if (i == task_list.size())
        break;

      if ( (signaltod = common.signal_caller(signaltod, 15 * 1000)) == 0)
        reportWaiters(start_wait);

      /* Give up when it takes too long: */
      int seconds = common.get_debug(common.LONG_SHUTDOWN) ? 15*60 : 5*60;
      if (Native.get_simple_tod() - start_wait > seconds * 1000 * 1000)
        return seconds / 60;

      common.sleep_some(99);
    }

    return 0;
  }


  private static void reportWaiters(long start_wait)
  {
    Vector txt = new Vector(8, 0);
    long tod   = Native.get_simple_tod();
    txt.add("task_wait_all(): Waiting " +
            ((tod - start_wait) / 1000000) +
            " seconds for active threads to complete: ");

    HashMap threads_done = new HashMap(64);
    for (int i = 0; i < task_list.size(); i++)
    {
      Task_num tn = (Task_num) task_list.elementAt(i);
      if (tn.task_status != Task_num.TERM)
      {
        String key = tn.task_name + " " + tn.task_status;
        Integer count = (Integer) threads_done.get(key);
        if (count == null)
          count = new Integer(0);
        threads_done.put(key, new Integer(count.intValue()+1));
      }
    }

    String[] keys = (String[]) threads_done.keySet().toArray(new String[0]);
    Arrays.sort(keys);
    for (int i = 0; i < keys.length; i++)
    {
      Integer count = (Integer) threads_done.get(keys[i]);
      txt.add(keys[i] + ": " + count.intValue() + " threads");
    }
    SlaveJvm.sendMessageToConsole(txt);
  }



  /**
   * Terminate requested tasks:
   */
  static void interrupt_tasks(String id)
  {
    long first_try = Native.get_simple_tod();
    long signaltod = 0;
    int tot_ints = 0;

    /* FwgThread may NOT be interrupted. Operations like native.OpenFile() */
    /* may fail instead of a java interrupt being recognized.              */
    /* For that, FwgThread uses the getUntilDone() method in FwgWaiter.    */
    if (id.startsWith("FwgThread"))
      common.failure("No interrupt may be requested for FwgThread.");

    while (true)
    {
      /* We give them three minutes before we just kill them: */
      if (Native.get_simple_tod() - first_try > 180*1000*1000)
      {
        for (int i = 0; i < task_list.size(); i++)
        {
          Task_num tn = (Task_num) task_list.elementAt(i);
          if (tn.task_name.startsWith(id))
          {
            if (tn.task_thread != null)
            {
              /* Had one occurrence that maybe caused the message to not get through */
              /* Therefore print the mesage first, and then stop (stop is bad) */
              /* However, it's maybe better to just kill myself: */
              common.failure(Format.f("Task %s stopped after 3 minutes of trying " +
                                      "to get it to terminate itself. " +
                                      "Unpredictable results may occur.", id));
            }
          }
        }

        return;
      }


      int new_interrupts = 0;
      int total_tasks = 0;

      for (int i = 0; i < task_list.size(); i++)
      {
        Task_num tn = (Task_num) task_list.elementAt(i);
        synchronized (tn)
        {
          if (tn.task_name.startsWith(id))
          {
            total_tasks++;
            if (tn.task_thread != null)
            {
              /* Is it necessary to repeat the interrupt request? */
              /* It works, so just leave it. */
              common.interruptThread(tn.task_thread);
              new_interrupts++;
              tot_ints++;
            }
          }
        }
      }

      /* If no new interrupts were sent we're done: */
      if (new_interrupts == 0)
      {
        /* Display a message if more than one interrupt per task was needed: */
        if (tot_ints > total_tasks)
          common.plog("Interrupts issued for tasks " + id + ": " + tot_ints);
        break;
      }

      if ( (signaltod = common.signal_caller(signaltod, 10000)) == 0)
      {
        common.plog("Trying to terminate tasks: " + id);
        common.plog("Interrupts issued for tasks " + id + ": " + tot_ints);
      }

      common.sleep_some(100);
    }
  }


  /**
   * Count tasks whose name start with a requested name
   */
  public synchronized static int countTasks(String name, int status)
  {
    int count = 0;
    //common.ptod("task_list.size(): " + task_list.size());
    for (int i = 0; i < task_list.size(); i++)
    {
      Task_num tn = (Task_num) task_list.elementAt(i);
      common.ptod("countTasks: " + name + " " + tn.task_name + " " + tn.task_status);
      if (tn.task_status == status && tn.task_name.startsWith(name))
        count++;
    }

    //common.ptod("count: " + count);
    return count;
  }

  /**
   * Scan through all FwgThread tasks to see if they are all in termination
   * status.
   */
  public synchronized static boolean checkAllInTermination()
  {
    /* Count the number of FwgThreads, also count TERM status: */
    int fwgt_count = 0;
    int term_count = 0;
    for (Task_num tn : task_list)
    {
      if (tn.task_name.startsWith("FwgThread"))
      {
        fwgt_count++;
        if (tn.task_status == TERM)
          term_count++;
      }
    }

    return fwgt_count == term_count;

  }

  public synchronized static void printTasks()
  {
    common.ptod("task_list: " + task_list.size());
    for (int i = 0; i < task_list.size(); i++)
    {
      Task_num tn = (Task_num) task_list.elementAt(i);
      common.ptod("tn.task_name: " + tn.task_name);
    }
  }
}






