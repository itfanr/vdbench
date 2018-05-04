package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.*;
import java.text.*;
import Utils.Semaphore;
import Utils.Format;


/**
 * Create and control a pool of threads of a certain class.
 *
 * The trick in controlling all this and keeping things in sync is to
 * each time when we are in synchronized ThreadControl code to ALWAYS
 * check for an out standing interrupt BEFORE we change a status..
 * If we don't do that, then asynch processing can overlay a status that
 * we have just set.
 */
public abstract class ThreadControl extends Thread
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private Semaphore work_sema  = new Semaphore(0);
  private String    classname  = null;
  private int       threadno   = 0;
  private int       status     = 0;
  private boolean   ack_needed = false;
  private Semaphore ack_sema   = new Semaphore(0);
  private boolean   independent = false;
  private boolean   shutting_down = false;

  private static final int THREAD_CREATED    = 0;  /* created                 */
  public  static final int THREAD_IDLE       = 1;  /* waiting for work (idle) */
  public  static final int THREAD_ACTIVE     = 2;  /* active                  */
  private static final int THREAD_WORKDONE   = 3;  /* work done, almost idle  */
  private static final int THREAD_SET_IDLE   = 4;  /* stop work, go idle      */
  private static final int SHUTDOWN_REQSTD   = 5;  /* shutdown requested      */
  private static final int SHUTDOWN_COMPLETE = 6;  /* shutdown requested      */

  private static String[] status_txt =
  {
    "THREAD_CREATED",
    "THREAD_IDLE",
    "THREAD_ACTIVE",
    "THREAD_WORKDONE",
    "THREAD_SET_IDLE",
    "SHUTDOWN_REQSTD",
    "SHUTDOWN_COMPLETE"
  };


  /* Contains all the idle threads:                                         */
  /* This list was necessary to prevent an idle thread from being picked up */
  /* in the regular active_list for a second time before it was able to     */
  /* change its status to non-idle.                                         */
  private static Vector idle_list = new Vector(128, 0);

  /* Contains all the threads that are not idle: */
  private static Vector active_list = new Vector(128, 0);

  private static Object thread_lock = new Object();

  private static String[] tracelog = new String[1024];
  private static int      traceoffset = 0;

  DateFormat df = new SimpleDateFormat( "HH:mm:ss.SSS" );


  public ThreadControl()
  {
    synchronized(thread_lock)
    {
      status    = THREAD_CREATED;
      classname = this.getClass().getName();
      active_list.addElement(this);
      threadno  = active_list.size();
      setName(classname + "_" + threadno);
      if (classname.equals("StreamStartList"))
      {
        common.ptod("new StreamStartList: " + classname + "_" + threadno);
        Thread.dumpStack();
      }
    }
  }


  public int getThreadno()
  {
    return threadno;
  }

  /**
   * Mark the thread as being up and running and ready to go.
   */
  public synchronized boolean setStartCompleted()
  {
    this.setName(this.getClass().getName());

    try
    {
      trace("setStartCompleted()");
      setStatus(THREAD_CREATED, THREAD_IDLE);
    }
    catch (InterruptedException e)
    {
      /* Either shutdown, or stop current workload and go idle.      */
      /* At this point we already are not even idle yet, so the only */
      /* real status we need to look for is shutdown.                */
      trace("setStartCompleted interrupt status");
      if (this.isShutdown())
      {
        trace("setStartCompleted interrupted. Shutting down");
        return false;
      }

      return true;

    }

    return true;
  }


  /**
   * Independent threads don't fiddle with all this status stuff.
   * ThreadControl is only used to keep track of what threads are running.
   */
  public void setIndependent()
  {
    independent = true;
  }
  public void removeIndependent()
  {
    if (active_list.removeElement(this))
      return;

    if (idle_list.removeElement(this))
      return;

    common.failure("Current thread not in active or idle list");
  }


  /**
   * Add thread to list.
   * This is only needed when the thread instance came from the master,
   * wich in this case is the StreamnStartList
   */
  public void addToList()
  {
    active_list.addElement(this);
    threadno  = active_list.size();
    setName(classname + "_" + threadno);
  }


  /**
   * Mark the thread as 'completely shutdown'.
   */
  public void setShutdownComplete()
  {
    try
    {
      setStatus(SHUTDOWN_REQSTD, SHUTDOWN_COMPLETE);
    }
    catch (InterruptedException e)
    {
      trace("setShutdownComplete(): Ignoring InterruptedException");
      common.ptod("ThreadControl.setShutdownComplete(): Ignoring InterruptedException");
    }
  }


  /**
   * Idle thread is waiting for work.
   */
  public void waitForWork() throws InterruptedException
  {
    trace("waitForWork()");

    work_sema.acquire();
    setStatus(THREAD_IDLE, THREAD_ACTIVE);

    trace("waitForWork() return");
  }


  public void startWorking()
  {
    trace("startWorking");
    work_sema.release();
  }


  /**
   * The thread completed its work, and goes idle.
   */
  public synchronized void setWorkCompleted() throws InterruptedException
  {
    trace("setWorkCompleted()");

    setStatus(THREAD_ACTIVE, THREAD_WORKDONE);
  }


  /**
   * Get a thread, if there are no idle threads available, create a new one.
   */
  public static ThreadControl getIdleThread(String thread_class)
  {
    ThreadControl tc = null;

    synchronized(thread_lock)
    {
      for (int i = 0; i < idle_list.size(); i++)
      {
        tc = (ThreadControl) idle_list.elementAt(i);
        synchronized (tc)
        {
          if (tc.classname.equals(thread_class) && tc.status == THREAD_IDLE)
          {
            if (!idle_list.removeElement(tc))
              common.failure("removeElement() failed");

            if (active_list.contains(tc))
              common.failure("thread list already contains thread");
            active_list.addElement(tc);

            tc.trace("getIdleThread() return");
            return tc;
          }
        }
      }

      try
      {
        Class cls = Class.forName(thread_class);
        tc = (ThreadControl) cls.newInstance();
        tc.trace("getIdleThread: created new thread: " + thread_class + " " + active_list.size());

        tc.start();

        return tc;
      }
      catch (ClassNotFoundException e)
      {
        common.failure(e);
      }
      catch (InstantiationException e)
      {
        common.failure(e);
      }
      catch (IllegalAccessException e)
      {
        common.failure(e);
      }
    }
    return null;
  }



  /**
   * Return the count of idle threads of a certain class
   */
  public static int getIdleCount(String thread_class)
  {
    ThreadControl tc = null;
    int count = 0;

    /* Don't synchronize here because the requestor of this number   */
    /* is in the middle of a getMessage() loop and won't receive a   */
    /* possible 'MASTER_ABORTING' message IF this requestor or slave */
    /* is also in a 'waiting for status change' loop.                */

    //synchronized(thread_lock)
    {

      for (int i = 0; i < idle_list.size(); i++)
      {
        tc = (ThreadControl) idle_list.elementAt(i);
        synchronized (tc)
        {
          if (tc.classname.equals(thread_class))
          {
            if (tc.isIdle())
              count++;
          }
        }
      }
    }

    return count;

  }

  /**
   * Count the number of active threads of a certain class
   */
  public static int getActiveCount(String thread_class)
  {
    ThreadControl tc = null;
    int count = 0;

    //synchronized(thread_lock)
    {
      /* regular list: */
      for (int i = 0; i < active_list.size(); i++)
      {
        tc = (ThreadControl) active_list.elementAt(i);
        //synchronized (tc)
        {
          if (tc.classname.equals(thread_class))
          {
            //tc.trace("count " + status_txt[reqstat] + " " + bool);
            if (tc.status == THREAD_ACTIVE)
              count++;
          }
        }
      }
    }

    return count;
  }


  public static void printActiveThreads()
  {
    ThreadControl tc = null;

    /* regular list: */
    for (int i = 0; i < active_list.size(); i++)
    {
      tc = (ThreadControl) active_list.elementAt(i);
      common.ptod("printActiveThreads: " + tc.classname);
    }
  }



  /**
   * Send an interrupt signal to stop all active threads.
   */
  public static void stopAllActive(String thread_class)
  {
    synchronized(thread_lock)
    {
      /* Create temporary copy of list: */
      Vector copy_list = new Vector(active_list);;

      for (int i = 0; i < copy_list.size(); i++)
      {
        ThreadControl tc = (ThreadControl) copy_list.elementAt(i);
        if (tc.classname.equals(thread_class) && tc.status == THREAD_ACTIVE)
        {
          tc.setThreadIdle();
        }
      }
    }
  }



  /**
   * Send an interrupt signal to all threads of a specific type for shutdown.
   * When type == "full_shutdown" kill everything. (not used)
   */
  public static void shutdownAll(String thread_class)
  {

    /* First kill the active ones: */
    synchronized(thread_lock)
    {
      int found = 0;
      for (int i = 0; i < active_list.size(); i++)
      {
        ThreadControl tc = (ThreadControl) active_list.elementAt(i);
        if (thread_class.equals("full_shutdown") || tc.classname.equals(thread_class))
        {
          found++;
          tc.shutdown();
        }
      }
    }

    /* Then kill the idle ones: */
    synchronized(thread_lock)
    {
      int found = 0;
      for (int i = 0; i < idle_list.size(); i++)
      {
        ThreadControl tc = (ThreadControl) idle_list.elementAt(i);
        if (thread_class.equals("full_shutdown") || tc.classname.equals(thread_class))
        {
          found++;
          tc.shutdown();
        }
      }
    }
  }


  /**
   * Wait for all threads to complete their shutdown.
   */
  public static void waitForShutdownAll()
  {
    long signaltod = 0;


    /* Check active list: */
    while (true)
    {
      boolean waited = false;
      for (int i = 0; i < active_list.size(); i++)
      {
        ThreadControl tc = (ThreadControl) active_list.elementAt(i);
        if (tc.status != SHUTDOWN_COMPLETE)// && !tc.independent)
        {
          tc.trace("Waiting for SHUTDOWN_COMPLETE");
          waited = true;

          common.sleep_some(100);

          break;
        }
      }
      if (!waited)
        break;

      //common.where(signaltod);
      if ((signaltod = common.signal_caller(signaltod, 30 * 1000)) == 0)
      {
        for (int i = 0; i < active_list.size(); i++)
        {
          ThreadControl tc = (ThreadControl) active_list.elementAt(i);
          if (tc.status != SHUTDOWN_COMPLETE)// && !tc.independent)
            common.ptod("waitForShutdownAll() active: " + tc.classname + "-" + tc.threadno + " " + status_txt[tc.status]);
        }
      }
    }



    /* Check idle list: */
    while (true)
    {
      boolean waited = false;
      for (int i = 0; i < idle_list.size(); i++)
      {
        ThreadControl tc = (ThreadControl) idle_list.elementAt(i);
        if (tc.status != SHUTDOWN_COMPLETE)
        {
          tc.trace("Waiting for SHUTDOWN_COMPLETE");
          waited = true;

          common.sleep_some(100);

          break;
        }
      }
      if (!waited)
        return;

      //common.where(signaltod);
      if ((signaltod = common.signal_caller(signaltod, 2000)) == 0)
      {
        for (int i = 0; i < idle_list.size(); i++)
        {
          ThreadControl tc = (ThreadControl) idle_list.elementAt(i);
          if (tc.status != SHUTDOWN_COMPLETE)
            common.plog("waitForShutdownAll() idle: " + tc.classname + "-" + tc.threadno + " " + status_txt[tc.status]);
        }
      }
    }



  }


  /**
   * Wait for all threads to complete goint idle.
   */
  public static void waitForIdleAll(String thread_class)
  {
    long start = System.currentTimeMillis();
    //synchronized(thread_lock)
    {
      while (true)
      {
        boolean waited = false;
        for (int i = 0; i < idle_list.size(); i++)
        {
          ThreadControl tc = (ThreadControl) idle_list.elementAt(i);
          if (tc.classname.equals(thread_class))
          {
            if (tc.status != THREAD_IDLE)
            {
              tc.trace("Waiting for THREAD_IDLE");
              waited = true;
              common.sleep_some(10);
              break;
            }
          }
        }
        //common.ptod("xxx");
        if (!waited)
        {
          //common.ptod("waitForIdleAll: " + (System.currentTimeMillis() - start) + " ms");
          return;
        }

        common.sleep_some(1);
      }
    }
  }


  /**
   * Wait for a thread to have the THREAD_ACTIVE status.
   * If we don't do this inbetween creating a new thread and that thread
   * becoming active then we take the risk of this same still idle thread
   * being picked up again since the code at that time does not known that
   * this thread is actually 'active pending', and not plain idle.
   *
   * This small sleep of one millisecond also has as benefit that the starting
   * of new threads will spread out a little more.
   */
  public void waitForActive()
  {
    trace("waitForActive");
    while (status != THREAD_ACTIVE)
    {
      common.sleep_some(1);
    }
    trace("waitForActive return");
  }


  /**
   * Set the status for a thread.
   * We wait for a correct old status to make sure that whoever the PREVIOUS
   * status was meant for has received it, so that he is ready for the next
   * status update.
   */
  private synchronized void setStatus(int oldstat, int newstat) throws InterruptedException
  {
    /* Shutting down requires special action: */
    if (newstat == SHUTDOWN_REQSTD)
      shutting_down = true;

    /* We wait for the old status to be there, unless this is a shutdown: */
    while (status != oldstat && !shutting_down)
    {
      //this.wait(1000);   // make shorter?
      common.sleep_some(500);
      if (status != oldstat)
      {
        Thread.dumpStack();
        dumpTrace();
        common.ptod("ThreadControl.setStatus(): " + this.classname +
                    " waiting for status change to " +
                    status_txt[newstat] + " is now: " + status_txt[status] +
                    "; should be: " + status_txt[oldstat]);
        //Thread.dumpStack();
        trace("setStatus(): waiting for status change to: " +
              status_txt[newstat] + " is now: " + status_txt[status]);
      }
    }
    trace("setStatus: " + status_txt[newstat]);
    status = newstat;
    /*
    if (this.interrupted())   // what was all this for?
    {
      trace("Interrupt skipped setStatus");
      Thread.currentThread().dumpStack();
      common.ptod("==> I am throwing a new interrupt in the wrong place");
      common.ptod("==> But the real question is, why did 'this' interrupt?");
      throw new InterruptedException();
    }
    */
  }

  public int getStatus()
  {
    return status;
  }
  private boolean isIdle()
  {
    trace("isIdle");
    return status == THREAD_IDLE;
  }
  public boolean goIdle()
  {
    return status == THREAD_SET_IDLE;
  }
  public void markIdle()
  {
    try
    {
      setStatus(THREAD_SET_IDLE, THREAD_IDLE);
    }
    catch (InterruptedException e)
    {
      trace("markIdle(): interrupt ignored");
    }
  }
  public void shutdown()
  {
    try
    {
      sendSignal(THREAD_IDLE, SHUTDOWN_REQSTD);
    }

    /* We are SENDING a signal, so the setStatus() exception won't happen here: */
    catch (InterruptedException e)
    {
      trace("shutdown(): interrupt ignored");
    }
  }
  public boolean isShutdown()
  {
    trace("isShutdown");
    return status == SHUTDOWN_REQSTD;
  }



  /**
   * Mark a thread idle.
   *
   * Thread is moved from the active_list to the idle list.
   */
  public synchronized void setThreadIdle()
  {
    try
    {
      sendSignal(THREAD_ACTIVE, THREAD_SET_IDLE);
    }

    /* We are SENDING a signal, so the setStatus() exception won't happen here: */
    catch (InterruptedException e)
    {
      trace("stop(): interrupt ignored");
    }

    /* Move this thread to the idle list: */
    if (idle_list.contains(this))
      common.failure("Thread is already in the idle list: " + this);
    idle_list.addElement(this);

    /* And get it out of the thread list: */
    if (!active_list.remove(this))
      common.failure("remove(): not in list");

    trace("Moved to idle list");
  }


  public void statusError()
  {
    trace("statusError");
    dumpTrace();
    common.failure("ThreadControl.statusError");
  }


  /**
   * Set a status and then interrupt the thread.
   */
  private synchronized void sendSignal(int oldstat, int newstat) throws InterruptedException
  {
    trace("sendSignal: " + status_txt[newstat]);
    this.setStatus(oldstat, newstat);
    //common.ptod("this.interrupt(): " + this);
    trace("b4 interrupt: " + status_txt[newstat]);
    //this.interrupt();
    common.interruptThread(this);
    trace("af interrupt: " + status_txt[newstat]);
  }


  /**
   * Debugging.
   * Maybe when we remove the printf, place it in a roundrobin table to be
   * dumped when there are problems?
   */
  public void trace(String txt)
  {
    synchronized (tracelog)
    {
      String tod = df.format( new Date() );

      String interupted = (this.isInterrupted()) ? "t" : "f";
      String tmp = tod +
                   Format.f(" %-12s", this.classname) +
                   Format.f(" %2d", threadno) +
                   Format.f(" %-18s", status_txt[status]) +
                   " " + interupted + " " +
                   txt;

      if (common.get_debug(common.THREADCONTROL_LOG))
        common.ptod(tmp);
      else
        tracelog[ traceoffset++ % tracelog.length] = tmp;
    }
  }


  private static boolean dumped_once = false;
  public static void dumpTrace()
  {
    if (!common.get_debug(common.DUMP_THREAD_TRACE))
      return;
    if (dumped_once)
      return;

    dumped_once = true;


    /* Synchronize so that we are not adding new logs while dumping! */
    synchronized (tracelog)
    {
      for (int i = 0; i < tracelog.length; i++)
      {
        String txt = tracelog[ traceoffset++ % tracelog.length ];
        if (txt != null)
          common.plog(txt);
      }
    }
  }

  public static void main(String[] args)
  {
  }
}


class test_thread extends ThreadControl
{


  public void run()
  {
    trace("test_thread run");

    /* Mark this thread as being ready to go. Check for shutdown: */
    if (!setStartCompleted())
      return;

    while (true)
    {
      try
      {
        waitForWork();

        //trace("I now have work");
        common.sleep_some(1);
        this.setWorkCompleted();
      }

      catch (InterruptedException e)
      {
        /* An interrupt here means either shutdown, or stop current work */
        /* and wait for more work.                                       */
        trace("interrupt status");
        if (this.isShutdown())
        {
          trace("Work interrupted. Shutting down2");
          return;
        }
        else if (this.goIdle())
        {
          trace("Work interrupted. stopping current work");
        }
        else
          this.statusError();
      }
    }
  }

  public void doit()
  {
    trace("inside threadcontrol doit");
  }
}
