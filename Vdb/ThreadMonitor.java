package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.lang.management.*;
import java.util.*;




/**
 * Pickup and report cpu usage data for a thread.
 * On windows this works on a 15ms granularity, so low use will not show.
  */
public class ThreadMonitor
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


  private String        label;

  private long          thread_id    = -1;

  private ThreadMonData latest;
  private ThreadMonData previous;
  private ThreadMonData delta;

  private long          counter1     = 0;
  private long          counter2     = 0;
  private long          counter3     = 0;

  private static long last_get_tod = 0;

  protected static int  processors = 0;

  private static boolean monitor_all     = false;
  private static boolean monitor_top10   = false;
  private static boolean monitor_console = false;
  private static boolean monitor         = false;

  protected static ThreadMXBean threadMXBean = init();



  private static ThreadMonList all_deltas = new ThreadMonList();

  /* Using a Vector here because I had ONE instance where the list did  */
  /* not contain ALL monitors which made me think that a lock would     */
  /* be needed.                                                         */
  private static Vector <ThreadMonitor> monitor_list = new Vector(64);




  public ThreadMonitor(String type, String nm1, String nm2)
  {
    label = type;
    if (nm1 != null)
      label += "," + nm1;
    if (nm2 != null)
      label += "," + nm2;

    if (threadMXBean == null)
      return;

    thread_id = Thread.currentThread().getId();
    if (thread_id < 0)
      return;

    monitor_list.add(this);

    latest   = new ThreadMonData(label);
    previous = new ThreadMonData(label);
    delta    = new ThreadMonData(label);
  }


  public static boolean active()
  {
    return monitor;
  }

  private static ThreadMXBean init()
  {
    monitor_all     = common.get_debug(common.THREAD_MONITOR_ALL    );
    monitor_top10   = common.get_debug(common.THREAD_MONITOR_TOP10  );
    monitor_console = common.get_debug(common.THREAD_MONITOR_CONSOLE);
    monitor         = (monitor_all || monitor_top10 || monitor_console);
    if (!monitor)
      return null;

    try
    {
      threadMXBean = ManagementFactory.getThreadMXBean();

      if (threadMXBean.isThreadContentionMonitoringSupported())
        threadMXBean.setThreadContentionMonitoringEnabled(true);
      else
        common.ptod("ThreadContentionMonitoring not supported");

      if (threadMXBean.isThreadCpuTimeSupported())
        threadMXBean.setThreadCpuTimeEnabled(true);
      else
        common.ptod("ThreadCpuTime not supported");

      /* Remember the #cpus for this host: */
      processors = Runtime.getRuntime().availableProcessors();

      return threadMXBean;
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Thread monitoring disabled");
      return null;
    }
  }



  public static void clear()
  {
    monitor_list.clear();
  }

  public void addCount(int c)
  {
    if (monitor) counter1 += c;
  }
  public void add1()
  {
    if (monitor) counter1++;
  }
  public void add2()
  {
    if (monitor) counter2++;
  }
  public void add3()
  {
    if (monitor) counter3++;
  }

  public void getLatestData()
  {
    latest.getThreadData(thread_id);
    latest.counter1 = counter1;
    latest.counter2 = counter2;
    latest.counter3 = counter3;
  }

  /**
   * Get interval data for all available thread monitors.
   */
  public static ThreadMonList getAllData()
  {
    /* First establish the elapsed time since the last time: */
    /* (last time always set as soon as work starts)         */
    long current_nanos = System.nanoTime();
    all_deltas.elapsed = current_nanos - last_get_tod;
    last_get_tod       = current_nanos;

    /* Quickly pick up all data: */
    all_deltas.list.clear();
    for (ThreadMonitor tm : monitor_list)
    {
      tm.getLatestData();
      tm.delta.calcDelta(tm.previous, tm.latest);
      all_deltas.list.add(tm.delta);
    }
    return all_deltas;
  }

  public static ThreadMonList getAllDeltas()
  {
    return all_deltas;
  }

  public static void saveAllPrevious()
  {
    for (ThreadMonitor tm : monitor_list)
      tm.previous.copyData(tm.latest);
  }


  public static void reportAllDetail(ThreadMonList deltas)
  {
    if (threadMXBean == null)
      return;

    /* Deltas have already been calcluated by the caller.                      */
    /* This is done separately to make sure that we don't do it more than once */
    /* per interval.                                                           */
    Collections.sort((ArrayList) deltas.list);
    common.ptod("");
    int iotasks = 0;
    int wgtasks = 0;
    for (ThreadMonData delta : deltas.list)
    {
      if (delta.label.contains("IO_task"))
      {
        if (++iotasks > 10 && common.get_debug(common.THREAD_MONITOR_TOP10))
          continue;
      }

      if (delta.label.contains("WG_task"))
      {
        if (++wgtasks > 10 && common.get_debug(common.THREAD_MONITOR_TOP10))
          continue;
      }

      common.ptod("ThreadMonitor cpu %6.2f%% user %6.2f%%  blkd %6.2f%% %6d wait %6.2f%% %8d %8d %s",
                  delta.cputime     * 100.           / deltas.elapsed,
                  delta.usertime    * 100.           / deltas.elapsed,
                  delta.blockedtime * 100. * 1000000 / deltas.elapsed,
                  delta.blockedcount,
                  delta.waitedtime  * 100. * 1000000 / deltas.elapsed,
                  delta.counter1,
                  delta.counter2,
                  delta.label);

    }
  }


  /**
   * Report the accumulated totals of ThreadMonData.
   * Note that because of the possibility of having different processor counts
   * on different hosts this reporting can not be used to report combined hosts.
   */
  public static void reportTotals(String txt, ThreadMonList deltas)
  {
    /* Empty data is possible when a run never completed one interval: */
    if (deltas.list.size() == 0)
      return;

    long elapsed    = deltas.elapsed;
    long processors = deltas.processors;

    Collections.sort((ArrayList) deltas.list);

    //for (ThreadMonData delta : deltas.list)
    //  common.ptod("delta: el: %d %s", elapsed / 1000000, delta);

    String line;
    if (common.get_debug(common.THREAD_MONITOR_CONSOLE))
      common.ptod("");
    else
      common.plog("");
    int iotasks = 0;
    int wgtasks = 0;
    for (ThreadMonData delta : deltas.list)
    {
      if (delta.label.contains("IO_task"))
      {
        if (++iotasks > 10 && common.get_debug(common.THREAD_MONITOR_TOP10))
          continue;
      }
      if (delta.label.contains("WG_task"))
      {
        if (++wgtasks > 10 && common.get_debug(common.THREAD_MONITOR_TOP10))
          continue;
      }

      line = String.format("%-12s cpu %6.2f%% user %6.2f%%  blkd %6.2f%% wait %6.2f%% %8d %8d %s",
                           txt,
                           delta.cputime     * 100.           / (elapsed * processors),
                           delta.usertime    * 100.           / (elapsed * processors),
                           delta.blockedtime * 100. * 1000000 / (elapsed * processors),
                           delta.waitedtime  * 100. * 1000000 / (elapsed * processors),
                           delta.counter1 * 1000000000 / elapsed,
                           delta.counter2 * 1000000000 / elapsed,
                           delta.label);
      if (common.get_debug(common.THREAD_MONITOR_CONSOLE))
        common.ptod(line);
      else
        common.plog(line);
    }
  }
}



class ThreadMonData implements Comparable, Serializable
{
  public String label;

  public long   cputime;        /* Nano seconds */
  public long   usertime;

  public long   blockedtime;    /* Milliseconds */
  public long   waitedtime;
  public long   blockedcount;

  public long   counter1;       /* Optional user fields */
  public long   counter2;
  public long   counter3;

  public ThreadMonData(String lbl)
  {
    label = lbl;
  }

  public void getThreadData(long thread_id)
  {
    ThreadInfo thread_info = ThreadMonitor.threadMXBean.getThreadInfo(thread_id, 8);
    if (thread_info == null)
    {
      blockedcount = 0;
      blockedtime  = 0;
      waitedtime   = 0;
      cputime      = 0;
      usertime     = 0;
      return;
    }

    blockedcount = thread_info.getBlockedCount();
    blockedtime  = thread_info.getBlockedTime();
    waitedtime   = thread_info.getWaitedTime();
    cputime      = ThreadMonitor.threadMXBean.getThreadCpuTime( thread_id);
    usertime     = ThreadMonitor.threadMXBean.getThreadUserTime(thread_id);

    /* This piece of extra diagnostic info gets very expensive, so never keep it active */
    if (false)
    {
      String lock = thread_info.getLockName();
      if (lock != null)
      {
        StackTraceElement[] stack = thread_info.getStackTrace();
        String txt = label + " is blocked at: ";
        for (int i = 0; stack != null && i < stack.length; i++)
          txt += " " + stack[i].toString();
        common.ptod(txt);
        // common.ptod("lock: " + lock + " " + thread_info.getLockInfo().getClassName());
      }

      thread_info = ThreadMonitor.threadMXBean.getThreadInfo(new long[] { thread_id}, true, true)[0];
      MonitorInfo[] infos = thread_info.getLockedMonitors();
      //common.ptod("info: " + infos.length);
      for (MonitorInfo info : infos)
      {
        StackTraceElement ste = info.getLockedStackFrame();
        common.ptod("%s blocked by lock owner: %s", label, ste.toString());
      }
    }
  }



  public String toString()
  {
    return String.format("cpu: %6d user: %6d wait: %6d %s",
                         cputime / 1000000, usertime / 1000000, waitedtime / 1000000, label);
  }

  public void calcDelta(ThreadMonData prev, ThreadMonData latest)
  {
    blockedcount = latest.blockedcount - prev.blockedcount;
    blockedtime  = latest.blockedtime  - prev.blockedtime;
    waitedtime   = latest.waitedtime   - prev.waitedtime;
    cputime      = latest.cputime      - prev.cputime;
    usertime     = latest.usertime     - prev.usertime;

    counter1     = latest.counter1     - prev.counter1;
    counter2     = latest.counter2     - prev.counter2;
    counter3     = latest.counter3     - prev.counter3;
  }

  public void copyData(ThreadMonData source)
  {
    blockedcount = source.blockedcount;
    blockedtime  = source.blockedtime;
    waitedtime   = source.waitedtime;
    cputime      = source.cputime;
    usertime     = source.usertime;

    counter1     = source.counter1;
    counter2     = source.counter2;
    counter3     = source.counter3;
  }

  public void accum(ThreadMonData source)
  {
    blockedcount += source.blockedcount;
    blockedtime  += source.blockedtime;
    waitedtime   += source.waitedtime;
    cputime      += source.cputime;
    usertime     += source.usertime;

    counter1     += source.counter1;
    counter2     += source.counter2;
    counter3     += source.counter3;
  }


  /**
   * Sort in descending order of cputime.
   */
  public int compareTo(Object o1)
  {
    ThreadMonData tmon1 = (ThreadMonData) o1;

    long rc = tmon1.cputime - cputime;
    if (rc < 0)
      return -1;
    else if (rc > 0)
      return +1;

    /* Equal, look at 'blocked': */
    rc = tmon1.blockedtime - blockedtime;
    if (rc == 0)
      return 0;
    else if (rc < 0)
      return -1;
    else
      return +1;
  }
}


/**
 * Collection of interval-level ThreadMonitor statistics.
 *
 * The list either contains interval data from all monitors, or it can also
 * contain a lits of the accumulates totals picked up from the map.
 */
class ThreadMonList implements Serializable
{
  long elapsed;   /* In nano seconds */
  int  processors = ThreadMonitor.processors;

  ArrayList <ThreadMonData>         list   = new ArrayList(8);

  HashMap   <String, ThreadMonData> map    = new HashMap(8);
}
