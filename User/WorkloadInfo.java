package User;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import Vdb.Cmd_entry;
import Vdb.SD_entry;
import Vdb.SlaveJvm;
import Vdb.SlaveWorker;
import Vdb.WG_entry;
import Vdb.WT_task;
import Vdb.common;
import Vdb.ownmath;

/**
 * This contains 'all' of the workload information specified in the parameter
 * file that can be of use to the user.
 */
public class WorkloadInfo
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private int distribution = 0; /* 0: exponential                             */
                                /* 1: uniform                                 */
                                /* 2: deterministic                           */

  private WG_entry wg;
  private SD_entry sd;
  private long     high_lba;
  private long     first_starttime;
  private long     interarrival;     /* Interarrival time in usecs            */
  private int      rd_pct;
  private int      seekpct;
  private int      xf_table[];       /* xfersize distribution table           */
  private boolean  use_block_zero = false;

  private int      calls_since_done = 0;
  private long     last_start_delta  = 0;

  private UserDeviceInfo device_info;

  private Cmd_entry cmd = null;

  private boolean   first_io = true;




  public WorkloadInfo(WG_entry w)
  {
    wg             = w;
    sd             = wg.sd_used;
    use_block_zero = sd.canWeUseBlockZero();
    distribution   = SlaveWorker.work.distribution;

    rd_pct         = (int) wg.readpct;
    seekpct        = (int) wg.seekpct;
    interarrival   = (long) wg.arrival;
    setXferTable(wg.getXfersizes());

    wg.suspend_fifo_use = true;
  }

  public SD_entry getSd()
  {
    return sd;
  }
  public int getDistribution()
  {
    return distribution;
  }

  /**
   * i/o is scheduled to run at 'last_start_delta' plus a new i/o specific
   * delta. If those ios complete later than anticipated we need the ability to
   * adjust this 'last_start_delta' value to go from its current 'expected'
   * delta to 'this is the real world' delta.
   */
  public void setFirstStartDelta(long l)
  {
    first_starttime = last_start_delta = l;
  }
  public long getLastStartDelta()
  {
    return last_start_delta;
  }
  public void setInterArrivalTime(long l)
  {
    interarrival = l;
  }
  public long getInterArrivalTime()
  {
    return interarrival;
  }
  public int getRdPct()
  {
    return rd_pct;
  }
  public int getSeekPct()
  {
    return seekpct;
  }
  public void setDeviceInfo(UserDeviceInfo di)
  {
    device_info = di;
  }
  public UserDeviceInfo getDeviceInfo()
  {
    if (device_info == null)
      common.failure("WorkloadInfo.getDeviceInfo(): info still null");
    return device_info;
  }
  public void setNotEOF()
  {
    wg.add_io(cmd);
  }
  public void setEOF(long ios_scheduled)
  {
    wg.seq_eof = true;

    /* If no i/o has been done the io threads will never recognize that this is */
    /* the end of the line for this WG so the run never 'ends': */
    if (ios_scheduled == 0)
      wg.sequentials_lower();
  }

  private void setXferTable(double[] d)
  {
    xf_table = new int[d.length];
    for (int i = 0; i < d.length; i++)
      xf_table[i] = (int) d[i];
  }

  public int getXferSize()
  {
    if (xf_table.length == 1)
      return xf_table[0];

    int pct = (int) (ownmath.zero_to_one() * 100);
    int cumpct = 0;
    int i;

    for (i = 0; i < xf_table.length; i+=2)
    {
      cumpct += xf_table[i+1];
      if (pct < cumpct)
        break;
    }
    int size =  xf_table[i];
    return size;
  }


  public int getAverageXferSize()
  {
    if (xf_table.length == 1)
      return xf_table[0];

    int bytes = 0;
    for (int i = 0; i < xf_table.length; i+=2)
      bytes += xf_table[i] * xf_table[i+1];
    int avg_xfersize = bytes / 100;
    avg_xfersize = avg_xfersize + 511 & ~0x1ff;

    return avg_xfersize;
  }


  /**
   * Determine whether an i/o should be read or write.
   */
  public boolean readOrWrite()
  {
    boolean rc;
    if (rd_pct == 100)
      rc = true;
    else if (rd_pct == 0)
      rc = false;
    if (ownmath.zero_to_one() * 100 < rd_pct)
      rc = true;
    else
      rc = false;
    //common.ptod("rc: " + rc);
    return rc;
  }


  /**
   * Calculate the next arrival time for the next i/o
   */
  public long calculateNextDelta()
  {
    long delta;

    /* distribution=e: */
    if (getDistribution() == 0)
    {
      delta = (long) ownmath.exponential(getInterArrivalTime());
      if (delta > 180 * 1000000)
        delta = 180 * 1000000;
    }

    /* distribution=u: */
    else if (getDistribution() == 1)
    {
      delta = (long) ownmath.uniform(0, getInterArrivalTime() * 2);
      if (delta > 1000000)
        delta = 1000000;
    }

    /* distribution=d: */
    else
      delta = getInterArrivalTime();

    //common.ptod("delta: " + delta);

    return delta;
  }

  /**
   * If the user did not give us a delta we'll calculate it for him.
   */
  public boolean scheduleIO(long    lba,
                            int     xfersize,
                            boolean read)
  {
    return scheduleIO(calculateNextDelta(), lba, xfersize, read);
  }

  /**
   * Schedule any i/o.
   * At UserClass.preIO() time it will be decided what i/o this will be.
   */
  public boolean scheduleIO()
  {
    return scheduleIO(calculateNextDelta(), 0, 0, true);
  }
  public boolean scheduleIO(long delta)
  {
    return scheduleIO(delta, 0, 0, true);
  }

  /**
   * Schedule the next i/o.
   * By asking as input the --delta-- in microseconds we assure that the user does
   * not give us an out-of-sequence starting time.
   */
  public boolean scheduleIO(long    usecs_later,
                            long    lba,
                            int     xfersize,
                            boolean read)
  {
    cmd               = new Cmd_entry();
    cmd.delta_tod     = (last_start_delta += usecs_later);
    cmd.cmd_lba       = lba;
    cmd.cmd_xfersize  = xfersize;
    cmd.cmd_wg        = wg;
    cmd.sd_ptr        = getSd();
    cmd.cmd_read_flag = read;
    cmd.cmd_hit       = false;
    cmd.cmd_rand      = true;
    cmd.jni_index     = wg.jni_index_list.get(0).jni_index;


    /* Keep track of how many ios are outstanding, so that we */
    /* later on after subtract_io() can trigger 'end of run': */
    wg.add_io(cmd);

    try
    {
      if (cmd.cmd_wg == null) common.failure("error2");
      if (cmd.cmd_wg.fifo_to_wait == null) common.failure("error1");
      cmd.cmd_wg.fifo_to_wait.waitForRoom();
      cmd.cmd_wg.fifo_to_wait.put(cmd);
    }

    catch (InterruptedException e)
    {
      //common.ptod("InterruptedException in WorkloadInfo.scheduleIO()");
      return false;
    }

    /* If this is the first i/o we need to tell WT_task about this: */
    if (first_io)
      addRuningWorkload();

    /* This is a safety valve: */
    if (SlaveJvm.isWorkloadDone())
    {
      if (calls_since_done++ > 100)
        common.failure("WorkloadInfo.scheduleIO(): continued calls beyond 'end of run'");
      else
      {
        return false;
      }
    }

    return true;
  }

  public boolean waitUntilEmpty()
  {
    cmd.cmd_wg.fifo_to_wait.waitUntilEmpty();
    return true;
  }

  public void drainQueue()
  {
    cmd.cmd_wg.fifo_to_wait.drainFifo();
  }

  /**
   * Notify the Waiter Task that this workload will be sending i/o requests.
   */
  public void setStartingWorkload()
  {
    if (!wg.suspend_fifo_use)
      common.failure("WorkloadInfo.openWorkloadFifo(): recursive call");
    wg.suspend_fifo_use = false;
    WT_task.buildFifoSearchList();
    first_io            = false;
  }

  /**
   * Notify the Waiter Task that we are adding a workload.
   * Must be called after the first command has been placed in the fifo.
   */
  public void addRuningWorkload()
  {
    // this is to get the first pending_cmd .
    // this should be done in wt_task!
    /*
    try
    {
      wg.pending_cmd = (Cmd_entry) wg.wg_to_wt.get();
    }
    catch (Exception e)
    {
      common.ptod("This 'catch' should never hit:");
      common.failure(e);
    }
    */

    setStartingWorkload();
  }
}
