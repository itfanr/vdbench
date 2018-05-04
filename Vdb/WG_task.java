package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

import User.WorkloadInfo;

import Utils.Format;


/**
 * This is the Workload Generator task
 */
public class WG_task extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private WG_entry wg;
  private int      distribution;
  private Task_num tn;

  private Cmd_entry cmd;
  private double start_ts;
  private boolean read_only;
  private boolean use_waiter;
  private boolean sd_concatenation;

  private boolean user_generate_needed = false;
  private boolean shared_threads       = false;

  private ThreadMonitor tmonitor = null;

  private DV_map  dv_map = null;

  private HashMap   <Long, Byte> pending_data_map = null;
  private ArrayList <Long>       pending_lbas     = null;

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

    /* These fields assist with the binary search within concatenated SDs: */
    wg.lba_search_key = new SD_entry();
    wg.search_method  = new ConcatLbaSearch();

    /* With concatenation we can ask all threads to be shared for all SDs: */
    shared_threads = SlaveWorker.sharedThreads();


    setName("WG_task " + wg.wg_name);

    /* Until 503 we refused to touch block zero to perevent messing up           */
    /* a vtoc. With the arrival of Dedup and compression that can mess up        */
    /* the results when using smaller tests.                                     */
    /* We will no longer follow this rule for anything that is not a raw device. */
    /* For Unix anything starting with /dev is considered a raw device,          */
    /* For Windows, anything starting with \\.                                   */
    wg.access_block_zero = wg.sd_used.canWeUseBlockZero();

    /* Paranoia check: */
    if (common.get_debug(common.FAST_JOURNAL_CHECK))
      common.failure("debug=110 no longer supported");

    /* KeyMap is needed for Dedup and DV: */
    if (Validate.isRealValidate())
    {
      dv_map     = wg.sd_used.dv_map;
      wg.key_map = new KeyMap(0l, wg.sd_used.getKeyBlockSize(), wg.sd_used.getMaxSdXfersize());
    }

    /* Setup UserClass API: */
    if (wg.user_class_parms != null)
      setupUserClass();

    /* Setup hotbands if requested: */
    if (wg.hotband_used)
      wg.setupHotBand();

    if (Validate.isJournalRecoveryActive() && dv_map.journal.before_map.pending_map != null)
    {
      pending_data_map = dv_map.journal.before_map.pending_map;
      pending_lbas     = new ArrayList(pending_data_map.keySet());

      /* It looks better if we read these in sequence: */
      Collections.sort(pending_lbas);
    }
  }



  /**
   * Workload Generator task.
   * This task creates all the i/o streams and sends them to the Waiter task
   */
  public void run()
  {
    int priorities  = FifoList.countPriorities(SlaveWorker.work.wgs_for_slave);

    sd_concatenation= Validate.sdConcatenation();

    //common.ptod("wg: " + wg.wd_name + " " +  wg.sd_used.sd_name + " " + wg.arrival);
    try
    {
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);


      tmonitor = new ThreadMonitor("WG_task", wg.wd_name, wg.sd_used.sd_name);
      tn.task_set_start_complete();
      tn.waitForMasterGo();

      //common.ptod("Starting WG_task for " + wg.sd_used.sd_name);

      /* Set first start time: */
      start_ts = calculateFirstStartTime();

      /* All UserClass workload generation is done here: */
      if (user_generate_needed)
      {
        if (wg.user_class.generate())
          sendEOF();

        tn.task_set_terminating(0);
        //common.ptod("Ending WG_task 1 for " + wg.sd_used.sd_name);
        return;
      }

      if (wg.bursts != null)
        start_ts = wg.bursts.getArrivalTime(start_ts, distribution);

      /* First new cmd entry: */
      cmd = new Cmd_entry();

      //common.ptod("ios_on_the_way: " + wg.ios_on_the_way);

      while (true)
      {
        /* During Journal recover read the pending blocks first: */
        if (pending_lbas != null && pending_lbas.size() > 0)
        {
          //common.ptod("pending_lbas.size(): " + pending_lbas.size());
          createPendingRead();
        }

        else
        {
          if (!createNormalIO())
          {
            sendEOF();
            break;
          }
        }

        /* Keep track of number of generated ios to handle EOF later: */
        wg.add_io(cmd);

        /* Put this io in the proper fifo. If the ultimate target fifo */
        /* is close to being full, wait for it to free up some space.  */
        /* This greatly eliminates context switches.                   */
        try
        {
          if (use_waiter)
            wg.fifo_to_wait.waitAndPut(cmd);

          else
            cmd.sd_ptr.fifo_to_iot.waitAndPut(cmd, wg.getpriority());
          tmonitor.add1();
        }
        catch (InterruptedException e)
        {
          //common.plog("WG Task interrupted 2");
          break;
        }

        /* Calculate the arrival time for the next i/o: */
        start_ts = calculateNextStartTime(start_ts);

        /* Next new cmd entry: */
        cmd = new Cmd_entry();
      }

      /* Send an EOF cmd entry: */
      sendEOF();
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

    tn.task_set_terminating(0);
    //common.ptod("Ending WG_task 2 for " + wg.sd_used.sd_name);
  }


  /**
   * The first start time must be properly set.
   *
   * Add an extra 100ms to avoid (when using dist=d) to always get the start of
   * an i/o exactly or too close to the Reporter waking up at the same time.
   */
  private double calculateFirstStartTime()
  {
    double start;
    if (distribution == 0)
      start = ownmath.exponential(wg.arrival);
    else if (distribution == 1)
      start = ownmath.uniform(0, wg.arrival * 2);
    else
      start = wg.ts_offset + 100000;

    return start;
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
    cmd.cmd_wg      = wg;
    cmd.jni_index   = wg.jni_index_list.get(0).jni_index;

    /* Set read or write: */
    cmd.cmd_read_flag = read_only ? true : wg.wg_read_or_write();

    /* Set relative starting time: */
    cmd.delta_tod = (long) start_ts;

    /* Set transfer size: */
    cmd.cmd_xfersize = wg.wg_dist_xfersize(wg.getXfersizes(), wg.wd_name);

    /* Hit or Miss? */
    cmd.cmd_hit = wg.wg_hit_or_miss(cmd);

    /* What type of i/o? */
    cmd.cmd_rand = wg.wg_random_or_seq(cmd);

    /* Calculate seek address, sequential EOF means STOP: */
    /* (For sequential the next lba is determined in JNI code) */
    if (wg.calculateNextLba(cmd))
    {
      /* Sequential EOF, write record to show eof: */
      wg.seq_eof = true;
      common.ptod("Reached eof in wg_task");

      // To force/debug duplicate calls:
      //common.where();
      //if (wg.sd_used.sd_name.equals("sd1"))
      //  wg.sequentials_lower();
      //common.where();

      /* If we reach EOF here while all outstanding ios have completed */
      /* then we must lower the 'sequential luns active' count:        */
      if (wg.ios_on_the_way == 0 &&
          (ReplayInfo.isReplay() || Validate.isValidate()))
      {
        common.ptod("calling seq lower for2: " + wg.wg_name);
        wg.sequentials_lower();
      }

      return false;
    }

    /* Did an interrupt show up? */
    if (Thread.interrupted())
      return false;

    return true;
  }


  /**
   * Create a read request for a block whose write was pending at the end of a
   * journal creation.
   */
  private void createPendingRead()
  {
    cmd.cmd_wg          = wg;
    cmd.jni_index       = wg.jni_index_list.get(0).jni_index;
    cmd.cmd_read_flag   = true;
    cmd.cmd_hit         = false;
    cmd.delta_tod       = (long) start_ts;
    cmd.cmd_rand        = true;
    cmd.sd_ptr          = wg.sd_used;
    cmd.type_of_dv_read = Validate.FLAG_PENDING_READ;

    /* Set transfer size to key block size: */
    cmd.cmd_xfersize = dv_map.getKeyBlockSize();

    /* Get the very first lba from the list and remove it immediately: */
    cmd.cmd_lba = pending_lbas.get(0);
    pending_lbas.remove(0);

    /* Mask the block busy. It may NOT already be in error: */
    if (dv_map.getKeyAndSetBusy(cmd.cmd_lba) == DV_map.DV_ERROR)
      common.failure("Invalid journal recovery pending status");

    ErrorLog.plog("Requesting read of pending key block at lba 0x%08x key 0x%02x flag 0x%02x",
                  cmd.cmd_lba, dv_map.dv_get(cmd.cmd_lba) & 0x7f,
                  pending_data_map.get(cmd.cmd_lba));
    common.ptod("Requesting read of pending key block at lba 0x%08x key 0x%02x flag 0x%02x",
                cmd.cmd_lba, dv_map.dv_get(cmd.cmd_lba) & 0x7f,
                pending_data_map.get(cmd.cmd_lba));
  }


  private void sendEOF()
  {
    /* Send an EOF cmd entry: */
    try
    {
      Cmd_entry cmd = new Cmd_entry();
      cmd.sd_ptr    = wg.sd_used;
      cmd.delta_tod = Long.MAX_VALUE;
      if (use_waiter)
      {
        if (wg.fifo_to_wait.getPutCount() == 0)
          common.failure("Sending EOF to work fifo for sd=%s without any work "+
                         "ever having been scheduled", wg.sd_used.sd_name);
        wg.fifo_to_wait.waitAndPut(cmd);
      }

      else
        cmd.sd_ptr.fifo_to_iot.waitAndPut(cmd, wg.getpriority());
    }
    catch (InterruptedException e)
    {
      //common.plog("WG Task interrupted 3");
    }
  }


  /**
   * Set up processing to be done for all UserClass instances.
   */
  private void setupUserClass()
  {
    /* Each UserClass gets his own WorkLoadInfo, containing information */
    /* that is obtained from Workload Definitions (WDs) */
    WorkloadInfo wi = new WorkloadInfo(wg);

    /* A 'first start time' needs to be set to prevent all i/o from */
    /* starting at the same time: */
    wi.setFirstStartDelta((long) calculateFirstStartTime());

    /* Create UserClass instances that generate the workloads: */
    wg.user_class = User.ControlUsers.createInstance(wg);
    wg.user_class.setWorkloadInfo(wi);
    wi.setDeviceInfo(wg.user_class.initialize(wi));
    user_generate_needed = wg.user_class.isGenerateNeeded();
  }
}


