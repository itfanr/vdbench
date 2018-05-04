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

import java.util.Vector;
import java.util.Random;
import java.io.Serializable;
import Utils.Format;
import Utils.Semaphore;
import Oracle.OracleMain;

/**
 * This class handles Workload Generator (WG) related functionality
 */
public class WG_entry extends VdbObject implements Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String   rd_name;

  String   wd_name;            /* Name of WD for this workload                */
  transient Slave    slave;    /* Which slave to run on                       */
  String   wg_name;            /* Concat of slave_label, wd_name and sd_name  */

  long     workload_number;    /* Sequence number/identifier for this WG      */
  private  int priority;       /* i/o selection priority                      */
                               /* (One less than the wd priority.)            */
  double   wg_iorate = 0;

  double   seekpct;            /* How often do we seek?                       */
  double   rhpct;              /* Read hit % obtained from WD                 */
  double   whpct;              /* Write hit % obtained from WD                */
  double   readpct;            /* Read% obtained from WD                      */
  double   skew;               /* Skew% obtained from WD                      */
  double   arrival;            /* From wg_set_interarrival() microseconds     */
  long     hitarea_used = -1;  /* Override from forhitarea=                   */
  int      forthreads  = -1;

  long     ts_offset;          /* tod offset for first io in WG               */

  int      xfersize;           /* Data transfer size for this WG              */
                               /* -1 means we have xfersize distribution list */
  double   xf_table[];         /* Transfersize distribution table             */
  private  long max_xfersize;  /* Highest value in xfer distribution          */

  SdStats dlt_stats = null;    /* Passed statistics from slave to master      */
  SdStats old_stats = null;    /* Only created on the slave                   */

  public SD_entry sd_used = null;     /* The SD that is used by this WG_entry */
  String   host_lun;           /* The lun name to use on this host     */

  transient Fifo wg_to_wt;     /* FIFO list from WG to WT             */

  private   Random  seek_randomizer;

  transient Cmd_entry pending_cmd;            /* CMD entry waiting to be picked in WT        */

  WG_context hcontext = new WG_context();  /* File context for hits and misses*/
  WG_context mcontext = new WG_context();

  WD_entry wd_used;

  long     ios_on_the_way = 0;
  long     ios_started = 0;       //debug only
                                  //
  /* This flag means that WG_task has reached EOF, so when io completeion */
  /* determines there are no more ios outstanding it means end-of-run */
  boolean  seq_eof = false;

  Bursts  bursts = null;

  boolean  fixed_seed = common.get_debug(common.FIXED_SEED);
  private static boolean print_seq_counts = common.get_debug(common.SEQUENTIAL_COUNTS);

  OpenFlags wd_open_flags = null;


  static   int sequential_files = 0;

  static   Object seq_lock = new Object();

  static   long length_of_vtoc = 512;

  static   long context_tbl; /* Shared memory for JNI to communicate */

  static    Object on_the_way_lock = new Object();



  /**
   * This is a DEEP clone!
   * We must make sure that after the regular shallow clone we create a new copy
   * of every single instance that is in here.
   */
  public Object clone()
  {
    try
    {
      WG_entry wg = (WG_entry) super.clone();

      if (xf_table != null)
        wg.xf_table = (double[]) xf_table.clone();

      return wg;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }



  /**
   * Count the SD's that request sequential processing.
   */
  public static int sequentials_count(Vector wg_list)
  {
    int files = 0;

    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);

      wg.seq_eof = false;
      if (wg.seekpct < 0)
        files++;
    }
    if (files == 0)
      files = -1;

    return files;
  }

  /**
   * Forcibly set the count, overriding above.
   * This is used for replay where it is the amount of replay devices
   * that determines how many 'sequential' processes we have to check.
   */
  public static void setSequentialFileCount(int count)
  {
    sequential_files = count;
  }


  /**
   * Lower the amount of sequential files that terminate after eof.
   * set to '-1' there are NO files to monitor.
   *
   * This method is called by JNI for pure sequential workloads!!!
   */
  public static void sequentials_lower()
  {
    synchronized(seq_lock)
    {
      /* If we're already done, just return:                          */
      /* This was needed to avoid an io thread from being interrupted */
      /* in the middle of this                                        */
      if (SlaveJvm.isWorkloadDone())
        return;

      /* For replay we have only ONE WG_entry. This means that */
      /* when we get here we're all done!                      */
      // That's not true; we have one WG_entry for each SD!
      if (false && SlaveJvm.isReplay())
      {
        /* We're done. Tell master about that: */
        common.ptod("Trace replay reached EOF; Workload generation completed.");
        SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_REACHED_EOF);
        return;
      }

      common.ptod("SlaveWorker.getSequentialCount(): " + SlaveWorker.getSequentialCount());
      int seq_files = SlaveWorker.getSequentialCount();

      common.plog("sequentials_lower(): " + seq_files);

      if (seq_files == -1)
        return;

      seq_files--;
      if (seq_files == 0)
      {
        if (SlaveJvm.isReplay())
          common.ptod("Trace replay reached EOF; Workload generation completed.");
        else
          common.ptod("All sequential runs reached EOF; Workload generation completed.");

        //Cmd_fifo.statistics(Vdbmain.sd_list, false);

        /* We're done. Tell master about that: */
        SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_REACHED_EOF);

        //Vdbmain.setWorkloadDone(true);
      }

      SlaveWorker.setSequentialCount(seq_files);
    }
  }


  /**
   * Increment count of generated i/o's for this Workload Generator
   */
  public void add_io(Cmd_entry cmd)
  {
    /* The rest of the stuff is only for sequential: */
    if (seekpct >= 0)
      return;

    synchronized(on_the_way_lock)
    {
      ios_on_the_way++;

      if (print_seq_counts)
        common.ptod("ios_on_the_way++: " + ios_on_the_way + " " + cmd.cmd_lba + " " + cmd.delta_tod);

      ios_started++;
    }
  }

  /**
   * Lower amount of generated i/o's.
   * If the count reaches zero, we check to see if this WG reached
   * sequential EOF, and then we lower the count of active sequential
   * file. If all sequential files are EOF we can terminate vdbench.
   */
  public void subtract_io(Cmd_entry cmd)
  {
    /* The rest of the stuff is only for sequential: */
    if (seekpct >= 0)
      return;

    synchronized(on_the_way_lock)
    {

      if (print_seq_counts)
        common.ptod("ios_on_the_way--: " + ios_on_the_way + " " + cmd.cmd_lba + " " + (cmd.delta_tod - SlaveWorker.first_tod));

      ios_on_the_way--;
      if (ios_on_the_way < 0)
      {
        common.ptod("ios_on_the_way: " + ios_on_the_way + " " + ios_started + " " +
                    this.wd_name);
        common.failure("negative i/o count");
      }

      if (ios_on_the_way == 0 && print_seq_counts)
      {
        common.ptod("Reached zero " + seq_eof + " " + Validate.isValidate());
      }

      /* sequentials_lower() is already called from JNI when it processes the */
      /* last block for normal processing.                                    */
      /* Since for Replay and DV the sequential LBA incrementing is done in   */
      /* Java we can only call it from here for REPLAY or DV:                 */
      if (ios_on_the_way == 0 && seq_eof &&
          (SlaveJvm.isReplay() || Validate.isValidate()))
      {
        if (print_seq_counts)
          common.ptod("calling seq lower for: " + this.wg_name);
        sequentials_lower();
      }
    }
  }


  /**
   * Calculate min and max xfersize for this wg
   */
  private void storeXfersizes(WD_entry wd)
  {
    int l;

    /* If we have a transfersize distribution, calculate low and high xfer: */
    max_xfersize = xfersize;
    if (xfersize == - 1)
    {
      max_xfersize = 0;
      for (l =0; l < xf_table.length; l+=2)
      {
        if (xf_table[l] == 0)
          break;
        max_xfersize = (int) Math.max(max_xfersize, xf_table[l]);
      }
    }

    SD_entry.setMaxXfersize(Math.max(SD_entry.getMaxXfersize(), max_xfersize));
  }


  /**
   * Debugging: print sizes
   */
  private static void print_sizes(WG_entry wg)
  {
    synchronized (common.ptod_lock)
    {

      common.ptod("wd=" + wg.wd_used.wd_name);
      //if (wg.hcontext.seek_width > 0)
      {
        common.ptod(Format.f("Byte addresses for hits. low: %12d", wg.hcontext.seek_low) +
                    Format.f(" high: %12d", wg.hcontext.seek_high) +
                    Format.f(" width: %12d", wg.hcontext.seek_width));
      }
      common.ptod(Format.f("Byte addresses for miss. low: %12d", wg.mcontext.seek_low) +
                  Format.f(" high: %12d", wg.mcontext.seek_high) +
                  Format.f(" width: %12d", wg.mcontext.seek_width));
      common.ptod("xfersize=" + wg.max_xfersize);
      common.ptod("hitarea_used: " + wg.hitarea_used);
      common.ptod("hitarea: "      + wg.sd_used.hitarea);

      common.ptod("");
    }

  }



  /**
   *  Set minimum and maximum lba values for hits and misses.
   */
  public void calculateContext(WD_entry wd, SD_entry sd)
  {
    if (sd.end_lba == 0)
      common.failure("Unknown lun size for sd=" + sd.sd_name);

    if (xfersize == 0)
      common.failure("No xfersize avaliable for wd=" + wd.wd_name);

    /* Determine which range= to use. WD overrides SD */
    double lowrange  = sd.lowrange;
    double highrange = sd.highrange;
    if (wd.lowrange >= 0)
    {
      lowrange  = wd.lowrange;
      highrange = wd.highrange;
    }

    if (SD_entry.isTapeTesting() && lowrange >= 0)
      common.failure("'range=' parameter not allowed for tape devices");

    /* Defaults are -1. Set to range=(0,100) if needed: */
    if (lowrange < 0)
      lowrange = 0;
    if (highrange < 0)
      highrange = 100;

    if (lowrange >= highrange)
      common.failure("'range=' parameter. Second value must be higher than first value");


    /* Calculate seek range, either % or bytes: */
    if (lowrange <= 100.0)
    {
      mcontext.seek_low  = (long) (sd.end_lba * lowrange / 100.) + hitarea_used;
      if (hitarea_used > 0)
        hcontext.seek_low  = (long) (sd.end_lba * lowrange / 100.);
    }

    if (lowrange > 100.0)
    {
      mcontext.seek_low  = (long) lowrange + hitarea_used;
      if (hitarea_used > 0)
        hcontext.seek_low  = (long) lowrange;
    }

    if (highrange <= 100.0)
    {
      mcontext.seek_high = (long) (sd.end_lba * highrange / 100);
      if (hitarea_used > 0)
        hcontext.seek_high = (long) hcontext.seek_low + hitarea_used;
    }

    if (highrange > 100.0)
    {
      if (highrange > sd.end_lba)
        common.failure("Requesting high seek range greater than lun size");
      mcontext.seek_high = (long) highrange;
      if (hitarea_used > 0)
        hcontext.seek_high = (long) hcontext.seek_low + hitarea_used;
    }


    /* Never touch sector zero! */
    if (mcontext.seek_low == 0)
      mcontext.seek_low = xfersize > 0 ? xfersize : max_xfersize;
    if (hitarea_used != 0 && hcontext.seek_low == 0)
      hcontext.seek_low = xfersize > 0 ? xfersize : max_xfersize;

    /* For a format run allow for a restart: */
    if (wd.wd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
    {
      long MB = 1024*1024l;
      if (sd.psize > 0)
      {
        mcontext.seek_low = (sd.psize / MB) * MB;
        //common.ptod("calculateContext(wd): restarting format for sd=" + sd.sd_name);
      }
    }

    /* Rounding: */
    mcontext.seek_width = mcontext.seek_high - mcontext.seek_low;
    hcontext.seek_width = hcontext.seek_high - hcontext.seek_low;
    if (mcontext.seek_width < max_xfersize)
      size_error(mcontext);

    mcontext.seek_low  = ownmath.round_lower(mcontext.seek_low,  max_xfersize);
    mcontext.seek_high = ownmath.round_lower(mcontext.seek_high, max_xfersize);

    hcontext.seek_low  = ownmath.round_lower(hcontext.seek_low,  max_xfersize);
    hcontext.seek_high = ownmath.round_lower(hcontext.seek_high, max_xfersize);

    /* Context may be adjusted with 'n' bytes.  */
    mcontext.seek_low  += sd.offset;
    hcontext.seek_low  += sd.offset;

    mcontext.seek_width = mcontext.seek_high - mcontext.seek_low;
    hcontext.seek_width = hcontext.seek_high - hcontext.seek_low;

    if (sd.offset > 0 && mcontext.seek_width < 0)
      common.failure("SD offset larger than available SD bytes");



    /* Make sure we have enough space for at least two blocks: */
    if (mcontext.seek_width < max_xfersize * 2)
    {
      if (!wd.wd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
        size_error(mcontext);
    }

    if (hitarea_used > 0 && hcontext.seek_width < max_xfersize * 2)
      size_error(hcontext);

    /* Data validation must have a large amount of blocks to prevent us from */
    /* being too slow with all or most of the blocks being busy. See DV_map  */
    /* This of course is not needed for a format (expect a format only in a  */
    /* test run) */
    if (Validate.isValidate())
    {
      if (!wd_used.wd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME) &&
          mcontext.seek_width < max_xfersize * 3000)
      {
        print_sizes(this);
        common.failure("The amount of lun size requested for cache misses must " +
                       "be at least 3000 times the data transfersize when " +
                       "using Data Validation");
      }

      if (hitarea_used != 0 && hcontext.seek_width < max_xfersize * 3000)
      {
        print_sizes(this);
        common.failure("The hitarea requested must " +
                       "be at least 3000 times the data transfersize when " +
                       "using Data Validation");
      }
    }


    /* Make sure for sequential processing that we don't touch block 0: */
    mcontext.last_lba     =
    hcontext.last_lba     =
    mcontext.max_xfersize =
    hcontext.max_xfersize = max_xfersize;

    /* Current setup:                                               */
    /* Example: 2mb size, 1mb hitarea, 512 xfersize                 */
    /*          hitarea: blocks 0-2047, missarea: blocks 2048-4095  */
    /*                                                              */
    /* Random hits go to the hitarea, blocks 0-2047                 */
    /* Random misses go to range between blocks 0-4095              */
    /* Seq hits go to the hitarea, blocks 0-2047                    */
    /* Seq misses go to the range between blocks 2048-4095          */
    /*                                                              */
    /* That means there is a little difference between where random */
    /* and sequential misses go. Look at it later!                  */


    if (common.get_debug(common.PRINT_SIZES))
      print_sizes(this);

    if (mcontext.seek_width == 0)
    {
      print_sizes(this);
      common.failure("SD size is rounded downward to a multiple of data transfer size. " +
                     "After rounding and subtracting of hitarea there is NO space left.");
    }

    if (mcontext.seek_high < mcontext.seek_low)
    {
      print_sizes(this);
      common.ptod("*\n*\n*\n*\n*");
      common.ptod("Negative seek range. Likely caused when asking for ");
      common.ptod("'rhpct=' or 'whpct=' control. In that case, the size ");
      common.ptod("of the SD (or range used) must be larger than the 'hitarea=' ");
      common.ptod("parameter used. The default for hitarea is 1MB.");
      common.failure("Negative seek range. ");
    }

    //common.ptod("mcontext.seek_high: " + mcontext.seek_high);
    //common.ptod("hcontext.seek_high: " + hcontext.seek_high);
  }



  /**
   * Set interarrival times for each Workload Generator.
   */
  static void wg_set_interarrival(Vector wg_list, RD_entry rd)
  {
    double target_rate = rd.iorate;
    WG_entry wg;
    long seed;

    /* In case any workload has an iorate specified, figure out the remainder: */
    double wg_rates = 0;
    for (int i = 0; i < wg_list.size(); i++)
      wg_rates += ((WG_entry) wg_list.elementAt(i)).wg_iorate;
    target_rate -= wg_rates;

    /* Set the individual WG target iorate and interarrival time: */
    for (int i = 0; i < wg_list.size(); i++)
    {
      wg = (WG_entry) wg_list.elementAt(i);
      double iorate = target_rate * wg.skew / 100;

      if (wg.wg_iorate == 0 && (rd.iorate == RD_entry.MAX_RATE || rd.iorate == RD_entry.CURVE_RATE))
        wg.arrival = 1;

      else if (wg.wg_iorate == 0)
        wg.arrival = (double)  (1000000 / iorate);

      else
        wg.arrival = (double)  (1000000 / wg.wg_iorate);

      FifoList.countPriorities(wg_list);

      //common.plog("++Interarrival time: wd=%s arrival=%.0f rdiorate=%.0f "+
      //            "target_rate=%.0f skew=%.2f, wgiorate=%.0f wgprio=%d",
      //            wg.wd_name, wg.arrival, rd.iorate, target_rate, wg.skew, wg.wg_iorate,
      //            wg.priority);

      /* Needed for deterministic arrival times so make sure            */
      /* that not all ios are started at the same time for all volumes: */
      wg.ts_offset = 100 * (i+1);

      /* Make sure we don't have an exact copy of i/o streams on each volume: */

      /* When we run on multiple hosts, the results of using the tod for any */
      /* kind of seed could end up to equal seeds on one or more hosts if    */
      /* the tod ends up being equal for those hosts!                        */
      if (!wg.fixed_seed)
        seed = Native.get_simple_tod() * wg.ts_offset;
      else
        seed = wg.ts_offset;
      wg.seek_randomizer = new Random(seed); // set seed
                                             //
      if (rd.bursts != null)
      {
        wg.bursts = new Bursts(rd.bursts.getList(), rd.bursts.isSpread());
        wg.bursts.createBurstList(wg.skew, rd.iorate, rd.getElapsed());
      }
    }
  }


  /**
   * Determine whether an i/o should be read or write.
   */
  public boolean wg_read_or_write()
  {

    if (readpct == 100)
      return true;
    else if (readpct == 0)
      return false;

    if (ownmath.zero_to_one() * 100 < readpct)
      return true;
    else
      return false;
  }


  /**
   * Determine whether an i/o should be random or sequential
   */
  public boolean wg_random_or_seq(Cmd_entry cmd)
  {
    /* Calculate what we should do: sequential or random seek: */
    /* Negative value means terminate after EOF */
    if (seekpct <= 0)
      return false;
    else if (seekpct == 100)
      return true;

    if (ownmath.zero_to_one() * 100 < seekpct)
      return true;
    else
      return false;
  }



  /**
   * Calculate next sequential address.
   * Needs to be synchronized to allow multiple concurrent callers true
   * sequential access.
   * (Although normal sequential lbas are determined by JNI at this time,
   * we keep doing it here for DV and for rdpct=copy.
   */
  public synchronized boolean wg_get_next_seq(Cmd_entry  cmd,
                                              WG_context context)
  {
    /* Set next sequential address for first time. */
    if (context.next_seq < 0)
      context.next_seq = context.seek_low;

    while (true)
    {

      /* If the next one crosses EOF: */
      if (context.next_seq  + context.max_xfersize > context.seek_high)
      {
        // An attempt here to allow for the processing of the last short
        // block failed because earlier context.seek_high was truncated
        // to a multiple of the xfersize!

        /* Negative seek% indicates stop after EOF: */
        if (seekpct < 0)
          return true;

        /* Start again at begin of file: */
        context.next_seq = context.seek_low;
      }

      /* Fill in the next seek address and save for next reference: */
      cmd.cmd_lba       = context.next_seq;
      context.next_seq += cmd.cmd_xfersize;

      /* Data validation after journal recovery? No, just return: */
      if (seekpct != Jnl_entry.RECOVERY_READ)
        return false;

      //common.ptod("cmd.cmd_lba1: " + cmd.cmd_lba);

      /* Get the key and see if record has changed: */
      int key = cmd.sd_ptr.getDvMap().dv_get(cmd.cmd_lba);
      if (key == DV_map.DV_ERROR)
      {
        common.ptod("Recovery validation: record already marked bad " +
                    cmd.sd_ptr.sd_name + "/" + cmd.cmd_lba);
        continue;
      }

      /* Key 0 does not get read. We force the last block to be read though */
      /* just in case someone is doing a journal recovery with ALL blocks   */
      /* having key 0!                                                      */
      else if (key == 0 && cmd.cmd_lba != (context.seek_high - cmd.cmd_xfersize))
        continue;

      //common.ptod("cmd.cmd_lba2: " + cmd.cmd_lba);

      /* Record must be read: */
      return false;
    }
  }



  /**
   * Calculate seek address within WD and then translate that to lba within SD
   * We also determine whether an operation is random or sequential
   */
  public boolean wg_calc_seek(Cmd_entry cmd)
  {
    int i;
    long blocks;
    int loop = 0;
    WG_context context;
    long align = sd_used.align;

    /* We don't ever want to access sector 0, so if the final lba for the */
    /* target device ends up being 0, we just loop back and try again:    */
    do  /* while (cmd.cmd_lba < length_of_vtoc); */
    {
      if (loop++ > 100000)
        common.failure("Loop protection in code that prevents lba=0 from being accessed. ");

      /* Set seek boundaries etc: */
      context = cmd.cmd_hit ? hcontext : mcontext;
      cmd.sd_ptr = sd_used;

      if (cmd.cmd_rand)
      {
        /* Calculate random seek address: */
        if (align == 0)
        {
          /* All i/o on block bounderies: */
          blocks      = context.seek_width / cmd.cmd_xfersize;
          double rand = seek_randomizer.nextDouble();
          blocks      = (long) (rand * blocks);
          cmd.cmd_lba = context.seek_low + blocks * cmd.cmd_xfersize;
        }

        else
        {
          /* All i/o on nnn-byte boundary (stay away from last incomplete block): */
          blocks      = (context.seek_width - cmd.cmd_xfersize) / align;
          double rand = seek_randomizer.nextDouble();
          blocks      = (long) (rand * blocks);
          cmd.cmd_lba = context.seek_low + blocks * align;
        }

        /* If we switch back to sequential, we have  to remember this lba: */
        context.next_seq = cmd.cmd_lba + cmd.cmd_xfersize;
      }
      else
      {
        /* For journal recovery we must keep track of eof: */
        if (wg_get_next_seq(cmd, context))
          return true;

        /* Sequential: lba will be ignored, but needs to have minimum value: */
        //cmd.cmd_lba = context.max_xfersize + context.seek_low;

        /* Sequential lba is determined inside of JNI code, but we      */
        /* still store an lba here for a write to a 'copy' target.      */
        /* To keep sequential writes to tape in order we mark the write */
        /* a random write in IO_task()                                  */

      }

      /* Some checking: */
      if (cmd.cmd_rand && cmd.cmd_lba + cmd.cmd_xfersize > context.seek_high)
      {
        cmd.cmd_print(sd_used, "lba:");
        print_sizes(this);
        common.failure("lba out of range for:\n" + cmd.cmd_wg.wd_name +
                       " lba: "     + cmd.cmd_lba +
                       " xf: "      + cmd.cmd_xfersize +
                       " seek_high: " + context.seek_high);
      }

      if (cmd.cmd_lba < context.seek_low)
      {
        cmd.cmd_print(sd_used, "lba:");
        common.failure("lba out of range for:\n" + cmd.cmd_wg.wd_name +
                       " lba: "    + cmd.cmd_lba +
                       " xf: "      + cmd.cmd_xfersize +
                       " seek_low: " + context.seek_low);
      }

    } while (cmd.cmd_lba < length_of_vtoc);

    /* false means no sequential eof yet: */
    return false;
  }



  /**
   * Determine whether an i/o should be a hit or a miss.
   */
  public boolean wg_hit_or_miss(Cmd_entry cmd)
  {
    double pct;

    if (cmd.cmd_read_flag)
      pct = rhpct;
    else
      pct = whpct;

    if (pct == 0)
      return false;
    else if (pct == 100)
      return true;

    if ( (ownmath.zero_to_one() * 100) < pct)
      return true;
    else
      return false;
  }



  /**
   * Calculate transfer size: fixed, or using distribution list
   */
  public int wg_dist_xfersize(Cmd_entry cmd)
  {
    int pct = (int) (ownmath.zero_to_one() * 100);
    int cumpct = 0;
    int i;

    for (i = 0; i < xf_table.length; i+=2)
    {
      cumpct += xf_table[i+1];
      if (pct < cumpct)
        break;
    }
    int size = (int)  xf_table[i];
    return size;
  }


  /**
   * Start all Workload Generator tasks.
   *
   * During the 'SlaveJvm' rewrite I concluded that I should not have more
   * than one ReplayRun thread running. With a single one there is more
   * guarantee that ios will happen in the same order than with more than
   * one.
   */
  public static void wg_start_sun_tasks(Vector wgs_for_rd)
  {
    int count = 0;

    if (common.get_debug(common.ORACLE))
    {
      OracleMain.initialize(wgs_for_rd);
      return;
    }

    /* Start a Workload Generator Task for each WG entry: */
    for (int i = 0; i < wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.elementAt(i);

      /* Replay has only ONE (dummy) WG_entry): */
      if (SlaveJvm.isReplay())
      {
        new ReplayRun(new Task_num("ReplayRun_task"), wg.sd_used).start();
        count ++;
        common.plog("Started ReplayRun thread.");

        /* We can have only ONE WG_task for replay: */
        //break;
      }

      else
      {
        new WG_task(new Task_num("WG_task"), wg).start();
        count ++;
      }
    }

    common.plog(Format.f("Started %d Workload Generator threads.", count));
  }


  /**
   * Allocate fifos.
   * Either:
   * - fifos from WG_task to WT_task and WT_task to IO_task,
   * or
   * - fifos from WG_task to IO_task
   */
  public static void alloc_fifos(Work work)
  {
    int priorities = FifoList.countPriorities(work.wgs_for_slave);

    /* Allocate one fifo per active SD: */
    for (int i = 0; i < SlaveWorker.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) SlaveWorker.sd_list.elementAt(i);

      /* When running with specific priorities, the fifo can not be too long. */
      /* With a low iops the low priority workloads can run too far ahead     */
      /* with the standard 2000 ios. Cutting it downavoids that.              */
      if (priorities == 1)
        sd.wt_to_iot = new FifoList("wt_to_iot_" + sd.sd_name, Fifo.WT_TO_IOT_SIZE, priorities);
      else
        sd.wt_to_iot = new FifoList("wt_to_iot_" + sd.sd_name, Fifo.WT_TO_IOT_SIZE_SHORT, priorities);
      sd.wt_to_iot.setThreadCount(work.getThreadsUsed(sd));
    }

    /* When we use the waiter task we need fifos: */
    if (work.use_waiter)
    {
      for (int i = 0; i < work.wgs_for_slave.size(); i++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
        wg.wg_to_wt = new Fifo("wg_to_wt_" + wg.sd_used.sd_name, Fifo.WG_TO_WT_SIZE);
      }
    }
  }


  public static void free_fifos(Work work)
  {
    for (int i = 0; i < SlaveWorker.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) SlaveWorker.sd_list.elementAt(i);
      if (sd.isActive())
        sd.wt_to_iot = null;
    }


    /* Clear all possibly existing fifo's: */
    for (int i = 0; i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      wg.wg_to_wt = null;
    }
  }

  public static void main2(String args[])
  {
    long xfersize = 4096;
    long gb = 1024 * 1024 * 1024;
    long width;
    Random seek_randomizer = new Random(0);
    int j;
    long maxseek = 0;
    long totseek = 0;
    long avg_f_seek;
    long avg_d_seek;
    long LOOP = 20;
    long SEED = 5550000;

    width = 200 * gb;
    for (int i = 0; i < 10; i++)
    {
      maxseek = 0;
      totseek = 0;
      seek_randomizer = new Random(SEED);
      for (j = 0; j < LOOP; j++)
      {
        float rand = seek_randomizer.nextFloat();
        System.out.println("float " + rand);
        long blocks = width / xfersize;
        blocks = (long) (rand * blocks);
        long seek = blocks * xfersize;
        maxseek = Math.max(maxseek, seek);
        totseek += seek;
      }
      avg_f_seek = (totseek / j);

      /*
      System.out.println("float gb: " + (width / gb) + " maxseek gb: " + (maxseek / gb) +
                         " avgseek gb: " + (avg_f_seek / gb));
      */

      maxseek = 0;
      totseek = 0;
      seek_randomizer = new Random(SEED);
      for (j = 0; j < LOOP; j++)
      {
        double rand = seek_randomizer.nextDouble();
        System.out.println("doubl " + rand);
        long blocks = width / xfersize;
        blocks = (long) (rand * blocks);
        long seek = blocks * xfersize;
        maxseek = Math.max(maxseek, seek);
        totseek += seek;
      }
      avg_d_seek = (totseek / j);

      /*
      System.out.println("doubl gb: " + (width / gb) + " maxseek gb: " + (maxseek / gb) +
                         " avgseek gb: " + (avg_d_seek / gb) +
                         Format.f(" delta: %7.2f g", (double) (avg_d_seek - avg_f_seek) / gb));
      */


      width += 50 * gb;
    }
  }




  private void size_error(WG_context context)
  {
    print_sizes(this);
    common.ptod("wd=" + wd_name +
                ": Lun space specified for either cache hits or cache misses (" +
                context.seek_width + " bytes) must be at least twice the xfersize (" +
                max_xfersize + ")");
    common.ptod("Remember that the first block in a lun is never accessed, " +
                "this to protect sector zero.");
    common.ptod("Please check the 'sd=nnn,size=xx,hitarea=' and 'wd=nnn,range=' parameters.");
    common.failure("Insufficient amount of lun size specified");
  }


  /**
   * JNI uses workload numbers to know where to store information like
   * response times and sequential next-lba.
   */
  public static void setWorkloadNumbers(Vector wg_list)
  {
    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);
      wg.workload_number = i;
    }
  }


  public static void overrideOpenflags(Vector wg_list)
  {
    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);
      if (wg.wd_open_flags != null)
        wg.sd_used.open_flags = wg.wd_open_flags;
    }
  }



  /**
   * Get parameters both from WD= and the forxxx= parameters and store them
   * in the WG_entry.
   */
  public void storeWorkloadParameters(RD_entry rd)
  {
    /* Store values requested by WD, or use overrides from RD: */
    readpct  = wd_used.readpct;
    seekpct  = wd_used.seekpct;
    xfersize = wd_used.xfersize;
    xf_table = wd_used.xf_table;
    wd_name  = wd_used.wd_name;
    rhpct    = wd_used.rhpct;
    whpct    = wd_used.whpct;

    /* For tape testing, always force seek to eof: */
    if (SD_entry.isTapeTesting() && seekpct != -1)
    {
      seekpct = -1;
      common.ptod("For tape workloads: forced 'seekpct=eof' for rd=" + rd.rd_name);
    }

    /* Copy rd=forxx overrides if needed: */
    if (rd.current_override != null)
      For_loop.useForOverrides(this, rd, rd.current_override, sd_used, false);

    /* A sequential run may be on only ONE host: */
    if (seekpct <= 0 && Host.countHostsForSD(sd_used.sd_name) > 1 &&
        !rd.rd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
      common.failure("Sequential workload for rd=" + rd.rd_name +
                     ",wd=" + wd_name +
                     ",sd=" + sd_used.sd_name + " requested \n" +
                     "to be run on more than one host concurrently. \n" +
                     "This is an invalid request since concurrent sequential \n" +
                     "workloads against the same device will produce \n" +
                     "useless performance numbers. ");

    /* Calculate low and high xfer: */
    storeXfersizes(wd_used);

    /* Replay runs get a negative seekpct to allow for EOF detection: */
    if (Vdbmain.isReplay())
      seekpct = -1;

  }


  /**
   * For slaves that did not have a JVM count specified look for the
   * total iops for that slave and adjust the JVM count if needed.
   * This is needed to prevent too many iops from going to the same JVM.
   * Limit is set to 5000 per JVM.
   *
   * Note: RD_entry.createWgListForOneRd() has already run at this time.
   * The new JVM count for that however is not needed. All WG_entry instances
   * are created there and then evenly spread over whatever amount of slaves
   * there is by then.
   * The only problem at this time is that the 'no duplicate sequential SDs'
   * message in createWgListForOneRd() will not appear since at that time there
   * is still only ONE JVM. Deal with it some time later if needed.
   */
  public static void adjustJvmCount(RD_entry rd)
  {
    /* Look at all slaves: */
    for (int i = 0; i < SlaveList.getSlaveCount(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* Calculate the total skew for this slave: */
      double total_skew = 0;
      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);
        if (wg.slave.getHost().getLabel().equals(slave.getHost().getLabel()))
          total_skew += wg.skew;
      }

      /* Figure out if this is too many iops: */
      double iorate = rd.iorate_req * total_skew / 100;
      //common.ptod(slave.getLabel() + " iorate: " + iorate + " " + total_skew + " " + rd.iorate);
      if (iorate > Vdbmain.IOS_PER_JVM)
      {
        int jvms = (int) Math.min((iorate + Vdbmain.IOS_PER_JVM - 1)
                                  / Vdbmain.IOS_PER_JVM,
                                  Vdbmain.DEFAULT_JVMS);


        /* But never more than the amount of SDs: */
        /* (If the user wants more than one JVM for an SD, he must use jvms= parameter) */
        jvms = Math.min(jvms, Vdbmain.sd_list.size());

        //common.ptod("(iorate + Vdbmain.IOS_PER_JVM - 1): " + (iorate + Vdbmain.IOS_PER_JVM - 1));
        //common.ptod("jvms: " + jvms);
        //common.ptod("slave.getHost().getJvmCount(): " + slave.getHost().getJvmCount());
        if (jvms > slave.getHost().getJvmCount())
        {
          common.plog("Adjusted default JVM count for " +
                      slave.getLabel() + " from " + slave.getHost().getJvmCount() +
                      " to " + jvms + " because of a total of " +
                      Format.f("%f iops", iorate) +
                      " and a total of " + Vdbmain.sd_list.size() + " sds.");
          slave.getHost().setJvmCount(jvms);

          Host.createSlaves();
        }
      }
    }
  }




  private static void printWgList(String rdname, Vector wg_list)
  {
    common.ptod("");
    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);

      common.ptod( wg);
    }
  }

  public String report(RD_entry rd)
  {
    String txt = Format.f("slv=%-11s ", slave.getLabel()) +
                 Format.f("rd=%-6s ",   rd_name         ) +
                 Format.f("wd=%-6s ",   wd_name         ) +
                 Format.f("sd=%-6s ",   sd_used.sd_name ) +
                 Format.f("rd=%3d ",    readpct         ) +
                 Format.f("sk=%3d ",    seekpct         ) +
                 Format.f("sw=%5.2f ",  skew            ) +
                 Format.f("xf=%7d ",    xfersize        ) +
                 Format.f("rh=%3d ",    rhpct           ) +
                 Format.f("hi=%d ",     hitarea_used    ) +
                 Format.f("th=%d ",     rd.getThreadsUsedForSlave(sd_used, slave));

    return txt;
  }

  public static void main(String[] args)
  {
    for (int i = 0; i < 20; i++)
    {
      common.ptod("num: " + ownmath.zero_to_one() * 100);
    }
  }

  /**
   * Check for a valid priority value.
   * Note the 'trick': The default priority is MAX_VALUE.
   * FifoList.countPriorities() resets it to the next available priority.
   */
  public boolean hasPriority()
  {
    return priority != Integer.MAX_VALUE;
  }
  public void setPriority(int p)
  {
    priority = p;
  }
  public int getpriority()
  {
    return priority;
  }

}


