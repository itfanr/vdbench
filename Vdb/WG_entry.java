package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import Utils.Format;

/**
 * This class handles Workload Generator (WG) related functionality
 */
public class WG_entry implements Serializable, Cloneable, Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public String   wd_name;     /* Name of WD for this workload                */
  private transient Slave    slave;    /* Which slave to run on               */
  String   wg_name;            /* Concat of slave_label, wd_name and sd_name  */

  private  int priority;       /* i/o selection priority                      */
                               /* (One less than the wd priority.)            */
  public double   wg_iorate = 0;

  public double   poisson_midpoint = 0;
  public double   seekpct;            /* How often do we seek?                       */
  public long     stride_min = -1;    /* Minimum to add when generating a random lba */
  public long     stride_max = -1;    /* Maximum for same.                           */


  public double   rhpct;              /* Read hit % obtained from WD            */
  public double   whpct;              /* Write hit % obtained from WD           */
  public double   readpct;            /* Read% obtained from WD                 */
  public double   skew;               /* Skew% obtained from WD                 */
  public double   arrival;            /* From wg_set_interarrival() microseconds*/
  long     hitarea_used = -1;  /* Override from forhitarea=                   */

  long     ts_offset;          /* tod offset for first io in WG               */

  private double   xf_table[];  /* Transfersize distribution table             */

  public boolean hotband_used = false;

  public ArrayList <JniIndex> jni_index_list = null;

  public WD_entry wd_used;
  public SD_entry sd_used = null;     /* The SD that is used by this WG_entry */
                                      /* It can either be a real SD, or a     */
                                      /* concatenated SD.                     */

  /* This contains a copy of the list found in the concatenated SD's SD_entry. */
  /* This list MUST be in the order specified by the user in sd=(x,y,z)        */
  public ArrayList <SD_entry> sds_in_concatenation = null;

  boolean  access_block_zero = false;

  /* Fifo from WG_task to WT_task when WT_task is used: */
  public transient Fifo     fifo_to_wait;


  public boolean   suspend_fifo_use = false;

  private   Random  seek_randomizer;

  public transient Cmd_entry pending_cmd; /* CMD entry waiting to be picked in WT */

  WG_context hcontext = new WG_context();  /* File context for hits and misses*/
  WG_context mcontext = new WG_context();


  long     ios_on_the_way = 0;
  long     ios_started = 0;       //debug only
                                  //
  /* This flag means that WG_task has reached EOF, so when io completeion */
  /* determines there are no more ios outstanding it means end-of-run */
  public boolean  seq_eof = false;

  Bursts  bursts = null;

  boolean  fixed_seed = common.get_debug(common.FIXED_SEED);

  OpenFlags wd_open_flags = null;

  public String[]       user_class_parms = null;
  public User.UserClass user_class       = null;


  public SD_entry        lba_search_key = null;
  public ConcatLbaSearch search_method  = null;


  /* Used for Dedup and/or Data Validation: */
  public KeyMap key_map;

  private Object on_the_way_lock;


  private static   int sequential_files = 0;
  private static   Object seq_lock = new Object();
  private int seqno = seq++;



  private static boolean print_seq_counts = common.get_debug(common.SEQUENTIAL_COUNTS);
  private static int seq = 1;

  public WG_entry()
  {
  }


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

      if (xf_table != null) wg.xf_table = (double[]) xf_table.clone();
      wg.hcontext = (WG_context) hcontext.clone();
      wg.mcontext = (WG_context) mcontext.clone();
      seqno       = seq++;

      return wg;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  public WG_entry initialize(RD_entry rd, SD_entry sd, WD_entry wd, Host current_host)
  {
    sds_in_concatenation = sd.sds_in_concatenation;
    sd_used              = sd;
    access_block_zero    = sd.canWeUseBlockZero();
    wd_used              = wd;

    wg_iorate            = wd.wd_iorate;
    wd_name              = wd.wd_name;
    wd_open_flags        = wd.open_flags;
    user_class_parms     = wd.user_class_parms;
    hotband_used         = wd.hotband_used;

    setPriority(wd.priority == Integer.MAX_VALUE ? wd.priority : wd.priority - 1);
    storeWorkloadParameters(rd);
    rd.willWeUseWaiterForWG();
    sd.trackSdXfersizes(getXfersizes());

    String lun   = current_host.getLunNameForSd(sd);
    String forxx = rd.current_override.getText();
    forxx = (forxx != null) ? " 'forxx: " + forxx.trim() : "";
    wg_name  = "rd=" + rd.rd_name + ",wd=" + wd.wd_name + ",sd=" + sd.sd_name +
               ",lun=" + lun + forxx +
               "' " + ((seekpct <=0) ? "seq" : "rnd");
    return this;
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
      {
        //common.ptod("wg.wd_used.stream_count: " + wg.wd_used.stream_count);
        //if (wg.wd_used.stream_count == 0)
        files ++;
        //else
        //  files += wg.wd_used.stream_count;
      }
    }
    if (files == 0)
      files = -1;

    common.ptod("files: " + files);
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
   * When set to '-1' there are NO files to monitor.
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
      //if (false && ReplayInfo.isReplay())
      //{
      //  /* We're done. Tell master about that: */
      //  common.ptod("Trace replay reached EOF; Workload generation completed.");
      //  SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_REACHED_EOF);
      //  return;
      //}

      int seq_files = SlaveWorker.getSequentialCount();
      //common.plog("sequentials_lower(): %d ", seq_files);
      //common.plog("sequentials_lower() for %s: %d ", cmd.sd_ptr.sd_name, seq_files);

      if (seq_files == -1)
        return;

      seq_files--;
      if (seq_files == 0)
      {
        if (ReplayInfo.isReplay())
          common.ptod("Trace replay reached EOF; Workload generation completed.");
        else
          common.ptod("All sequential runs reached EOF; Workload generation completed.");

        //Cmd_fifo.statistics(Vdbmain.sd_list, false);

        /* We're done. Tell master about that: */
        SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_REACHED_EOF);

        //Vdbmain.setWorkloadDone(true);
      }

      // Workaround for bug when for some reason we get stuck with one seq device.
      //if (false) // (SlaveJvm.isReplay())
      //{
      //  long outstanding = 0;
      //  for (int i = 0; i < SlaveWorker.work.wgs_for_slave.size(); i++)
      //  {
      //    WG_entry wg  = (WG_entry) SlaveWorker.work.wgs_for_slave.elementAt(i);
      //    outstanding += wg.ios_on_the_way;
      //  }
      //
      //  if (seq_files < 3 && outstanding < 100)
      //  {
      //    String txt = "Due to a bug in the Replay code we may have terminated early: " + outstanding;
      //    SlaveJvm.sendMessageToConsole(txt);
      //    SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_REACHED_EOF);
      //    seq_files = 0;
      //  }
      //}

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
        common.ptod("ios_on_the_way++: %-8s %4d %12d %8d ", cmd.sd_ptr.sd_name,
                    ios_on_the_way, cmd.cmd_lba, cmd.delta_tod);

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
        common.ptod("ios_on_the_way--: %-8s %4d %12d %8d ", cmd.sd_ptr.sd_name,
                    ios_on_the_way, cmd.cmd_lba, cmd.delta_tod);

      ios_on_the_way--;
      if (ios_on_the_way < 0)
      {
        common.ptod("ios_on_the_way-1: %-8s %4d %12d delta: %8d ", cmd.sd_ptr.sd_name,
                    ios_on_the_way, cmd.cmd_lba, cmd.delta_tod);
        common.failure("negative i/o count");
      }

      if (ios_on_the_way == 0 && print_seq_counts)
      {
        common.ptod("Reached zero " + seq_eof + " " + Validate.isValidate());
        common.ptod("SlaveJvm.isReplay():      " + ReplayInfo.isReplay());
        common.ptod("Validate.isValidate():    " + Validate.isValidate());
        common.ptod("Validate.isCompression(): " + Validate.isCompression());
        common.ptod("seq_eof:                  " + seq_eof);
      }

      /* We reached this because we had seek=eof and no more i/o to do: */
      if (ios_on_the_way == 0 && seq_eof)
      {
        if (print_seq_counts)
          common.ptod("calling seq lower for1: " + this.wg_name);
        sequentials_lower();
      }
    }
  }


  /**
   * Debugging: print sizes
   */
  public void print_sizes()
  {
    synchronized (common.ptod_lock)
    {

      common.ptod("wd=" + wd_used.wd_name);
      //if (hcontext.seek_width > 0)
      {
        common.ptod("Byte addresses for hits: Low: %,16d; High: %,16d; Width: %,16d",
                    hcontext.seek_low, hcontext.seek_high,hcontext.seek_width);
      }
      common.ptod("Byte addresses for miss: Low: %,16d; High: %,16d; Width: %,16d",
                  mcontext.seek_low, mcontext.seek_high,mcontext.seek_width);

      common.ptod("sd max xfersize=" + sd_used.getMaxSdXfersize());
      common.ptod("hitarea_used: " + hitarea_used);
      common.ptod("hitarea: "      + sd_used.hitarea);

      common.ptod("");
    }

  }



  /**
   *  Set minimum and maximum lba values for hits and misses.
   */
  public void calculateContext(RD_entry rd, WD_entry wd, SD_entry sd)
  {
    int max_xfersize = sd.getMaxSdXfersize();
    if (sd.end_lba == 0)
      common.failure("Unknown lun size for sd=" + sd.sd_name);

    /* Determine which range= to use. WD overrides SD */
    double tmp_low  = sd.lowrange;
    double tmp_high = sd.highrange;
    if (wd.lowrange >= 0)
    {
      tmp_low  = wd.lowrange;
      tmp_high = wd.highrange;
    }
    //common.ptod("tmp_low: %s %8.2f %8.2f", sd.sd_name, tmp_low, tmp_high);

    /* Defaults are -1. Set to range=(0,100) if needed: */
    if (tmp_low < 0)
      tmp_low = 0;
    if (tmp_high < 0)
      tmp_high = 100;

    if (tmp_low >= tmp_high)
      common.failure("'range=' parameter. Second value must be higher than first value");

    if (tmp_low < 100 && tmp_high < 200 && tmp_high > 100 && tmp_high - tmp_low >= 100)
      common.failure("range=(%.0f,%.0f) may not have a total range of 100%% or more",
                     tmp_low, tmp_high);

    /* Calculate seek range, either % or bytes: */
    if (tmp_low <= 100.0)
    {
      mcontext.seek_low  = (long) (sd.end_lba * tmp_low / 100.) + hitarea_used;
      if (hitarea_used > 0)
        hcontext.seek_low  = (long) (sd.end_lba * tmp_low / 100.);
    }

    if (tmp_low > 100.0)
    {
      mcontext.seek_low  = (long) tmp_low + hitarea_used;
      if (hitarea_used > 0)
        hcontext.seek_low  = (long) tmp_low;
    }

    if (tmp_high <= 200.)
    {
      if (tmp_high > 100)
      {
        if (seekpct < 0)
        {
          common.ptod("Greater than 100 value in range=(%.0f,%.0f) ignored when "+
                      "using seekpct=eof", tmp_low, tmp_high);
          tmp_high = 100;
          common.ptod("Reset to range=(%.0f,%.0f)", tmp_low, tmp_high);
        }
        else
          mcontext.crossing_range_boundary = true;
      }
      mcontext.seek_high = (long) (sd.end_lba * tmp_high / 100);
      if (hitarea_used > 0)
        hcontext.seek_high = (long) hcontext.seek_low + hitarea_used;
    }

    if (tmp_high > 200.)
    {
      if (tmp_high > sd.end_lba)
        common.failure("Requesting high seek range greater than lun size");
      mcontext.seek_high = (long) tmp_high;
      if (hitarea_used > 0)
        hcontext.seek_high = (long) hcontext.seek_low + hitarea_used;
    }


    /* Never touch sector zero on raw devices! */
    if (!sd.canWeUseBlockZero())
    {
      if (mcontext.seek_low == 0)
        mcontext.seek_low = max_xfersize;
      if (hitarea_used != 0 && hcontext.seek_low == 0)
        hcontext.seek_low = max_xfersize;
    }

    /* For a format run allow for a restart, but not with format_sds=yes: */
    if (!MiscParms.format_sds && wd.wd_name.startsWith(SD_entry.SD_FORMAT_NAME))
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

    /* Truncation of seek_high has been removed. If I recall correctly this    */
    /* was only in place to get around boundary cases, trying to access beyond */
    /* EOF. Those problems by now should have all been resolved.               */

    /* Reinstated due to problems in multi_io in JNI with lun sizes that are   */
    /* not a multiple of the xfersize.                                         */
    /* It may cause some problems with ranges beyond 100%, but we'll see.      */
    /* It will definitely cause problems when pre-formatting strange file sizes*/

    /* As a workaround to still allow files to be created with any size we now */
    /* skip the truncation for seekpct=eof. This means that yes, the file will */
    /* be created the proper size, but when it is then being used in any kind  */
    /* of workload it is still truncated, preventing the last bytes from ever  */
    /* being used.                                                             */

    /* Note that the real problem lies in the multi_io JNI code, where the     */
    /* check for EOF is done at the wrong place, too early in the code, and    */
    /* that the 'short block size' generated in wg_get_next_seq() may not be   */
    /* used in the proper order because of the async behavior of multi_io.     */
    /* This must be fixed at some point in time.                               */

    /* Note: why create file sizes of 'any' size if they can't be used?        */
    /* I'd be just lying to the user.                                          */
    /* So, an other flipflop, I could be a politician.                         */
    /* Maybe fix the REAL problem some day? Politicians NEVER do that though!  */

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
      if (!wd.wd_name.startsWith(SD_entry.SD_FORMAT_NAME))
        size_error(mcontext);
    }

    if (hitarea_used > 0 && hcontext.seek_width < max_xfersize * 2)
      size_error(hcontext);


    /* Data validation must have a large amount of blocks to prevent us from */
    /* being too slow with all or most of the blocks being busy. See DV_map  */
    /* This of course is not needed for a format (expect a format only in a  */
    /* test run)                                                             */
    /* Starting 503 this was resolved by lowering the fifo length.           */
    /* 50404 this went back to 2000, plus a multiplication of 1.5            */

    /* Note that a too large thread count can still cause too many blocks    */
    /* to be busy, though at this point in the life of the code I do not     */
    /* know what that thread count will be.                                  */
    if (Validate.isRealValidate() && !wd_used.wd_name.startsWith(SD_entry.SD_FORMAT_NAME))
    {
      int    threads_used   = rd.getThreadsFromSdOrRd(sd);
      int    fifo_size      = Fifo.getSizeNeeded(0);
      double multiply       = (rd.use_waiter) ? 2.0 : 1.0;
      int    fudge          = 256;
      long   blocks_needed  = (long) (fifo_size * multiply);
      blocks_needed        += fudge;
      blocks_needed        += threads_used;
      long   hit_blocks     = hcontext.seek_width / max_xfersize;
      long   miss_blocks    = mcontext.seek_width / max_xfersize;

      if (miss_blocks < blocks_needed)
      {
        print_sizes();
        BoxPrint box = new BoxPrint();
        box.add("");
        box.add("Data Validation has some extra SD size requirements. ");
        box.add("The internal queue depth of Vdbench is %d for an uncontrolled maximum i/o rate, ", fifo_size);
        box.add("and twice that, %d, for all other workloads." , fifo_size * 2);
        box.add("To that is added the number of threads requested (%d), ", threads_used);
        box.add("plus a fudge factor of %d, all for a total of %d.", fudge, blocks_needed);
        box.add("");
        box.add("The SD size (or range) must be large enough to accomodate %d times the maximum xfersize=%d value,",
                blocks_needed, max_xfersize);
        box.add("for a total of %,d bytes (%s), but we only have %,d bytes (%s).",
                (blocks_needed * max_xfersize),
                FileAnchor.whatSize((blocks_needed * max_xfersize)),
                mcontext.seek_width, FileAnchor.whatSize(mcontext.seek_width));
        box.add("");
        box.add("");
        box.add("Why all this? To ensure Data Validation integrity each xfersize= block may have only ONE ");
        box.add("read or write outstanding at the same time. Each block internally is therefore");
        box.add("marked 'busy' with a dead-lock possible if ALL blocks are busy");
        box.add("");
        box.add("You may override the internal queue depth with 'misc=(fifo=nnn)', ");
        box.add("though lower queue depth may throttle throughput at high i/o rates.");
        box.add("");
        box.print();
        common.failure("SD size for cache misses not large enough.");
      }


      if (hcontext.seek_width > 0 && hit_blocks < blocks_needed)
      {
        print_sizes();
        BoxPrint box = new BoxPrint();
        box.add("");
        box.add("Data Validation has some extra SD size requirements. ");
        box.add("The internal queue depth of Vdbench is %d for an uncontrolled maximum i/o rate, ", fifo_size);
        box.add("and twice that, %d, for all other workloads." , fifo_size * 2);
        box.add("To that is added the number of threads requested (%d), ", threads_used);
        box.add("plus a fudge factor of %d, all for a total of %d.", fudge, blocks_needed);
        box.add("");
        box.add("The hitarea must be large enough to accomodate %d times the maximum xfersize=%d value,",
                blocks_needed, max_xfersize);
        box.add("for a total of %,d bytes (%s), but we only have %,d bytes (%s).",
                (blocks_needed * max_xfersize),
                FileAnchor.whatSize((blocks_needed * max_xfersize)),
                hcontext.seek_width, FileAnchor.whatSize(hcontext.seek_width));
        box.add("");
        box.add("");
        box.add("Why all this? To ensure Data Validation integrity each xfersize= block may have only ONE ");
        box.add("read or write outstanding at the same time. Each block internally is therefore");
        box.add("marked 'busy' with a dead-lock possible if ALL blocks are busy");
        box.add("");
        box.add("You may override the internal queue depth with 'misc=(fifo=nnn)', ");
        box.add("though lower queue depth may throttle throughput at high i/o rates.");
        box.add("");
        box.print();
        common.failure("SD size for cache hits not large enough");
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
      print_sizes();

    if (mcontext.seek_width == 0)
    {
      print_sizes();
      common.failure("SD size is rounded downward to a multiple of data transfer size. " +
                     "After rounding and subtracting of hitarea there is NO space left.");
    }

    if (mcontext.seek_high < mcontext.seek_low)
    {
      print_sizes();
      common.ptod("*\n*\n*\n*\n*");
      common.ptod("Negative seek range. Likely caused when asking for ");
      common.ptod("'rhpct=' or 'whpct=' control. In that case, the size ");
      common.ptod("of the SD (or range used) must be larger than the 'hitarea=' ");
      common.ptod("parameter used. The default for hitarea is 1MB.");
      common.failure("Negative seek range. ");
    }

    //common.ptod("mcontext.seek_low: " + mcontext.seek_low);
    //common.ptod("hcontext.seek_low: " + hcontext.seek_low);
  }



  /**
   * Set interarrival times for each Workload Generator.
   */
  static void wg_set_interarrival(ArrayList <WG_entry> wg_list, RD_entry rd)
  {
    double target_rate = rd.iorate;
    WG_entry wg;
    long seed;

    /* In case any workload has an iorate specified, figure out the remainder: */
    double wg_rates = 0;
    for (int i = 0; i < wg_list.size(); i++)
      wg_rates += ((WG_entry) wg_list.get(i)).wg_iorate;
    target_rate -= wg_rates;

    /* Depending int the coded wg_iorate (from wd=xxx,iorate=nn) we can get negative: */
    target_rate = Math.max(0, target_rate);

    /* Set the individual WG target iorate and interarrival time: */
    for (int i = 0; i < wg_list.size(); i++)
    {
      wg = (WG_entry) wg_list.get(i);
      double iorate = target_rate * wg.skew / 100;

      //if (wg.wg_iorate == 0 && (rd.iorate == RD_entry.MAX_RATE || rd.iorate == RD_entry.CURVE_RATE))
      //  wg.arrival = 1;

      /* else */ if (wg.wg_iorate == 0)
      {
        wg.arrival = (double)  (1000000 / iorate);
      }

      else
        wg.arrival = (double)  (1000000 / wg.wg_iorate);

      FifoList.countPriorities(wg_list);

      //common.ptod("++Interarrival time: wd=%s arrival=%7.0f rdiorate=%.0f "+
      //            "target_rate=%.0f skew=%.2f, wgiorate=%7.0f wgprio=%d iorate=%.0f",
      //            wg.wd_name, wg.arrival, rd.iorate, target_rate, wg.skew, wg.wg_iorate,
      //            wg.priority, iorate);

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
        wg.bursts.createBurstList(wg.skew, rd);
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
   * we keep doing it here for those cases where we do not use multi_io, like DV.
   * See also IO_task.initialize().
   *
   * I am wondering about the need for synchronized, but need more research.
   */
  public synchronized boolean wg_get_next_seq(Cmd_entry  cmd,
                                              WG_context context)
  {
    /* Set next sequential address for first time. */
    if (context.next_seq < 0)
    {
      if (common.get_debug(common.FIRST_SEQ_SEEK_RANDOM))
      {
        blockBoundarySeek(context, cmd);
        context.next_seq = cmd.cmd_lba;
      }
      else
        context.next_seq = context.seek_low;
    }

    while (true)
    {
      /* Allow a short block to close off the lun size: */
      if (context.next_seq + cmd.cmd_xfersize > context.seek_high)
      {
        long bytes = context.seek_high - context.next_seq;
        if (bytes != 0)
          cmd.cmd_xfersize = bytes;
      }

      /* If the next one crosses EOF: */
      if (context.next_seq + cmd.cmd_xfersize > context.seek_high)
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

      /* Record must be read: */
      return false;
    }
  }



  /**
   * Calculate seek address within WD and then translate that to lba within SD
   * We also determine whether an operation is random or sequential
   */
  private long blocks_returned = 0;
  private Signal seek_eof_dv_read = new Signal(15);
  public boolean calculateNextLba(Cmd_entry cmd)
  {
    /* The user may now ant to re-read the whole disk.         */
    /* Send at least ONE read, making coding this much easier. */
    if (Validate.isJournalRecoveryActive())
    {
      //Validate.setSkipRead();
      if (Validate.skipRead() && blocks_returned > 0)
      {
        /* This message may be confusing getting it fopr each WG_entry ... */
        Vector txt = BoxPrint.getOne("Re-reading of sd=%s after journal "+
                                     "recovery skipped upon user's request "+
                                     "after the first block.", sd_used.sd_name);
        SlaveJvm.sendMessageToConsole(txt);
        return true;
      }
    }

    /* We don't ever want to access sector 0, so if the final lba for the */
    /* target device ends up being 0, we just loop back and try again:    */
    long loop    = 0;
    long reason1 = 0;
    long reason2 = 0;
    long reason3 = 0;
    long reason4 = 0;
    long reason5 = 0;
    while (true)
    {
      if (loop++ > 100000)
      {
        common.ptod("reason1 block0:         %,8d", reason1);
        common.ptod("reason2 block in error: %,8d", reason2);
        common.ptod("reason3 read skipped:   %,8d", reason3);
        common.ptod("reason4 block in error: %,8d", reason4);
        common.ptod("reason5 busy failed:    %,8d", reason5);
        common.failure("Loop protection in code that prevents lba=0 from being accessed. ");
      }

      /* Set seek boundaries etc: */
      WG_context context = cmd.cmd_hit ? hcontext : mcontext;
      cmd.sd_ptr = sd_used;
      if (sd_used.concatenated_sd)
        cmd.concat_sd = sd_used;


      /* Sequential i/o: */
      if (!cmd.cmd_rand)
      {
        /* 'true' for seek=eof: */
        if (wg_get_next_seq(cmd, context))
          return true;
      }


      /* Random i/o: */
      else
      {
        /* Generate seek within the hotband (hotband only for --miss-- workloads) */
        if (context.hotband != null)
          hotBandSeek(context, cmd);

        /* Calculate random seek address: */
        else if (sd_used.align == 0)
          blockBoundarySeek(context, cmd);
        else
          alignBoundarySeek(context, cmd);
      }


      /* When using a high range of > 100%, fall back if needed: */
      if (context.crossing_range_boundary &&
          cmd.cmd_lba + cmd.cmd_xfersize > sd_used.end_lba)
      {
        /* end_lba must be truncated in case it is not a multiple of xfersize: */
        long rounded = ownmath.round_lower(sd_used.end_lba,  cmd.sd_ptr.getMaxSdXfersize());
        cmd.cmd_lba -= rounded;

        /* If we end up with a negative lba because of (not) rounding, just set to zero: */
        if (cmd.cmd_lba < 0)
          cmd.cmd_lba = 0;
      }

      /* We will never touch block zero on a lun: */
      if (!access_block_zero && cmd.cmd_lba == 0)
      {
        reason1++;
        continue;
      }


      /* rdpct=100,seekpct=eof with DV bypasses blocks that we don't know.     */
      /* We MUST allow one block to pass through in case we bypass everything. */
      /* That would cause problems with detecting 'all eof'.                   */
      if (Validate.isRealValidate() && seekpct < 0 && readpct == 100)
      {
        /* Get Keys, but skip block when any key block in the data block is in error: */
        if (!key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map))
        {
          reason2++;
          continue;
        }

        /* If we don't know anything about this block, skip, unless */
        /* this is the first one. See note above for 'all eof'      */
        if (blocks_returned != 0 && !key_map.anyDataToCompare())
        {
          //common.ptod("reason3: %,16d %d", cmd.cmd_lba, blocks_returned);
          if ((reason3++ % 10000) == 0 && seek_eof_dv_read.go())
            SlaveJvm.sendMessageToSummary("100%% seekpct=eof Data Validation read: looking "+
                                          "for modified blocks to read");

          /* 'loop' must be turned off because we can end up not doing ANYTHING: */
          loop = 0;
          continue;
        }

        /* Ignore if lba already busy or in error: */
        /* (error check already done above, so double check) */
        //if (!key_map.getKeysFromMapAndSetBusy(cmd.cmd_lba, cmd.cmd_xfersize))
        //  continue;
      }

      if (Validate.isRealValidate())
      {
        /* Get Keys, but skip block when any key block in the data block is in error: */
        if (!key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map))
        {
          reason4++;
          continue;
        }

        /* Get keys again, but this time 'set busy'.                             */
        /* The step just above is partly redundant, but servers to pass DV_map */
        if (!key_map.getKeysFromMapAndSetBusy(cmd.cmd_lba, cmd.cmd_xfersize))
        {
          reason5++;
          continue;
        }
      }

      /* Returning false tells caller "use this block": */
      blocks_returned++;
      return false;
    }
  }


  /**
   * Generate lba on a block boundary.
   *
   * When requesting a stride, no pre-validation is done.
   * If someone creates a very small max-min range, that's his responsibility.
   * It means that all we end up doing is add 'min' to the previous lba and call
   * it quits.
   * No verification is done to even see if the current xfersize fits within the
   * max-min range, which is also just fine.
   */
  private void blockBoundarySeek(WG_context context, Cmd_entry cmd)
  {
    //common.ptod("context.seek_width: %,12d", context.seek_width);
    /* Generate a brandnew lba? */
    if (stride_min < 0)
    {
      long blocks = context.seek_width / cmd.cmd_xfersize;

      if (poisson_midpoint == 0)
      {
        double rand  = seek_randomizer.nextDouble();
        long   block = (long) (rand * blocks);
        cmd.cmd_lba  = context.seek_low + block * cmd.cmd_xfersize;
      }
      else
      {
        long block  = ownmath.distPoisson(blocks, poisson_midpoint);
        cmd.cmd_lba = (long) (context.seek_low + block * cmd.cmd_xfersize);
      }
    }

    else
    {
      /* First time, just start at the beginning: */
      if (context.next_seq < 0)
      {
        cmd.cmd_lba      = context.seek_low;
        context.next_seq = cmd.cmd_lba + cmd.cmd_xfersize;
        return;
      }

      /* Using stride. Generate forward skipping lba: */
      long stride_blocks = (stride_max - stride_min) / cmd.cmd_xfersize;
      double rand        = seek_randomizer.nextDouble();
      stride_blocks      = (long) (rand * stride_blocks);
      long stride_offset = stride_blocks * cmd.cmd_xfersize;

      /* Now take the previous lba, add the minimum stride and the offset: */
      cmd.cmd_lba        = context.next_seq + stride_min + stride_offset;

      /* We just skipped forward. If the stride min and max is identical */
      /* this implies that we want to stay on a multiple of the stride.  */
      /* This alllows stride=(1m,1m),xfersize=512,seekpct=100 to read    */
      /* only the first 512 bytes from each block of 1m.                 */
      //if (stride_min == stride_max)
      //  cmd.cmd_lba -= (cmd.cmd_lba % stride_min);

      /* If this brings us beyond the end, go to the beginning: */
      // Maybe should start at stride_offset?
      // If the 'max' is set outside of seek_high that will fail.
      if (cmd.cmd_lba + cmd.cmd_xfersize > context.seek_high)
        cmd.cmd_lba = context.seek_low;
    }

    /* If we switch back to sequential, we have to remember this lba: */
    context.next_seq = cmd.cmd_lba + cmd.cmd_xfersize;
  }


  /**
   * Generate lba on 'align' boundary
   * (stay away from last incomplete block)
   */
  private void alignBoundarySeek(WG_context context, Cmd_entry cmd)
  {
    /* Generate a brandnew lba? */
    if (stride_min < 0)
    {
      long align  = sd_used.align;
      long blocks = (context.seek_width - cmd.cmd_xfersize) / align;
      double rand = seek_randomizer.nextDouble();
      blocks      = (long) (rand * blocks);
      cmd.cmd_lba = context.seek_low + blocks * align;
    }

    else
    {
      /* First time: */
      if (context.next_seq < 0)
        context.next_seq = context.seek_low;

      long align         = sd_used.align;
      long stride_blocks = (stride_max - stride_min) / align;
      double rand        = seek_randomizer.nextDouble();
      stride_blocks      = (long) (rand * stride_blocks);
      long stride_offset = stride_blocks * align;

      /* Now take the previous lba, add the minimum stride and the offset: */
      cmd.cmd_lba        = context.next_seq + stride_min + stride_offset;

      /* If this brings us beyond the end, go to the beginning: */
      // Maybe should start at stride_offset?
      // If the 'max' is set outside of seek_high that will fail.
      if (cmd.cmd_lba + cmd.cmd_xfersize > context.seek_high)
        cmd.cmd_lba = context.seek_low;
    }

    /* If we switch back to sequential, we have  to remember this lba: */
    context.next_seq = cmd.cmd_lba + cmd.cmd_xfersize;
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
   * xfersize= parameter. Either a single xfersize, a distribution list with
   * xfersizes and percentages, or a three-part xfersize with min, max, align
   * coded to allow for a random xfersize between min and max with an xfersize
   * on an 'align' boundary.
   */
  public static int wg_dist_xfersize(double xfersizes[], String wd_name)
  {
    /* Single xfersize: */
    if (xfersizes.length == 1)
      return(int) xfersizes[0];

    /* Distribution list: */
    if (xfersizes.length != 3)
    {
      double pct = (ownmath.zero_to_one() * 100);
      double cumpct = 0;
      int i;

      for (i = 0; i < xfersizes.length; i+=2)
      {
        cumpct += xfersizes[i+1];
        if (pct < cumpct)
          return(int) xfersizes[i];
      }

      /* The parser in WD_entry allows a little rounding error to 100%, so do the */
      /* same here. If we get this far we're at the end of the list, so use it: */
      return(int) xfersizes[ xfersizes.length - 2];
    }

    /* xfersize=(min,max,align): */
    long   min      = (long) xfersizes[0];
    long   max      = (long) xfersizes[1];
    long   align    = (long) xfersizes[2];
    long   blocks   = (max - min) / align;

    long   random   = (long) (ownmath.zero_to_one() * (blocks + 1));

    long   xfersize = (long) (random * align + min);

    //common.ptod("xfersize: %8d min: %d max: %d blocks: %d random: %d align: %d",
    //            xfersize, (int) min, (int) max, (int) blocks, (int) random, align);

    return(int) xfersize;
  }


  /**
   * Start all Workload Generator tasks.
   *
   * During the 'SlaveJvm' rewrite I concluded that I should not have more
   * than one ReplayRun thread running. With a single one there is more
   * guarantee that ios will happen in the same order than with more than
   * one.
   * This of course turns out to not be how it works right now. See below.
   */
  public static void wg_start_sun_tasks(ArrayList <WG_entry> wgs_for_rd)
  {
    int count = 0;

    /* Start a Workload Generator Task for each WG entry: */
    for (int i = 0; i < wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.get(i);
      wg.on_the_way_lock = new Object();

      /* When replaying we only start a WG_entry when the device is actually used: */
      if (ReplayInfo.isReplay())
      {
        long[] used = ReplayDevice.getDeviceNumbersForSd(wg.sd_used.sd_name);
        //common.ptod("+=======used: " + used.length );
        //common.ptod("+wg.sd_used.sd_name: " + wg.sd_used.sd_name);
        if (used.length == 0)
        {
          common.ptod("Bypassed starting of WG_task for sd=" + wg.sd_used.sd_name);
          continue;
        }
      }

      new WG_task(new Task_num("WG_task"), wg).start();
      count ++;
    }

    common.plog("Started %d Workload Generator threads.", count);

    if (count == 0)
      common.failure("Started %d Workload Generator threads.", count);
  }


  /**
   * Allocate fifos.
   * When using the waiter task:
   * - fifos from WG_task to WT_task and WT_task to IO_task,
   * or,
   * - fifos from WG_task to IO_task
   */
  private static FifoList shared_fifo = null;
  public static void alloc_fifos(Work work)
  {
    int priorities = FifoList.countPriorities(work.wgs_for_slave);
    int fifosize   = Fifo.getSizeNeeded(priorities);

    /* First clean up old stuff: */
    Fifo.clearFifoVector();

    /* In case we share threads: */
    shared_fifo = new FifoList("shared_fifo", fifosize, priorities);


    /* Allocate one FifoList per active (real or concat) SD: */
    /* When we use the waiter task we need fifos going from WG_task to WT_task: */
    if (work.use_waiter)
    {
      for (WG_entry wg : work.wgs_for_slave)
      {
        wg.fifo_to_wait = new Fifo("to_waiter_" + wg.sd_used.sd_name + "_" + wg.wd_name, fifosize);
      }
    }

    /* Allocate fifos going to the IO tasks, either directly from */
    /* WG_task to IO_task or from WT_task to IO_task:             */
    for (SD_entry sd : SlaveWorker.sd_list)
    {
      if (!SlaveWorker.sharedThreads())
      {
        sd.fifo_to_iot = new FifoList("to_iot_" + sd.sd_name, fifosize, priorities);
        sd.fifo_to_iot.setThreadCount(work.getThreadsForSlave(sd.sd_name));
      }
      else
      {
        sd.fifo_to_iot = shared_fifo;
      }
    }
  }

  public static FifoList getSharedFifo()
  {
    return shared_fifo;
  }

  public static void free_fifos(Work work)
  {
    for (SD_entry sd : SlaveWorker.sd_list)
      sd.fifo_to_iot = null;

    /* Clear all possibly existing fifo's: */
    for (WG_entry wg : work.wgs_for_slave)
      wg.fifo_to_wait = null;
  }

  /**
   * Return an array with the SD names used for this WG_entry, accounting for
   * concatenated SDs.
   */
  public String[] getRealSdNames()
  {
    HashMap <String, Object> map = new HashMap(16);
    if (!Validate.sdConcatenation())
      map.put(sd_used.sd_name, null);
    else
    {
      for (int k = 0; k < sds_in_concatenation.size(); k++)
        map.put(sds_in_concatenation.get(k).sd_name, null);
    }
    return(String[]) map.keySet().toArray(new String[0]);
  }

  public SD_entry[] getRealSds()
  {
    HashMap <String, SD_entry> map = getRealSdMap();
    return(SD_entry[]) map.values().toArray(new SD_entry[0]);
  }

  public HashMap <String, SD_entry> getRealSdMap()
  {
    HashMap <String, SD_entry> map = new HashMap(16);
    if (!Validate.sdConcatenation())
      map.put(sd_used.sd_name, sd_used);
    else
    {
      for (int k = 0; k < sds_in_concatenation.size(); k++)
        map.put(sds_in_concatenation.get(k).sd_name, sds_in_concatenation.get(k));
    }
    return map;
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
    print_sizes();
    common.ptod("wd=" + wd_name +
                ": Lun space specified for either cache hits or cache misses (" +
                context.seek_width + " bytes) must be at least twice the xfersize (" +
                sd_used.getMaxSdXfersize() + ")");
    common.ptod("Remember that the first block in a lun is never accessed, " +
                "this to protect sector zero.");
    common.ptod("Please check the 'sd=nnn,size=xx,hitarea=' and 'wd=nnn,range=' parameters.");
    common.failure("Insufficient amount of lun size specified");
  }



  public static void overrideOpenflags(ArrayList <WG_entry> wg_list)
  {
    for (WG_entry wg : wg_list)
    {
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
    readpct          = wd_used.readpct;
    seekpct          = wd_used.seekpct;
    poisson_midpoint = wd_used.poisson_midpoint;
    stride_min       = wd_used.stride_min;
    stride_max       = wd_used.stride_max;
    wd_name          = wd_used.wd_name;
    rhpct            = wd_used.rhpct;
    whpct            = wd_used.whpct;

    setXfersizes(wd_used.xf_table);

    /* Copy rd=forxx overrides if needed: */
    if (rd.current_override != null)
      For_loop.useForOverrides(this, rd, rd.current_override, sd_used, false);

    if (rd.rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
      return;

    /* Replay runs get a negative seekpct to allow for EOF detection: */
    if (ReplayInfo.isReplay())
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
   *
   * The only problem at this time is that the 'no duplicate sequential SDs'
   * message in createWgListForOneRd() will not appear since at that time there
   * is still only ONE JVM. Deal with it some time later if needed.
   */
  public static void adjustJvmCount(RD_entry rd)
  {
    /* Don't do this for any format run: */
    if (rd.rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
      return;

    /* Look at all slaves: */
    int adjusts = 0;
    for (int i = 0; i < SlaveList.getSlaveCount(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* Calculate the total skew for this slave: */
      double total_skew = 0;
      for (int j = 0; j < Host.getAllWorkloads().size(); j++)
      {
        WG_entry wg = Host.getAllWorkloads().get(j);
        if (wg.slave.getHost().getLabel().equals(slave.getHost().getLabel()))
        {
          total_skew += wg.skew;
          //common.ptod("slave: %s %-15s %7.2f %7.2f", slave.getLabel(), wg.wd_name , wg.skew, total_skew);
        }
      }

      int IOS_PER_JVM  = 100000;
      int DEFAULT_JVMS = 8;

      /* Figure out if this is too many iops: */
      double iorate = rd.iorate_req * total_skew / 100;
      //common.ptod(slave.getLabel() + " iorate: " + iorate + " " + total_skew + " " + rd.iorate);
      if (iorate > IOS_PER_JVM)
      {
        int jvms = (int) Math.min((iorate + IOS_PER_JVM - 1)
                                  / IOS_PER_JVM, DEFAULT_JVMS);

        /* But never more than the amount of SDs: */
        /* (If the user wants more than one JVM for an SD, he must use jvms= parameter) */
        /* Technically it would be more accurate if we only use the SD count */
        /* from those SDs that will be really USED there. Accept. */
        jvms = Math.min(jvms, slave.getHost().getLunCount());
        if (jvms > slave.getHost().getJvmCount())
        {
          if (adjusts++ == 0)
            common.ptod("");
          common.ptod("Adjusted default JVM count for host=%s from jvms=%d to jvms=%d because of "+
                      "iorate=%s and a total of %d sds.",
                      slave.getHost().getLabel(),
                      slave.getHost().getJvmCount(),
                      jvms,
                      (rd.iorate_req == RD_entry.MAX_RATE) ? "max" : (int) iorate,
                      slave.getHost().getLunCount());

          slave.getHost().setJvmCount(jvms);
          Host.createSlaves();
        }
      }
    }

    /* For readability: */
    if (adjusts > 0)
      common.ptod("");
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
    String slave_mask = String.format("slv=%%-%ds ", Slave.max_slave_name);
    String wd_mask    = String.format("wd=%%-%ds ",  WD_entry.max_wd_name);
    String sd_mask    = String.format("sd=%%-%ds ",  SD_entry.max_sd_name);

    if (!rd.areWeSharingThreads())
    {
      return String.format("   " +
                           slave_mask   +
                           wd_mask      +
                           sd_mask      +
                           "rdpct=%3d "    +
                           "seek=%3d "    +
                           "rh=%3d "    +
                           "skew=%6.2f "  +
                           "th=%d ",
                           slave.getLabel(),
                           wd_name,
                           sd_used.sd_name,
                           (int) readpct,
                           (int) seekpct,
                           (int) rhpct,
                           skew,
                           rd.getSdThreadsUsedForSlave(sd_used.sd_name, slave));
    }
    else
    {
      return String.format("   " +
                           slave_mask   +
                           wd_mask      +
                           sd_mask      +
                           "rdpct=%3d "    +
                           "seek=%3d "    +
                           "skew=%6.2f " ,
                           slave.getLabel(),
                           wd_name,
                           sd_used.sd_name,
                           (int) readpct,
                           (int) seekpct,
                           skew);
    }
  }

  public static void main(String[] args)
  {
    double[] xfers = new double[] { 4096, 32768, 4096};
    int[] counters = new int[32768 / 4096 + 1];

    for (int i = 0; i < Integer.parseInt(args[0]); i++)
    {
      int xfersize = WG_entry.wg_dist_xfersize(xfers, "xxx");
      int index = xfersize / 4096;
      //common.ptod("index: " + index);
      counters[ index ]++;
    }

    for (int i = 0; i < counters.length; i++)
      common.ptod("xfersize: %5d, count: %5d", (i * 4096), counters[i]);
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

  public void setSlave(Slave sl)
  {
    slave = sl;
    //common.ptod("setSlave: " + sl.getLabel() + " " + wd_name);
    //common.where(8, "setSlave: " + sl.getLabel() + " " + wd_name);
  }
  public Slave getSlave()
  {
    return slave;
  }


  /**
   * Hotbands are only used for cache misses.
   * (we're only setting it up for miss-context)
   */
  public void setupHotBand()
  {
    mcontext.hotband = new HotBand(mcontext.seek_width);
  }

  private void hotBandSeek(WG_context context, Cmd_entry cmd)
  {
    long block  = context.hotband.get_rec_index((int) cmd.cmd_xfersize, cmd.cmd_read_flag);
    long offset = block * cmd.cmd_xfersize;
    cmd.cmd_lba = context.seek_low + offset;
    if (cmd.cmd_lba + cmd.cmd_xfersize > context.seek_high)
    {
      common.ptod("sd:     " + sd_used.sd_name);
      common.ptod("block:  " + block);
      common.ptod("offset: " + offset);
      cmd.cmd_print("hotband seek");
      print_sizes();
      common.failure("Invalid HotBand seek address");
    }

    /* If we switch back to sequential, we have  to remember this lba: */
    context.next_seq = cmd.cmd_lba + cmd.cmd_xfersize;
  }


  public void setXfersizes(double[] xf)
  {
    xf_table = xf;

    //if (wd_name.startsWith("journal"))
    //{
    //  common.ptod("setxfersizes: " + wd_name + " " + xf.length);
    //  for (int i = 0; i < xf.length; i++)
    //    common.ptod("xf_table: " + xf_table[i]);
    //  common.where(8);
    //}
  }

  public double[] getXfersizes()
  {
    return xf_table;
  }


  /**
   * Some workloads may be run on only ONE slave
   * - 100% sequential workloads (not using streams)
   * - Swat Replay
   * - Data Validation
   *
   * However, streams MAY run on multiple slaves; the code makes sure that these
   * slaves use different StreamContext() instances.
   */
  public boolean obsolete_mustRunOnSingleSlave()
  {
    if (wd_used.stream_count != 0)
      return false;

    if (common.get_debug(common.PLOG_WG_STUFF))
      common.ptod("mustRunOnSingleSlave: dv=%b seek=%d", Validate.isValidate(), (int) seekpct);

    if (Validate.isRealValidate()           ||
        seekpct <= 0                    ||
        ReplayInfo.isReplay())
      return true;
    else
      return false;
  }

  public boolean obsolete_mustStayOnSingleSlave()
  {
    if (common.get_debug(common.PLOG_WG_STUFF))
      common.ptod("mustStayOnSingleSlave: dv=%b ", Validate.isRealValidate());

    if (Validate.isRealValidate())
      return true;
    else
      return false;
  }

  /**
   * Primitive sorting of workloads.
   *
   * Note that the sort order used for Slave first uses the relative order in
   * which that Slave's Host was defined in the parameter file.
   */
  private static String sort_by = null;
  public static ArrayList <WG_entry> sortWorkloads(ArrayList <WG_entry> list, String by)
  {
    sort_by = by;
    Collections.sort(list);
    return list;
  }

  public int compareTo(Object obj)
  {
    int diff    = 0;
    WG_entry wg = (WG_entry) obj;

    if (sort_by.equals("slave") && this.slave != null && wg.slave != null)
    {
      if ((diff = this.slave.getHost().relative_hostno - wg.slave.getHost().relative_hostno) != 0)
        return diff;
      if ((diff = this.slave.getLabel().compareTo(wg.slave.getLabel())) != 0)
        return diff;
      if ((diff = this.wd_name.compareTo(wg.wd_name)) != 0)
        return diff;
      if ((diff = this.sd_used.sd_name.compareTo(wg.sd_used.sd_name)) != 0)
        return diff;

      return 0;
    }

    else if (sort_by.equals("wd"))
    {
      if ((diff = this.wd_name.compareTo(wg.wd_name)) != 0)
        return diff;
      if ((diff = this.sd_used.sd_name.compareTo(wg.sd_used.sd_name)) != 0)
        return diff;
      if ((diff = this.slave.getHost().relative_hostno - wg.slave.getHost().relative_hostno) != 0)
        return diff;
      if ((diff = this.slave.getLabel().compareTo(wg.slave.getLabel())) != 0)
        return diff;

      return 0;
    }

    else if (sort_by.equals("sd"))
    {
      if ((diff = this.sd_used.sd_name.compareTo(wg.sd_used.sd_name)) != 0)
        return diff;
      if ((diff = this.wd_name.compareTo(wg.wd_name)) != 0)
        return diff;
      if ((diff = this.slave.getHost().relative_hostno - wg.slave.getHost().relative_hostno) != 0)
        return diff;
      if ((diff = this.slave.getLabel().compareTo(wg.slave.getLabel())) != 0)
        return diff;

      return 0;
    }

    else
      common.failure("Invalid sort key: " + sort_by);

    return 0;
  }
}


