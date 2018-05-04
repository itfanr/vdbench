package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.ClassPath;
import Utils.Format;
import Utils.printf;

/**
 * This class stores entered Run Definition (RD) parameters
 */
public class RD_entry implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String  rd_name;

  String  wd_names[]         = new String[0];
  String  sd_names[]         = null;

  SD_entry concat_sd = null;

  Vector  <WD_entry>  wds_for_rd = new Vector(4, 0);
  Vector  <FwdEntry>  fwds_for_rd            = new Vector(4, 0);

  String  fwd_names[]        = new String[0];
  String  fsd_names[]        = new String[0];
  String  fwd_operations[]   = new String[0];
  double  skew[]             = null;   // no longer supported!!!!
  boolean foroperations_used = false;
  boolean operations_used    = false;
  Vector  <FwgEntry> fwgs_for_rd        = new Vector(4, 0);
  double  fwd_rate           = 0;

  FormatFlags format         = new FormatFlags();

  double  compression_ratio_to_use = -1;

  Bursts  bursts = null;

  String  rd_mount = null;

  public  int rd_stream_count = 0;

  /* Don't ever set a default iorate. Code depends on not having a default!   */
  double  iorate;              /* io rate to generate:                        */
  double  iorate_pct = 0;      /* Contains last requested iorate percentage   */
  double  iorate_req = 0;      /* If negative, take percentage of earlier rate*/

  static  long  NO_ELAPSED = 99999998;
  private long  elapsed      = 30;     /* Duration in seconds for run                 */
  private long  interval     = 1;      /* Reporting interval                          */
  private long  warmup       = 0;

  private boolean  variable      = false;
  private boolean  spread        = true;
  private double[] pending_rates = null;

  int   distribution = 0;      /* 0: exponential                              */
                               /* 1: uniform                                  */
                               /* 2: deterministic                            */
  /* Deterministic is very dangerous in multi wd because all the ios could    */
  /* start always at the same time for each wd, or, ios can get completely    */
  /* out of sync because one's start times are mostly smaller than the others */
  /* This problem does NOT happen if we don't use the waiter task.            */
  /* Not sure if this is still relevant. Henk 03/17/09                        */

  long  pause = 0;             /* How long to pause after each run            */



  Vector for_list     = new Vector(4, 0); /* List of requested for loops     */

  public ArrayList <WG_entry> wgs_for_rd = null; /* List of WG entries for this RD  */

  boolean use_waiter = true;


  static RD_entry next_rd       = null;   /* Set by rd_next_workload().   */
  static Vector   next_do_list  = null;
  static int      next_do_entry = 0;

  double max_data   = Double.MAX_VALUE;
  double max_data_r = Double.MAX_VALUE;
  double max_data_w = Double.MAX_VALUE;

  For_loop current_override = null;

  double  curve_points[] = null;
  boolean doing_curve_max   = false;
  boolean doing_curve_point = false;
  boolean curve_used_waiter = false;
  double  curve_end         = Double.MAX_VALUE;

  public  OpenFlags open_flags = null;

  static RD_entry dflt = new RD_entry();

  /* In Vdbench 5.00 this was changed from 99999998 to a smaller value. */
  /* This was done because the inter arrival time used for such a high  */
  /* iorate prevented a decent granularity between different workload's */
  /* io start time. e.g. there were 7000 ios within a 100 usec range,   */
  /* causing it to take a looong time before the waiter task picked up  */
  /* io from a different sd or workload.                                */

  /* Keep in mind though that a workload running with such a high IOPS  */
  /* will run over multiple slaves, and when targeting 50k to 100k of   */
  /* iops per second per JVM, that really is no issue!  01/25/2015      */
  public static int MAX_RATE              = 9999988;  // 9999988;
  public static int CURVE_RATE            = 9999977;  // 9999977;


  public Debug_cmds start_cmd = new Debug_cmds();
  public Debug_cmds end_cmd   = new Debug_cmds();

  public  static    RD_entry recovery_rd  = null;

  private static int sequence = 0;
  private        int seqno = sequence++;

  /* Map containing as key sd_name + '/' + slave label.                     */
  /* Value stored: an ArrayList of StreamContext, one instance per thread.  */
  /* The StreamContext instance will contain 'null' for a normal thread, */
  /* or it will contain a StreamContext for the thread */
  private HashMap <String, ArrayList <StreamContext> > threads_per_slave_map = null;



  public static String FSD_FORMAT_RUN = "format_for_";

  private static boolean debug_rd_list = false;



  /**
   * This is a DEEP clone!
   * We must make sure that after the regular shallow clone we create a new copy
   * of every single instance that is an RD parameter.
   */
  public Object clone()
  {
    try
    {
      RD_entry rd = (RD_entry) super.clone();
      rd.seqno = sequence++ + 1000;

      rd.wd_names       = (String[]) wd_names.clone();
      rd.fwd_names      = (String[]) fwd_names.clone();
      rd.fsd_names      = (String[]) fsd_names.clone();
      rd.fwd_operations = (String[]) fwd_operations.clone();

      rd.for_list       = (Vector)     for_list.clone();
      rd.start_cmd      = (Debug_cmds) start_cmd.clone();
      rd.end_cmd        = (Debug_cmds) end_cmd.clone();

      rd.wds_for_rd     = (Vector)     wds_for_rd.clone();
      rd.fwds_for_rd    = (Vector)     fwds_for_rd.clone();
      rd.fwgs_for_rd    = (Vector)     fwgs_for_rd.clone();

      if (rd.bursts != null)
        rd.bursts       = (Bursts)     bursts.clone();

      if (curve_points != null)
        rd.curve_points = (double[]) curve_points.clone();

      if (skew != null)
        rd.skew = (double[]) skew.clone();

      if (open_flags != null)
        rd.open_flags = (OpenFlags)  open_flags.clone();

      /* We don't clone format to make sure that if we have a forxx= */
      /* parameter they all use the same format flags:               */
      rd.format = null;

      return rd;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  /**
   * Pick up the first workload.
   * Beware that Journal Recovery may insert a run ahead of whatever is defined.
   * This inserted run will sequentially read and validate all modified data.
   */
  private static void rd_get_first_workload(Vector rd_list)
  {

    next_rd = (RD_entry) rd_list.firstElement();

    /* Create list of 'for' parameter overrides: */
    next_do_entry = 0;
    next_do_list  = new Vector(32, 0);
    For_loop.for_get(0, next_rd, next_do_list);

    next_rd.current_override = (For_loop) next_do_list.elementAt(next_do_entry++);

    return;
  }


  /**
   * Get the next (not first) RD entry
   */
  private static void rd_get_next_rd(Vector rd_list)
  {
    /* Previous RD complete: find index of the previous one and get next: */
    int index = rd_list.indexOf(next_rd);
    if (index < 0)
      common.failure("rd_get_next_rd(): rd_list search failed()");

    /* See if there are any left: */
    if (index + 1 >= rd_list.size())
    {
      next_rd = null;
      return;
    }

    next_rd = (RD_entry) rd_list.elementAt(index+1);

    /* Create list of 'for' parameter overrides: */
    next_do_entry = 0;
    next_do_list  = new Vector(32, 0);
    For_loop.for_get(0, next_rd, next_do_list);
    next_rd.current_override = (For_loop) next_do_list.elementAt(next_do_entry++);

    return;
  }


  /**
   * Get next RD_entry from the list
   * This is used only in 'master' mode where we will send the RD_entry
   * to the slaves.
   */
  private static RD_entry rd_next_workload(Vector rd_list)
  {
    /* First time around, just pick the first RD: */
    if (next_rd == null)
      rd_get_first_workload(rd_list);

    else
    {
      /* Get the next overrides from the do_list: */
      if (next_do_entry < next_do_list.size() )
        next_rd.current_override = (For_loop) next_do_list.elementAt(next_do_entry++);

      /* Previous RD complete; get the next one: */
      else
        rd_get_next_rd(rd_list);
    }

    return next_rd;
  }


  private static int workload_index = 0;
  public static RD_entry getNextWorkload()
  {
    if (next_rd == null)
      workload_index = 0;

    if (Vdbmain.rd_list.size() == workload_index)
      next_rd = null;

    else
    {
      next_rd = (RD_entry) Vdbmain.rd_list.elementAt(workload_index++);
    }

    return next_rd;
  }


  public static void displaySimulatedRuns()
  {
    for (int i = 0; i < Vdbmain.rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) Vdbmain.rd_list.elementAt(i);
      rd.display_run();
    }
  }

  /**
   * Display run information and 'for' overrides
   */
  public void display_run()
  {
    boolean zero = false;
    String txt_run = rd_name;
    String txt = null;
    String overrides = "";
    if (!rd_name.startsWith(RD_entry.FSD_FORMAT_RUN) && current_override.getText() != null)
      overrides = current_override.getText();


    Status.printStatus("Starting", this);

    String elapsed_txt;
    if (getElapsed() == NO_ELAPSED)
      elapsed_txt = "; elapsed=(none); ";
    else
    {
      if (getWarmup() > 0)
        elapsed_txt = "; elapsed=" + getElapsed() + " warmup=" + getWarmup() + "; ";
      else
        elapsed_txt = "; elapsed=" + getElapsed() + "; ";
    }

    /* Don't display if no WDs (only FWDs) are specified: */
    if (Vdbmain.isFwdWorkload())
    {
      String fwd_rate_txt = "fwdrate=";
      if (fwd_rate == MAX_RATE)
        fwd_rate_txt += "max. ";
      else if (fwd_rate == CURVE_RATE)
        fwd_rate_txt += "curve. ";
      else
        fwd_rate_txt += (int) fwd_rate + "; ";

      txt = (Vdbmain.simulate ? "Simulating RD=" : "Starting RD=") + rd_name;

      if (!rd_name.startsWith(FSD_FORMAT_RUN))
        txt += elapsed_txt + fwd_rate_txt + overrides;
    }

    else
    {
      String t2;
      //txt_run = txt_run + ((iorate_pct == 0) ? "" : Format.f("_(%d%%)", (int) iorate_pct) );

      String t1 = Vdbmain.simulate ? "Simulating RD=" : "Starting RD=";
      if (ReplayInfo.isReplay())
        t2 = Format.f("; I/O rate (Replay): %6d", iorate);
      else if (iorate == MAX_RATE && use_waiter)
        t2 = "; I/O rate: Controlled MAX";
      else if (iorate == MAX_RATE && !use_waiter)
        t2 = "; I/O rate: Uncontrolled MAX";
      else if (iorate == CURVE_RATE && use_waiter)
        t2 = "; I/O rate: Controlled curve";
      else if (iorate == CURVE_RATE && !use_waiter)
        t2 = "; I/O rate: Uncontrolled curve";
      else if (bursts != null)
        t2 = "; I/O rate: Variable";
      else
      {
        zero = iorate == 0;
        t2 = Format.f("; I/O rate: %d", iorate);
      }

      txt = t1 + txt_run + t2 + elapsed_txt + overrides;
    }


    if (Vdbmain.isWdWorkload() && !Vdbmain.simulate)
      rd_report_parameters_on_flatfile();

    if (Vdbmain.simulate)
      common.pboth(txt);
    else
      Report.displayRunStart(txt, this);

    if (zero)
      common.failure("iorate= parameter is missing.");


    if (Vdbmain.simulate)
      return;

    Flat.put_col("Run", txt_run);
    if (Vdbmain.isWdWorkload())
      Flat.put_col("reqrate", iorate);
    else
      Flat.put_col("reqrate", fwd_rate);
  }


  /**
   * Check to see if a duplicate RD exists. Not allowed.
   */
  static RD_entry rd_check_duplicate(Vector rd_list, String new_rd)
  {
    RD_entry rd;

    for (int i = 0; i < rd_list.size(); i++)
    {
      rd = (RD_entry) rd_list.elementAt(i);
      if (rd.rd_name.compareTo(new_rd) == 0)
      {
        common.failure("Duplicate RD names not allowed: " + new_rd);
      }
    }
    return null;
  }



  /**
   * Read Run Definition input and interpret and store parameters.
   * Henk: need to add more data validations like 'did I get a numeric'?
   */
  static String readParms(Vector rd_list, String first)
  {
    String str = first;
    Vdb_scan prm = null;
    RD_entry rd = null;

    /* EOF was already found before we got here: */
    if (first == null)
      return null;

    try
    {
      while (true)
      {
        prm = Vdb_scan.parms_split(str);
        if (prm.keyword.equals("rd"))
        {
          if (prm.alphas[0].equals("default") )
            rd = dflt;
          else
          {
            rd          = (RD_entry) dflt.clone();
            rd.format   = new FormatFlags();
            rd.rd_name  = prm.alphas[0];

            /* This call picks up possible '-cX' execution parameters: */
            if (Vdbmain.isFwdWorkload())
              rd.format.parseParameters(new String[0]);

            if (rd.rd_name.equals(Jnl_entry.RECOVERY_RUN_NAME))
              recovery_rd = rd;
            else
            {
              rd_check_duplicate(rd_list, prm.alphas[0]);
              rd_list.addElement(rd);
            }
          }
        }

        else if (prm.keyword.equals("wd"))
          rd.addWD(prm.alphas);

        else if (prm.keyword.equals("sd"))
          rd.sd_names = prm.alphas;

        else if (prm.keyword.equals("fsd"))
        {
          if (Host.getHostNames().length > 1)
            common.ptod("Warning: rd=xx,fsd=yyy parameter does not work with multi-host. TBD");
          rd.fsd_names = prm.alphas;
        }

        else if ("iorate".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() == 1 && prm.alphas[0].compareTo("curve") == 0)
          {
            //if (!Report.sdDetailNeeded())
            //  common.failure("The combination 'report=no_sd_detail' and 'iorate=curve' alas "+
            //                 "is not allowed due to a bug that needs to be fixed.");
            rd.iorate_req      = RD_entry.CURVE_RATE;
            rd.doing_curve_max = true;
          }

          else if (prm.getAlphaCount() == 1 &&
                   prm.alphas[0].toLowerCase().compareTo("max") == 0)
            rd.iorate_req = RD_entry.MAX_RATE;

          else
          {
            if (prm.getNumCount() > 1)
              rd.pending_rates = (double[]) prm.numerics.clone();

            rd.iorate_req = prm.numerics[0];
          }
        }

        else if (prm.keyword.startsWith("maxdata"))
        {
          if (prm.numerics[0] < 100 && !Vdbmain.isWdWorkload())
            common.failure("maxdata= (<100) only allowed for SD/WD workloads");
          if (prm.keyword.equals("maxdata"))
            rd.max_data = prm.numerics[0];
          else if (prm.keyword.equals("maxdataread"))
            rd.max_data_r = prm.numerics[0];
          else if (prm.keyword.equals("maxdatawritten"))
            rd.max_data_w = prm.numerics[0];
          else
            common.failure("Unknown parameter: " + prm.keyword);
        }

        // See notes at parserCleanup()
        //else if ("skew".startsWith(prm.keyword))
        //  rd.skew = prm.numerics;

        else if ("curve".startsWith(prm.keyword))
          rd.curve_points = prm.numerics;

        else if ("stopcurve".startsWith(prm.keyword))
          rd.curve_end = prm.numerics[0];

        else if ("elapsed".startsWith(prm.keyword))
          rd.setElapsed((long) prm.numerics[0]);

        else if ("interval".startsWith(prm.keyword))
          rd.setInterval((long) prm.numerics[0]);

        else if ("warmup".startsWith(prm.keyword))
          rd.setWarmup((long) prm.numerics[0]);

        else if (prm.keyword.equals("replay"))
        {
          ReplayInfo.setReplay();
          ReplayInfo.getInfo().parseParameters(prm.alphas);
        }

        else if ("pause".startsWith(prm.keyword))
          rd.pause = (long) prm.numerics[0];

        /* This checks ALL forxxx parameters: */
        else if (For_loop.checkForLoop(rd, prm))
        {
        }

        else if ("distribution".startsWith(prm.keyword))
          rd.useDistribution(prm);

        else if ("startcmd".startsWith(prm.keyword) || "start_cmd".startsWith(prm.keyword))
          rd.start_cmd.storeCommands(prm.alphas);

        else if ("endcmd".startsWith(prm.keyword) || "end_cmd".startsWith(prm.keyword))
          rd.end_cmd.storeCommands(prm.alphas);


        /* FWD specific stuff (fwd must be first in sequence to avoid colliding with fwdrate): */
        else if (prm.keyword.equals("fwd"))
          rd.fwd_names = prm.alphas;

        else if ("fwdrate".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() == 1 && prm.alphas[0].compareTo("curve") == 0)
          {
            rd.fwd_rate = RD_entry.CURVE_RATE;
            rd.doing_curve_max = true;
          }

          else if (prm.getAlphaCount() > 0 && prm.alphas[0].equalsIgnoreCase("max"))
            rd.fwd_rate = RD_entry.MAX_RATE;

          else
          {
            if (prm.getNumCount() > 1)
            {
              rd.fwd_rate = 1;   // satisfy need to have at least any fwdrate
              new For_loop("foriorate", prm.numerics, rd.for_list);
            }
            else
              rd.fwd_rate = prm.numerics[0];
          }
        }

        else if ("operations".startsWith(prm.keyword))
          rd.storeOperations(prm);

        else if ("format".startsWith(prm.keyword))
        {
          if (rd == dflt)
            common.failure("'format=' parameter is not allowed for rd=default");

          rd.format.parseParameters(prm.alphas);
        }

        else if ("openflags".startsWith(prm.keyword))
          rd.open_flags = new OpenFlags(prm.alphas, prm.numerics);

        else if ("mount".startsWith(prm.keyword))
        {
          if (!Host.anyMountCommands())
            common.failure("'rd=" + rd.rd_name + ",mount=' requested without " +
                           "an accompanying 'hd=xxx,mount=' request");
          rd.rd_mount = prm.alphas[0];
          if (rd.rd_mount.startsWith("mount"))
            common.failure("'rd=" + rd.rd_name + ",mount=' may only contain mount options ");
        }


        else
          common.failure("Unknown keyword: " + prm.keyword);

        str = Vdb_scan.parms_get();
        if (str == null)
          break;
      }


      if (rd_list.size() == 0)
        common.failure("No RD parameters specified");
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }

    return str;
  }


  /**
   * Validate some of the parameters used.
   */
  public static void parserCleanup(Vector rd_list)
  {
    /* Check 'format=once' abuse: */
    for (int i = 0; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);
      if (rd.format.format_once_requested && rd.for_list.size() == 0)
        common.failure("'format=once' can only be used with 'forxxx=' parameters");
    }


    /* Fiddle with variable distribution and iorates: */
    for (int i = 0; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);
      if (rd.variable)
      {
        rd.bursts        = new Bursts(rd.pending_rates, rd.spread);
        rd.pending_rates = null;
        rd.iorate_req    = rd.bursts.getMaxRate();
      }

      /* 'foriorate' had to be postponed so that we can reuse the rates */
      /* for dist=variable if needed:                                   */
      if (rd.pending_rates != null)
        new For_loop("foriorate", rd.pending_rates, rd.for_list);
    }

    /* Check skew percentages: */
    // I have no idea what this RD skew is for........  3/3/2015
    // Could it be for using 'operations=(read,write),skew=(20,80)?????
    // Since it is not in the doc, starting today it will no longer exist
    for (int i = 999990; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);
      if (rd.skew != null)
      {
        if (!Vdbmain.isFwdWorkload())
          common.failure("'skew=' parameter may only be used for FWD workloads.");
        double total = 0;
        for (int j = 0; j < rd.skew.length; j++)
          total += rd.skew[j];
        if (total != 100)
          common.failure("rd=" + rd.rd_name + ",skew= parameters must total 100%");

        for (int j = 0; j < rd.fwd_names.length; j++)
        {
          if (rd.fwd_names[j].indexOf("*") != -1)
            common.failure("rd=" + rd.rd_name +  ",skew= parameter may only be used "+
                           "when not having a wildcard in  FWD names: " + rd.fwd_names[j]);
        }

        if (rd.fwd_operations.length == 0 && rd.skew.length != rd.fwd_names.length)
          common.failure("rd=" + rd.rd_name + ". The 'skew=' and 'fwd=' (or operations=) parameters " +
                         "must have the same amount of values");

        if (rd.fwd_operations.length != 0 && rd.fwd_operations.length != rd.skew.length)
          common.failure("rd=" + rd.rd_name + ". The 'skew=' and 'operations=' parameters "+
                         "must have the same amount of values");
      }
    }


    /* Copy execution overrides, if specified: */
    for (int i = 0; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);
      if (Vdbmain.elapsed > 0 && !rd.rd_name.equals(Jnl_entry.RECOVERY_RUN_NAME))
        rd.setElapsed(Vdbmain.elapsed);

      if (Vdbmain.interval > 0)
        rd.setInterval(Vdbmain.interval);

      if (Vdbmain.warmup > 0)
        rd.setWarmup(Vdbmain.warmup);

      /* Default interval is half of elapsed with a maximum of 60 seconds: */
      if (rd.getInterval() == 0)
      {
        rd.setInterval(Math.min(rd.getElapsed() / 2, 60));
        common.ptod("'interval=' not specified. Setting to " +
                    rd.getInterval() + " seconds. ( 'min(elapsed/2,60)' )");
      }

      /* Burst may not be used with 'max': */
      if (rd.bursts != null && rd.iorate_req == RD_entry.MAX_RATE)
        common.failure("'bursts=' and 'iorate=max' are mutually exclusive.");
      //if (rd.bursts != null && rd.iorate_req != 0)
      //  common.failure("'bursts=' and 'iorate=' are mutually exclusive.");

      /* Check some values: */
      if (rd.getElapsed() / rd.getInterval() < 2)
        common.failure("'elapsed=' time value must be at least twice the 'interval=' value");

      //if (Vdbmain.isWdWorkload() && rd.iorate_req == 0 &&
      //    !ReplayInfo.isReplay() && rd.bursts == null)
      //  common.failure("No iorate= specified for rd=" + rd.rd_name);
      if (Vdbmain.isFwdWorkload() && rd.fwd_rate == 0)
        common.failure("No fwdrate= specified for rd=" + rd.rd_name);

      /* Default warmup equals one interval: */
      //if (rd.getWarmup() == 0)
      //  rd.setWarmup(rd.getInterval());

      if (rd.getElapsed() != RD_entry.NO_ELAPSED && rd.getElapsed() % rd.getInterval() != 0)
        common.failure("'elapsed="  + rd.getElapsed() +
                       "' must be multiple of 'interval=" + rd.getInterval() + "'.");

      //if (rd.getElapsed() <= rd.getWarmup())
      //  common.failure("'elapsed="  + rd.getElapsed() +
      //                 "' must be at least twice 'warmup=" + rd.getWarmup() + "'.");

      if (rd.getWarmup() % rd.getInterval() != 0)
        common.failure("'interval="  + rd.getInterval() +
                       "' must be multiple of 'warmup=" + rd.getWarmup() + "'.");

      // removed after introduction of rd=...,fsd=
      //if (FsdEntry.getFsdList().size() > 0 && rd.fwd_names.length == 0)
      //  common.failure("File System Definition (FSD) parameters are found but " +
      //                 "\n\tthis RD does not have any Filesystem Workload Definitions (FWD). " +
      //                 "rd=" + rd.rd_name);
    }

    /* Specifying 'sd=single' clones the requesting RD to repeat for each SD: */
    Vdbmain.rd_list = rd_list = repeatSdSingles();

    /* Determine which WD_entry instances we need, also override SDs if needed: */
    if (Vdbmain.isWdWorkload())
      selectWhichWdsToUse(rd_list);
    else
      selectWhichFwdsToUse(rd_list);


    //if (Vdbmain.isWdWorkload() && Validate.isNoPreRead())
    //  common.failure("'validate=no_preread' for raw i/o no longer allowed");
  }


  /**
   * Add a new WD to this RD_entry.
   * If the (non-wildcard) WD is not found, but this WD is found in the
   * /workloads/ directory, save the name for later inclusion.
   *
   * This was some attempt to implement things like the filebench 'personality'
   * function, but I decided it would make Vdbench just as ugly as filebench, so
   * this is not used.
   */
  private static Vector extra_wds = new Vector(8, 0);
  private void addWD(String[] wds)
  {
    if (wd_names.length != 0)
      common.failure("rd=%s: the 'wd=' parameter may be specified only once per RD, but you " +
                     "may specify it as follows: 'rd=%s,wd=(w1,w2,w3)', etc.", rd_name, rd_name);


    wd_names = wds;
    for (int i = 0; i < wds.length; i++)
    {
      /* I don't care about wildcards here: */
      if (wds[i].indexOf("*") != -1)
        continue;

      for (int j = 0; j < Vdbmain.wd_list.size(); j++)
      {
        WD_entry wd = (WD_entry) Vdbmain.wd_list.elementAt(j);

        /* If we have this WD, then just exit: */
        if (wd.wd_name.equals(wds[i]))
          return;
      }

      /* If the file exists, scan it later: */
      String fname = ClassPath.classPath() + "workloads" + File.separator + wds[i];
      if (new File(fname).exists())
        extra_wds.add(fname);
    }
  }

  public static Vector getExtraWDs()
  {
    return extra_wds;
  }


  /**
   * Store the requested operations.
   */
  private void storeOperations(Vdb_scan prm)
  {
    if (foroperations_used)
      common.failure("rd=" + rd_name + ": 'operations' and 'foroperations' parameters are mutually exclusive");

    for (int i = 0; i < prm.getAlphaCount(); i++)
    {
      int operation = Operations.getOperationIdentifier(prm.alphas[i]);
      if (operation < 0)
        common.failure("Unknown operation in 'operation': " + prm.alphas[i]);
    }

    fwd_operations = prm.alphas;
    operations_used = true;
  }


  public void storeForOperations(Vdb_scan prm)
  {
    foroperations_used = true;
    if (operations_used)
      common.failure("rd=" + rd_name + ": 'operations' and 'foroperations' parameters are mutually exclusive");

    for (int i = 0; i < prm.getAlphaCount(); i++)
    {
      int operation = Operations.getOperationIdentifier(prm.alphas[i]);
      if (operation < 0)
        common.failure("Unknown operation in 'foroperation': " + prm.alphas[i]);
    }

    /* Translate operations to an integer for For_lop(): */
    /* Copy all other operations: */
    double[] new_operations = new double[ prm.getAlphaCount() ];
    for (int i = 0; i < prm.getAlphaCount(); i++)
    {
      int operation = Operations.getOperationIdentifier(prm.alphas[i]);
      new_operations[i] = operation;
    }

    /* Store the operations as a for loop: */
    new For_loop("foroperation", new_operations, for_list);
  }


  /**
   * Figure out what iorate needs to be run.
   */
  public void set_iorate()
  {
    iorate = iorate_req;

    /* If the requested io rate is negative, we take x% of earlier rate: */
    iorate_pct = 0;
    if (iorate < 0)
    {
      int newrate;
      if (doing_curve_point)
        newrate = (int) Vdbmain.last_curve_max * (int) iorate * -1 / 100;
      else
        newrate = (int) Vdbmain.observed_iorate * (int) iorate * -1 / 100;

      if (newrate > 1000)
        newrate = ((newrate + 99) / 100) * 100;    // round up to 100
      else
        newrate = ((newrate +  9) /  10) *  10;    // round up to 10

      iorate_pct = iorate * -1;
      iorate     = newrate;
      fwd_rate   = newrate;
    }

    //common.ptod("set_iorate(): " + iorate + "   " +
    //            Vdbmain.observed_iorate + "  " + Vdbmain.last_curve_max);

  }

  /**
   * Report on the flatfile those parameters that are identical in each workload
   */
  public void rd_report_parameters_on_flatfile()
  {
    ArrayList <WG_entry> wg_list = getAllWorkloads();
    WG_entry wg     = wg_list.get(0);
    int    xfersize = (wg.getXfersizes().length > 0) ? (int) wg.getXfersizes()[0] : 0;
    double rhpct    = wg.rhpct;
    double whpct    = wg.whpct;
    double readpct  = wg.readpct;
    double seekpct  = wg.seekpct;

    for (int i = 1; i < wg_list.size(); i++)
    {
      wg = wg_list.get(i);
      if (wg.getXfersizes().length > 0 && xfersize != wg.getXfersizes()[0])
        xfersize = -1;
      if (rhpct != wg.rhpct)
        rhpct = -1;
      if (whpct != wg.whpct)
        whpct = -1;
      if (readpct != wg.readpct)
        readpct = -1;
      if (seekpct != wg.seekpct)
        seekpct = -1;
    }

    Flat.put_col("xfersize");
    Flat.put_col("rhpct");
    Flat.put_col("whpct");
    Flat.put_col("rdpct");
    Flat.put_col("threads");
    Flat.put_col("seekpct");

    if (xfersize != -1)
      Flat.put_col("xfersize", xfersize);
    if (rhpct != -1)
      Flat.put_col("rhpct", rhpct);
    if (whpct != -1)
      Flat.put_col("whpct", whpct);
    if (readpct != -1)
      Flat.put_col("rdpct", readpct);
    if (seekpct != -1)
      Flat.put_col("seekpct", seekpct);

    /* Keep track of #threads in SD parameter. If they're all the same, report */
    /* that count, otherwise report -1: */
    long threads_in_sds = 0;

    long lunsize = 0;
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (sd.isActive())
      {
        if (threads_in_sds == 0)
          threads_in_sds = sd.threads;
        if (threads_in_sds != sd.threads)
          threads_in_sds = -1;

        lunsize += sd.end_lba;
      }
    }

    if (current_override.getThreads() != For_loop.NOVALUE)
      Flat.put_col("threads", current_override.getThreads());
    else
      Flat.put_col("threads", threads_in_sds);
    Flat.put_col("lunsize", (double) lunsize / Report.GB);
  }


  /**
   * Create a list of the WDs needed for this RD.
   */
  public void getWdsForRd()
  {
    if (wd_names.length == 0)
      common.failure("No 'wd=' parameter specified for rd=" + rd_name);

    if (wds_for_rd.size() > 0)
      common.failure("This code may only be called once");

    /* Estimate code may have left stuff here: */
    Vector wds = new Vector(8, 0);

    /* Scan the complete list of WDs looking for the ones I need: */
    for (int i = 0; i < wd_names.length; i++)
    {
      boolean found = false;
      for (int k = 0; k < Vdbmain.wd_list.size(); k++)
      {
        WD_entry wd = (WD_entry) Vdbmain.wd_list.elementAt(k);

        /* A WD name starting with 'rd=' is a cloned 'wd=default' used */
        /* for this RDs that do not speficy a WD, instead using 'default': */
        if (wd.wd_name.startsWith("rd="))
          continue;

        if ( common.simple_wildcard(wd_names[i], wd.wd_name))
        {
          /* If we happen to find the journal recovery WD and we are NOT */
          /* working on the journal recovery RD, ignore: */
          if (!rd_name.equals(Jnl_entry.RECOVERY_RUN_NAME) &&
              wd.wd_name.startsWith(Jnl_entry.RECOVERY_RUN_NAME))
            continue;

          /* Set skew from parameter input. Could have been changed by 'curve': */
          wd.setSkew(wd.skew_original);

          wds.addElement(wd);
          found = true;
          //common.ptod("getWdsForRd: rd=%s,wd=%s", rd_name, wd.wd_name );
        }
      }

      if (!found)
        common.failure("Could not find WD=" + wd_names[i] + " for RD=" + rd_name);
    }

    wds_for_rd = wds;

    if (wds_for_rd.size() == 0)
      common.failure("No wds_for_rd for rd=" + rd_name);

    /* This is to prevent different workloads specifying different stream counts. */
    if (wds_for_rd.get(0).stream_count > 0 && wds_for_rd.size() > 1)
      common.failure("Only ONE Workload Definition (WD) allowed when using streams.");

    /* Check priority settings: */
    WD_entry.checkPriorities(wds);
  }


  /**
   * Get a list of FWDs that are requested for this RD
   */
  private void getFwdsForRd()
  {
    fwds_for_rd.clear();

    /* Scan the complete list of FWDs looking for the ones I need: */
    for (int i = 0; i < fwd_names.length; i++)
    {
      boolean found = false;
      for (int k = 0; k < FwdEntry.getFwdList().size(); k++)
      {
        FwdEntry fwd = (FwdEntry) FwdEntry.getFwdList().elementAt(k);

        if (common.simple_wildcard(fwd_names[i], fwd.fwd_name))
        {
          fwds_for_rd.addElement(fwd);
          //common.ptod("getFwdsForRd(): added fwd for rd=" + rd_name + " " + fwd.fwd_name);
          found = true;
        }
      }

      if (!found)
        common.failure("Could not find fwd=" + fwd_names[i] + " for RD=" + rd_name);
    }



    /* When skew is used on ANY fwd all host selections must be the same: */
    boolean skew_used = false;
    for (FwdEntry fwd : fwds_for_rd)
    {
      if (fwd.skew != 0)
        skew_used = true;
    }

    if (!skew_used)
      return;

    boolean problem = false;
    for (int i = 1; i < fwds_for_rd.size(); i++)
    {
      FwdEntry fwd1 = fwds_for_rd.get(0);
      FwdEntry fwd2 = fwds_for_rd.get(i);
      if (fwd1.host_names.length != fwd2.host_names.length)
        problem = true;
      else
      {
        for (int n = 0; n < fwd1.host_names.length; n++)
        {
          if (!fwd1.host_names[n].equals(fwd2.host_names[n]))
            problem = true;
        }
      }
    }

    if (problem)
    {
      BoxPrint box = new BoxPrint();
      box.add("When specifying FWD workload skew, all requested host names for 'rd=%s' must be identical", rd_name);
      box.add("");
      box.add("Workload skew control requires that all workloads run at the same time on all host.");
      box.print();
      common.failure("FWD workload skew control not possible");
    }


  }



  /**
   * Determine if the waiter task is needed.
   *
   * It appears that during Journal recovery the use_waiter flag is
   * active, though that should not be. The wg.arrival time however is
   * 10us, which just looks fine?????
   */
  public void willWeUseWaiterForWG()
  {
    int rc = 0;
    while (true)  // fake while() to avoid using goto.
    {
      /* With replay we MUST use the waiter: */
      if (ReplayInfo.isReplay())
      {
        use_waiter = true;
        rc = 1;
        break;
      }


      /* We won't use waiter if we have no WD's defined: */
      // This should be an error?
      if (wds_for_rd.size() == 0)
      {
        use_waiter = false;
        rc = 2;
        break;
      }
      /* Format of new files. We keep the iorate to 5000 to avoid having to */
      /* increase the JVM count, but still want max iops: */
      if (rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
      {
        use_waiter = false;
        rc = 3;
        break ;
      }

      /* If any skew is defined we may not run an uncontrolled workload: */
      for (int i = 0; i < wds_for_rd.size(); i++ )
      {
        WD_entry wd = (WD_entry) wds_for_rd.elementAt(i);

        /* If any skew or iorate or priority was requested we must use the waiter: */
        if (wd.skew_original != 0 || wd.wd_iorate != 0 || wd.priority != Integer.MAX_VALUE)
        {
          use_waiter = true;
          rc = 4;
          break;
        }

        /* User API requires the waiter. */
        if (wd.user_class_parms != null)
        {
          use_waiter = true;
          rc = 5;
          break;
        }
      }

      if (rc == 0)
      {
        //use_waiter = (next_rd.iorate_req != MAX_RATE && next_rd.iorate_req != CURVE_RATE);
        use_waiter = (iorate_req != MAX_RATE && iorate_req != CURVE_RATE);
        rc = 6;
      }

      break;
    }

    /* Remember the waiter settings for a curve run. We need it so that we    */
    /* can adjust the skew for the workloads after the initial CURVE_MAX run: */
    if (!doing_curve_max)
      curve_used_waiter = false;
    else
      curve_used_waiter = use_waiter;
  }



  /**
   * Using the fwd names specified, create a list of FwgEntry instances that
   * will ultimately contain the to be executed workload information.
   */
  private void createFwgListForOneRd()
  {
    FwgEntry fwg = null;
    fwgs_for_rd = new Vector(8, 0);


    /* Create an FwgEntry for all FWDs: */
    for (FwdEntry fwd : fwds_for_rd)
    {
      /* Get all fsds needed: */
      FsdEntry[] fsds = fwd.findFsdNames(this);


      /* What hosts do we want to run this FWD on? */
      Vector <String> run_hosts = Host.findSelectedHosts(fwd.host_names);

      /* Figure out how to adjust the skew: */
      double skew_divisor = fsds.length;

      /* More than one host is not allowed without 'shared=yes': */
      if (run_hosts.size() > 1)
      {
        for (FsdEntry fsd : fsds)
        {
          if (!fsd.shared)
            common.failure("Multiple hosts may only be requested for an FSD when "+
                           "'shared=yes' is specified. rd=" + rd_name);
        }
      }


      /* Create an FwgEntry for all FSDs in this FWD: */
      for (String host_to_run : run_hosts)
      {
        /* Create an FwgEntry for each FSD and for each host: */
        for (FsdEntry fsd : fsds)
        {
          /* If there are no operations used in rd=xxx,operations=(..,..) use the */
          /* single operation from this fwd: */
          if (fwd_operations.length == 0)
          {
            fwg = new FwgEntry(fsd, fwd, this, host_to_run, fwd.getOperation());
            fwgs_for_rd.addElement(fwg);

            // This may no longer be needed now that Dedup can handle any kind of xfersize
            //if (Dedup.isDedup())
            //{
            //  fwg.anchor.trackXfersizes(new double[] { fwg.dedup.getDedupUnit() });
            //}

            /* Workingset size and total size must be adjusted for the # of hosts: */
            /* (Also needs to be adjusted for JVMs, but that is TBD) */
            fwg.working_set /= run_hosts.size();
            if (fwg.total_size != Long.MAX_VALUE)
              fwg.total_size  /= run_hosts.size();

            /* We need to pick up the thread count early: */
            int threads = fwd.threads;
            if (current_override != null && current_override.getThreads() != For_loop.NOVALUE)
              threads = (int) current_override.getThreads();

            /* Now that we create an FwgEntry for each operation, split */
            /* the amount of threads per operation evenly:              */
            fwg.threads = Math.max(1, threads /  fsds.length / run_hosts.size());

            if (common.get_debug(common.PLOG_WG_STUFF))
              common.ptod("createFwgListForOneRd: " + rd_name + " " +
                          host_to_run + " fwg.threads: " + fwg.threads +
                          " threads: " + threads + " fsds.length: " +
                          fsds.length + " fwds_for_rd: " + fwds_for_rd.size());

            /* Override skew value: */
            //common.ptod("skew_divisor: " + skew_divisor);
            //common.ptod("fwg.skew1: %5.3f", fwg.skew);
            if (fwg.skew != 0)
              fwg.skew /= skew_divisor;
            //common.ptod("fwg.skew2: %-12s %5.3f %.0f", fsd.name, fwg.skew, skew_divisor);
          }

          else
          {
            /* rdpct= may only specify operation=read or write, not both (or others) */
            checkRdpctOperations(fwd);

            /* Create one FwgEntry for each operation in RD. And that is per host: */
            for (int j = 0; j < fwd_operations.length; j++)
            {
              fwg = new FwgEntry(fsd, fwd, this, host_to_run, Operations.getOperationIdentifier(fwd_operations[j]));
              fwgs_for_rd.addElement(fwg);

              /* We need to pick up the thread count early: */
              int threads = fwd.threads;
              if (current_override.getThreads() != For_loop.NOVALUE)
                threads = (int) current_override.getThreads();

              /* Now that we create an FwgEntry for each operation, split */
              /* the amount of threads per operation evenly: */
              fwg.threads = Math.max(1, threads / fwd_operations.length / fsds.length / run_hosts.size());

              if (common.get_debug(common.PLOG_WG_STUFF))
                common.ptod("createFwgListForOneRd: " + rd_name + " " + host_to_run +
                            " fwg.threads: " + fwg.threads +
                            " threads: " + threads + " fsds.length: " +
                            fsds.length + " fwds_for_rd: " + fwds_for_rd.size() +
                            " operation: " + fwd_operations[j]);

              /* Override skew value: */
              if (fwg.skew != 0)
                fwg.skew /= skew_divisor;
            }
          }
        }
      }
    }

    /* debugging: verify the number of threads for an FWD: */
    checkThreadCount();
  }


  /**
   * Splitting thread count across hosts and slaves can become tricky.
   * Integer truncation and/or minimum one thread per slave can create issues.
   */
  private static int once = 0;
  private void checkThreadCount()
  {
    /* debugging: verify the number of threads for an FWD: */
    HashMap <FwdEntry, Integer> map = new HashMap(32);
    for (int i = 0; i < fwgs_for_rd.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_rd.elementAt(i);
      Integer count = map.get(fwg.fwd_used);
      if (count == null)
        count = new Integer(0);
      count = new Integer(count.intValue() + fwg.threads);
      map.put(fwg.fwd_used, count);
      //common.ptod("fwg.fwd_used: " + fwg.fwd_used.fwd_name + " " + count);
    }

    FwdEntry[] fwds = (FwdEntry[]) map.keySet().toArray(new FwdEntry[0]);
    for (int i = 0; i < fwds.length; i++)
    {
      int count = map.get(fwds[i]).intValue();
      int threads_requested = fwds[i].threads;
      if (current_override != null && current_override.getThreads() != For_loop.NOVALUE)
        threads_requested = (int) current_override.getThreads();
      //common.ptod("count: " + count + " " + fwds[i].threads);
      if (count != threads_requested)
      {
        common.ptod("");
        common.ptod("Note: fwd=%s,threads=%d,...", fwds[i].fwd_name, threads_requested);
        common.ptod("Note: rd=%s,fwd=%s,... ",    rd_name, fwds[i].fwd_name);
        common.ptod("Note: Mismatch between threads requested (%d) and used (%d)",
                    threads_requested, count);

        if (once++ ==0)
        {
          common.ptod("");
          common.ptod("Note: Requesting threads across multiple fsds, operations, slaves or hosts ");
          common.ptod("      may result in fractions of threads. ");
          common.ptod("      There will be a minimum of one thread for each fsd, operation, ");
          common.ptod("      slave or host, or integer truncation of the resulting thread count.");
          common.ptod("      Contact the author of Vdbench if there are unexplained differences.");
          common.ptod("");
        }
      }

      //common.ptod("threads: " + fwds[i].fwd_name + " " + count);
    }
  }

  /**
   * rdpct= may only specify operation=read or write, not both (or others).
   */
  private void checkRdpctOperations(FwdEntry fwd)
  {
    if (fwd.readpct >= 0)
    {
      for (int j = 0; j < fwd_operations.length; j++)
      {
        int op = Operations.getOperationIdentifier(fwd_operations[j]);
        if (op != Operations.READ && op != Operations.WRITE)
          common.failure("When using 'rdpct=' you may only request 'operation=read' " +
                         "or 'operation=write', and then only one of these.");
      }
      if (fwd_operations.length > 1)
        common.failure("When using 'rdpct=' you may only request 'operation=read' " +
                       "or 'operation=write', and then only one of these.");
    }
  }


  private void printFwgs()
  {
    /* Go through all Fwg entries: */
    for (int i = 0; i < fwgs_for_rd.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_rd.elementAt(i);
      common.ptod( fwg );
    }
  }


  /**
   * Set workload values for a file system format operation.
   */
  private void setFormatOperation(RD_entry rd)
  {
    for (int i = 0; i < fwgs_for_rd.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_rd.elementAt(i);

      fwg.xfersizes     = FwdEntry.format_fwd.xfersizes;
      fwg.threads       = FwdEntry.format_fwd.threads;
      if (FwdEntry.format_fwd.open_flags != null)
        fwg.open_flags    = FwdEntry.format_fwd.open_flags;

      if (rd.open_flags != null)
        fwg.open_flags = rd.open_flags;

      fwg.select_random = false;
      fwg.sequential_io = true;
      fwg.anchor.trackXfersizes(fwg.xfersizes);
      fwg.anchor.trackFileSizes(fwg.filesizes);
    }

    fwd_rate = RD_entry.MAX_RATE;

    /* Format runs do not have an elapsed time, except when limited: */
    if (!rd.format.format_limited)
      setNoElapsed();
    else
      setElapsed(rd.getElapsed());

    if (common.get_debug(common.USE_FORMAT_RATE))
      fwd_rate = rd.fwd_rate;
  }


  public String toString()
  {
    //common.where(8);
    printf pf = new printf("rd=%-18s el=%4s in=%2d fr=%3s ");
    pf.add(rd_name);
    pf.add((getElapsed() == RD_entry.NO_ELAPSED) ? "none" : Format.f("%3d", getElapsed()) );
    pf.add(getInterval());
    pf.add((fwd_rate == RD_entry.MAX_RATE) ? "max" : Format.f("%3d",fwd_rate) );

    if (current_override != null)
      return Format.f("rd:%4d ", seqno) + " " + pf.text + current_override.getText();
    else
      return Format.f("rd:%4d ", seqno) + " " + pf.text;
  }



  /**
   * Insert an RD for 'format' if any of the anchor values change.
   */
  private boolean format_inserted = false;
  private boolean insertFwdFormatIfNeeded(Vector new_rd_list)
  {
    boolean debug = false;
    if (debug) common.ptod("format.format_requested: " + rd_name + " " + format.format_requested);

    /* 'format=once' formats only once for any sequence of 'forxx=' runs: */
    if (format.format_once_requested && format.one_format_done)
      return false;

    /* Get a list of fsd names and host names: */
    HashMap fsds_to_format = createFormatList();

    /* Get the fsd names that need formatting. Exit if none: */
    String[] fsds = (String[]) fsds_to_format.keySet().toArray(new String[0]);
    if (fsds.length == 0)
    {
      if (debug) common.where();
      return false;
    }

    /* If we never saw an fwd=format, insert the default now into fwd_list: */
    if (!format_inserted)
    {
      format_inserted = true;
      FwdEntry.format_fwd.fwd_name = "format";
      FwdEntry.getFwdList().add(FwdEntry.format_fwd);
    }

    /* Create an RD to be used for format: */
    RD_entry format_rd       = new RD_entry();
    format_rd.format         = format;
    format_rd.rd_name        = RD_entry.FSD_FORMAT_RUN + rd_name;
    format_rd.fwd_names      = null;  // tbd
    format_rd.fwd_operations = new String[0];
    format_rd.fwd_rate       = RD_entry.MAX_RATE;
    format_rd.setInterval(getInterval());
    format_rd.fwds_for_rd = new Vector(8);
    new_rd_list.addElement(format_rd);

    /* Need to protect 'format=once' */
    format.one_format_done = true;

    /* Create a format for each fsd and host: */
    for (int f = 0; f < fsds.length; f++)
    {
      common.plog(rd_name + " fsd to format: " + fsds[f]);
      HashMap hostmap = (HashMap) fsds_to_format.get(fsds[f]);
      String[] hosts = (String[]) hostmap.keySet().toArray(new String[0]);
      for (int h = 0; h < hosts.length; h++)
      {
        common.plog(rd_name + " format on host: " + hosts[h]);

        FwdEntry fwd = (FwdEntry.format_fwd != null) ? FwdEntry.format_fwd : FwdEntry.dflt;
        fwd = (FwdEntry) fwd.clone();
        fwd.fwd_name   = "format";
        fwd.host_names = new String[] { hosts[h]};
        format_rd.fwds_for_rd.add(fwd);
        fwd.fsd_names = new String[] { fsds[f]};
      }

      format_rd.createFwgListForOneRd();
      format_rd.current_override = current_override;
      For_loop.forLoopOverrideFwd(format_rd);
      format_rd.setFormatOperation(this);
      format_rd.start_cmd = start_cmd;
      format_rd.end_cmd   = end_cmd;

      if (debug_rd_list)
      {
        common.ptod("format_rd: " + format_rd);
        common.ptod("format_requested: " + format.format_requested);
        common.ptod("format_rd.format.format_requested: " + format_rd.format.format_requested);
        common.ptod("");
        common.ptod(format_rd.toString());
      }
    }

    return true;
  }

  /**
   * Determine which fsds need to be formatted on which host. (multiple hosts only
   * when shared=yes is used). Return a HashMap where each key is an fsd name, and
   * the stored value is a HashMap with the host names for this FSD..
   */
  private HashMap createFormatList()
  {
    /* Create a list of FSDs that need formatting: */
    HashMap fsds_to_format = new HashMap(16);
    for (FwgEntry fwg : fwgs_for_rd)
    {
      /* Prevent format=yes/only/restart for shared FSDs: */
      if (fwg.shared)
      {
        if (format.format_requested && !format.format_clean && !format.format_restart)
        {
          BoxPrint box = new BoxPrint();
          box.add("fsd=%s is requesting shared threads.", fwg.fsd_name);
          box.add("This requires you to split your format into two steps:");
          box.add("rd=step1,.....,format=(clean,only)   This removes the old file structure");
          box.add("rd=step2,.....,format=(restart,only) This recreates the new file structure");
          box.add("");
          box.add("This is necessary because otherwise hostA, still in the 'clean' phase of");
          box.add("the format, could delete files or directories that hostB has just created. ");
          box.add("");
          box.print();
          common.failure("'format=yes' not allowed for shared FSDs used in rd=%s", rd_name);
        }
      }

      /* I insert a format for 'format_only' whether the structure changes or not */
      // we'll see if this is OK!
      // It is starting to look as if we ALWAYS have to insert A format, which makes sense
      // because why would someone ask for a format?
      // But what about forx=(xx,yy)   ??????
      // In that case (forx..) they can just put in one 'format=only'.

      // Decision time: 'any format=yes does a clean and format'.

      /* During all this we maintain in buildNewRdListForFwd() the depth etc */
      /* in the anchor so that we can see what, if anything, changes.        */
      /*
      if (fwg.anchor.depth      != fwg.depth      ||
          fwg.anchor.width      != fwg.width      ||
          fwg.anchor.total_size != fwg.total_size ||
          fwg.anchor.files      != fwg.files      ||
          !fwg.compareSizes(fwg.anchor.filesizes) ||
          format.format_clean                     ||
          format.format_restart                   ||
          format.format_only_requested            ||
          format.format_dirs_requested)
      */
      {

        /* The HashMap with FSD names contains a HashMap for */
        /* hosts where a format must done:                   */
        HashMap hosts_for_this_fsd = (HashMap) fsds_to_format.get(fwg.fsd_name);
        if (hosts_for_this_fsd == null)
        {
          /* Create the host map for this FSD. Also put in the current host   */
          /* so that in case we are not using shared fsds, we have the first host: */
          hosts_for_this_fsd = new HashMap(16);
          fsds_to_format.put(fwg.fsd_name, hosts_for_this_fsd);
          hosts_for_this_fsd.put(fwg.host_name, fwg.host_name);
        }

        /* If we allow shared fsds, add (possible) more hosts: */
        if (fwg.shared)
          hosts_for_this_fsd.put(fwg.host_name, fwg.host_name);
      }
    }

    return fsds_to_format;
  }


  public static boolean isThisFormatRun()
  {
    return next_rd.rd_name.startsWith(RD_entry.FSD_FORMAT_RUN);
  }


  /**
   * Create a new rd_list that contains all requested runs, and all
   * inserted formats and forxxx loops.
   */
  public static Vector buildNewRdListForFwd()
  {
    RD_entry rd;
    Vector new_list = new Vector(16, 0);

    /* Start 'next workload search' at the beginning: */
    RD_entry.next_rd = null;


    while (true)
    {
      if ((rd = RD_entry.rd_next_workload(Vdbmain.rd_list)) == null)
        break;

      /* Create a clone of this RD: */
      RD_entry new_rd = (RD_entry) rd.clone();
      new_rd.format   = rd.format;
      new_rd.createFwgListForOneRd();
      For_loop.forLoopOverrideFwd(new_rd);

      /* If this workload requests a format, insert an extra workload: */
      if (new_rd.format.format_requested)
        new_rd.insertFwdFormatIfNeeded(new_list);

      /* Add this RD to the proper place in the list, after the possible format: */
      if (!rd.format.format_only_requested)
      {
        new_list.addElement(new_rd);

        /* If this is a curve run, insert the proper amount of extra runs: */
        if (rd.fwd_rate == CURVE_RATE)
          new_rd.insertCurveRuns(new_list, true);

        if (debug_rd_list)
          common.ptod(new_rd);
        //else
        //  common.plog(new_rd.toString());
        //new_rd.printFwgs();
      }

      /* Whether a format is inserted or not at this time, modify the anchor */
      /* settings so that for the next go-around we at least have the last   */
      /* anchor settings available:                                          */
      for (int i = 0; i < new_rd.fwgs_for_rd.size(); i++)
      {
        FwgEntry fwg = (FwgEntry) new_rd.fwgs_for_rd.elementAt(i);
        fwg.anchor.depth      = fwg.depth;
        fwg.anchor.width      = fwg.width;
        fwg.anchor.files      = fwg.files;
        fwg.anchor.total_size = fwg.total_size;
        fwg.anchor.filesizes  = fwg.filesizes;

        fwg.anchor.calculateStructureSize(fwg, true);
      }

    }

    /* Report totals if more than one anchor: */
    long dirs  = 0;
    long files = 0;
    long bytes = 0;
    for (FileAnchor anchor : FileAnchor.getAnchorList())
    {
      dirs  += anchor.total_directories;
      files += anchor.maximum_file_count;
      bytes += anchor.bytes_in_file_list;
    }

    if (FileAnchor.getAnchorList().size() > 1)
      common.ptod("Estimated totals for all %d anchors: dirs: %,d; files: %,d; bytes: %s ",
                  FileAnchor.getAnchorList().size(),
                  dirs, files, common.whatSize(bytes));

    /* Before we return, check the xfersizes requested with Data Validation: */
    if (Validate.isRealValidate() || Validate.isValidateForDedup())
    {
      Vector anchors = FileAnchor.getAnchorList();
      for (int i = 0; i < anchors.size(); i++)
      {
        FileAnchor anchor = (FileAnchor) anchors.elementAt(i);
        anchor.calculateKeyBlockSize();
        //anchor.matchFileAndXfersizes();
      }
    }

    /* For dedup we'll need to calculate sizes and sets values: */
    //if (Dedup.isDedup())
    //{
    //  common.where();
    //  common.failure("not yet");
    //  Dedup.adjustFsdDedupValue();
    //}

    /* report the expected anchor information: */
    FileAnchor.reportCalculatedMemorySizes(new_list);

    return new_list;
  }



  /**
   * Create a new rd_list that contains all requested runs, and all
   * inserted formats and forxxx loops.
   * This code is run twice: once to allow the code to increase the JVM count, and
   * once to do the final setup.
   */
  public static Vector buildNewRdListForWd()
  {
    RD_entry rd;
    Vector new_list = new Vector(16, 0);

    /* Start 'next workload search' at the beginning: */
    RD_entry.next_rd = null;

    while (true)
    {
      if ((rd = RD_entry.rd_next_workload(Vdbmain.rd_list)) == null)
        break;

      /* Create a clone of this RD: */
      RD_entry new_rd = (RD_entry) rd.clone();

      /* If we are concatenating and do not have an rd threads= */
      /* then all WDs must have a threads= parameter: */
      ConcatSds.checkForWdThreads(rd);

      RD_entry.createWgListForOneRd(new_rd, false);
      For_loop.forLoopOverrideWd(new_rd);
      new_rd.format = rd.format;

      /* Add this RD to the proper place in the list, after the possible format: */
      new_list.add(new_rd);

      /* If this is a curve run, insert the proper amount of extra runs: */
      if (rd.iorate_req == CURVE_RATE)
        new_rd.insertCurveRuns(new_list, false);
    }

    return new_list;
  }



  public static void finalizeWgEntries()
  {
    RD_entry rd      = null;
    RD_entry.next_rd = null;

    /* Start by clearing all possible relative CSD lbas: */
    for (SD_entry sd : Vdbmain.sd_list)
      sd.csd_start_lba = -1;

    while (true)
    {
      if ((rd = RD_entry.getNextWorkload()) == null)
        break;

      for (WG_entry wg : RD_entry.getAllWorkloads())
      {
        /* Calculate seek range, either % or bytes: */
        wg.calculateContext(rd, wg.wd_used, wg.sd_used);

        /* For concatenated SDs create lba ranges: */
        if (Validate.sdConcatenation())
          ConcatSds.calculateLbaRanges(wg);

        /* Create the UserClass for parsing and checking only: */
        if (wg.user_class_parms != null)
          User.ControlUsers.createInstance(wg);
      }
    }
  }


  public void finalizeWgEntry()
  {
    /* Start by clearing all possible relative CSD lbas: */
    for (SD_entry sd : Vdbmain.sd_list)
      sd.csd_start_lba = -1;

    for (WG_entry wg : getAllWorkloads())
    {
      /* Calculate seek range, either % or bytes: */
      wg.calculateContext(this, wg.wd_used, wg.sd_used);

      /* For concatenated SDs create lba ranges: */
      if (Validate.sdConcatenation())
        ConcatSds.calculateLbaRanges(wg);

      /* Create the UserClass for parsing and checking only: */
      if (wg.user_class_parms != null)
        User.ControlUsers.createInstance(wg);
    }
  }


  /**
   * Create one WG_entry per WD per host requested per SD
   *
   * - (wd*host*sd*slave)
   *
   * A 100% sequential workload for an SD (seekpct<=0) is an exception. Only ONE
   * workload will be created.
   *
   * This gives control to Vdbench to decide how many jvms/slaves will be run on
   * each host for those runs that don't specify anything, and will look as close
   * to the old Vdbench as possible.
   *
   * The problem with the old Vdbench was that even on a single host, at maximum
   * only ONE slave was used for a single SD. With single luns now being able to
   * do 40000+ iops this was becoming a limitiation.
   * By now allowing Vdbench to split an SD over multiple slaves (not sequential
   * of course) we will be able to get unlimited random IOPS for an SD.
   *
   * Note: This code is called THREE times, once to help figure out how many
   * JVMs may be needed, then after after. However, even if the amount of JVMs
   * does not change, or even if the amount of jvms has been user specified,
   * this still is called twice. This 'always called three times' is something
   * that has just crept in and was not the original objective. Fixing it is not
   * worth any time though.
   * The third time it is called just when we are about to send the work to the
   * slaves. This THIRD time makes all FINAL decisions.
   *
   * As I said: a lot of history there that I do not want to risk breaking.
   *
   */
  public static void createWgListForOneRd(RD_entry rd, boolean last_pass)
  {
    /* Clear all information about work for this RD that may go to a host or slave: */
    for (Host host : Host.getDefinedHosts())
    {
      for (Slave slave : host.getSlaves())
        slave.clearWorkloads();
    }


    /* Add i/o % to rd name if needed (curve already did so): */
    if (!rd.doing_curve_point)
      rd.rd_name += ((rd.iorate_pct == 0) ? "" : Format.f("_(%d%%)", (int) rd.iorate_pct) );

    /* Though the iorate can change later, it still needs to be called here: */
    rd.set_iorate();


    /* We create a list of work for each WD_entry: */
    for (WD_entry wd : rd.wds_for_rd)
    {
      /* Just a generic warning: */
      if (last_pass                  &&
          Validate.sdConcatenation() &&
          wd.seekpct <= 0            &&
          wd.stream_count == 0)
      {
        common.ptod("rd=%s,wd=%s: 100%% sequential i/o while using SD concatenation without "+
                    "the streams= parameter may underestimate your performance",
                    rd.rd_name, wd.wd_name);
      }

      /* Pick up the stream count here. We can easily pick this up from     */
      /* the current WD and put it in the RD since for streaming there will */
      /* be only ONE WD. And since the WD possibly can be reused we may NOT */
      /* have adjustThreadCount() replace the WD contents.                  */
      /* Pick it up only ONCE, since this  code is run multiple times and   */
      /* we want to keep the (possibly) adjusted value from the first time. */
      if (rd.rd_stream_count == 0)
        rd.rd_stream_count = wd.stream_count;




      /* For each host requested by this WD: */
      for (Host current_host : wd.getSelectedHosts())
      {
        /* Get a list of SDs that this WD wants on this host.              */
        /* (If this host has not been requested for this SD then of course */
        /* it won't be part of this list)                                  */
        Vector <SD_entry> sds_for_wd_on_host = wd.getSdsForHost(current_host);

        /* For concatenation we only pick up the concatenated sd: */
        if (Validate.sdConcatenation())
        {
          sds_for_wd_on_host.removeAllElements();
          sds_for_wd_on_host.add( (rd.concat_sd == null) ? wd.concat_sd : rd.concat_sd);

          /* Mark the real SDs in use: */
          for (SD_entry sd : wd.concat_sd.sds_in_concatenation)
            sd.sd_is_referenced = true;
        }

        /* Create a WG_entry for each SD (which CAN be one concatenated SD): */
        for (SD_entry sd : sds_for_wd_on_host)
        {
          sd.sd_is_referenced = true;
          WG_entry wg         = new WG_entry().initialize(rd, sd, wd, current_host);

          /* Data Validation for an SD may run on only ONE slave.    */
          /* If one has this workload already, add it to that slave: */
          Slave slave = findWorkForSd(sd);
          if (Validate.isRealValidate() && slave != null)
          {
            rd.giveWorkloadToSlave(wg, slave);
            printWgInfo("Data Validation Work for wd=%s,sd=%s given to slave=%s",
                        wd.wd_name, sd.sd_name, slave.getLabel());
            continue;
          }


          /* Some workloads may run on only one slave.      */
          /* If one has this workload already, just ignore. */
          if (WhereWhatWork.mustRunOnSingleSlave(wd) && findWorkForWdSdCombo(wd, sd) != null)
          {
            printWgInfo("Work not given to host=%s due to mustRunOnSingleSlave: wd=%s,sd=%s",
                        current_host.getLabel(),
                        wg.wd_used.wd_name, wg.sd_used.sd_name);
            continue;
          }





          /* Add this WG_entry to the current host: */
          rd.giveWorkloadToSlaves(wg, current_host);




          //printWgInfo("createWgListForOneRd adding to host=%s: sd=%s wg=%s",
          //            current_host.getLabel(), wg.sd_used.sd_name, wg.wg_name);
        }
      }  // for each host
    } // for each WD_entry

    printWgInfo("");


    /* Now give those workloads to the proper slaves: */
    //rd.removeUnwantedWorkloads();
    rd.checkWdUsed();


    /* Take the given thread counts and hand them out across slaves: */
    if (rd.areWeSharingThreads())
      rd.spreadSharedThreadsAcrossSlaves(last_pass);
    else
      rd.spreadSdThreadsAcrossSlaves(last_pass);


    /* Preserve a copy of the last workloads for this RD: */
    rd.wgs_for_rd = getAllWorkloads();



    /* For each occurence of a WD_entry we must adjust the skew.   */
    /* e.g. if wd1 has skew 50 and we have the workload run in two */
    /* slaves, each slave gets 50/2=25%                            */
    /* This must be repeated at the last moment before each run starts */
    HandleSkew.spreadWdSkew(rd);
    HandleSkew.spreadWgSkew(rd);
    HandleSkew.calcLeftoverWgSkew(rd);
    //Dedup.adjustSdDedupValue(rd.wgs_for_rd);


    /* When using the default JVM count we may have to adjust it:           */
    /* Note: the adjustment can happen for ANY run. This means that we have */
    /* to look at ALL runs to make sure we do this right. We therefore      */
    /* can not stop after one adjustment, meaning we have to go through     */
    /* this method twice for every run.                                     */

    /* 'isJournaling()' has been added to prevent a journal file from being used */
    /* across slaves. Though Vdbench makes sure that a WORKLOAD is not shared    */
    /* across slaves for DV, (createWgListForOneRd) there currently is no        */
    /* check to keep the SDs separated. This journal check is a quick fix,       */
    /* though of course -m1 is a quicker workaround.                             */
    /* This is now taken care of by WG_entry.mustRunSingleSlave()                */

    if (!last_pass && !Host.anyJvmOverrides() &&
        //   !Validate.isValidate() &&
        //!Validate.isJournaling() &&
        !ReplayInfo.isReplay())
      WG_entry.adjustJvmCount(rd);
  }


  /**
   * Give a workload to a Host and all its slaves.
   *
   * Some workloads may run only on one slave, e.g. 100% sequential, Replay,
   * Data Validation. Those will be given to the slave that has the least amount
   * of workloads already given to it.
   * (Of course, later on, once all work has been handed out it may no longer be
   * the least busy, but that's OK.).
   *
   * Data Validation MUST stay on the same slave.
   */
  private void giveWorkloadToSlaves(WG_entry wg, Host host)
  {
    /* Data Validation must keep an SD (not only the workload) on the same slave: */
    /* ('slave_used_for_dv' is set when work is actually SENT to the slave)       */
    if (Validate.isValidate() && wg.sd_used.slave_used_for_dv != null)
    {
      Slave slave = wg.sd_used.slave_used_for_dv;
      giveWorkloadToSlave(wg, slave);
      printWgInfo("giveWorkloadToSlaves2 added wd=%s,sd=%s to slave=%s",
                  wg.wd_used.wd_name, wg.sd_used.sd_name, slave.getLabel());
    }

    /* 100% sequential, replay, etc must be on one slave: */
    else if (WhereWhatWork.mustRunOnSingleSlave(wg.wd_used))
    {
      Slave slave = host.getLeastBusySlave();
      giveWorkloadToSlave(wg, slave);
      printWgInfo("giveWorkloadToSlaves3 added wd=%s,sd=%s to slave=%s",
                  wg.wd_used.wd_name, wg.sd_used.sd_name, slave.getLabel());
    }

    else
    {
      /* The rest may be spread over all slaves on this host:       */
      /* Though spreadSdThreadsAcrossSlaves() may remove it again.  */
      for (Slave slave : host.getSlaves())
      {
        WG_entry wg_clone = (WG_entry) wg.clone();
        giveWorkloadToSlave(wg_clone, slave);
        printWgInfo("giveWorkloadToSlaves4 added wd=%s,sd=%s to slave=%s",
                    wg.wd_used.wd_name, wg.sd_used.sd_name, slave.getLabel());
      }
    }
  }

  private void giveWorkloadToSlave(WG_entry wg, Slave slave)
  {
    wg.setSlave(slave);
    wg.wg_name  = slave.getLabel() + " " + wg.wg_name;
    slave.addWorkload(wg, this);
  }


  private void testPrint()
  {
    common.ptod("");
    for (Host host : Host.getDefinedHosts())
    {
      SD_entry[] sds = host.getSds();

      //common.ptod("testPrint: #sds for host=%s: %d", host.getLabel(), sds.length);

      for (SD_entry sd : sds)
      {
        /* Get a list of WGs for this host that use this SD: */
        for (WG_entry wg : host.getWgsForSd(sd))
        {
          common.ptod("testPrint: rd: %s host: %s sd: %s wg: %s",
                      rd_name,
                      host.getLabel(),
                      sd.sd_name,
                      wg.wg_name);
        }
      }
    }
    common.ptod("");

  }



  /**
   * Remove from all hosts those workloads that should not run there, e.g.
   * 100% sequential workloads and replay workloads.
   *
   * Those workloads have been added to the host earlier regardless of its
   * status so that, at this time, we know how much work we have on each host,
   * and can remove the unwanted workloads from the BUSIEST hosts.
   * If we would have attempted to make this decision before we had a total work
   * count we may have ended up with too much work for a host.
   *
   * Note that if this mechanism is not acceptable for some reason, the user can
   * always hardcode host names in the parameter file to whatever workload he
   * wants to run where.
   */
  private void obsolete_removeUnwantedWorkloads()
  {
    int count = 0;
    int loop = 0;

    toploop:
    while (true)
    {
      //testPrint();
      HashMap <String, WD_entry> seq_wd_map = new HashMap(8);

      for (Host host : Host.getDefinedHosts())
      {
        if (loop++ > 10000)
          common.failure("loop protection");

        /* Get SDs for this host: */
        SD_entry[] sds = host.getSds();

        for (SD_entry sd : sds)
        {
          /* Get WGs for this SD: */
          ArrayList <WG_entry> wgs_for_sd_on_host = host.getWgsForSd(sd);
          for (WG_entry wg : wgs_for_sd_on_host)
          {
            //boolean sequential   = (wg.wd_used.seekpct <= 0 || ReplayInfo.isReplay());
            boolean single_slave = WhereWhatWork.mustRunOnSingleSlave(wg.wd_used);

            //common.ptod("adding dup?: sd=%s key=%s host=%s %s",
            //            sd.sd_name, sd.sd_name + wg.wd_used.wd_name, host.getLabel(), sd.sd_name + wg.wd_used.wd_name);

            if (false)
            //if (single_slave && seq_wd_map.put(sd.sd_name + wg.wd_used.wd_name, wg.wd_used) != null)
            {
              /* We have a duplicate sequential workload. */
              /* Remove this workload from the host with the most work: */
              //common.ptod("duplicate: sd=%s", sd.sd_name);

              int max_work = 0;
              Host busiest = null;
              for (Host h2 : Host.getDefinedHosts())
              {
                SD_entry[] sds2 = h2.getSds();

                /* If equal, the LAST ('>=') will be used: */
                if (sds2.length >= max_work)
                {
                  busiest  = h2;
                  max_work = sds2.length;
                }
              }

              //common.ptod("Busiest host=%s %d", busiest.getLabel(), max_work);
              //if (max_work == 1)
              //  common.failure("max should never be one");


              //common.ptod("removing dup?: sd=%s wd=%s host=%s", sd.sd_name, wg.wd_name, busiest.getLabel());
              int removes = busiest.removeWorkload(wg);
              if (removes == 0)
                common.failure("unable to remove slave=%s,wg=%s", wg.getSlave().getLabel(),
                               wg.wd_used.wd_name);
              if (count++ == 0)
                printWgInfo(" ");
              printWgInfo("Removed wd=%s,sd=%s from host=%s ",
                          wg.wd_used.wd_name,
                          wg.sd_used.sd_name,
                          busiest.getLabel());


              //ArrayList <WG_entry> wgs_for_sd_on_busiest = busiest.getWgsForSd(sd);
              //
              //for (int i = 0; i < wgs_for_sd_on_busiest.size(); i++)
              //{
              //  WG_entry wg2 = wgs_for_sd_on_busiest.get(i);
              //  if (wg2.wd_used == wg.wd_used)
              //  {
              //    common.ptod("removing dup?: sd=%s wd=%s host=%s", sd.sd_name, wg.wd_name, busiest.getLabel());
              //    boolean rc = wgs_for_sd_on_busiest.remove(wg2);
              //    if (!rc)
              //      common.failure("not found?");
              //    break;
              //  }
              //}

              continue toploop;




              //  break hostloop;
            }
          }
        }
      }
      break;
    }

    if (count > 0)
      printWgInfo(" ");
  }



  /**
   * Spread the amount of requested threads for an SD over the hosts and slaves
   * that want to use this SD.
   *
   * Note: if we have work for a host, we must make sure that slave0 has some
   * work because slave0 returns cpu info.
   *
   * This is NOT called when sharing threads!
   */
  private void spreadSdThreadsAcrossSlaves(boolean last_pass)
  {
    int slave_count = SlaveList.getSlaveList().size();
    boolean debug = common.get_debug(common.DEBUG_SPREAD) && last_pass;
    int robin_host = 0;

    if (debug) common.ptod("spreadSdThreadsAcrossSlaves: spreadThreads for rd=" + rd_name);

    /* Clear current thread information: */
    threads_per_slave_map = new HashMap(32);

    for (Host host : Host.getDefinedHosts())
    {
      for (Slave slave : host.getSlaves())
        slave.threads_given_to_slave = 0;
    }

    /* Identify which SDs are used in this run: */
    HashMap <SD_entry, SD_entry> sd_used_map = new HashMap(16);
    for (WG_entry wg : getAllWorkloads())
      sd_used_map.put(wg.sd_used, wg.sd_used);


    /* The thread adjustment is done in the 'first pass', and then possibly */
    /* again after the JVM count has been adjusted. This of course can      */
    /* cause duplicate reporting, but I can deal with that separately.      */
    /* BTW: we only come here when NOT sharing threads but still doing      */
    /* concatenation.                                                       */
    if (!last_pass && Validate.sdConcatenation())
    {
      String wd_mask = String.format("wd=%%-%ds ", WD_entry.max_wd_name);

      BoxPrint box = new BoxPrint();
      for (SD_entry sd : sd_used_map.values().toArray(new SD_entry[0]))
      {
        /* This is NOT called when sharing threads! */
        int threads_requested = sd.threads;
        if (sd.threads % slave_count != 0)
        {
          int new_threads = threads_requested + slave_count - (threads_requested % slave_count);

          String txt = String.format("Workload thread count for " + wd_mask +
                                     "increased from %2d to %2d to make it a " +
                                     "multiple of the number of slaves (%d).",
                                     sd.concat_wd_name, threads_requested, new_threads, slave_count);
          box.add(txt);

          /* We are using concatenation. This means that the SD is a unique */
          /* concatenated SD created just for the current WD.               */
          /* That means that the sd.threads count that we are setting here  */
          /* will be preserved for the following 'pass'.                    */
          sd.threads = new_threads;
        }
      }

      /* If any changes were made, report. The sort is just for prettiness */
      if (box.size() > 0)
      {
        box.sort();
        box.print();
      }
    }


    /* Check for each SD: */
    for (SD_entry sd : getSdsForRD())
    {
      sd.setActive();

      /* How many hosts are using this SD in this run? If none, ignore */
      ArrayList <Host> hosts = getHostsForSd(sd);
      if (hosts.size() == 0)
        continue;

      /* How many threads do we want for this SD? */
      int threads_requested = getThreadsFromSdOrRd(sd);


      /* With streams we have some extra requirements: */
      int stream_count = rd_stream_count;
      if (last_pass && stream_count > 0)
      {
        if (debug) common.ptod("spreadSdThreadsAcrossSlaves: threads_requested: " + threads_requested);
        if (debug) common.ptod("spreadSdThreadsAcrossSlaves: slave_count:       " + slave_count);
        if (debug) common.ptod("spreadSdThreadsAcrossSlaves: stream_count:      " + stream_count);

        /* Stream count must be multiple of slave count: */
        if (threads_requested % stream_count != 0)
        {
          common.failure("'threads=%d' must be a multiple of the amount of streams=%d",
                         threads_requested,
                         stream_count);
        }
      }


      /* Round-robin these threads through all hosts:                             */
      /* Note that with SD concatenation we have nicely rounded thread counts,    */
      /* so round-robin is really not needed for them, but the result is the same */
      int[] threads_for_host = new int[ hosts.size() ];
      for (int threads_used = 0; threads_used < threads_requested;)
      {
        int round = robin_host++ % hosts.size();
        Host host = (Host) hosts.get(round);
        if (isSdUsedOnHost(host, sd))
        {
          /* When not using streams we can simply round-robin all threads: */
          if (stream_count == 0)
          {
            threads_for_host[ round ]++;
            threads_used++;
          }

          /* With streams though we must round-robin them in sets of 'threads per stream': */
          else
          {
            int threads_per_stream = threads_requested / stream_count;
            if (threads_per_stream == 0)
            {
              common.ptod("threads_requested: " + threads_requested);
              common.ptod("stream_count: " + stream_count);
              common.failure("threads_per_stream may not be zero");
            }
            threads_for_host[ round ] += threads_per_stream;
            threads_used              += threads_per_stream;
          }
        }
      }



      /* Now we round-robin all host threads through the slaves of those hosts: */
      /* Note: this is being done per SD.                                       */
      long loop = 0;
      for (int host_index = 0; host_index < hosts.size(); host_index++)
      {
        /* We must make sure that if a host has work, slave0 has at least */
        /* some of it because slave0 returns cpu + Kstat info:            */

        Host host               = (Host) hosts.get(host_index);
        Vector <Slave> slaves   = host.getSlaves();
        int[] threads_for_slave = new int[ slaves.size() ];

        for (int host_thread_count = 0; host_thread_count < threads_for_host[host_index];)
        {
          Slave slave = host.getLeastBusyThreads(sd);

          /* Need to find the relative slave# for 'round robin': */
          int round = 0;
          for (round = 0; round < slaves.size(); round++)
          {
            Slave sl2 = slaves.get(round);
            if (sl2 == slave)
              break;
          }

          if (loop++ > 100000) // only a low value for debugging
          {
            common.ptod("host_thread_count: " + host_thread_count);
            common.ptod("threads_for_host[host_index]: " + threads_for_host[host_index]);
            common.failure("loop protection slave=%s,sd=%s", slave.getLabel(), sd.sd_name);
          }

          if (isSdUsedOnSlave(slave, sd))
          {
            /* When not using streams we can simply round-robin the threads: */
            if (stream_count == 0)
            {
              threads_for_slave[ round ]++;
              host_thread_count++;
              slave.threads_given_to_slave++;
            }

            /* With streams though we must round-robin them in sets of 'threads per stream': */
            else
            {
              int threads_per_stream = threads_requested / stream_count;
              if (threads_per_stream == 0)
                common.failure("threads_per_stream may not be zero");
              threads_for_slave[ round ]   += threads_per_stream;
              host_thread_count            += threads_per_stream;
              slave.threads_given_to_slave += threads_per_stream;
            }
          }
        }

        /* Preserve the thread counts per SD/slave: */
        for (int k = 0; k < slaves.size(); k++)
          setThreadsUsedForSlave(sd.sd_name, slaves.get(k), threads_for_slave[k]);


        // report
        for (int k = 0; k < slaves.size(); k++)
        {
          if (last_pass)
          {
            Slave slave = (Slave) slaves.elementAt(k);
            if (getSdThreadsUsedForSlave(sd.sd_name, slave) > 0)
            {
              if (debug)
                common.ptod("---spreadSdThreadsAcrossSlaves: slave=%s gets %d threads for sd=%s",
                            slave.getLabel(), getSdThreadsUsedForSlave(sd.sd_name, slave), sd.sd_name);
            }
          }
        }
      }
    }


    /* Now remove all WGs that have a zero thread count for its SD: */
    for (WG_entry wg : getAllWorkloads())
    {
      //common.ptod("getSdThreadsUsedForSlave: %-15s %s", wg.sd_used.sd_name, wg.getSlave().getLabel()) ;
      if (getSdThreadsUsedForSlave(wg.sd_used.sd_name, wg.getSlave()) == 0)
      {
        if (last_pass) // && debug)
          common.ptod("spreadSdThreadsAcrossSlaves: Workload removed, not enough threads for slave=%s,wd=%s,sd=%s",
                      wg.getSlave().getLabel(), wg.wd_name, wg.sd_used.sd_name);
        int removes = wg.getSlave().removeWorkload(wg);
        if (removes == 0)
          common.failure("Unable to remove wd=%s from host=%s", wg.wd_used.wd_name, wg.getSlave().getLabel());
        //printWgInfo("spreadSdThreadsAcrossSlaves Not enough threads for slave=%s,wd=%s,sd=%s",
        //            wg.getSlave().getLabel(), wg.wd_name, wg.sd_used.sd_name);
      }
    }



    /* Report: */
    for (Host host : Host.getDefinedHosts())
    {
      int total     = 0;

      for (Slave slave : host.getSlaves())
      {
        int threads = getThreadsUsedForSlave(slave);
        total += threads;
        if (debug) common.ptod("Threads for rd=" + rd_name + " " + slave.getLabel() + ": " + threads);
      }
      if (debug) common.ptod("Total threads for rd=" + rd_name + ": " + total);
    }

    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      //if (report) common.plog("Threads for sd=" + sd.sd_name + " " + getThreadsUsedForSd(sd));
    }
  }


  /**
   * The total amount of threads specified are evenly spread around all the
   * slaves that we have.
   *
   * What happens if we have a mixed amount of slaves on each host? Not sure.
   * That should not matter. The objective is for the workload skew to be
   * controlled, and as long as all the workloads run in all slaves the skew
   * should stay in tact.
   *
   *
   */
  private void spreadSharedThreadsAcrossSlaves(boolean last_pass)
  {
    boolean debug = common.get_debug(common.DEBUG_SPREAD) && last_pass;
    int robin_host = 0;

    if (debug) common.ptod("spreadSdThreadsAcrossSlaves: spreadThreads for rd=" + rd_name);

    /* How many shared threads? */
    threads_per_slave_map = new HashMap(32);
    int slave_count       = SlaveList.getSlaveList().size();

    /* For starters, make sure we have enough to divide by slave and/or stream count: */
    int threads_requested = (int) current_override.getThreads();
    if (last_pass)
    {
      BoxPrint box = new BoxPrint();
      threads_requested = adjustThreadCount(threads_requested, box);
      if (box.size() > 0)
        box.print();
    }

    /* We have a nice multiple of threads/slaves/streams, give them out to slaves: */
    int threads_per_slave = threads_requested / slave_count;

    /* Shared threads use only concatenated SDs.                                       */
    /* When using streams there is only one WD and therefore only ONE concatenated SD. */
    for (SD_entry csd : getSdsForRD())
    {
      csd.setActive();
      for (Slave slave : SlaveList.getSlaveList())
        setThreadsUsedForSlave(csd.sd_name, slave, threads_per_slave);
    }


    /* Report: */
    for (Host host : Host.getDefinedHosts())
    {
      int total     = 0;

      for (Slave slave : host.getSlaves())
      {
        int threads = getThreadsUsedForSlave(slave);
        total += threads;
        if (debug) common.ptod("Threads for rd=" + rd_name + " " + slave.getLabel() + ": " + threads);
      }
      if (debug) common.ptod("Total threads for rd=" + rd_name + ": " + total);
    }
  }



  /**
   * Make sure that when streams are requested all streams are nicely
   * round-robined across all hosts and slaves.
   */
  public void spreadStreamsAcrossSlaves(boolean last_pass)
  {
    boolean debug = common.get_debug(common.DEBUG_SPREAD) && last_pass;

    /* No streaming used? If we use streaming we have only ONE WD, so get(0) is OK: */
    if (rd_stream_count == 0)
      return;

    /* Create a round robin list of all slaves that have work: */
    int looking_for_relative_slave        = -1;
    ArrayList <Slave> round_list = new ArrayList(32);
    boolean added_one            = false;
    do
    {
      added_one = false;
      looking_for_relative_slave++;

      for (Host host : Host.getDefinedHosts())
      {
        int slave_with_some_work = -1;
        for (Slave slave : host.getSlaves())
        {
          /* If slave has no work, skip: */
          //common.ptod("slave: %s has %d wgs", slave.getLabel(), slave.getWorkloads().size());
          if (slave.getWorkloads().size() == 0)
            continue;
          slave_with_some_work++;

          /* If this is not the relative slave we're looking for, skip: */
          if (slave_with_some_work != looking_for_relative_slave)
            continue;

          /* This is the next round robin slave we want, add to list: */
          round_list.add(slave);
          added_one = true;
          if (debug) common.ptod("spreadStreamsAcrossSlaves, added " + slave.getLabel());
        }
      }
    } while (added_one);


    //I now need to go through this RR list one SD at the time?
    //By taking, for each SD, the thread count and divvy that up for the stream count.
    //  Could we have multiple wds with stream counts? NO!
    //  The latest wds stream count will be used.
    // getSdThreadsUsedForSlave()?

    /* How many streams for this (always) one workload? */
    int stream_count = rd_stream_count;

    /* Spread the threads for each SD: */
    for (SD_entry sd : getSdsForRD())
    {
      sd.setActive();
      int sd_threads = getThreadsUsedForSD(sd.sd_name);
      int threads_per_stream = sd_threads / stream_count;


      if (threads_per_stream == 0)
      {
        common.ptod("sd.sd_name:         " + sd.sd_name);
        common.ptod("sd_threads:         " + sd_threads);
        common.ptod("stream_count:       " + stream_count);
        common.ptod("threads_per_stream: " + threads_per_stream);
        common.failure("rd=%s Not enough threads per stream: threads: %d, streams: %d, threads/streams: %d ",
                       rd_name, sd_threads, stream_count, (int) sd_threads / stream_count);
      }


      /* Pass one stream at the time to the slaves in the round robin list: */
      int current_stream = 0;
      int loop_protect   = 0;
      while (current_stream < stream_count)
      {
        if (loop_protect++ > 100000)
        {
          common.ptod("current_stream:     " + current_stream);
          common.ptod("stream_count:       " + stream_count);
          common.ptod("threads_per_stream: " + threads_per_stream);
          common.ptod("round_list:         " + round_list.size());
          common.failure("loop protection");
        }

        for (Slave slave : round_list)
        {
          ArrayList <StreamContext> stream_list = getStreamListForSD(sd.sd_name, slave);

          /* Look for the first free entry: */
          for (int i = 0; i < stream_list.size(); i++)
          {
            if (stream_list.get(i) == null)
            {
              /* Create the stream: */
              StreamContext sc = new StreamContext(sd, stream_count, current_stream);
              printWgInfo("Stream: slave: %s threads: %2d %s",
                          slave.getLabel(), threads_per_stream, sc);

              /* And give this stream to the next 'threads_per_stream' threads: */
              for (int t = 0; t < threads_per_stream; t++)
              {
                if (i+t >= stream_list.size())
                {
                  common.ptod("i/t: " + i + " " + t);
                  common.failure("Improper thread count: %d/%d", stream_list.size(), threads_per_stream);
                }

                stream_list.set(i+t, sc);
                //common.ptod("slave=%s sd=%s thread=%02d stream=%02d",
                //            slave.getLabel(), sd.sd_name, i+t, current_stream);
              }

              /* Finished this stream, now continue round-robin to next slave: */
              current_stream++;
              break;
            }
          }
        }
      }
    }
  }




  private boolean isSdUsedOnHost(Host host, SD_entry sd)
  {
    for (WG_entry wg : getAllWorkloads())
    {
      //common.ptod("wg.slave.getHost(): " + wg.slave.getHost() + " " + host +
      //            " " + wg.sd_used.sd_name + " " + sd.sd_name);
      if (wg.getSlave().getHost() == host && wg.sd_used == sd)
        return true;
    }
    return false;
  }

  private boolean isSdUsedOnSlave(Slave slave, SD_entry sd)
  {
    for (WG_entry wg : getAllWorkloads())
    {
      //common.ptod("wg.slave.getHost(): " + wg.slave.getHost() + " " + host +
      //            " " + wg.sd_used.sd_name + " " + sd.sd_name);
      if (wg.getSlave() == slave && wg.sd_used == sd)
        return true;
    }
    return false;
  }

  private void checkWdUsed()
  {
    for (WD_entry wd : wds_for_rd)
    {
      int used = 0;
      for (WG_entry wg : getAllWorkloads())
      {
        //common.ptod("wg.wd_used: " + wg.wd_used.wd_name + " " + wd.wd_name +
        //            " " + wg.wd_used + " " + wd + " " + rd_name);
        if (wg.wd_used == wd)
        {
          //common.where();
          used++;
        }
      }

      if (used == 0)
      {
        common.failure("rd=" + rd_name + ",wd=" + wd.wd_name +
                       " not used. Could it be that more hosts "+
                       "have been requested than there are threads?");
      }
    }
  }

  /**
   * Count the number of hosts that want to use a specific SD.
   */
  private ArrayList <Host> getHostsForSd(SD_entry sd)
  {
    HashMap <Host, Object> hosts_for_sd = new HashMap(8);
    for (WG_entry wg : getAllWorkloads())
    {
      //common.ptod("countHostsForSd wg: " + wg.sd_used + " " + sd.sd_name);
      if (wg.sd_used == sd)
        hosts_for_sd.put(wg.getSlave().getHost(), null);
    }

    ArrayList <Host> list = new ArrayList(hosts_for_sd.keySet());
    Collections.sort(list);

    //for (Host h : vec)
    //  common.ptod("getHostsForSd: " + h.getLabel());

    return list;
  }

  /**
   * Count the number of slaves on a host that want to use a specific SD.
   */
  private Vector obsolete_countSlavesForSd(Host host, SD_entry sd)
  {
    HashMap slaves_for_sd = new HashMap(8);
    for (WG_entry wg : getAllWorkloads())
    {
      if (wg.sd_used == sd && wg.getSlave().getHost() == host)
        slaves_for_sd.put(wg.getSlave(), null);
    }

    return new Vector(slaves_for_sd.keySet());
  }

  /**
   * Check to see if the requested WD_entry is already in use for a slave.
   * This is needed to prevent a sequential workload from being serviced
   * by more than one slave, making this workload invalid.
   * For Data Validation it also keeps the map on just one single slave.
   *
   * (Note that in theory depending on the workload definitions the SD can switch
   * between slaves. Need to preserve the slave inside of the SD and then always
   * force it there). TBD.
   */
  private boolean thisWdAlreadyUsed(WG_entry wg)
  {
    if (Validate.isValidate()           ||
        wg.seekpct <= 0                 ||
        Validate.removeAfterError()     ||
        ReplayInfo.getInfo().isReplay())
    {
      for (Slave slave : SlaveList.getSlaveList())
      {
        for (WG_entry wg2 : slave.getWorkloads())
        {
          if (wg2.wd_used == wg.wd_used &&
              wg2.sd_used == wg.sd_used)
          // I removed this check because of snia prefill using concatenation.
          // I do NOT remember why I did allow sequential streaming to run
          // concurrently on multiple slaves anyway. We'll see what happens.
          // && wg2.wd_used.stream_count == 0)
          {
            if (!wg2.wd_used.one_slave_warning_given)
            {
              if (Validate.removeAfterError())
                common.ptod("'data_errors=remove_device' requested for sd=" +
                            wg.sd_used.sd_name + ". It will run on only one slave.");
              else if (!Validate.isValidate())
                common.plog("Sequential workload for wd=" + wg.wd_used.wd_name +
                            " may run on only ONE slave. " +
                            "It will run on slave=" + slave.getLabel());
              else
                common.plog("Data Validation: Workload for wd=" + wg.wd_used.wd_name +
                            " may run on only ONE slave. " +
                            "It will run on slave=" + slave.getLabel());
            }
            wg2.wd_used.one_slave_warning_given = true;
            return true;
          }
        }
      }
    }

    return false;
  }


  private void insertCurveRuns(Vector new_list, boolean last_pass)
  {
    /* These are the default curve points: */
    double rtable[] = new double[] {10, 50, 70, 80, 90, 100};

    /* Override curve point default: */
    if (curve_points != null)
      rtable = curve_points;

    for (int i = 0; i < rtable.length; i++)
    {
      RD_entry rd = (RD_entry) this.clone();
      rd.doing_curve_max   = false;
      rd.doing_curve_point = true;
      rd.iorate_req =
      rd.iorate     = rtable[i] * -1;
      rd.rd_name += Format.f("_(%d%%)", (int) rd.iorate * -1);

      new_list.add(rd);

      if (Vdbmain.isWdWorkload())
        RD_entry.createWgListForOneRd(rd, last_pass);
      else
        rd.createFwgListForOneRd();
    }
  }


  /**
   * At the end of the RD parameter scan, determine which WDs are needed.
   *
   * Note: the cloned WDs for SD overrides are NOT added to Vdbmain.wd_list
   * to prevent them from being picked up by rd=xxx,wd=wd* selections.
   */
  private static void selectWhichWdsToUse(Vector rd_list)
  {
    for (int i = 0; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);

      /* If SDs are requested but no WDs, clone 'default': */
      if (rd.sd_names != null && rd.wd_names.length == 0)
      {
        rd.wds_for_rd.removeAllElements();
        WD_entry clone = (WD_entry) WD_entry.dflt.clone();
        clone.wd_name = "rd=" + rd.rd_name;
        Vdbmain.wd_list.add(clone);
        rd.wds_for_rd.add(clone);
        common.plog("No Workload Definitions defined for rd=" + rd.rd_name +
                    "; using the latest wd=default instead.");
      }

      else
      {
        /* Find the WDs, if none found, this will abort: */
        rd.getWdsForRd();
      }

      /* If there are no SD overrides we're done with this rd: */
      if (rd.sd_names == null)
        continue;

      /* Clone the selected WDs so that we can override the SDs without */
      /* impacting the original SDs specified in the WDs:               */
      Vector new_wds = new Vector(4, 0);
      for (int j = 0; j < rd.wds_for_rd.size(); j++)
      {
        WD_entry wd = (WD_entry) ((WD_entry) rd.wds_for_rd.elementAt(j)).clone();
        new_wds.add(wd);
        wd.sd_names = rd.sd_names;
        wd.concat_sd = rd.concat_sd;
      }

      /* Now replace the original wd list for this rd: */
      rd.wds_for_rd = new_wds;
    }
  }

  /**
   * At the end of the RD parameter scan, determine which FWDs are needed.
   *
   * Note: the cloned FWDs for FSD overrides are NOT added to fwd_list
   * to prevent them from being picked up by rd=xxx,fwd=fwd* selections.
   */
  private static void selectWhichFwdsToUse(Vector rd_list)
  {
    for (int i = 0; i < rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(i);

      if (rd.fwd_names.length + rd.fsd_names.length == 0)
        common.failure("No 'fwd=' or 'fsd=' specified for rd=" + rd.rd_name);

      /* If no FSDs have been specified we're done: */
      if (rd.fsd_names.length == 0)
      {
        /* Find the FWDs, if none found, this will abort: */
        rd.getFwdsForRd();
        continue;
      }

      /* If no FWDs are specified, use 'default': */
      if (rd.fwd_names.length == 0)
      {
        rd.fwds_for_rd.removeAllElements();
        rd.fwds_for_rd.add(FwdEntry.dflt);
        common.ptod("No Filesystem Workload Definitions defined for rd=" +
                    rd.rd_name + "; using fwd=default instead");

        //for (int j = 0; j < rd.fwds_for_rd.size(); j++)
        //{
        //  FwdEntry fwd = (FwdEntry) rd.fwds_for_rd.elementAt(j);
        //  if (fwd.fsd_names.length == 0)
        //    common.failure("rd=default: no FSD specified.");
        //}
      }

      else
      {
        /* Find the FWDs, if none found, this will abort: */
        rd.getFwdsForRd();
      }

      /* If there are no SD overrides we're done with this rd: */
      if (rd.fsd_names.length == 0)
        continue;

      /* Clone the selected FWDs so that we can override the FSDs without */
      /* impacting the original FSDs specified in the FWDs:               */
      Vector new_fwds  = new Vector(4, 0);
      HashMap new_names = new HashMap(8);
      for (int j = 0; j < rd.fwds_for_rd.size(); j++)
      {
        FwdEntry fwd = (FwdEntry) ((FwdEntry) rd.fwds_for_rd.elementAt(j)).clone();
        new_fwds.add(fwd);
        new_names.put(fwd.fwd_name, fwd.fwd_name);
        if (rd.fsd_names.length == 0)
          common.failure("No fsd names specified in rd=" + rd.rd_name);
        fwd.fsd_names = rd.fsd_names;
      }

      /* Now replace the original wd list for this rd: */
      rd.fwds_for_rd = new_fwds;

      /* And set the proper FWD names: */
      rd.fwd_names = (String[]) new_names.values().toArray(new String[0]);
    }
  }


  /**
   * Get thread counts to be used for this SD on the current slave.
   */
  public int getSdThreadsUsedForSlave(String sd_name, Slave slave)
  {
    if (threads_per_slave_map == null)
      return -1;

    String key     = sd_name + "/" + slave.getLabel();
    ArrayList list = threads_per_slave_map.get(key);
    if (list == null)
    {
      //common.ptod("Thread count requested for unknown: '%s'", key);
      return 0;
    }
    return list.size();
  }

  public ArrayList <StreamContext>  getStreamListForSD(String sd_name, Slave slave)
  {
    String key     = sd_name + "/" + slave.getLabel();
    ArrayList list = threads_per_slave_map.get(key);
    if (list == null)
      common.failure("Thread count requested for unknown: '%s'", key);
    return list;
  }

  /**
   * Get thread counts to be used for the current slave.
   */
  public int getThreadsUsedForSlave(Slave slave)
  {
    String[] keys = threads_per_slave_map.keySet().toArray(new String[0]);
    int threads   = 0;

    for (int i = 0; i < keys.length; i++)
    {
      if (keys[i].endsWith("/" + slave.getLabel()))
      {
        ArrayList list = threads_per_slave_map.get(keys[i]);
        threads += list.size();
      }
    }

    return threads;
  }

  public int getThreadsUsedForSD(String sd_name)
  {
    String[] keys = (String[]) threads_per_slave_map.keySet().toArray(new String[0]);
    int threads   = 0;

    //common.ptod("getThreadsUsedForSD: key count: " + keys.length);
    //for (String key : keys)
    //  common.ptod("getThreadsUsedForSD: key: %s threads=%d", key, threads_per_slave_map.get(key).size());

    for (int i = 0; i < keys.length; i++)
    {
      if (keys[i].startsWith(sd_name + "/"))
      {
        threads += threads_per_slave_map.get(keys[i]).size();
      }
    }

    if (threads == 0)
      common.failure("getThreadsUsedForSD: sd=%s not found", sd_name);

    return threads;
  }

  /**
   * Store the thread count to be used for this SD on the requested slave.
   * Initialize the stream number to 'no streams'.
   */
  public void setThreadsUsedForSlave(String sd_name, Slave slave, int threads)
  {
    //common.ptod("setThreadsUsedForSlave: %s %s %2d", sd_name, slave.getLabel(), threads);
    ArrayList <StreamContext> list = new ArrayList(threads);
    for (int i = 0; i < threads; i++)
      list.add(null);
    String key = sd_name + "/" + slave.getLabel();
    threads_per_slave_map.put(key, list);
  }

  /**
   * Return the complete map.
   */
  public HashMap <String, ArrayList <StreamContext> > getThreadsPerSlaveMap()
  {
    return threads_per_slave_map;
  }


  /**
   * Get the threadcount to be used for this SD.
   * It is either the forthreads= or the threads=.
   */
  public int getThreadsFromSdOrRd(SD_entry sd)
  {
    if (current_override.getThreads() == For_loop.NOVALUE)
      return sd.threads;

    return(int) current_override.getThreads();
  }

  public long getWarmup()
  {
    return warmup;
  }
  public void setWarmup(long warm)
  {
    warmup = warm;
  }
  public long getElapsed()
  {
    return elapsed;
  }
  public void setElapsed(long el)
  {
    elapsed = el;
  }
  public void setNoElapsed()
  {
    elapsed = NO_ELAPSED;
  }
  public long getInterval()
  {
    return interval;
  }
  public void setInterval(long in)
  {
    interval = in;
  }

  /**
   * Check to see if there is anything else but sequential EOF writes.
   * This info is used by DV so that we do not abort a run where NO DV validates
   * have been done.
   */
  public boolean checkForSequentialWritesOnly()
  {
    for (WG_entry wg : getAllWorkloads())
    {
      if (wg.seekpct >= 0)
        return false;
      if (wg.readpct != 0)
        return false;
    }
    return true;
  }


  /**
   * Replace the list of RDs with a list of possible new RDs, which take care of
   * the extra sd=xxx options.
   */
  private static Vector <RD_entry> repeatSdSingles()
  {
    Vector <SD_entry> sd_list  = Vdbmain.sd_list;
    Vector <RD_entry> old_rds  = Vdbmain.rd_list;
    Vector <RD_entry> new_rds  = new Vector(old_rds.size());

    /* In case we need to fiddle with 'setsofX', create an SD list */
    /* which is sorted on some random number:                      */
    Vector <SD_entry> rnd_list = new Vector(sd_list.size());
    for (SD_entry sd : sd_list)
      rnd_list.add(sd);

    //for (SD_entry sd : rnd_list)
    //  common.ptod("sd1: " + sd.sd_name);

    /* Swap random SDs 'n*8' times: */
    Random random = new Random(0);
    for (int i = 0; i < rnd_list.size() * 8; i++)
    {
      int rand1 = random.nextInt(rnd_list.size());
      int rand2 = random.nextInt(rnd_list.size());
      if (rand1 == rand2)
        continue;

      SD_entry saved = rnd_list.get(rand1);
      rnd_list.set(rand1, rnd_list.get(rand2));
      rnd_list.set(rand2, saved);
    }

    //for (SD_entry sd : rnd_list)
    //  common.ptod("sd2: " + sd.sd_name);

    for (RD_entry rd : old_rds)
    {
      /* No single sd name? Just copy RD: */
      if (rd.sd_names == null || rd.sd_names.length != 1)
      {
        new_rds.add(rd);
        continue;
      }

      /* Not one of the special ones? Just copy RD: */
      String sdname = rd.sd_names[0];
      if (sdname.equals("single"))
        sdname = "setsof1";

      if (!sdname.equals("range") && !sdname.startsWith("setsof"))
      {
        new_rds.add(rd);
        continue;
      }


      else if (sdname.startsWith("setsof") ||
               common.isNumeric((sdname + "           ").substring(6)))
      {
        String tmp = (sdname + " ").substring(6);
        int    set = Integer.parseInt(tmp.trim());

        Vector <SD_entry> list_to_use = (set == 1) ? sd_list : rnd_list;

        /* Create a new RD for each SD: */
        for (int j = 0; j < list_to_use.size(); j++)
        {
          SD_entry sd  = list_to_use.get(j);
          RD_entry rd2 = (RD_entry) rd.clone();
          String rdname = rd.rd_name;

          ArrayList <String> sds = new ArrayList(8);
          for (int s = 0; s < set && j < list_to_use.size(); j++,s++)
          {
            sds.add(list_to_use.get(j).sd_name);
            rdname += "_" + list_to_use.get(j).sd_name;
          }

          rd2.sd_names = (String[]) sds.toArray(new String[0]);
          rd2.rd_name = rdname;

          new_rds.add(rd2);
          j --;
        }
      }

      else if (sdname.equals("range"))
      {
        for (int j = 0; j < sd_list.size(); j++)
        {
          SD_entry sd_first = sd_list.get(0);
          SD_entry sd_last  = sd_list.get(j);
          RD_entry rd2 = (RD_entry) rd.clone();

          rd2.sd_names = new String[j+1] ;
          rd2.rd_name += "_" + sd_first.sd_name + "-" + sd_last.sd_name;
          for (int k = 0; k < j+1; k++)
          {
            SD_entry sd2 = sd_list.get(k);
            rd2.sd_names[k] = sd2.sd_name;
          }
          new_rds.add(rd2);
        }
      }

      else
        new_rds.add(rd);
    }

    /* Now just replace the list: */
    return new_rds;
  }


  private void useDistribution(Vdb_scan prm)
  {
    for (int i = 0; i < prm.getAlphaCount(); i++)
    {
      String parm = prm.alphas[i].toLowerCase();

      if ("exponential".startsWith(parm))
        distribution = 0;
      else if ("uniform".startsWith(parm))
        distribution = 1;
      else if ("deterministic".startsWith(parm))
        distribution = 2;

      else if ("variable".startsWith(parm))
        variable = true;
      else if ("spike".startsWith(parm))
      {
        variable = true;
        spread = false;
      }

      else
        common.failure("Unknown keyword value: " + prm.alphas[0]);
    }
  }

  private SD_entry[] getSdsForRD()
  {
    HashMap <SD_entry, SD_entry> sd_map = new HashMap(64);
    for (WG_entry wg : getAllWorkloads())
      sd_map.put(wg.sd_used, wg.sd_used);
    return sd_map.values().toArray(new SD_entry[0]);
  }


  /**
   * Print workload debugging info:
   * - Default: don't print
   * - -d80:    print on console + logfile
   * - -d81:    print on logfile
   */
  public static void printWgInfo(String format, Object ... args)
  {
    if (common.get_debug(common.PTOD_WG_STUFF))          // -d80
      common.ptod("WgInfo: " + format, args);

    else if (common.get_debug(common.PLOG_WG_STUFF))     // -d81
      common.plog("WgInfo: " + format, args);
  }


  /**
   * Print workload debugging info:
   * - Default: print on logfile
   * - -d80:    print on console + logfile
   */
  public static void printWgInfo2(String format, Object ... args)
  {
    if (common.get_debug(common.PTOD_WG_STUFF))           // -d80
      common.ptod("WgInfo2: " + format, args);

    else
      common.plog("WgInfo2: " + format, args);
  }


  /**
   * This method is confusing, why not just call Host.getAllWorkloads()
   * immediately?
   * This info is really only properly available for one RD at the time.
   * By keeping it static under RD_entry this then can serve as a little
   * reminder.
   */
  public static ArrayList <WG_entry> getAllWorkloads()
  {
    return Host.getAllWorkloads();
  }


  /**
   * Return SD names used for this RD_entry
   */
  public String[] getSdNamesUsedThisRd()
  {
    HashMap map = new HashMap(16);

    for (WG_entry wg : wgs_for_rd)
    {
      for (String name : wg.getRealSdNames())
        map.put(name, null);
    }

    return(String[]) map.keySet().toArray(new String[0]);
  }



  /**
   * Return SDs used anywhere
   */
  public static String[] getSdNamesUsed()
  {
    HashMap map = new HashMap(16);

    for (RD_entry rd : Vdbmain.rd_list)
    {
      for (WG_entry wg : rd.wgs_for_rd)
      {
        for (String name : wg.getRealSdNames())
          map.put(name, null);
      }
    }

    return(String[]) map.keySet().toArray(new String[0]);
  }


  /**
   * Return SDs used for a host
   */
  public String[] getSdsUsedForHostThisRd(Host host)
  {
    HashMap map = new HashMap(16);

    for (WG_entry wg : wgs_for_rd)
    {
      if (wg.getSlave().getHost() == host)
      {
        for (String name : wg.getRealSdNames())
          map.put(name, null);
      }
    }

    return(String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used for a host
   */
  public static String[] getSdsUsedForHost(Host host)
  {
    HashMap map = new HashMap(16);

    for (RD_entry rd : Vdbmain.rd_list)
    {
      for (WG_entry wg : rd.wgs_for_rd)
      {
        if (wg.getSlave().getHost() == host)
        {
          for (String name : wg.getRealSdNames())
            map.put(name, null);
        }
      }
    }

    return(String[]) map.keySet().toArray(new String[0]);
  }

  public boolean areWeSharingThreads()
  {
    return current_override.sharingThreads();
  }


  /**
   * Threads must be all evenly spread out over slaves.
   * To do that, auto-increase thread count if needed.
   *
   * We have the same with stream count.
   *
   * Do both.
   */
  private int adjustThreadCount(int threads_requested, BoxPrint messages)
  {
    int loop_protection = 0;

    int slave_count       = SlaveList.getSlaveCount();

    while (true)
    {
      if (loop_protection++ > 50)
        common.failure("loop protection");

      /* Adjust stream count to #slaves: */
      if (rd_stream_count != 0 && rd_stream_count % slave_count != 0)
      {
        int new_streams = rd_stream_count + slave_count - (rd_stream_count % slave_count);
        messages.add(String.format("Workload stream count for rd=%s increased from %2d to %2d to make it a " +
                                   "multiple of the number of slaves (%d).",
                                   rd_name, rd_stream_count, new_streams, slave_count));
        rd_stream_count = new_streams;
        continue;
      }


      /* Adjust #threads to #slaves: */
      if (threads_requested % slave_count != 0)
      {
        int new_threads = threads_requested + slave_count - (threads_requested % slave_count);
        messages.add(String.format("Workload thread count for rd=%s increased from %2d to %2d to make it a " +
                                   "multiple of the number of slaves (%d).",
                                   rd_name, threads_requested, new_threads, slave_count));
        current_override.changeThreads(new_threads);
        threads_requested = new_threads;
        continue;
      }


      /* Adjust #streams to #threads: */
      if (rd_stream_count > 0)
      {
        if (threads_requested % rd_stream_count != 0)
        {
          int new_threads = threads_requested + rd_stream_count - (threads_requested % rd_stream_count);

          messages.add(String.format("Workload thread count for rd=%s increased from %2d to %2d to make it a " +
                                     "multiple of the number of streams (%d).",
                                     rd_name, threads_requested, new_threads, rd_stream_count));

          current_override.changeThreads(new_threads);
          threads_requested = new_threads;
          continue;
        }
      }

      /* We fell through, which means we're all happy now. */
      return threads_requested;
    }
  }

  /**
   * Look for a slave already doing the work for a wd+sd
   *
   * Paranoia check: there may be only ONE of these.
   */
  private static Slave findWorkForWdSdCombo(WD_entry wd, SD_entry sd)
  {
    Slave slave = null;
    for (WG_entry wg : getAllWorkloads())
    {
      if (wg.wd_name.equals(wd.wd_name) &&
          wg.sd_used.sd_name.equals(sd.sd_name))
      {
        if (slave != null)
          common.failure("Duplicate WD/SD combination found: %s/%s",
                         wd.wd_name,sd.sd_name);
        slave = wg.getSlave();
      }
    }
    return slave;
  }
  private static Slave findWorkForSd(SD_entry sd)
  {
    for (WG_entry wg : getAllWorkloads())
    {
      if (wg.sd_used.sd_name.equals(sd.sd_name))
        return wg.getSlave();
    }
    return null;
  }
}










