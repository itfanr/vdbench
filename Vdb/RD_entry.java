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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String  rd_name;

  String  wd_names[]         = new String[0];
  String  sd_names[]         = null;
  Vector  wds_for_rd         = new Vector(4, 0);
  Vector  fwds_for_rd        = new Vector(4, 0);

  String  fwd_names[]        = new String[0];
  String  fsd_names[]        = new String[0];
  String  fwd_operations[]   = new String[0];
  double  skew[]             = null;
  boolean foroperations_used = false;
  boolean operations_used    = false;
  Vector  fwgs_for_rd        = new Vector(4, 0);
  double  fwd_rate           = 0;

  FormatFlags format         = new FormatFlags();

  double  compression_rate_to_use = -1;

  Bursts  bursts = null;

  String  rd_mount = null;

  /* Don't ever set a default iorate. Code depends on not having a default!   */
  double  iorate;              /* io rate to generate:                        */
  double  iorate_pct = 0;      /* Contains last requested iorate percentage   */
  double  iorate_req = 0;      /* If negative, take percentage of earlier rate*/

  static  long  NO_ELAPSED = 99999998;
  private long  elapsed      = 30;     /* Duration in seconds for run                 */
  private long  interval     = 1;      /* Reporting interval                          */
  private long  warmup       = 0;

  int   distribution = 0;      /* 0: exponential                              */
                               /* 1: uniform                                  */
                               /* 2: deterministic                            */
  /* Deterministic is very dangerous in multi wd because all the ios could    */
  /* start always at the same time for each wd, or, ios can get completely    */
  /* out of sync because one's start times are mostly smaller than the others */
  /* This problem does NOT happen if we don't use the waiter task.            */
  /* Not sure if this is still relevant. Henk 03/17/09                        */

  long  pause = 0;             /* How long to pause after each run            */


  double curve_points[] = null;

  Vector for_list     = new Vector(4, 0); /* List of requested for loops     */

  Vector wgs_for_rd      = null; /* List of WG entries for this RD  */
  Vector wgs_for_rd_last = null; /* From previous run (for curve)   */

  long     replay_start    = 0;
  long     replay_stop     = Long.MAX_VALUE;

  boolean use_waiter = true;


  static RD_entry next_rd       = null;   /* Set by rd_next_workload().   */
  static Vector   next_do_list  = null;
  static int      next_do_entry = 0;

  For_loop current_override = null;

  boolean doing_curve_max   = false;
  boolean doing_curve_point = false;
  boolean curve_used_waiter = false;

  public  OpenFlags open_flags = null;

  static RD_entry dflt = new RD_entry();

  /* In Vdbench 5.00 this was changed from 99999998 to a smaller value. */
  /* This was done because the inter arrival time used for such a high  */
  /* iorate prevented a decent granularity between different workload's */
  /* io start time. e.g. there were 7000 ios within a 100 usec range,   */
  /* causing it to take a looong time before the waiter task picked up  */
  /* io from a different sd or workload.                                */
  public static int MAX_RATE              = 999988;
  public static int CURVE_RATE            = 999977;


  public Debug_cmds start_cmd = new Debug_cmds(null);
  public Debug_cmds end_cmd   = new Debug_cmds(null);

  public  static    RD_entry recovery_rd  = null;

  public static String replay_filename = null;

  private static int sequence = 0;
  private        int seqno = sequence++;

  private HashMap threads_for_sd_per_slave = null;

  public static String FORMAT_RUN = "format_for_";

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
    String txt_run = rd_name;
    String txt = null;
    String overrides = "";
    if (!rd_name.startsWith(RD_entry.FORMAT_RUN) && current_override.getText() != null)
      overrides = current_override.getText();

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

      if (!rd_name.startsWith(FORMAT_RUN))
        txt += elapsed_txt + fwd_rate_txt + overrides;
    }

    else
    {
      String t2;
      //txt_run = txt_run + ((iorate_pct == 0) ? "" : Format.f("_(%d%%)", (int) iorate_pct) );

      String t1 = Vdbmain.simulate ? "Simulating RD=" : "Starting RD=";
      if (replay_filename != null)
        t2 = Format.f("; I/O rate (Replay): %6d", iorate);
      else if (replay_filename != null)
        t2 = "; I/O rate: (Replay)";
      else if (iorate == MAX_RATE && use_waiter)
        t2 = "; I/O rate: Controlled MAX";
      else if (iorate == MAX_RATE && !use_waiter)
        t2 = "; I/O rate: Uncontrolled MAX";
      else if (iorate == CURVE_RATE && use_waiter)
        t2 = "; I/O rate: Controlled curve";
      else if (iorate == CURVE_RATE && !use_waiter)
        t2 = "; I/O rate: Uncontrolled curve";
      else
        t2 = Format.f("; I/O rate: %d", iorate);

      txt = t1 + txt_run + t2 + elapsed_txt + overrides;
    }


    if (Vdbmain.isWdWorkload() && !Vdbmain.simulate)
      rd_report_parameters_on_flatfile();

    if (Vdbmain.simulate)
    {
      common.ptod(txt);
      common.psum(txt);
    }

    else
      Report.displayRunStart(txt, this);

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
          common.failure("Warning: rd=xx,fsd=yyy parameter does not work with multi-host. TBD");
          rd.fsd_names = prm.alphas;
        }

        else if ("bursts".startsWith(prm.keyword))
          rd.bursts = new Bursts(prm.numerics, false);

        else if ("spread".startsWith(prm.keyword))
          rd.bursts = new Bursts(prm.numerics, true);

        else if ("iorate".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() == 1 && prm.alphas[0].compareTo("curve") == 0)
          {
            rd.iorate_req      = RD_entry.CURVE_RATE;
            rd.doing_curve_max = true;
          }

          else if (prm.getAlphaCount() == 1 &&
                   prm.alphas[0].toLowerCase().compareTo("max") == 0)
            rd.iorate_req = RD_entry.MAX_RATE;

          else
          {
            if (prm.getNumCount() > 1)
              new For_loop("foriorate", prm.numerics, rd.for_list);

            rd.iorate_req = prm.numerics[0];
          }
        }


        else if ("skew".startsWith(prm.keyword))
          rd.skew = prm.numerics;

        else if ("curve".startsWith(prm.keyword))
          rd.curve_points = prm.numerics;

        else if ("elapsed".startsWith(prm.keyword))
        {
          rd.setElapsed((long) prm.numerics[0]);
          if (prm.getNumCount() > 1)
            common.failure("'elapsed=(nn,mm)': second parameter for tape reads no longer supported");
        }

        else if ("interval".startsWith(prm.keyword))
          rd.setInterval((long) prm.numerics[0]);

        else if ("warmup".startsWith(prm.keyword))
          rd.setWarmup((long) prm.numerics[0]);

        else if ("replay".startsWith(prm.keyword))
        {
          Vdbmain.setReplay(true);
          //if (!Vdbmain.isReplay())
          //  common.failure("Replay filename specified in RD=" + rd.rd_name +
          //                 " but no replay information specified in SD and/or RG parameters");
          replay_filename = prm.alphas[0];
          common.ptod("replay_filename: " + replay_filename);
        }

        else if ("select".startsWith(prm.keyword))
        {
          rd.replay_start = (long) prm.numerics[0] * 1000000;
          if (prm.getNumCount() > 1)
            rd.replay_stop = (long) prm.numerics[1] * 1000000;
        }

        else if ("pause".startsWith(prm.keyword))
          rd.pause = (long) prm.numerics[0];

        /* This checks ALL forxxx parameters: */
        else if (For_loop.checkForLoop(rd, prm))
        {
        }

        else if ("distribution".startsWith(prm.keyword))
        {
          if ("exponential".startsWith(prm.alphas[0]))
            rd.distribution = 0;
          else if ("uniform".startsWith(prm.alphas[0]))
            rd.distribution = 1;
          else if ("deterministic".startsWith(prm.alphas[0]))
            rd.distribution = 2;
          else
            common.failure("Unknown keyword value: " + prm.alphas[0]);
        }

        else if ("start_cmd".startsWith(prm.keyword))
        {
          rd.start_cmd = new Debug_cmds(prm.alphas[0]);
          if (prm.getAlphaCount() > 1)
            rd.start_cmd.setTarget(prm.alphas[1]);
        }

        else if ("end_cmd".startsWith(prm.keyword))
        {
          rd.end_cmd = new Debug_cmds(prm.alphas[0]);
          if (prm.getAlphaCount() > 1)
            rd.end_cmd.setTarget(prm.alphas[1]);
        }


        /* FWD specific stuff (fwd must be first in sequence): */
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
          rd.open_flags = new OpenFlags(prm.alphas);

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

    /* Check skew percentages: */
    for (int i = 0; i < rd_list.size(); i++)
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
      //    !Vdbmain.isReplay() && rd.bursts == null)
      //  common.failure("No iorate= specified for rd=" + rd.rd_name);
      if (Vdbmain.isFwdWorkload() && rd.fwd_rate == 0)
        common.failure("No fwdrate= specified for rd=" + rd.rd_name);

      /* Default warmup equals one interval: */
      //if (rd.getWarmup() == 0)
      //  rd.setWarmup(rd.getInterval());

      if (rd.getElapsed() % rd.getInterval() != 0)
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


    /* Determine which WD_entry instances we need, also override SDs if needed: */
    if (Vdbmain.isWdWorkload())
      selectWhichWdsToUse(rd_list);
    else
      selectWhichFwdsToUse(rd_list);
  }


  /**
   * Add a new WD to this RD_entry.
   * If the (non-wildcard) WD is not found, but this WD is found in the
   * /workloads/ directory, save the name for later inclusion.
   */
  private static Vector extra_wds = new Vector(8, 0);
  private void addWD(String[] wds)
  {
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
    WG_entry wg = (WG_entry) next_rd.wgs_for_rd.elementAt(0);
    int    xfersize = wg.xfersize;
    double rhpct    = wg.rhpct;
    double whpct    = wg.whpct;
    double readpct  = wg.readpct;
    double seekpct  = wg.seekpct;

    for (int i = 1; i < next_rd.wgs_for_rd.size(); i++)
    {
      wg = (WG_entry) next_rd.wgs_for_rd.elementAt(i);
      if (xfersize != wg.xfersize)
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
    boolean debug = false;
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

        if (debug) common.ptod("wd_names[i]: " + wd_names[i] + " " + wd.wd_name);
        if ( common.simple_wildcard(wd_names[i], wd.wd_name))
        {
          /* Set skew from parameter input. Could have been changed by 'curve': */
          wd.setSkew(wd.skew_original);

          wds.addElement(wd);
          found = true;
          if (debug) common.ptod(rd_name + " added wd: " + wd.wd_name );
        }
      }

      if (!found)
        common.failure("Could not find WD=" + wd_names[i] + " for RD=" + rd_name);
    }

    wds_for_rd = wds;

    if (wds_for_rd.size() == 0)
      common.failure("No wds_for_rd for rd=" + rd_name);

    /* Check priority settings: */
    WD_entry.checkPriorities(wds);
  }


  /**
   * Get a list of FWDs that are requested for this RD
   */
  private void getFwdsForRd()
  {
    Vector list = new Vector(16, 0);

    /* Scan the complete list of FWDs looking for the ones I need: */
    for (int i = 0; i < fwd_names.length; i++)
    {
      boolean found = false;
      for (int k = 0; k < FwdEntry.getFwdList().size(); k++)
      {
        FwdEntry fwd = (FwdEntry) FwdEntry.getFwdList().elementAt(k);

        if (common.simple_wildcard(fwd_names[i], fwd.fwd_name))
        {
          list.addElement(fwd);
          //common.ptod("getFwdsForRd(): added fwd for rd=" + rd_name + " " + fwd.fwd_name);
          found = true;
        }
      }

      if (!found)
        common.failure("Could not find fwd=" + fwd_names[i] + " for RD=" + rd_name);
    }

    fwds_for_rd = list;
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
    if (common.get_debug(common.ORACLE))
    {
      use_waiter = false;
      return;
    }

    int rc = 0;
    while (true)  // fake while() to avoid using goto.
    {
      /* With replay we MUST use the waiter: */
      if (Vdbmain.isReplay())
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
      if (rd_name.equals(SD_entry.NEW_FILE_FORMAT_NAME))
      {
        use_waiter = false;
        rc = 3;
        break ;
      }

      /* If any skew is defined we may not run an uncontrolled workload: */
      for (int i = 0; i < wds_for_rd.size(); i++ )
      {
        WD_entry wd = (WD_entry) wds_for_rd.elementAt(i);

        /* If any skew or iorate was requested we must use the waiter: */
        if (wd.skew_original != 0 || wd.wd_iorate != 0)
        {
          use_waiter = true;
          rc = 4;
          break;
        }
      }

      if (rc == 0)
      {
        //use_waiter = (next_rd.iorate_req != MAX_RATE && next_rd.iorate_req != CURVE_RATE);
        use_waiter = (iorate != MAX_RATE && iorate != CURVE_RATE);
        rc = 5;
      }

      break;

    }

    //common.plog("will_we_use_waiter: " + use_waiter +
    //            " rc=" + rc + " " + (long) iorate_req + " " + iorate);

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

    for (int i = 0; i < fwds_for_rd.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwds_for_rd.elementAt(i);

      /* Get all fsds needed: */
      FsdEntry[] fsds = fwd.findFsdNames(this);

      /* Figure out how do adjust the skew: */
      double skew_divisor = fwds_for_rd.size() * fsds.length;

      /* What hosts do we want to run this FWD on? */
      Vector run_hosts = Host.findSelectedHosts(fwd.host_names);
      //common.ptod("run_hosts: " + run_hosts.size() + " " + rd_name);

      /* More than one host is not allowed without 'shared=yes': */
      if (run_hosts.size() > 1)
      {
        for (int f = 0; f < fsds.length; f++)
        {
          if (!fsds[f].shared)
            common.failure("Multiple hosts may only be requested for an FSD when "+
                           "'shared=yes' is specified. rd=" + rd_name);
        }
      }


      for (int h = 0; h < run_hosts.size(); h++)
      {
        String host_to_run = (String) run_hosts.elementAt(h);
        //common.ptod("host_to_run: " + rd_name + " " + fwd.fwd_name + " " +  host_to_run);

        /* Create an FwgEntry for each FSD and for each host: */
        for (int f = 0; f < fsds.length; f++)
        {
          FsdEntry fsd = fsds[f];

          /* If there are no operations used in rd=xxx,operations=(..,..) use the */
          /* single operation from this fwd: */
          if (fwd_operations.length == 0)
          {
            fwg = new FwgEntry(fsd, fwd, this, host_to_run, fwd.getOperation());
            fwgs_for_rd.addElement(fwg);

            /* We need to pick up the thread count early: */
            int threads = fwd.threads;
            if (current_override != null && current_override.getThreads() != For_loop.NOVALUE)
              threads = (int) current_override.getThreads();

            /* Now that we create an FwgEntry for each operation, split */
            /* the amount of threads per operation evenly:              */
            fwg.threads = Math.max(1, threads /  fsds.length / fwds_for_rd.size());

            /* Override skew value: */
            if (skew != null)
              fwg.skew = skew[i] / skew_divisor;
          }

          else
          {
            /* rdpct= may only specify operation=read or wrtie, not both (or others) */
            checkRdpctOperations(fwd);

            /* Create one FwgEntry for each operation in RD: */
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
              fwg.threads = Math.max(1, threads / fwd_operations.length / fsds.length);

              /* Override skew value: */
              if (skew != null)
                fwg.skew = skew[j] / skew_divisor;
            }
          }
        }
      }
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



  /**
   * A 'forxxx' RD parameter may override certain values.
   */
  private void forLoopOverrideWd()
  {
    /* Go through all Wg entries: */
    for (int i = 0; i < wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.elementAt(i);
      For_loop.useForOverrides(wg, this, current_override, wg.sd_used, false);
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

      /* Now overlay whatever the 'fwd=format' parameters are: */
      // is never null:
      //if (FwdEntry.format_fwd == null)
      //{
      //  fwg.xfersizes     = new double[] { 64*1024};
      //  fwg.threads       = 8;
      //}
      //else
      {
        fwg.xfersizes     = FwdEntry.format_fwd.xfersizes;
        fwg.threads       = FwdEntry.format_fwd.threads;
        if (FwdEntry.format_fwd.open_flags != null)
          fwg.open_flags    = FwdEntry.format_fwd.open_flags;
      }

      if (rd.open_flags != null)
        fwg.open_flags = rd.open_flags;

      fwg.select_random = false;
      fwg.sequential_io = true;
      fwg.anchor.trackXfersizes(fwg.xfersizes);
      fwg.anchor.trackFileSizes(fwg.filesizes);
    }

    fwd_rate = RD_entry.MAX_RATE;
    setNoElapsed();

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
    format_rd.rd_name        = RD_entry.FORMAT_RUN + rd_name;
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
    for (int i = 0; i < fwgs_for_rd.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_rd.elementAt(i);

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
    return next_rd.rd_name.startsWith(RD_entry.FORMAT_RUN);
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

    /* Before we return, check the xfersizes requested with Data Validation: */
    if (Validate.isValidate())
    {
      Vector anchors = FileAnchor.getAnchorList();
      for (int i = 0; i < anchors.size(); i++)
      {
        FileAnchor anchor = (FileAnchor) anchors.elementAt(i);
        anchor.calculateKeyBlockSize();
        anchor.matchFileAndXfersizes();
      }
    }

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
  public static Vector buildNewRdListForWd(boolean report)
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
      RD_entry.createWgListForOneRd(new_rd, report);
      new_rd.forLoopOverrideWd();
      new_rd.format = rd.format;

      /* Add this RD to the proper place in the list, after the possible format: */
      new_list.add(new_rd);

      /* If this is a curve run, insert the proper amount of extra runs: */
      if (rd.iorate_req == CURVE_RATE)
        new_rd.insertCurveRuns(new_list, report);
    }

    return new_list;
  }



  public static void finalizeWgEntries()
  {
    RD_entry rd = null;
    RD_entry.next_rd = null;


    while (true)
    {
      if ((rd = RD_entry.getNextWorkload()) == null)
        break;

      for (int i = 0; i < rd.wgs_for_rd.size(); i++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(i);

        /* Calculate seek range, either % or bytes: */
        wg.calculateContext(wg.wd_used, wg.sd_used);
      }
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
   * By now allowing Vdbench to split an SD over mutliple slaves (not sequential
   * of course) we will be able to get unlimited random IOPS for an SD.
   *
   * Note: This code is run twice IF it is determined that more iops have
   * been requested that that the default one slave can handle.
   * If ANY jvms= parameter is specified we'll stick with that count though.
   */
  public static void createWgListForOneRd(RD_entry rd, boolean report)
  {
    HashMap sequential_sd_map = new HashMap(4);

    /* Each host gets a HashMap of Vectors containing WG_entry instances, */
    /* using the SD as a key: */
    HashMap host_map = new HashMap(16);

    SD_entry.clearHostMaps();

    /* Preserve possible previous list for curve processing: */
    rd.wgs_for_rd_last = rd.wgs_for_rd;
    rd.wgs_for_rd      = new Vector(16, 0);

    /* Add i/o % to rd name if needed: */
    rd.rd_name += ((rd.iorate_pct == 0) ? "" : Format.f("_(%d%%)", (int) rd.iorate_pct) );

    /* Though the iorate can change later, it still needs to be called here: */
    rd.set_iorate();
    rd.willWeUseWaiterForWG();

    /* Clear work in all slaves: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      host_map.put(host, new HashMap(16));

      for (int k = 0; k < host.getSlaves().size(); k++)
      {
        Slave slave = (Slave) host.getSlaves().elementAt(k);
        slave.wgs_for_slave = new Vector(4, 0);
      }
    }

    /* We first create a list of work for each Host: */

    /* For each requested WD: */
    for (int i = 0; i < rd.wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) rd.wds_for_rd.elementAt(i);

      /* Tape has some special requirements: */
      wd.checkTapeParms();

      /* Get a list of hosts that this workload must run on: */
      String[] wd_hosts = wd.getSelectedHostNames();

      /* For each requested host: */
      for (int j = 0; j < wd_hosts.length; j++)
      {
        Host host = Host.findHost(wd_hosts[j]);

        /* Get a list of SDs that this WD wants on this host.              */
        /* (If this host has not been requested for this SD then of course */
        /* it won't be part of this list)                                  */
        Vector sds = wd.getSdsForHost(host);

        /* And then one per SD: */
        for (int l = 0; l < sds.size(); l++)
        {
          SD_entry sd = (SD_entry) sds.elementAt(l);
          sd.sd_is_referenced = true;
          sd.threads_used     = 0;

          /* Remember which hosts use this SD: */
          sd.hosts_using_sd.put(host, host);

          WG_entry wg  = new WG_entry();
          wg.rd_name   = rd.rd_name;
          wg.host_lun  = host.getLunNameForSd(sd.sd_name);
          wg.sd_used   = sd;
          wg.wd_used   = wd;
          wg.wg_iorate = wd.wd_iorate;
          wg.wd_name   = wd.wd_name;
          wg.wd_open_flags = wd.open_flags;
          wg.setPriority(wd.priority == Integer.MAX_VALUE ? wd.priority : wd.priority - 1);

          wg.storeWorkloadParameters(rd);

          String forxx = rd.current_override.getText();
          forxx = (forxx != null) ? " 'forxx: " + forxx.trim() : "";
          wg.wg_name  = "rd=" + rd.rd_name + ",wd=" + wd.wd_name + ",sd=" + sd.sd_name +
                        ",lun=" + wg.host_lun + forxx +
                        "' " + ((wg.seekpct <=0) ? "seq" : "rnd");

          /* Sequential workloads may only run ONCE: */
          if (wd.seekpct <= 0)
          {
            WG_entry wg2 = (WG_entry) sequential_sd_map.get(sd.sd_name);
            if (wg2 != null)
            {
              /* This message may only be displayed if the user specifically asked */
              /* for more than one host and/or JVM: */
              /* Shall we just not display? */
              if (!rd.rd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME) && report)
              {
                common.ptod("");
                common.ptod("Only one 100% sequential workload against any SD allowed, " +
                            "so only one will be active.");
                common.ptod("wd=" + wg2.wd_used.wd_name + ",sd=" + sd.sd_name +
                            " will be run on only one slave");
              }
              continue;
            }

            sequential_sd_map.put(sd.sd_name, wg);
          }

          /* This Vector holds all WG_entry instances for an SD for a host: */
          Vector sds_on_host = (Vector) ((HashMap) host_map.get(host)).get(sd);
          if (sds_on_host == null)
            ((HashMap) host_map.get(host)).put(sd, sds_on_host = new Vector(4, 0));

          sds_on_host.add(wg);
        }
      }
    }

    /* Blank line before first createWgListForOneRd message: */
    if (report) common.plog("");

    /* Now give those workloads to the proper slaves: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);

      /* Get list of SDs for this host: */
      HashMap sds_on_host = (HashMap) host_map.get(host);
      if (sds_on_host == null)
        continue;


      /* Pass the workloads for these SDs to the slaves. */
      /* - each slave gets all SDs.                */
      /* - sequential only to ONE slave.           */
      SD_entry[] sds = (SD_entry[]) sds_on_host.keySet().toArray(new SD_entry[0]);
      Arrays.sort(sds, new SdSort());

      int robin = 0;
      for (int j = 0; j < sds.length; j++)
      {
        SD_entry sd = (SD_entry) sds[j];

        /* Get a list of WGs for this host that use this SD: */
        Vector wgs_for_sd_on_host = (Vector) sds_on_host.get(sd);
        for (int k = 0; k < wgs_for_sd_on_host.size(); k++)
        {
          WG_entry wg = (WG_entry) wgs_for_sd_on_host.elementAt(k);

          /* Give this WG_entry to each slave (with limits below): */
          Vector slaves = host.getSlaves();
          int l = (wg.seekpct <= 0) ? (robin % slaves.size()) : 0;
          for (; l < slaves.size(); l++)
          {
            Slave slave = (Slave) slaves.elementAt(l);

            /* Make sure that a sequential workload is only requested once: */
            if (rd.thisSeqWdAlreadyUsed(wg))
              break;


            WG_entry wg_clone = (WG_entry) wg.clone();
            wg_clone.slave    = slave;
            wg_clone.wg_name  = slave.getLabel() + " " + wg.wg_name;
            rd.wgs_for_rd.add(wg_clone);

            /* This slave will run this requested workload: */
            /* Though spreadThreads() may remove it again. */
            slave.wgs_for_slave.add(wg_clone);

            /* This slave now has work. Increment round-robin: */
            robin++;

            //if (wg.seekpct <= 0 && report)
            //  common.ptod("sequential workload for sd=" + sd.sd_name + " to slave=" + slave.getLabel());

            /* Removed this. Just too much output. */
            //if (report) common.plog("createWgListForOneRd: " + wg_clone.wg_name );
          }
        }
      }
    }

    /* Blank line after last createWgListForOneRd message: */
    if (report) common.plog("");

    rd.spreadThreads(report);
    rd.checkWdUsed();


    /* For each occurence of a WD_entry we must adjust the skew.   */
    /* e.g. if wd1 has skew 50 and we have the workload run in two */
    /* slaves, each slave gets 50/2=25%                            */
    /* This must be repeated at the last moment before each run starts */
    HandleSkew.spreadWdSkew(rd);
    HandleSkew.spreadWgSkew(rd);
    HandleSkew.calcLeftoverWgSkew(rd);


    /* When using the default JVM count we may have to adjust it: */
    /* Note: the adjustment can happen for ANY run. This means that we have */
    /* to look at ALL runs to make sure we do this right. We therefore      */
    /* can not stop after one adjustment, meaning we have to go through     */
    /* this method twice for every run.                                     */
    if (!Host.anyJvmOverrides() &&
        !Validate.isValidate() &&
        !SD_entry.isTapeTesting() &&
        !Vdbmain.isReplay())
      WG_entry.adjustJvmCount(rd);
  }


  /**
   * Spread the amount of requested threads for an SD over the hosts and JVMs
   * that want to use this SD.
   *
   * Note: having 'robin_slave' start with zero means that we'll always have
   * at least ONE thread running on slave-0 on the host. This is needed since the
   * first slave on a host always is used to collect Kstat and cpu statistics.
   * If there is no work on that first slave, the current code will not request
   * any statistics from that host and we'll therefore not get kstat and cpu
   * statistics.
   */
  private void spreadThreads(boolean report)
  {
    boolean debug = common.get_debug(common.DEBUG_SPREAD) && report;
    int robin_host = 0;
    int robin_slave = 0;

    if (debug) common.plog("spreadThreads for rd=" + rd_name);

    /* Clear current thread information: */
    threads_for_sd_per_slave = new HashMap(32);

    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);

      /* How many hosts are using this SD in this run? */
      Vector hosts = countHostsForSd(sd);
      if (hosts.size() == 0)
        continue;

      /* How many threads do we want for this SD: */
      int threads_requested = getThreadsFromSdOrRd(sd);

      /* Round-robin these threads through all hosts: */
      int[] threads_for_host = new int[hosts.size()];
      for (int j = 0; j < threads_requested;)
      {
        int round = robin_host++ % hosts.size();
        Host host = (Host) hosts.elementAt(round);
        if (isSdUsedOnHost(host, sd))
        {
          threads_for_host[ round ]++;
          j++;
        }
      }

      // report:
      for (int j = 0; debug && j < hosts.size(); j++)
      {
        Host host = (Host) hosts.elementAt(j);
        common.plog("spreadThreads host: " + sd.sd_name + " " + host.getLabel() + " " + threads_for_host[j]);
      }

      /* Now we round-robin all host threads through the slaves of those hosts: */
      for (int j = 0; j < hosts.size(); j++)
      {
        Host host     = (Host) hosts.elementAt(j);
        Vector slaves = host.getSlaves();
        int[] threads_for_slave = new int[slaves.size()];
        for (int l = 0; l < threads_for_host[j];)
        {
          int round = robin_slave++ % slaves.size();
          Slave slave = (Slave) slaves.elementAt(round);
          if (isSdUsedOnSlave(slave, sd))
          {
            threads_for_slave[ round ]++;
            l++;
          }
        }

        /* Preserve the thread counts: */
        for (int k = 0; k < slaves.size(); k++)
        {
          Slave slave = (Slave) slaves.elementAt(k);
          setThreadsUsedForSlave(sd, slave, threads_for_slave[k]);
        }

        // report
        for (int k = 0; debug && k < slaves.size(); k++)
        {
          Slave slave = (Slave) slaves.elementAt(k);
          common.plog("spreadThreads slave: " + sd.sd_name + " " +
                      slave.getLabel() + " " + getThreadsUsedForSlave(sd, slave));
        }
      }
    }

    /* Now remove all WGs that have a zero thread count for its SD: */
    Vector wgs = wgs_for_rd;
    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs.elementAt(i);
      if (getThreadsUsedForSlave(wg.sd_used, wg.slave) == 0)
      {
        wgs.set(i, null);
        if (debug) common.plog("WG_entry removed: " + wg.wd_name + " " +
                               wg.slave.getLabel() + " " +
                               wg.sd_used.sd_name + " " + getThreadsUsedForSlave(wg.sd_used, wg.slave));
      }
    }
    while (wgs.remove(null));

    /* Report: */
    Vector hosts = Host.getDefinedHosts();
    for (int i= 0; i < hosts.size(); i++)
    {
      Host host     = (Host) hosts.elementAt(i);
      Vector slaves = host.getSlaves();
      int total     = 0;
      for (int j = 0; j < slaves.size(); j++)
      {
        Slave slave = (Slave) slaves.elementAt(j);
        int threads = getThreadsUsedForSlave(slave);
        total += threads;
        if (debug) common.plog("Threads for rd=" + rd_name + " " + slave.getLabel() + ": " + threads);
      }
      //if (report) common.plog("Total threads for rd=" + rd_name + ": " + total);
    }

    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      //if (report) common.plog("Threads for sd=" + sd.sd_name + " " + getThreadsUsedForSd(sd));
    }

    //common.ptod("threads_for_sd_per_slave1: " + threads_for_sd_per_slave);
  }




  private boolean isSdUsedOnHost(Host host, SD_entry sd)
  {
    for (int i = 0; i < wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.elementAt(i);
      //common.ptod("wg.slave.getHost(): " + wg.slave.getHost() + " " + host +
      //            " " + wg.sd_used.sd_name + " " + sd.sd_name);
      if (wg.slave.getHost() == host && wg.sd_used == sd)
        return true;
    }
    return false;
  }

  private boolean isSdUsedOnSlave(Slave slave, SD_entry sd)
  {
    for (int i = 0; i < wgs_for_rd.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.elementAt(i);
      //common.ptod("wg.slave.getHost(): " + wg.slave.getHost() + " " + host +
      //            " " + wg.sd_used.sd_name + " " + sd.sd_name);
      if (wg.slave == slave && wg.sd_used == sd)
        return true;
    }
    return false;
  }

  private void checkWdUsed()
  {
    boolean debug = false;
    for (int i = 0; i < wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) wds_for_rd.elementAt(i);
      if (debug) common.ptod("checkWdUsed wd: " + wd.wd_name);
    }

    for (int j = 0; j < wgs_for_rd.size(); j++)
    {
      WG_entry wg = (WG_entry) wgs_for_rd.elementAt(j);
      if (debug) common.ptod("checkWdUsed wg: " + wg.wd_used.wd_name);
    }


    for (int i = 0; i < wds_for_rd.size(); i++)
    {
      WD_entry wd = (WD_entry) wds_for_rd.elementAt(i);

      if (debug) common.ptod("scanning for wd=" + wd.wd_name);
      //if (debug) common.ptod("wgs_for_rd.size(): " + wgs_for_rd.size());

      int used = 0;
      for (int j = 0; j < wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) wgs_for_rd.elementAt(j);
        if (debug) common.ptod("wd: " + wd.wd_name + " wg.wd_used: " +
                               wg.wd_used.wd_name + " wd.wd_name" + wd.wd_name +
                    " rd: " + rd_name);
        if (wg.wd_used == wd)
        {
          if (debug) common.ptod("used");
          used++;
        }
      }

      if (used == 0)
      {
        common.failure("rd=" + rd_name + ",wd=" + wd.wd_name +
                       " not used. Could it be that more hosts "+
                       "have been requested than there are threads? Or that you "+
                       " asking for multiple sequential workloads against the same device?");
      }
    }
  }

  /**
   * Count the number of hosts that want to use a specific SD.
   */
  private Vector countHostsForSd(SD_entry sd)
  {
    HashMap hosts_for_sd = new HashMap(8);
    Vector wgs = wgs_for_rd;
    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs.elementAt(i);
      if (wg.sd_used == sd)
        hosts_for_sd.put(wg.slave.getHost(), null);
    }

    return new Vector(hosts_for_sd.keySet());
  }

  /**
   * Count the number of slaves on a host that want to use a specific SD.
   */
  private Vector countSlavesForSd(Host host, SD_entry sd)
  {
    HashMap slaves_for_sd = new HashMap(8);
    Vector wgs = wgs_for_rd;
    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs.elementAt(i);
      if (wg.sd_used == sd && wg.slave.getHost() == host)
        slaves_for_sd.put(wg.slave, null);
    }

    return new Vector(slaves_for_sd.keySet());
  }

  /**
   * Check to see if the requested WD_entry is already in use for a slave.
   * This is needed to prevent a sequential workload from being serviced
   * by more than one slave, making this workload invalid.
   */
  private boolean thisSeqWdAlreadyUsed(WG_entry wg)
  {
    //if (!rd_name.startsWith(SD_entry.NEW_FILE_FORMAT_NAME))
    {
      if (!(wg.wd_used.seekpct <= 0))
        return false;
    }

    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      for (int j = 0; j < slave.wgs_for_slave.size(); j++)
      {
        WG_entry wg2 = (WG_entry) slave.wgs_for_slave.elementAt(j);
        if (wg2.wd_used == wg.wd_used && wg2.sd_used == wg.sd_used)
        {
          if (!wg2.wd_used.seq_workload_warning_given)
            common.plog("Sequential workload for wd=" + wg.wd_used.wd_name +
                        " may run on only ONE slave. " +
                        "It will run on slave=" + slave.getLabel());
          wg2.wd_used.seq_workload_warning_given = true;
          return true;
        }
      }
    }
    return false;
  }


  private void insertCurveRuns(Vector new_list, boolean report)
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
        RD_entry.createWgListForOneRd(rd, report);
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

      /* If SDs are requested but no WDs, use 'default': */
      if (rd.sd_names != null && rd.wd_names.length == 0)
      {
        rd.wds_for_rd.removeAllElements();
        rd.wds_for_rd.add(WD_entry.dflt);
        common.ptod("No Workload Definitions defined for rd=" + rd.rd_name +
                    "; using wd=default instead");
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

  public int getThreadsUsedForSlave(SD_entry sd, Slave slave)
  {
    String key = sd.sd_name + "/" + slave.getLabel();
    Integer count = (Integer) threads_for_sd_per_slave.get(key);
    if (count == null)
      common.failure("Thread count requested for unknown slave: " + key);
    return count.intValue();
  }

  public int getThreadsUsedForSlave(Slave slave)
  {
    String[] keys = (String[]) threads_for_sd_per_slave.keySet().toArray(new String[0]);
    int threads = 0;

    for (int i = 0; i < keys.length; i++)
    {
      if (keys[i].endsWith("/" + slave.getLabel()))
      {
        Integer count = (Integer) threads_for_sd_per_slave.get(keys[i]);
        threads += count.intValue();
      }
    }

    return threads;
  }

  public int getThreadsUsedForSd(SD_entry sd)
  {
    String[] keys = (String[]) threads_for_sd_per_slave.keySet().toArray(new String[0]);
    int threads = 0;

    for (int i = 0; i < keys.length; i++)
    {
      if (keys[i].startsWith(sd.sd_name + "/"))
      {
        Integer count = (Integer) threads_for_sd_per_slave.get(keys[i]);
        threads += count.intValue();
      }
    }

    return threads;
  }

  public void setThreadsUsedForSlave(SD_entry sd, Slave slave, int c)
  {
    String key    = sd.sd_name + "/" + slave.getLabel();
    Integer count = new Integer(c);
    threads_for_sd_per_slave.put(key, count);
  }
  public HashMap getThreadMap()
  {
    //common.ptod("threads_for_sd_per_slave2: " + threads_for_sd_per_slave);

    return threads_for_sd_per_slave;
  }


  /**
   * Get the threadcount to be used for this SD.
   * It is either the forthreads= or the threads=.
   */
  public int getThreadsFromSdOrRd(SD_entry sd)
  {
    int thread_count = (int) current_override.getThreads();
    thread_count = (current_override.getThreads() == For_loop.NOVALUE) ? sd.threads : thread_count;
    return thread_count;
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


  public static void main(String[] args)
  {
    int thread_count = 3;
    int slaves = 2;

    /* Calculate how many threads this slave gets: */
    double threads = (double) thread_count / slaves;
    common.ptod("threads: " + threads);
    threads = (int) (threads + .999);
    common.ptod("threads: " + threads);
    common.ptod("threads: " + (int) threads);
  }
}



