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
import java.io.PrintWriter;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import Utils.*;
import Utils.Message;
import Utils.Fput;
import Utils.Fget;
import Utils.Semaphore;
import VdbGui.VDBenchGUI;



/**
 * Main method for vdbench workload Generator, see main() for overview.
 */
public class Vdbmain
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  static Vector sd_list  = new Vector(64, 0); /* List of Storage Definitions  */
  static Vector wd_list  = new Vector(64, 0); /* List of Workload Definitions */
  static Vector rd_list  = new Vector(64, 0); /* List of Run Definitions      */

  private static boolean workload_done;
  private static Semaphore workload_done_semaphore = new Semaphore(0);

  static boolean   fwd_workload = false;
  static int       gui_port   = 0;
  static GuiServer gui_server = null;

  static boolean simulate       = false;
  static String  output_dir     = "output";
  static PrintWriter parms_report;

  static boolean slaves_started = false;

  static double observed_iorate = 10000; /* Value calculated from vdb stats   */
  static double last_curve_max  = 10000;

  static int elapsed  =  0;
  static int warmup   =  0;
  static int interval =  0;



  private static boolean replay = false;

  static boolean force_fsd_cleanup = false;
  static boolean force_format_only = false;
  static boolean force_format_no   = false;
  static boolean force_format_yes  = false;
  static boolean loop_all_runs     = false;

  static boolean checking_for_errors = true;

  static int IOS_PER_JVM  = 5000;
  static int DEFAULT_JVMS = 8;

  private static Vector parm_files;


  static boolean kstat_console = false;


  /* Unique run time value to make sure we don't fiddle with */
  /* old masters and slaves                                  */
  private static String run_time = new SimpleDateFormat("yyMMdd-HH.mm.ss.SSS" ).format(new Date());

  public static String getRunTime()
  {
    return run_time;
  }


  public static void setReplay(boolean bool)
  {
    replay = bool;
  }
  public static boolean isReplay()
  {
    return replay;
  }

  public static boolean isWorkloadDone()
  {
    return workload_done;
  }
  public static void setWorkloadDone(boolean bool)
  {
    workload_done = bool;
    if (workload_done)
      workload_done_semaphore.release();
  }
  public static void waitWorkloadDone()
  {
    try
    {
      workload_done_semaphore.acquire();
    }
    catch (InterruptedException e)
    {
    }
  }


  public static void print_property(String prop)
  {
    common.plog(Format.f("%-30s", prop) + System.getProperty(prop));
  }

  /**
   * Print Java properties
   */
  public static void print_system()
  {
    print_property("java.vendor");
    //print_property("java.vendor.url");
    print_property("java.home");
    print_property("java.vm.specification.version");
    //print_property("java.vm.specification.vendor");
    //print_property("java.vm.specification.name");
    print_property("java.vm.version");
    print_property("java.vm.vendor");
    //print_property("java.vm.name");
    print_property("java.specification.version");
    //print_property("java.specification.vendor");
    //print_property("java.specification.name");
    print_property("java.class.version");
    print_property("user.name");
    print_property("user.dir");
    print_property("java.class.path");
    print_property("os.name");
    print_property("os.arch");
    print_property("sun.arch.data.model");
    print_property("os.version");
    //print_property("file.separator");
    //print_property("path.separator");
    //print_property("line.separator");
    //print_property("user.home");
    //print_property("user.dir");
  }

  /**
   * Open some reporting files
   */
  public static void open_print_files()
  {
    Report summary_report = Report.getSummaryReport();
    ErrorLog.create();

    summary_report.printHtmlLink("Link to errorlog", "errorlog", "errorlog");
    summary_report.printHtmlLink("Link to flatfile", "flatfile", "flatfile");
    if (Vdbmain.isFwdWorkload())
    {
      summary_report.printHtmlLink("Link to anchor status", "anchors", "anchors");
      FwdStats.defineNamedData(output_dir);
    }

    String[] debug_targets = Debug_cmds.getTargets();
    for (int i = 0; i < debug_targets.length; i++)
      summary_report.printHtmlLink("Start_cmd/end_cmd output",
                                   debug_targets[i], debug_targets[i]);
  }

  /**
   * Concatenate parameters that do not have a '-' to the previous one.
   * We only do that once. As soon as we hit a standalone ' - ' we stop.
   * (Everything following the standalone '-' goes to parmfile).
   * This was written to allow any parameter to be either -xYY or -x YY
   */
  private static void check_args(String args[])
  {
    String newargs[] = new String[1024];
    int out = 0;
    boolean concatenated = false;
    boolean standalone_dash = false;

    for (int i = 0; i < args.length; i++)
    {
      if (i == 0                    ||
          args[i].startsWith("-")   ||
          concatenated              ||
          standalone_dash           ||
          !newargs[out-1].startsWith("-") ||
          (newargs[out-1].startsWith("-f") && newargs[out-1].length() != 2) ||
          (newargs[out-1].startsWith("-o") && newargs[out-1].length() != 2) )
      {
        if (args[i].compareTo("-") == 0)
          standalone_dash = true;
        newargs[out++] = args[i];
        concatenated = false;
      }
      else
      {
        newargs[out-1] = newargs[out-1] + args[i];
        concatenated = true;
      }
    }

    String nargs[] = new String[out];
    System.arraycopy(newargs, 0, nargs, 0, out);

    checkJavaVersion();

    scan_args(nargs);

    /* Open summary file as early as possible: */
    common.summ_html = Report.createHmtlFile("summary.html", "Vdbench summary report:");
    displayBetaWarning();
    Report.setSummaryReport(new Report(common.summ_html, "summary", "common.summ_html"));
    Report.getSummaryReport().printHtmlLink("Link to logfile", "logfile", "logfile");

    //warning!!!

    print_system();

    //Utils.ClassPath.reportVdbenchScript();
  }


  /**
   * Scan execution parameters
   */
  public static void scan_args(String args[])
  {
    parm_files = new Vector(8, 0);

    /* First scan looks only for -o parameter so that we can get the name: */
    for (int i = 0; i < args.length; i++)
    {
      String thisarg = args[ i ];
      if (thisarg == null)
        break;

      if (thisarg.startsWith("-o") )
      {
        output_dir = thisarg.substring(2);
        break;
      }
      else if (thisarg.compareTo("-s") == 0 )
        simulate = true;
    }

    if (simulate)
    {
      common.ptod("Changed output directory name from '" + output_dir +
                  "' to '" + output_dir + ".simulate'");
      output_dir = output_dir + ".simulate";
    }

    /* Create requested directory. Delete files inside if it already exists: */
    output_dir      = reporting.rep_mkdir(output_dir);
    Validate.setOutput(output_dir);
    common.log_html = Report.createHmtlFile("logfile");
    Report.setLogReport(new Report(common.log_html, "logfile", "common.log_html"));
    Utils.common.setPlog(common.log_html);

    /* Scan through all arguments: */
    boolean scan_for_parmfile = false;
    boolean inline_parms_found = false;

    /* Do a very quick test to a temp file if requested: */
    for (int i = 0; i < args.length; i++)
    {
      if (args[i].startsWith("-t"))
        args[i] = "-f" + createQuickTest(args[i]);
    }

    /* Scan all parameters: */
    for (int i = 0; i < args.length; i++)
    {
      String thisarg = args[ i ];

      if (thisarg == null)
        break;

      /* Obfuscation of '-expired' */
      String tmpe = "pir"; tmpe = "-" + "ex" + tmpe + "ed";

      common.ptod("input argument scanned: '" + thisarg + "'");

      /* -f parameter allows multiple filenames: */
      if (thisarg.startsWith("-f") )
      {
        parm_files.add(thisarg.substring(2));
        scan_for_parmfile = true;
        continue;
      }

      else if (scan_for_parmfile && !thisarg.startsWith("-") )
      {
        parm_files.add(thisarg);
        continue;
      }
      scan_for_parmfile = false;

      if (thisarg.startsWith("-o"))  // we already did '-o'
      {
      }

      else if (thisarg.startsWith("-j"))
      {
        Validate.setValidate();
        Validate.setJournaling();

        if (thisarg.indexOf("r") != -1)  Validate.setJournalRecovery();
        if (thisarg.indexOf("n") != -1)  Validate.setNoJournalFlush();
        if (thisarg.indexOf("m") != -1)  Validate.setMapOnly();
        if (thisarg.indexOf("o") != -1)  Validate.setRecoveryOnly();
      }

      else if (thisarg.startsWith("-d") )
        common.set_debug(Integer.valueOf(thisarg.substring(2)).intValue());

      else if (thisarg.startsWith("-v"))
      {
        Validate.setValidate();
        if (thisarg.indexOf("r") != -1) Validate.setImmediateRead();
        if (thisarg.indexOf("w") != -1) Validate.setNoPreRead();
        if (thisarg.indexOf("t") != -1) Validate.setStoreTime();
      }

      else if (thisarg.startsWith("-c"))
      {
        if /**/    (thisarg.indexOf("o") != -1) force_format_only = true;
        else if (thisarg.indexOf("y") != -1) force_format_yes  = true;
        else if (thisarg.indexOf("n") != -1) force_format_no   = true;
        else /*                         */   force_fsd_cleanup = true;
      }

      else if (thisarg.compareTo("-l") == 0 )
        loop_all_runs = true;

      else if (thisarg.compareTo("-s") == 0 )
        simulate = true;

      else if (thisarg.compareToIgnoreCase(tmpe) == 0)
        common.set_debug(Utils.common.TMP_TEST255);

      else if (thisarg.startsWith("-e"))
        elapsed = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-w"))
        warmup = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-i"))
        interval = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-g"))
        gui_port = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-p"))
        SlaveSocket.setMasterPort(Integer.valueOf(thisarg.substring(2)).intValue());

      else if (thisarg.startsWith("-mb"))
        Report.setDecimal();

      else if (thisarg.startsWith("-m"))
        Host.jvmcount_override = Integer.parseInt(thisarg.substring(2));

      else if (thisarg.compareTo("-k") == 0 )
        Vdbmain.kstat_console = true;

      else if (thisarg.compareTo("-") == 0)
      {
        Vdb_scan.xlate_command_line(args, i);
        inline_parms_found = true;
        break;
      }

      else
      {
        usage();
        common.failure("Invalid execution parameter: '" + thisarg + "'");
      }
    }

    if (parm_files.size() == 0 && !inline_parms_found)
    {
      usage();
      common.ptod("No input parameters specified: ");
    }

  }

  /*Complex run: ./vdbench [- fxxx yyy zzz] [-o xxx] [-j] [-jr] [-jm] [-jn] [-jro] [-c] [-l] [-v] [-vr] [-vw] [-vt] [-s] [-k] [-e nn] [-i nn] [-w nn ] [-m nn] [-p nnnn] [-t] [-l][ - ]
  */

  public static void usage()
  {
    common.ptod("");
    common.ptod(" Usage: ");
    common.ptod(" ./vdbench vdbench [- fxxx yyy zzz] [-o xxx] [-c] [-l] [-s] [-k] [-e nn] [-i nn] [-w nn ] [-m nn] [-v] [-vr] [-vw] [-vt] [-j] [-jr] [-jm] [-jn] [-jro]  [-p nnnn] [-t] [-l][ - ]");
    common.ptod(" ");
    common.ptod(" '-t'        Run quick hardcoded test workload aainst a small temp file. ");
    common.ptod(" '-f xx yy'  Vdbench parameter file name(s). ");
    common.ptod(" '-o xxx'    Output directory for reporting. Default 'output' in current directory");
    common.ptod(" '-c'        Clean (delete) existing FSD file structure");
    common.ptod(" '-l'        Endless loop. Start again at first run when the last one is done");
    common.ptod(" '-s'        Simulate execution. Do everything but I/O. ");
    common.ptod(" '-k'        Solaris only: Report kstat statistics on console. ");
    common.ptod(" '-e nn'     Override elapsed=seconds. ");
    common.ptod(" '-i nn'     Override interval=seconds. ");
    common.ptod(" '-w nn'     Override wamrup=seconds. ");
    common.ptod(" '-m nn'     Modify JVM count used. ");
    common.ptod(" '-v'        Activate Data Validation. ");
    common.ptod(" '-vr'       Activate Data Validation with immediate read-after-write. ");
    common.ptod(" '-vw'       Activate Data Validation without read-before-rewrite. ");
    common.ptod(" '-vt'       Activate Data Validation, also record last time written for each block. ");
    common.ptod(" '-j'        Activate Data Validation with Journaling. ");
    common.ptod(" '-jr'       Recover existing Journal, Validate data and run workload ");
    common.ptod(" '-jro'      Recover existing Journal, Validate data and do not run workload ");
    common.ptod(" '-jm'       Activate Data Validation, but only write maps, not before/after records. ");
    common.ptod(" '-jn'       Activate Data Validation with Journaling, but use asynchronous writes. ");
    common.ptod(" '-p nn'     Override java socket number used. ");
    common.ptod(" ");
    common.ptod(" ' - '       Ignore parameter file, take everything from command line after this ' - '");
    common.ptod(" ");
    common.ptod(" ");
    common.ptod("See the /examples/ directory for some sample parameter files");
    common.ptod("  ");
    common.ptod("  ");
    common.ptod(" ./vdbench [compare] [gui] [dvpost] [edit] [jstack] [parse] [print] [sds] [rsh]");
    common.ptod(" ");
    common.ptod(" 'compare'   Compare workloads");
    common.ptod(" 'gui'       Demonstration GUI (contains on very small subset of Vdbench functionality)");
    common.ptod(" 'dvpost'    Data Validation post-processing");
    common.ptod(" 'edit'      Primitive full screen editor: ./vdbench edit file/name");
    common.ptod(" 'jstack'    Print current Java execution stacks");
    common.ptod(" 'parse'     Flatfile parser");
    common.ptod(" 'print'     Print any block anywhere");
    common.ptod(" 'sds'       Generate SD parameters using current disk configuration");
    common.ptod(" 'rsh'       Vdbench RSH daemon.");
    common.ptod("  ");
    common.ptod("For documentation, browse file 'vdbench.pdf' ");
    common.ptod("  ");
    common.ptod("For revision updates:");
    common.ptod("(Sun internal only): http://webhome.sfbay/nwsspe/speweb/vdbench/index.html");
    common.ptod("or  ");
    common.ptod("(External to Sun:    http://vdbench.org  ");
    common.ptod("\n\n\n\n ");
  }


  /**
   * Main method for vdbench
   * <pre>
   * SD, WD, and RD input parameters are all stored in three vectors.
   * RD entries are taken one at the time. The requested WDs are found, and
   * the requested SDs.
   *
   * For each WD/combination a Workload Generator entry and WG_task is created.
   * For each thread for each SD an IO_task is created.
   *
   * The Waiter Task (WT_task) and the Reporter Task (Reporter) is started.
   *
   * The end result is that there is one WG task for each WD, and one IO task
   * for each thread for each SD.
   *
   * The Workload Generator task (WG_task) generates one Cmd_entry for each
   * i/o generated using the parameters specified.
   * The Cmd entries are placed in a FIFO list (one per WG). The FIFO list of
   * all Workload Generators are read and merged on timestamp by the Waiter task.
   * The Waiter task waits for the correct time of day for the lowest timestamp
   * found in the FIFO lists.
   * When the correct time of day arrives, the Cmd entry is sent to a FIFO list
   * for the specific SD. If multiple threads are used for an SD, each IO_task
   * started for that SD willpick up CMD entries from the fifo.
   *
   * The IO_tasks start the i/o and report statistics.
   * The Reporter task every 'interval' seconds will print these statistics.
   *
   */
  public static void main(String args[])
  {
    String[] original_args = args;
    String next;

    Thread.currentThread().setName("Vdbmain");

    /* First make sure we don't just try to run a utility: */
    if (Utils.Util.checkUtil(args))
      return;

    if (args.length == 0)
    {
      usage();
      common.failure("No input parameters specified");
    }

    try
    {
      /* If this is a request for SlaveJvm, pass it along: */
      for (int i = 0; i < args.length; i++)
      {
        if (args[i].equals("SlaveJvm"))
        {
          SlaveJvm.main(args);
          return;
        }
      }

      /* If this is a Gui start, do so: */
      if (checkForGui(args))
        return;

      /* Is this one of the special functions? */
      if (args.length > 0 && anySpecialTricks(args))
        return;

      /* Start interceptor: */
      Ctrl_c.activateShutdownHook();
      Shutdown.activateShutdownHook();

      common.stdout = new PrintWriter(System.out, true);
      System.out.println("\n\nVdbench distribution: " + Utils.Dist.version);
      System.out.println("For documentation, see 'vdbench.pdf'.");
      System.out.println();


      /* Get execution parameters:   */
      check_args(args);
      RshDeamon.readPortNumbers();

      /* Parse everything we've got: */
      parms_report = Report.createHmtlFile("parmscan.html");
      Vdb_scan.copy_file = Report.createHmtlFile("parmfile.html");
      Report.getSummaryReport().printHtmlLink("Copy of input parameter files",
                                              "parmfile", "parmfile");
      Report.getSummaryReport().printHtmlLink("Copy of parameter scan detail",
                                              "parmscan", "parmscan");

      Vdb_scan.Vdb_scan_read(parm_files, true);
      readParameterFiles();

      /* If there are any missing WDs, read and parse them now: */
      // Decided that for Vdbench to start looking like the filebench
      // 'personality' stuff would be a serious degrade of the usability of
      // Vdbench and I therefore decided to NOT do this.
      if (RD_entry.getExtraWDs().size() > 0)
      {
        Vdb_scan.Vdb_scan_read(RD_entry.getExtraWDs(), false);
        readParameterFiles();
      }

      /* Check some RD stuff that still needs to be done: */
      RD_entry.parserCleanup(rd_list);

      /* Connect to Gui if needed: */
      Vdbmain.connect_gui();

      Flat.createFlatFile();
      Flat.define_column_headers();

      if (common.get_debug(common.DV_UNIQUE_SD))
        DV_map.create_unique_dv_names(sd_list);


      masterRun();

      if (!Vdbmain.simulate)
      {
        common.ptod("Vdbench execution completed successfully. Output directory: " + output_dir);
        common.ptod("Vdbench execution completed successfully", common.summ_html);
      }
      else
      {
        RD_entry.displaySimulatedRuns();
        common.ptod("Vdbench simulation completed successfully. Output directory: " + output_dir);
        common.ptod("Vdbench simulation completed successfully", common.summ_html);
      }

      /* Stop Adm message scan: */
      Adm_msgs.terminate();
      System.out.flush();

      Tnfe_data.close();

      Host.closeAdmMessagesFiles();

    }


    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

    Ctrl_c.removeShutdownHook();

    ThreadControl.waitForShutdownAll();
    ThreadControl.printActiveThreads();

    OS_cmd.killAll();

    common.memory_usage();

    Report.closeAllReports();
  }


  /**
   * Read all lines from the parameter files.
   * Parameters have a required order.
   */
  private static void readParameterFiles()
  {
    /* Get all misc parameters first, then hosts: */
    String next = MiscParms.readParms();
    next = HostParms.readParms(next);

    /* To support automatically repeating of parameters containing $host: */
    String[] new_lines = InsertHosts.repeatParameters(Vdb_scan.list_of_parameters);
    if (new_lines != null)
      next = Vdb_scan.completedHostRepeat(new_lines);

    /* Get all replay info: */
    next = ReplayRun.readParms(next);

    /* Get all File System Definitions: */
    next = FsdEntry.readParms(next);

    /* Get all Storage Definitions: */
    next = SD_entry.readParms(sd_list, next);

    /* Get all Workload Definitions: */
    next = WD_entry.readParms(wd_list, next);

    /* Accomodate for rescanning an extra wd=: */
    if (next == null)
      return;

    /* Get all Filesystem Workload Definitions: */
    next = FwdEntry.readParms(next);

    /* Get all Run Definitions: */
    next = RD_entry.readParms(rd_list, next);


    /* If needed put a 'journal recovery' RD at the beginning: */
    if (Validate.isJournalRecovery())
    {
      if (Vdbmain.isWdWorkload())
        DV_map.setupSDJournalRecoveryRun();
      else
        DV_map.setupFsdJournalRecoveryRun();
    }

    /* We may not define duplicate hosts: */
    Host.checkDuplicates();

  }

  private static void masterRun()
  {
    /* Using the host names and the requested JVM counts, create a list of
    /* slaves (this list can be expanded later): */
    Host.createSlaves();

    /* Convert the RD list that we got from reading the parameter file  */
    /* to one containing all extra RDs needed for the forxx parameters: */
    /* See also WG_entry.adjustJvmCount()                               */
    if (Vdbmain.isFwdWorkload())
      rd_list = RD_entry.buildNewRdListForFwd();

    else
    {
      /* Build the list allowing adjustment of JVM count: */
      RD_entry.buildNewRdListForWd(false);

      /* Rebuild the list again in case the JVM count changed: */
      rd_list = RD_entry.buildNewRdListForWd(true);
    }

    /* For a simulate we're done now: */
    if (Vdbmain.simulate)
      return;

    /* Create some of the report files: */
    open_print_files();
    Report.createHostSummaryFiles();
    Report.createSlaveSummaryFiles();
    if (isFwdWorkload())
    {
      //Report.createSlaveFsdFiles();    tbd
      Report.createSummaryHistogramFile();
      //Report.createHostHistogramFiles();   tbd
      //Report.createSlaveHistogramFiles();  tbd
    }

    /* Make sure everyone can read these reports before we go further: */
    Report.chModAllReports();

    /* Create socket for slaves to connect to: */
    ConnectSlaves.createSocketToSlaves();

    /* Connect to all slaves. Return after we have them all: */
    SlaveList.startSlaves();
    ConnectSlaves.connectToSlaves();
    slaves_started = true;
    new HeartBeat(true).start();

    /* Now get info from the hosts, things like 'does lun exist' etc: */
    InfoFromHost.askHostsForStuff();

    /* Format or expand files if any: */
    if (isWdWorkload() && CreateSdFile.insertFormatsIfNeeded())
      RD_entry.createWgListForOneRd((RD_entry) rd_list.firstElement(), true);


    if (isReplay())
      ReplayGroup.calculateGroupSDSizes(sd_list);

    /* Add just obtained info from hosts: */
    if (Vdbmain.isWdWorkload())
      RD_entry.finalizeWgEntries();

    /* We have asked the hosts for info. We now have enough data to */
    /* create all the reporting files: */
    Report.createOtherReportFiles();

    /* Find out which hosts can report their cpu usage: */
    InfoFromHost.checkCpuReporting();

    /* Run config file, except when we have a 'noconfig' file: */
    if (!new File(ClassPath.classPath("noconfig")).exists())
      common.run_config_scripts();
    else
      common.plog("Bypassing 'config.sh'");

    if (Vdbmain.isReplay())
      ReplayRun.initializeTraceRun();

    /* For a simulate we're done now: */
    if (Vdbmain.simulate)
    {
      SlaveList.shutdownAllSlaves();
      ThreadControl.shutdownAll("Vdb.HeartBeat");
      SlaveList.waitForAllSlavesShutdown();
      return;
    }


    /* Reporter takes care of the whole run: */
    Reporter master = new Reporter();
    master.start();
    while (master.isAlive())
      common.sleep_some(500);


    if (!Vdbmain.simulate)
      SlaveList.shutdownAllSlaves();
    ThreadControl.shutdownAll("Vdb.HeartBeat");
    SlaveList.waitForAllSlavesShutdown();
  }


  /**
   * Connect to Gui if needed
   */
  public static void connect_gui()
  {
    if (gui_port != 0)
    {
      gui_server = new GuiServer();
      gui_server.connect_to_server("localhost", gui_port);
    }

  }


  private static boolean checkForGui(String[] args)
  {
    /* If one of the input parameters contains 'gui' or '-gui', then     */
    /* we know that we have been started from the proper environment     */
    /* and we know we don't have to preload everything; the classpath    */
    /* in this case is already 'vdbench_dir:vdbench.jar:vdbench_gui.jar' */
    /* All we have to do is transfer to the Gui:                         */
    for (int i = 0; i < args.length; i++)
    {
      if (args[i].startsWith("-d") )
        common.set_debug(Integer.valueOf(args[i].substring(2)).intValue());


      else if (args[i].indexOf("gui") != -1)
      {
        if (!JVMCheck.isJREValid(System.getProperty("java.version"), 1, 5, 0))
        {
          Message.infoMsg("Minimum required Java version for the Vdbench GUI is 1.5.0; \n" +
                          "You are currently running " + System.getProperty("java.version") +
                          "\nVdbench terminated");

          common.exit(-99);
        }

        VDBenchGUI.main(args);
        return true;
      }
    }

    return false;
  }

  public static void setFwdWorkload()
  {
    fwd_workload = true;
  }
  public static boolean isFwdWorkload()
  {
    if (SlaveJvm.isThisSlave())
      common.failure("'isFwdWorkload' for Vdbmain requested on slave");
    return fwd_workload;
  }
  public static boolean isWdWorkload()
  {
    if (SlaveJvm.isThisSlave())
      common.failure("'isWdWorkload' for Vdbmain requested on slave");
    return !fwd_workload;
  }


  /**
   * Issue a warning if a beta version is more than 90 days old.
   */
  private static void displayBetaWarning()
  {
    common.plog("Vdbench distribution: " + Dist.version);
    common.plog("");

    if (Fget.file_exists(ClassPath.classPath("Owner.txt")))
    {
      Fget fg = new Fget(ClassPath.classPath("Owner.txt"));
      common.plog("Owner string: " + fg.get());
      fg.close();
    }

    try
    {
      // Mon Aug 18 15:37:28 MDT 2008
      DateFormat df = new SimpleDateFormat( "EEE MMM dd HH:mm:ss zzz yyyy" );

      //Dist.compiled = "Mon may 18 15:37:28 MDT 2008";
      //Dist.version = "beta5";

      /* If there is no date or version we don't care: */
      if (Dist.compiled == null || Dist.compiled == null)
        return;

      /* Beta versions have 'rc' or 'beta' in the version: */
      if (Dist.version.indexOf("rc")   != -1 ||
          Dist.version.indexOf("beta") != -1)
      {
        Date comp = df.parse(Dist.compiled);
        Date now  = new Date();
        if (now.getTime() - comp.getTime() > (90 * 24 * 60 * 60 * 1000l))
        {
          common.ptod("*");
          common.ptod("* This beta version '" + Dist.version + "' was built on " + Dist.compiled + ".");
          common.ptod("* which is more than 90 days ago.");
          common.ptod("* It is recommended that you look for a newer beta or possibly");
          common.ptod("* a newer GA version.");
          common.ptod("* Of course, it is preferred you download a newer beta version");
          common.ptod("* if available to help test the latest code.");
          common.ptod("*");

          common.psum("*");
          common.psum("* This beta version '" + Dist.version + "' was built on " + Dist.compiled + ".");
          common.psum("* which is more than 90 days ago.");
          common.psum("* It is recommended that you look for a newer beta or possibly");
          common.psum("* a newer GA version.");
          common.psum("* Of course, it is preferred you download a newer beta version");
          common.psum("* if available to help test the latest code.");
          common.psum("*");
        }
      }
    }
    catch (Exception e)
    {
      System.out.println("No valid compile date present: " +
                         Dist.version + "/" + Dist.compiled);
    }

    return;
  }

  private static void checkJavaVersion()
  {
    if (common.get_debug(common.USE_ANY_JAVA))
      return;
    if (!JVMCheck.isJREValid(System.getProperty("java.version"), 1, 5, 0))
    {
      System.out.print("*\n*\n*\n");
      System.out.println("* Minimum required Java version for Vdbench is 1.5.0; \n" +
                         "* You are currently running " + System.getProperty("java.version") +
                         "\n* Vdbench terminated.");
      System.out.println("*\n*\n*\n");

      System.exit(-99);
    }
  }

  private static boolean anySpecialTricks(String[] args)
  {
    try
    {
      /* Vdbench compare? */
      if (args[0].equalsIgnoreCase("compare"))
        VdbComp.WlComp.main(args);

      /* Vdbench parseflat?? */
      else if (args[0].toLowerCase().startsWith("parse"))
        ParseFlat.main(args);

      /* Vdbench jstack?? */
      else if ("jstack".startsWith(args[0]))
        Jstack.main(args);

      /* Vdbench amberroad??
      else if (args[0].equalsIgnoreCase("analytics"))
        AmberRoad.main(args); */

      /* SD building? */
      else if (args[0].equalsIgnoreCase("sds"))
        VdbComp.SdBuild.main(args);

      /* RSH deamon? */
      else if (args[0].equalsIgnoreCase("rsh"))
        RshDeamon.main(args);

      else if (args.length > 0 &&
          (args[0].equalsIgnoreCase("print") ||
           args[0].equalsIgnoreCase("-print")))
        PrintBlock.print(args);

      else if (args[0].equalsIgnoreCase("dvpost"))
        DVPost.main(args);

      //else if (args[0].equalsIgnoreCase("dvpostold"))
      //  DVPostOld.main(args);

      else if (args[0].equalsIgnoreCase("edit"))
      {
        String[] nargs = new String[args.length - 1];
        System.arraycopy(args, 1, nargs, 0, nargs.length);
        Utils.Editor.main(nargs);
      }

      else
        return false;

      return true;
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    return false;
  }

  /**
   * Using the -t parameter creates a very simple test run.
   */
  private static String createQuickTest(String arg)
  {
    try
    {
      String tmpdir = File.createTempFile("vdb", "tmp").getParent();
      Fput fp = new Fput(tmpdir, "parmfile");
      String extra = "";

      if (arg.equals("-t1"))
      {
        if (common.onSolaris()) extra = ",openflags=o_sync";
        if (common.onWindows()) extra = ",openflags=directio";
        if (common.onLinux())   extra = ",openflags=o_sync";
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 4k 50% read, 50% write test.");
        fp.println("* (This file is too small to be used for data validation.)");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + File.separator + "quick_vdbench_test,size=40m" + extra);
        fp.println("wd=wd1,sd=sd1,xf=1k,rdpct=50");
        fp.println("rd=rd1,wd=wd1,iorate=100,elapsed=5,interval=1");
      }

      else if (arg.equals("-t2"))
      {
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 4k 50% read, 50% write test.");
        fp.println("* (This file is too small to be used for data validation.)");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + File.separator + "quick_vdbench_test,size=40m");
        fp.println("wd=wd1,sd=sd1,xf=1k,rdpct=50");
        fp.println("rd=rd1,wd=wd1,iorate=500,elapsed=5,interval=1");
      }
      else
      {
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 4k 50% read, 50% write test.");
        fp.println("* (This file is too small to be used for data validation.)");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + File.separator + "quick_vdbench_test,size=40m");
        fp.println("wd=wd1,sd=sd1,xf=1k,rdpct=50");
        fp.println("rd=rd1,wd=wd1,iorate=100,elapsed=5,interval=1");
      }


      fp.close();
      return fp.getName();
    }

    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }
}
