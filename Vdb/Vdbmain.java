package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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



/**
 * Main method for vdbench workload Generator, see main() for overview.
 */
public class Vdbmain
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  static Vector <SD_entry> sd_list  = new Vector(64, 0); /* List of Storage Definitions  */
  static Vector <SD_entry> csd_list = new Vector(8);
  static Vector <WD_entry> wd_list  = new Vector(64, 0); /* List of Workload Definitions */
  static Vector <RD_entry> rd_list  = new Vector(64, 0); /* List of Run Definitions      */

  private static boolean workload_done;
  private static Semaphore workload_done_semaphore = new Semaphore(0);

  static boolean fwd_workload = false;

  static boolean simulate       = false;
  static String  output_dir     = "output";
  static PrintWriter parms_report;

  static boolean slaves_started = false;

  static double observed_iorate = 10000;
  static double observed_resp   = 0;
  static double last_curve_max  = 10000;

  static int elapsed  =  0;
  static int warmup   =  0;
  static int interval =  0;

  public static String rd_parm_extras = "";

  static boolean force_fsd_cleanup    = false;
  static boolean force_format_only    = false;
  static boolean force_format_no      = false;
  static boolean force_format_yes     = false;

  static boolean loop_all_runs        = false;
  static long    loop_duration        = Long.MAX_VALUE;
  static long    loop_count           = Long.MAX_VALUE;
  static long    loops_done           = 0;

  static boolean checking_for_errors  = true;

  private static Vector parm_files;


  static boolean kstat_console = false;


  /* Unique run time value to make sure we don't fiddle with */
  /* old masters and slaves                                  */
  private static String run_time = new SimpleDateFormat("yyMMdd-HH.mm.ss.SSS" ).format(new Date());

  public static String getRunTime()
  {
    return run_time;
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
    //if (bool)
    //  common.where(8);
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
    print_property("java.io.tmpdir");
    print_property("java.class.path");
    print_property("os.name");
    print_property("os.arch");
    print_property("os.version");
    print_property("sun.arch.data.model");
    //print_property("file.separator");
    //print_property("path.separator");
    //print_property("line.separator");
    //print_property("user.home");
    //print_property("user.dir");

    /* Extra info to display possible fixes: */
    if (!Dist.version.equals("Henk's personal development library"))
    {
      File[] dirptrs = new File(ClassPath.classPath("classes")).listFiles();
      if (dirptrs != null)
      {
        for (File dirptr : dirptrs)
        {
          File[] fptrs = new File(dirptr.getAbsolutePath()).listFiles();
          if (fptrs != null)
          {
            for (File fptr : fptrs)
              common.plog("Fixes: %s/%s %d", dirptr.getName(), fptr.getName(), fptr.length());
          }
        }
      }
    }

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
  }

  /**
   * Concatenate parameters that do not have a '-' to the previous one.
   * We only do that once. As soon as we hit a standalone ' - ' we stop.
   * (Everything following the standalone '-' goes to parmfile).
   * This was written to allow any parameter to be either -xYY or -x YY
   *
   * Any parameter of with contents xx=yy will not be treated this way, it will
   * be used later on as a parameter substitution, replacing $xx with 'yy'.
   */
  private static void check_args(String args[])
  {
    String newargs[] = new String[1024];
    int out = 0;
    boolean concatenated = false;
    boolean standalone_dash = false;

    for (int i = 0; i < args.length; i++)
    {
      /* A '+' will be an addition to a one-line RD parameter: */
      if (args[i].startsWith("+") )
      {
        saveRdExtras(args, i);
        break;
      }

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

      else if (args[i].indexOf("=") != -1)
        newargs[out++] = args[i];

      else
      {
        newargs[out-1] = newargs[out-1] + args[i];
        concatenated = true;
      }
    }

    String nargs[] = new String[out];
    System.arraycopy(newargs, 0, nargs, 0, out);

    /* We need the output directory ASAP: */
    for (int i = 0; i < nargs.length; i++)
    {
      if (nargs[i].startsWith("-o"))
        output_dir = nargs[i].substring(2);
      else if (nargs[i].compareTo("-s") == 0 )
        simulate = true;
    }

    checkJavaVersion();

    /* Create requested directory. Delete files inside if it already exists: */
    output_dir      = reporting.rep_mkdir(output_dir);
    Validate.setOutput(output_dir);
    common.log_html = Report.createHmtlFile("logfile");
    Report.setLogReport(new Report(common.log_html, "logfile", "common.log_html"));
    Utils.common.setPlog(common.log_html);

    /* Open summary file as early as possible: */
    String txt = c + "\nVdbench summary report, created ";
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss MMM dd yyyy zzz"  );
    common.summ_html = Report.createHmtlFile("summary.html", txt + df.format(new Date()));

    Report.setSummaryReport(new Report(common.summ_html, "summary", "common.summ_html"));
    Report.getSummaryReport().printHtmlLink("Link to logfile", "logfile", "logfile");
    displayBetaWarning();

    /* This report to only contain run averages/totals. */
    /* This file is used in a vdbench wrapper. Be careful with changes! */
    Report.setTotalReport(new Report("totals", "Run totals"));
    Report.getSummaryReport().printHtmlLink("Run totals", "totals", "totals");

    new Status();

    Vdb_scan.parm_html = Report.createHmtlFile("parmfile.html");

    scan_args(nargs);

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

      /* we already picked up the directory: */
      if (thisarg.startsWith("-o") )
        continue;

      else if (thisarg.compareTo("-s") == 0 )
        continue;
    }

    /* Scan through all arguments: */
    boolean scan_for_parmfile = false;

    /* Do a very quick test to a temp file if requested: */
    for (int i = 0; i < args.length; i++)
    {
      if (args[i].startsWith("-t"))
        args[i] = "-f" + createQuickTest(args[i]);

      /* Debugging must be set before I can honor '-d100': */
      if (args[i].startsWith("-d"))
        common.set_debug(Integer.valueOf(args[i].substring(2)).intValue());
    }

    /* Scan all parameters: */
    for (int i = 0; i < args.length; i++)
    {
      String thisarg = args[ i ];

      if (thisarg == null)
        break;

      common.ptod("input argument scanned: '" + thisarg + "'");

      if (thisarg.indexOf("=") != -1)
      {
        InsertHosts.addSubstitute(thisarg);
        scan_for_parmfile = true;
        continue;
      }

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

        if (thisarg.contains("r")) Validate.setJournalRecovery();
        if (thisarg.contains("n")) Validate.setNoJournalFlush();
        if (thisarg.contains("m")) Validate.setMapOnly();
        if (thisarg.contains("o")) Validate.setRecoveryOnly();
        if (thisarg.contains("2")) Validate.setImmediateRead();
        if (thisarg.contains("t")) Validate.setStoreTime();
        if (thisarg.contains("i")) Validate.setIgnorePending();
        if (thisarg.contains("s")) Validate.setSkipRead();
      }

      else if (thisarg.startsWith("-d") )
        common.set_debug(Integer.valueOf(thisarg.substring(2)).intValue());

      else if (thisarg.startsWith("-v"))
      {
        Validate.setValidate();
        if (thisarg.contains("r")) Validate.setImmediateRead();
        if (thisarg.contains("2")) Validate.setImmediateRead();
        if (thisarg.contains("w")) Validate.setNoPreRead();
        if (thisarg.contains("t")) Validate.setStoreTime();
        if (thisarg.contains("c"))
        {
          common.failure("'validate=continue' no longer supported");
          Validate.setContinueOldMap();
          return;
        }

      }

      else if (thisarg.startsWith("-c"))
      {
        if (     thisarg.indexOf("o") != -1) force_format_only    = true;
        else if (thisarg.indexOf("y") != -1) force_format_yes     = true;
        else if (thisarg.indexOf("n") != -1) force_format_no      = true;
        else /*                         */   force_fsd_cleanup    = true;
      }

      else if (thisarg.startsWith("-l"))
        scanLoopParameter(thisarg);

      else if (thisarg.compareTo("-s") == 0 )
        simulate = true;

      else if (thisarg.startsWith("-e"))
        elapsed = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-w"))
        warmup = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-i"))
        interval = Integer.valueOf(thisarg.substring(2)).intValue();

      else if (thisarg.startsWith("-p"))
        SlaveSocket.setMasterPort(Integer.valueOf(thisarg.substring(2)).intValue());

      else if (thisarg.startsWith("-mb"))
        Report.setDecimal();

      else if (thisarg.startsWith("-m"))
        Host.jvmcount_override = Integer.parseInt(thisarg.substring(2));

      else if (thisarg.compareTo("-k") == 0 )
        Vdbmain.kstat_console = true;

      else if (thisarg.compareTo("-") == 0)
        Vdb_scan.xlate_command_line(args, i);

      else
      {
        common.ptod("");
        common.ptod("Invalid execution parameter: '" + thisarg + "'");
        usage();
        common.failure("Invalid execution parameter: '" + thisarg + "'");
      }
    }

    if (parm_files.size() == 0)
    {
      usage();
      common.ptod("No input parameters specified: ");
    }

  }

  public static void usage()
  {
    common.ptod("");
    common.ptod(" usage: ");
    common.ptod(" ./vdbench [compare] [-f xxx] [-o xxx] [-e nn] [-i nn] [-j] [-jr] [-v] [-vq] [-s] [-k] [- \"parmfile parameters\"]");
    common.ptod(" ");
    common.ptod(" '-f xxx': Workload parameter file name(s). Default 'parmfile' in current directory");
    common.ptod(" '-o xxx': Output directory for reporting. Default 'output' in current directory");
    common.ptod(" '-e nn':  Override elapsed=seconds. ");
    common.ptod(" '-i nn':  Override interval=seconds. ");
    common.ptod(" '-v':     Activate Data validation. ");
    common.ptod(" '-vq':    Activate Data validation, validate lba and data key (saves cpu) ");
    common.ptod(" '-j':     Activate Data validation with Journaling. ");
    common.ptod(" '-jr':    Recover existing Journal, Validate data and run workload ");
    common.ptod(" '-s':     Simulate execution. Do everything but I/O. ");
    common.ptod(" '-k':     Solaris only: Report kstat statistics on console. ");
    common.ptod(" ");
    common.ptod(" ");
    common.ptod("                                                                                   ");
    common.ptod("*example 1 parameter file contents, execute 'vdbench -f example1'                  ");
    common.ptod("*Single raw disk, 100% random read of 4k records at i/o rate of 100 for 10 seconds ");
    common.ptod("                                                                                   ");
    common.ptod("*SD:    Storage Definition                                                         ");
    common.ptod("*WD:    Workload Definition                                                        ");
    common.ptod("*RD:    Run Definition                                                             ");
    common.ptod("*                                                                                  ");
    common.ptod("sd=sd1,lun=/dev/rdsk/cxt0d0s0                                                      ");
    common.ptod("wd=wd1,sd=sd1,xfersize=4096,readpct=100                                            ");
    common.ptod("rd=run1,wd=wd1,iorate=100,elapsed=10,interval=1                                    ");
    common.ptod("                                                                                   ");
    common.ptod("                                                                                   ");
    common.ptod("                                                                                   ");
    common.ptod("*example 2 parameter file contents, execute 'vdbench -f example2'                  ");
    common.ptod("*Two raw disks:                                                      ");
    common.ptod("*sd1 does 80 i/o's per second, read-to-write ratio 4:1, 4k records.  ");
    common.ptod("*sd2 does 120 i/o's per second, 100% write at 8k records.            ");
    common.ptod("                                                                     ");
    common.ptod("sd=sd1,lun=/dev/rdsk/cxt0d0s0                                        ");
    common.ptod("sd=sd2,lun=/dev/rdsk/cxt0d1s0                                        ");
    common.ptod("wd=wd1,sd=sd1,xfersize=4k,readpct=80,skew=40                         ");
    common.ptod("wd=wd2,sd=sd2,xfersize=8k,readpct=0                                  ");
    common.ptod("rd=run1,wd=wd*,iorate=200,elapsed=10,interval=1                      ");
    common.ptod("                                                                     ");
    common.ptod("                                                                     ");
    common.ptod("For documentation, browse file 'vdbench.pdf' ");
    //common.ptod("For documentation or revision updates, use your Web Browser and go to:");
    //common.ptod("http://webhome.sfbay/nwsspe/speweb/vdbench/index.html\n\n");
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

      /* Is this one of the special functions? */
      if (args.length > 0 && anySpecialTricks(args))
        return;

      /* Start interceptor: */
      Ctrl_c.activateShutdownHook();
      Shutdown.activateShutdownHook();

      common.stdout = new PrintWriter(System.out, true);

      System.out.println("\n\n" + c);
      System.out.printf("Vdbench distribution: %s %s\n", Dist.version, Dist.compiled);
      System.out.println("For documentation, see 'vdbench.pdf'.");
      System.out.println();


      /* Get execution parameters:   */
      check_args(args);
      RshDeamon.readPortNumbers();

      /* Parse everything we've got: */
      parms_report = Report.createHmtlFile("parmscan.html");
      //Vdb_scan.copy_file = Report.createHmtlFile("parmfile.html");
      Report.getSummaryReport().printHtmlLink("Copy of input parameter files",
                                              "parmfile", "parmfile");
      Report.getSummaryReport().printHtmlLink("Copy of parameter scan detail",
                                              "parmscan", "parmscan");

      Vdb_scan.Vdb_scan_read(parm_files, true);
      parseParameterLines();

      /* If there are any missing WDs, read and parse them now: */
      // Decided that for Vdbench to start looking like the filebench
      // 'personality' stuff would be a serious degrade of the usability of
      // Vdbench and I therefore decided to NOT do this.
      if (RD_entry.getExtraWDs().size() > 0)
      {
        Vdb_scan.Vdb_scan_read(RD_entry.getExtraWDs(), false);
        parseParameterLines();
      }

      /* Check some RD stuff that still needs to be done: */
      RD_entry.parserCleanup(rd_list);

      /* Activate DV for dedup: */
      if (Dedup.isDedup())
        Validate.setValidateOptionsForDedup();

      Flat.createFlatFile();
      Flat.define_column_headers();


      masterRun();

      if (!Vdbmain.simulate)
      {
        /* This String may not change!! */
        common.ptod("Vdbench execution completed successfully. Output directory: %s \n", output_dir);
        common.ptod("Vdbench execution completed successfully", common.summ_html);
      }
      else
      {
        RD_entry.displaySimulatedRuns();
        common.ptod("Vdbench simulation completed successfully. Output directory: %s \n", output_dir);
        common.ptod("Vdbench simulation completed successfully", common.summ_html);
      }


      Status.printStatus("Vdbench complete", null);

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
   * Parse all lines from the parameter files.
   * Parameters have a required order.
   */
  private static void parseParameterLines()
  {

    /* Get all misc parameters first, then hosts: */
    String next = MiscParms.readParms();
    next = HostParms.readParms(next);

    /* To support automatically repeating of parameters containing $host: */
    String[] original = Vdb_scan.list_of_parameters;
    Vdb_scan.list_of_parameters = InsertHosts.replaceNumberOfHosts(original);
    Vdb_scan.list_of_parameters = InsertHosts.repeatParameters(Vdb_scan.list_of_parameters);
    if (InsertHosts.anyChangesMade(original, Vdb_scan.list_of_parameters))
    {
      Vdb_scan.external_parms_used = true;
      next = Vdb_scan.completedHostRepeat();
    }

    InsertHosts.lookForMissingSubstitutes(Vdb_scan.list_of_parameters);

    /* If any extra stuff like includes or command line overrides were done */
    /* it mayu be difficult to quickly see what we all have.                */
    /* This is an abandoned attempt to copy everything to parmscan.html.    */
    //if (Vdb_scan.external_parms_used)
    //{
    //  for (String prm : Vdb_scan.list_of_parameters)
    //  {
    //    if (prm == null)
    //      break;
    //    common.ptod("Some external used: " + prm);
    //  }
    //}

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

    /* Create Concatenated Sds if needed: */
    if (Validate.sdConcatenation())
      ConcatSds.createConcatSds(wd_list, rd_list);

    /* If needed put a 'journal recovery' RD at the beginning: */
    if (Validate.isJournalRecovery())
    {
      if (Vdbmain.isWdWorkload())
        Jnl_entry.setupSDJournalRecoveryRun();
      else
        Jnl_entry.setupFsdJournalRecoveryRun();
    }

    /* We may not define duplicate hosts: */
    Host.checkDuplicates();

    Report.setupAuxReporting();
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
      RD_entry.buildNewRdListForWd();

      /* Rebuild the list again in case the JVM count changed:           */
      /* Redoing this is required because the first thing we do is start */
      /* all slaves and ask them for config info.                        */
      RD_entry.printWgInfo("Begin second pass....");
      rd_list = RD_entry.buildNewRdListForWd();
      RD_entry.printWgInfo("End second pass....");
      RD_entry.printWgInfo("");
    }

    /* For a simulate we're done now: */
    if (Vdbmain.simulate)
      return;

    /* Create some of the report files: */
    open_print_files();
    Report.createHostSummaryFiles();
    Report.createSlaveSummaryFiles();
    //if (isFwdWorkload())
    {
      //Report.createSlaveFsdFiles();    tbd
      Report.createSummaryHistogramFile();
      //Report.createHostHistogramFiles();   tbd
      //Report.createSlaveHistogramFiles();  tbd
    }

    //if (Vdbmain.isWdWorkload())
    Report.createSkewFile();

    /* Make sure everyone can read these reports before we go further: */
    Report.chModAllReports();

    /* Create socket for slaves to connect to: */
    ConnectSlaves.createSocketToSlaves();

    /* Connect to all slaves. Return after we have them all: */
    SlaveList.startSlaves();
    ConnectSlaves.connectToSlaves();
    slaves_started = true;
    new HeartBeat(true).start();

    /* Some last moment adjustments: */
    if (ReplayInfo.isReplay())
      SD_entry.adjustReplay(sd_list);

    /* Write markers for concatenation if needed: */
    if (Validate.sdConcatenation())
      ConcatMarkers.writeMarkers();

    /* Now get info from the hosts, things like 'does lun exist' etc: */
    InfoFromHost.askHostsForStuff();

    /* Format or expand files if any: */
    if (isWdWorkload() && !MiscParms.do_not_format_sds && CreateSdFile.insertFormatsIfNeeded())
      RD_entry.createWgListForOneRd((RD_entry) rd_list.firstElement(), true);

    /* Before we continue, check the xfersizes requested with Data Validation: */
    if (Validate.isRealValidate() || Validate.isValidateForDedup())
      SD_entry.calculateKeyBlockSizes();

    if (ReplayInfo.isReplay())
      ReplayGroup.calculateGroupSDSizes(sd_list);

    /* Do some last moment stuff: */
    if (Vdbmain.isWdWorkload())
      RD_entry.finalizeWgEntries();

    /* We have asked the hosts for info. We now have enough data to */
    /* create all the reporting files: */
    Report.createOtherReportFiles();

    /* Find out which hosts can report their cpu usage: */
    InfoFromHost.checkCpuReporting();

    /* Run config file, except when we have a 'noconfig' file: */
    if (!common.onWindows())
    {
      if (!new File(ClassPath.classPath("noconfig")).exists())
        common.run_config_scripts();
      else
        common.plog("Bypassing 'config.sh'");
    }

    if (ReplayInfo.isReplay())
      ReplayRun.setupTraceRun();

    /* For a simulate we're done now: */
    if (Vdbmain.simulate)
    {
      SlaveList.shutdownAllSlaves();
      ThreadControl.shutdownAll("Vdb.HeartBeat");
      SlaveList.waitForAllSlavesShutdown();
      return;
    }


    /* Reporter takes care of the whole Vdbench execution: */
    Reporter reporter = new Reporter();
    CollectSlaveStats css = new CollectSlaveStats(reporter);
    reporter.start();
    css.start();
    while (reporter.isAlive())
      common.sleep_some(1000);

    /* When we come here ALL RDs have been completed */
    if (!Vdbmain.simulate)
      SlaveList.shutdownAllSlaves();
    ThreadControl.shutdownAll("Vdb.HeartBeat");
    SlaveList.waitForAllSlavesShutdown();
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
    common.log_html.println(c);
    common.log_html.println("Vdbench distribution: " + Dist.version +"\n");

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
    if (!JVMCheck.isJREValid(System.getProperty("java.version"), 1, 7, 0))
    {
      System.out.print("*\n*\n*\n");
      System.out.println("* Minimum required Java version for Vdbench is 1.7.0; \n" +
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
      /* Some main() calls expect the first argument to be removed: */
      String[] nargs = new String[args.length - 1];
      System.arraycopy(args, 1, nargs, 0, nargs.length);

      /* Vdbench compare? */
      if (args[0].equalsIgnoreCase("compare"))
        VdbComp.WlComp.main(args);

      /* Vdbench parseflat?? */
      else if (args[0].toLowerCase().startsWith("parse"))
        ParseFlat.main(args);

      /* Vdbench jstack?? */
      else if (args[0].equals("jstack"))
        Jstack.main(args);

      /* Vdbench jmap?? */
      else if (args[0].equals("jmap"))
      {
        Jmap.main(args);
        common.exit(0);;
      }

      /* SD building? */
      else if (args[0].equalsIgnoreCase("sds"))
        VdbComp.SdBuild.main(args);

      else if (args[0].equalsIgnoreCase("showlba"))
        ShowLba.main(args);

      /* RSH daemon? */
      else if (args[0].equalsIgnoreCase("rsh"))
        RshDeamon.main(args);

      else if (args.length > 0 &&
               (args[0].equalsIgnoreCase("print") ||
                args[0].equalsIgnoreCase("-print")))
        PrintBlock.print(args);

      else if (args[0].equalsIgnoreCase("dvpost"))
      {
        common.failure("DvPost no longer exists in the current verion. "+
                       "That decision possibly needs to be re-evaluated");
        DVPost.main(args);
      }

      else if (args[0].equalsIgnoreCase("edit"))
        Utils.Editor.main(nargs);

      /* Compression simulation? */
      else if (args[0].equalsIgnoreCase("csim"))
        csim.main(nargs);

      /* Dedup simulation? */
      else if (args[0].equalsIgnoreCase("dsim"))
        dsim.main(nargs);

      /* Journal print? */
      else if (args[0].equalsIgnoreCase("printjournal"))
        PrintJournal.main(nargs);

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
      String tmpdir = Fput.getTmpDir();
      Fput fp = new Fput(tmpdir, "parmfile");
      String extra = "";

      if (arg.equals("-td"))
        new File(tmpdir, "quick_vdbench_test").delete();

      if (arg.equals("-t1"))
      {
        if (common.onSolaris()) extra = ",openflags=o_sync";
        if (common.onWindows()) extra = ",openflags=directio";
        if (common.onLinux())   extra = ",openflags=o_sync";
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 1k 50% read, 50% write test.");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + "quick_vdbench_test,size=40m" + extra);
        fp.println("wd=wd1,sd=sd1,xf=1k,rdpct=50");
        fp.println("rd=rd1,wd=wd1,iorate=100,elapsed=5,interval=1");
      }

      else if (arg.equals("-t2"))
      {
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 1k 50% read, 50% write test.");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + "quick_vdbench_test,size=40m");
        fp.println("wd=wd1,sd=sd1,xf=1k,rdpct=0");
        fp.println("rd=rd1,wd=wd1,iorate=max,elapsed=5,interval=1");
      }

      else if (arg.equals("-tf"))
      {
        fp.println("");
        fp.println("                                                                             ");
        fp.println("* Random read and write of randomly selected files.                          ");
        fp.println("*                                                                            ");
        fp.println("* 'format=yes' first creates (depth*width*files) 100 128k files.             ");
        fp.println("* Test then will have eight threads each randomly select a file and do random");
        fp.println("* reads.  After 'stopafter=100' reads the file is closed and a new           ");
        fp.println("* file is randomly selected.                                                 ");
        fp.println("* An other eight threads will do the same, but only for writes.              ");
        fp.println("*                                                                            ");
        fp.println("                                                                             ");
        fp.println("fsd=fsd1,anchor=" + tmpdir + "fsd1,depth=1,width=1,files=100,size=128k       ");
        fp.println("                                                                             ");
        fp.println("fwd=default,xfersize=4k,fileio=random,fileselect=random,threads=8,           ");
        fp.println("stopafter=100                                                                ");
        fp.println("fwd=fwd1,fsd=fsd1,operation=read                                             ");
        fp.println("fwd=fwd2,fsd=fsd1,operation=write                                            ");
        fp.println("                                                                             ");
        fp.println("rd=rd1,fwd=fwd*,fwdrate=100,format=yes,elapsed=5,interval=1                  ");
      }

      else
      {
        fp.println("*");
        fp.println("* This sample parameter file first creates a temporary file");
        fp.println("* if this is the first time the file is referenced.");
        fp.println("* It then does a five second 4k 50% read, 50% write test.");
        fp.println("*");
        fp.println("sd=sd1,lun=" + tmpdir + "quick_vdbench_test,size=40m");
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

  /**
   * Experiment: allow rd overrides from the command line.
   * ./vdbench -f xxx + iorate=max
   */
  private static void saveRdExtras(String[] args, int i)
  {
    /* If the first parameter is more than just a '+', strip it: */
    String arg = args[i].trim();
    if (!arg.equals("+"))
      rd_parm_extras = arg.substring(1).trim() + " ";

    /* Now just store the remainder: */
    for (i++; i < args.length; i++)
      rd_parm_extras += args[i].trim() + " ";
    rd_parm_extras = rd_parm_extras.trim();

    //common.ptod("Adding ',%s' to each one-line RD parameter.", rd_parm_extras);
    if (rd_parm_extras.indexOf(" ") != -1)
      common.failure("Embedded blanks not acceptable: '%s'", rd_parm_extras);
  }


  /**
   * -l nnn parameter.
   * Without 'nn', loop is endless, with it ends after 'nn' seconds.
   */
  private static void scanLoopParameter(String parm)
  {
    loop_all_runs = true;
    if (parm.equals("-l"))
      return;

    String number = parm.substring(2);

    int multiplier = 1;
    if (number.endsWith("s"))
    {
      multiplier = 1;
      number = number.substring(0, number.length() -1);
    }
    else if (number.endsWith("m"))
    {
      multiplier = 60;
      number = number.substring(0, number.length() -1);
    }
    else if (number.endsWith("h"))
    {
      multiplier = 3600;
      number = number.substring(0, number.length() -1);
    }
    else if (!common.isNumeric(number))
    {
      common.failure("Expecting no parameter or a numeric paramater after '-l': " + parm);
    }

    loop_duration = Long.parseLong(number) * multiplier;
    loop_duration *= 1000;
  }
}
