package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.*;




/**
  * The common class contains some general service methods
  *
  * Warning: some calls from code in the Utils package to similary named methods
  * here will NOT actually use the code below!
  * Need to prevent that some day.
  */
public class common
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  static PrintWriter stdout;

  static PrintWriter log_html;

  static PrintWriter summ_html;

  static Object failure_lock = new Object();
  static Object ptod_lock = new Object();
  {
    Utils.common.ptod_lock = ptod_lock; /* Make sure Utils.common uses same lock! */
  }

  static PrintWriter common_pw = null;

  static boolean out_of_memory = false;
  static Object out_of_memory_lock = new Object();

  static long last_interval_tod = 0;

  /* Debugging flags. The settings are done in Utils.common() */

  public static int NEVER_OPEN_FOR_WRITE  = 1;

  public static int PRINT_MEMORY          = 3;
  public static int NO_PRINT_FLUSH        = 4;

  public static int DEVXLATE              = 6;
  public static int PRINT_BLOCK_COUNTERS  = 7;
  public static int FIXED_SEED            = 8;
  public static int REUSE_IOSTAT          = 9;
  public static int PRINT_SIZES           = 10;
  public static int FAST_HEADERS          = 11;

  public static int TIMERS                = 13;
  public static int PRINT_OPEN_FLAGS      = 14;
  public static int PRINT_IO_COMP         = 15;
  public static int EXTERNAL_SYNCH        = 16;
  public static int NO_KSTAT              = 17;
  public static int NO_CONFIG_SCRIPT      = 18;
  public static int NO_CPU_STATS          = 19;
  public static int SPIN                  = 20;   /* set hires_tick=1 works just as well */
  public static int PTOD_TO_DISK          = 21;

  public static int PRINT_IN_MICROSECONDS = 23;

  public static int IGNORE_MISSING_REPLAY = 24;

  public static int LONGER_HEARTBEAT      = 27;   // -d27 is documented in blog!
  public static int SHORTER_HEARTBEAT     = 28;
  public static int FILEENTRY_SET_BUSY    = 29;

  public static int DEBUG_COMPRESSION     = 31;
  public static int DIRECTORY_SET_BUSY    = 32;
  public static int DIRECTORY_CREATED     = 33;

  public static int REPORT_CREATES        = 35;
  public static int FAST_SYNCTIME         = 36;
  public static int FAST_BLOCK_KILL       = 37;
  public static int USE_FORMAT_RATE       = 38;
  public static int FORCE_REPLAY_SPLIT    = 39;
  public static int FORCE_KSTAT_ERROR     = 40;
  public static int SCSI_RESET_AT_START   = 41;
  public static int SCSI_RESET_ALL_START  = 42;
  public static int SOCKET_TRAFFIC        = 43;
  public static int SLAVE_LOG_ON_CONSOLE  = 44;   // does not work with Vdbench RSH!!!
  public static int THREADCONTROL_LOG     = 45;
  public static int DUMP_THREAD_TRACE     = 46;
  public static int SHOW_REPORTS          = 47;
  public static int SHOW_SOCKET_MESSAGES  = 48;
  public static int SMALL_FILE_COUNT      = 49;
  public static int PRINT_FS_COUNTERS     = 50;
  public static int FAKE_LIBDEV           = 51;

  public static int NO_CONTROLFILE_DETAIL = 53;
  public static int SEQUENTIAL_COUNTS     = 54;

  public static int RESTART_FILLING       = 56;

  public static int LONG_SHUTDOWN         = 58;
  public static int ANCHOR_FIXED_SEED     = 59;
  public static int FIFO_STATS            = 60;

  public static int ALLOW_BLOCK0_ACCESS   = 62;
  public static int NO_BLOCK0_ACCESS      = 63;

  public static int cant_use_64           = 64;   // treated as 64bit request by java!!
  public static int HOLD_UP_STATISTICS    = 65;
  public static int LONGER_FILENAME       = 66;
  public static int DV_ALLOW_PATTERN      = 67;
  public static int SIMULATE              = 68;
  public static int SKEW_ON_CONSOLE       = 70;
  public static int DETAIL_SLV_REPORT     = 71;
  public static int GENERATE_WORK_INFO    = 72;

  public static int USE_TVDBENCH          = 75;
  public static int USE_ANY_JAVA          = 76;
  public static int DEBUG_SPREAD          = 77;
  public static int NATIVE_SLEEP          = 78;
  public static int USE_PSRSET            = 79;
  public static int PTOD_WG_STUFF         = 80;
  public static int PLOG_WG_STUFF         = 81;
  public static int RUN_JMAP              = 82;
  public static int SHORT_FS_STDOUT       = 83;
  public static int TIMEBEGINPERIOD       = 84;  // for Windows
  public static int DEBUG_AUX_REPORT      = 85;
  public static int FILE_FORMAT_TRUNCATE  = 86;
  public static int WT_TASK_LIST_SORT     = 87;

  public static int REPORT_MESSAGE_SIZE   = 89;
  public static int GCTRACKER             = 90;

  public static int NO_ERROR_ABORT        = 96;
  public static int ALWAYS_ERASE_MAPS     = 97;
  public static int NO_MISSING_SUB_CHECK  = 98;
  public static int IGNORE_PARM_COMMENT   = 99;

  public static int USE_TMP_SHARED_LIBRARY = 100;

  public static int DONT_DUMP_MAPS         = 102;
  public static int NO_RESPONSE_TIMES      = 103;

  public static int CREATE_FILE_LIST       = 106;
  public static int FIXED_HOTBAND_SEED     = 107;
  public static int CREATE_READ_WRITE_LOG  = 108;
  public static int PRINT_WIDE_WHERE       = 109;

  public static int FAST_JOURNAL_CHECK     = 110;  // obsolete
  public static int JOURNAL_ADD_TIMESTAMP  = 111;

  public static int THREAD_MONITOR_ALL     = 120;
  public static int THREAD_MONITOR_TOP10   = 121;
  public static int THREAD_MONITOR_CONSOLE = 122;  // implies top10
  public static int FIRST_SEQ_SEEK_RANDOM  = 123;
  public static int FAKE_RSH               = 124;

  public static int DV_PRINT_SECTOR_IMMED  = 131;
  public static int DONT_ZIP_SOCKET_MSGS   = 132;

  private static String shared_library_dir = null;   /* Directory with shared library */
  private static boolean arch_64_bit = false;


  private static boolean solaris;
  private static boolean windows;
  private static boolean zlinux;
  private static boolean aix;
  private static boolean hp;
  private static boolean linux;
  private static boolean mac;

  static
  {
    solaris = (System.getProperty("os.name").toLowerCase().startsWith("sunos") ||
               System.getProperty("os.name").toLowerCase().startsWith("solaris"));

    windows = (System.getProperty("os.name").toLowerCase().startsWith("windows"));

    zlinux  = (System.getProperty("os.name").toLowerCase().startsWith("linux") &&
               System.getProperty("os.arch").startsWith("s390"));

    aix     = (System.getProperty("os.name").toLowerCase().startsWith("aix"));

    hp      = (System.getProperty("os.name").toLowerCase().startsWith("hp-ux"));

    linux   = (System.getProperty("os.name").toLowerCase().startsWith("linux") ||
               System.getProperty("os.name").toLowerCase().startsWith("freebsd"));

    mac     = (System.getProperty("os.name").toLowerCase().startsWith("mac"));
  }


  /**
   * We need to blow up this program because of a fatal error.
   * We always keep some spare memory around to help us clean up!
   */
  static byte[] spare_memory = new byte[8*1024*1024];
  /**
   * Terminate run. A message text is displayed.
   *
   * Forcing two locks to make sure that if we get an other failure we already
   * have this lock. This forces failure_lock and ptod_lock to be obtained in
   * the same order, preventing dead locks.
   */
  public static void failure(Exception e)
  {
    synchronized(ptod_lock)
    {
      synchronized(failure_lock)
      {
        if (spare_memory != null)
          spare_memory = null;
        else
        {
          common.ptod("common.failure(): System.exit(-99)");
          common.where(8);
          exit(-99);
        }

        /* Give slaves a bity of time to clean up: */
        if (!SlaveJvm.isThisSlave())
        {
          SlaveList.sendWorkloadDone();
          common.sleep_some(500);
          SlaveList.shutdownAllSlaves();
        }

        FileAnchor.closeAllLogs();
        SD_entry.closeAllLogs();

        OS_cmd.killAll();

        /* On a slave immediately mark workload as done: */
        if (SlaveJvm.isThisSlave())
          SlaveJvm.setWorkloadDone(true);

        common.ptod("");
        common.ptod("common.failure(): \n\t\t" + e);
        e.printStackTrace();
        if (common.log_html != null)
          e.printStackTrace(common.log_html);

        Ctrl_c.removeShutdownHook();

        if (SlaveJvm.isFirstSlaveOnHost())
          Adm_msgs.copy_varadmmsgs();

        /* On a slave, notify master that we're aborting: */
        if (SlaveJvm.isThisSlave())
          SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_ABORTING, e.getMessage());

        /* Before we go further: see if an 'end_cmd' must be run: */
        if (!Vdbmain.simulate)
          Debug_cmds.ending_command.run_command();
      }

      /* Exit must be outside of the lock: */
      common.exit(-99);
    }
  }


  /**
   * Terminate run. A message text is displayed.
   */
  public static void failure(String format, Object ... args)
  {
    failure(String.format(format,args));
  }
  public static void failure(String txt)
  {
    synchronized(ptod_lock)
    {
      synchronized(failure_lock)
      {
        if (spare_memory != null)
          spare_memory = null;
        else
        {
          common.ptod("common.failure(): System.exit(-99)");
          exit(-99);
        }

        /* Give slaves a bity of time to clean up: */
        if (!SlaveJvm.isThisSlave())
        {
          SlaveList.sendWorkloadDone();
          common.sleep_some(500);
          SlaveList.shutdownAllSlaves();
        }

        OS_cmd.killAll();

        /* On a slave immediately mark workload as done: */
        if (SlaveJvm.isThisSlave() && !txt.equals(SlaveJvm.getMasterAbortMessage()))
          SlaveJvm.setWorkloadDone(true);

        /* Allow for proper multi-line error text. First single line: */
        if (txt.indexOf("\n") == -1)
        {
          common.ptod("");
          common.ptod(txt);
          common.ptod("");
        }

        else
        {
          common.ptod("");
          StringTokenizer st = new StringTokenizer(txt, "\n");
          while (st.hasMoreTokens())
            common.ptod(st.nextToken());
          common.ptod("");
        }

        if (get_debug(DIRECTORY_CREATED))
          FileAnchor.printAnchorStatus();


        Throwable t = new RuntimeException(txt);

        /* stderr from the slaves gets mixed in with whatever stdout data has */
        /* just been written, so for slaves stdout, for master stderr:        */
        t.printStackTrace( (SlaveJvm.isThisSlave()) ? System.out : System.err);

        /* Since we just went to stderr, make sure that log_html is not stdout: */
        if (common.log_html != null && common.log_html != stdout)
          t.printStackTrace(common.log_html);

        Ctrl_c.removeShutdownHook();

        if (SlaveJvm.isFirstSlaveOnHost())
          Adm_msgs.copy_varadmmsgs();

        /* On a slave, notify master that we're aborting: */
        if (SlaveJvm.isThisSlave() && !txt.equals(SlaveJvm.getMasterAbortMessage()))
          SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_ABORTING, txt);

        /* Before we go further: see if an 'end_cmd' must be run: */
        if (!Vdbmain.simulate)
          Debug_cmds.ending_command.run_command();

        /* Before we call System.exit() sleep for one second.  */
        /* This is done to allow the master to receive outstanding  */
        /* messages from a slave before the slave goes away.        */
        /* Without the master may get an EOFException from the socket   */
        /* Before it gets the chance to pick up the latest messages. */
        common.sleep_some(1000);
      }

      /* Exit must be outside of the lock: */
      common.exit(-99);
    }
  }


  /**
   * Preferred method to shut down a JVM.
   *
   *
   */
  public static void exit(int rc)
  {
    Ctrl_c.removeShutdownHook();
    OS_cmd.killAll();
    FileAnchor.closeAllLogs();
    SD_entry.closeAllLogs();

    //common.ptod("common.exit(): " + rc);
    //System.out.println("common.exit(): " + rc);

    System.exit(rc);
  }

  /**
   * Sleep x milliseconds
   */
  public static void sleep_some(long msecs)
  {
    if (msecs == 0)
      return;
    sleep_some_usecs(msecs * 1000);
  }


  /**
   * Sleep x microseconds
   */
  public static void sleep_some_usecs(long usecs)
  {
    //if (Vdbmain.isWorkloadDone() || SlaveJvm.isWorkloadDone())
    //{
    //  common.ptod("usecs: " + usecs);
    //  common.where(8);
    //}

    try
    {
      Thread.sleep(usecs / 1000, (int) (usecs % 1000) * 1000);
    }

    catch (InterruptedException x)
    {
      /* Basically ignore this interrupt now, but allow someone else to check */
      /* for interrupted()                                                    */
      common.interruptThread();
    }
  }


  /**
   * Print a timestamp followed by the included string.
   * If string is null, do not print a carriage return.
   */
  public static void ptod(String txt, PrintWriter pw)
  {
    //if (txt.contains("Searching for file names"))
    //  common.where(8);
    //if (txt.length() == 0)
    //  common.where(8);

    String tod = tod();

    synchronized(ptod_lock)
    {
      ptodFileIfNeeded(txt);

      /* During 'CTRL-C, don't bother writing any more messages to the console. */
      /* That just gets too confusing: */
      if (!(pw == stdout && Ctrl_c.active()))
      {
        if (txt != null)
          pw.println(tod + " " + txt);
        else
          pw.print(tod);
      }

      /* On a slave all messages go to stdout: */
      /* This is a shortcut for now. Preferably change all calls to plog() */
      /* to ptod() */
      if (SlaveJvm.isThisSlave() && pw == stdout)
        return;


      /* This acts the same as ptod(txt); stdout + logfile: */
      if (pw == stdout && common.log_html != null)
      {
        if (txt != null)
          common.log_html.println(tod + " " + txt);
        else
          common.log_html.print(tod);
      }
    }
  }

  private static Fput fp = null;
  private static void ptodFileIfNeeded(String txt)
  {
    if (!get_debug(PTOD_TO_DISK))
      return;
    if (fp == null)
    {
      for (int i = 0; i < 999; i++)
      {
        if (!new File("ptoddebug" + i).exists())
        {
          fp = new Fput("ptoddebug" + i);
          System.out.println("Created debug file: " + fp.getName());
          break;
        }
      }
    }

    fp.println(tod() + " " + txt);
    fp.flush();
  }

  public static synchronized void ptod(Exception e)
  {
    ptod(e.getClass().getName());
    if (common.log_html != null)
    {
      e.printStackTrace(common.log_html);
    }
    e.printStackTrace(System.out);
  }

  public static void ptod(String format, Object ... args)
  {
    ptod(String.format(format, args));
  }

  public static void pboth(String format, Object ... args)
  {
    /* If there are no arguments, don't use 'format': there may be some %% there: */
    if (args.length == 0)
    {
      psum(format);
      ptod(format);
    }
    else
    {
      psum(String.format(format, args));
      ptod(String.format(format, args));
    }
  }

  /**
   * Print timestamped line to stdout and logfile
   */
  public static void ptod(String txt)
  {
    //if (txt.trim().length() == 0)
    //  common.where(8);
    synchronized(ptod_lock)
    {
      /* For debugging: See also interruptThread() below: */
      if (txt.startsWith("+") && SlaveJvm.isThisSlave())
      {
        if (!Thread.currentThread().isInterrupted())
        {
          SlaveJvm.sendMessageToConsole(txt);
          return;
        }
        else
          txt = "sendMessageToConsole() bypassed due to interrupt: " + txt;
      }

      if (stdout == null)
      {
        if (common_pw == null )
          common_pw = new PrintWriter(System.out, true);
        common.ptod(txt, common_pw);
      }
      else
        common.ptod(txt, stdout);
    }
  }


  public static void ptod(ArrayList <String> lines)
  {
    synchronized (ptod_lock)
    {
      for (String line : lines)
        ptod(line);
    }
  }
  public static void ptod(Vector <String> lines)
  {
    synchronized (ptod_lock)
    {
      for (String line : lines)
        ptod(line);
    }
  }
  public static void ptod(Object obj)
  {
    ptod("" + obj);
  }
  public static void plog(Object obj)
  {
    plog("" + obj);
  }


  /**
   * Print timestamped line to the summary file
   */
  public static void psum(String format, Object ... args)
  {
    psum(String.format(format, args));
  }
  public static void psum(String txt)
  {
    /* If the summary file is not open yet, do not print the message: */
    if (common.summ_html != null)
      common.ptod(txt, common.summ_html);
  }


  /**
   * Print timestamped line to logfile
   */
  public static void plog(String format, Object ... args)
  {
    plog(String.format(format, args));
  }
  public static void plog(String txt)
  {
    /* If the logfile is not open yet, do not print the message: */
    if (common.log_html != null)
      common.ptod(txt, common.log_html);
    else if (!ignore_plog)
    {
      common.ptod("(plog) " + txt);
      //common.where(8);
    }
  }
  private static boolean ignore_plog = false;
  public static void ignoreIfNoPlog()
  {
    ignore_plog = true;
  }


  /**
   * Create a printable timestamp.
   * The reason why this is locked is a very unique hang in Java during the
   * termination of the master. Java (1.6?) was locked in
   * java.util.ResourceBundle.findBundle() trying create this
   * SimpleDateFormat.
   * No clue what caused the hang, but making this a locked static
   * SimpleDateFormat may prevent this from happening again.
   * (SimpleDateFormat is not thread-safe, so that's why the lock)
   */
  private static DateFormat locked_df = new SimpleDateFormat("HH:mm:ss.SSS");
  private static boolean more_detail = false;
  public static String tod()
  {

    synchronized(locked_df)
    {
      String ret = locked_df.format( new Date() );
      if (more_detail)
      {
        long simple = Native.get_simple_tod();
        long day    = simple % (24*3600000000l);  // 86.400.000.000
        long hour   = day    /     3600000000l;
        long min    = day    /       60000000l % 60;
        long sec    = day    /        1000000l % 60;
        long usec   = day    %        1000000l;
        ret += " " + String.format("%02d:%02d:%02d.%06d", hour, min, sec, usec);
      }
      return ret;
    }

  }

  /**
   * Print methods to write output to console AND log
   */
  public static void println(String text, PrintWriter pw)
  {

    pw.println(text);
    if (pw == stdout)
      common.log_html.println(text);
  }

  public static void print(String text, PrintWriter pw)
  {
    pw.print(text);
    if (pw == stdout)
      common.log_html.print(text);
  }


  /**
   * Simple wildcard search. Only trailing '*' is a wildcard.
   */
  public static boolean simple_wildcard(String key, String name)
  {
    if (key.charAt(key.length() -1) == '*')
    {
      String w1 = key.substring(0, key.length() - 1);

      if (name.length() < key.length() -1)
        return false;

      String w2 = name.substring(0, key.length() - 1);

      if (w1.compareTo(w2) == 0)
        return true;
      else
        return false;
    }

    if (key.compareTo(name) == 0)
      return true;
    else
      return false;
  }


  /**
   * Remove a comma from a string
   */
  public static String remove_comma(String name)
  {
    if (name.indexOf(",") == -1)
      return name;

    return name.substring(0, name.indexOf(",")) + name.substring(name.indexOf(",") + 1);
  }


  /**
   * Signal caller after n milliseconds.
   * Returns zero if more than 'msecs' time elapsed since the first call.
   */
  public static long signal_caller(long base, long msecs)
  {
    long tod = Native.get_simple_tod();

    /* First call, just set base tod: */
    if (base == 0)
      return tod;

    /* If tod expired, return 0 which is the signal that time expired */
    if (base + msecs * 1000 < tod)
      return 0;

    return base;
  }


  /**
   * Identify specific task completion failures and display appropriate message
   */
  public static void abnormal_term(Throwable t)
  {
    String bad_class = t.getClass().getName();

    synchronized(out_of_memory_lock)
    {
      if (out_of_memory)
        return;

      /* Free the spare memory we have just for this: */
      spare_memory = null;

      if (bad_class.compareTo("java.lang.OutOfMemoryError") == 0)
        displayOutOfMemory();

      if (bad_class.compareTo("java.lang.UnsatisfiedLinkError") == 0)
      {
        /* Report memory usage: */
        common.ptod("Missing or incorrect shared library file. \n\n");
      }

      if (common.log_html != null)
        t.printStackTrace(common.log_html);
      t.printStackTrace();
      common.failure("Abormal task completion");
    }
  }

  private static void displayOutOfMemory()
  {
    /* Report memory usage: */
    common.ptod("Out of memory: \n\n\t\t modify the vdbench script and increase the '-Xmx512m' value " +
                "where 512m equals the java heap size requested. ");
    common.ptod("If the error message says 'unable to create new native thread' " +
                "modify the vdbench script adding '-Xss256k' or lower value for " +
                "the java thread stack size. \n\n");
    common.ptod("Examples are for Solaris. For other platforms see the Java provider's documentation");

    common.memory_usage();
    out_of_memory = true;

    if (get_debug(RUN_JMAP))
    {
      spare_memory = null;
      runJmap();
    }
  }

  public static int getProcessId()
  {
    return Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
  }
  public static String getProcessIdString()
  {
    return "" + getProcessId();
  }

  public static void runJmap()
  {
    String[] args = new String[]
    {
      "xx", getProcessIdString()
      //"xx", pid, "-m0"
    };
    Jmap.main(args);

  }

  /**
   * Report Java specific memory usage
   */
  public static void memory_usage()
  {
    double free    = (double) Runtime.getRuntime().freeMemory()  / 1048576.0;
    double current = (double) Runtime.getRuntime().totalMemory() / 1048576.0;
    double max     = (double) Runtime.getRuntime().maxMemory()   / 1048576.0;
    double used    = current - free;

    if (common.get_debug(common.PRINT_MEMORY))
      common.ptod("Java Heap in MB. max: %8.3f; current: %8.3f; used: %8.3f; free: %8.3f",
                  max, current, used, free);
    else
      common.plog("Java Heap in MB. max: %8.3f; current: %8.3f; used: %8.3f; free: %8.3f",
                  max, current, used, free);
  }


  /**
   * Debugging flags: set
   */
  public static void set_debug(int number)
  {
    Utils.common.set_debug(number);
  }



  /**
   * Debugging flags: query
   */
  public static boolean get_debug(int number)
  {
    /* Someone asks for zero, he'll get it no matter what: */
    if (number == 0)
      return true;

    return Utils.common.get_debug(number);
  }


  /**
   * Debugging flags: get string with flags to pass to others
   */
  public static String get_debug_string()
  {
    return Utils.common.get_debug_string();
  }


  /**
   * Check where we're running
   */
  static boolean onAix()
  {
    return aix;
  }
  static boolean onHp()
  {
    return hp;
  }
  public static boolean onLinux()
  {
    return linux;
  }
  public static boolean onZLinux()
  {
    return zlinux;
  }
  public static boolean onWindows()
  {
    return windows;
  }
  public static boolean onSolaris()
  {
    return solaris;
  }
  static boolean onMac()
  {
    return mac;
  }
  static boolean running64Bit()
  {
    common.get_shared_lib();
    return arch_64_bit;
  }



  /**
   * Tell Java where he can find the JNI modules
   */
  public static String get_shared_lib()
  {
    if (shared_library_dir != null)
      return shared_library_dir;

    String shared_library = null;
    String sep       = System.getProperty("file.separator");
    String bits      = System.getProperty("sun.arch.data.model");
    String arch      = System.getProperty("os.arch");
    String classpath = ClassPath.classPath();
    String dir       = null;

    if (onWindows())
    {
      if (bits.equals("64"))
      {
        if (Fget.file_exists(classpath + "windows", "vdbench64.dll"))
          dir = "windows" + sep + "vdbench64.dll";
        else
        {
          String txt = "\n\n";
          txt += "Vdbench does not support 64-bit java on Windows.";
          txt += "\n\tPlease install 32-bit java and change file vdbench.bat ";
          txt += "\n\tto use that new 32-bit version.";
          txt += "\n\n";
          common.failure(txt);
        }
      }
      else
        dir = "windows" + sep + "vdbench32.dll";
    }

    else if (onSolaris())
    {
      if (arch.equals("amd64"))
      {
        dir = "solx86" + sep + "solx86-64.so";
        arch_64_bit = true;
      }

      else if (arch.equals("x86"))
        dir = "solx86" + sep + "solx86-32.so";

      else if (arch.equals("sparc"))
        dir = "solaris" + sep + "sparc32.so";

      else if (arch.equals("sparcv9"))
      {
        dir = "solaris" + sep + "sparc64.so";
        arch_64_bit = true;
      }

      else common.failure("Unknown system architecture for solaris: " + " " + arch);
    }

    else if (onAix())
    {
      if (bits != null && bits.equals("64"))
      {
        dir = "aix" + sep + "aix-64.so";
        arch_64_bit = true;
      }
      else if (arch.indexOf("64") != -1)
      {
        dir = "aix" + sep + "aix-64.so";
        arch_64_bit = true;
      }
      else
        dir = "aix" + sep + "aix-32.so";
    }

    else if (onMac())
      dir = "mac" + sep + "libvdbench.dylib";

    //18:04:17.319 os.name                       HP-UX
    //18:04:17.319 os.arch                       IA64N
    else if (onHp())
      dir = "hp" + sep + "libvdbench.sl";

    else if (onZLinux())
    {
      if (arch.equals("s390"))
        dir = "linux" + sep + "zlinux32.so";
      else
      {
        dir = "linux" + sep + "zlinux64.so";
        arch_64_bit = true;
      }
    }

    else if (onLinux())
    {
      if (arch.equals("sparcv9"))
      {
        dir = "linux" + sep + "sparc64.so";
        arch_64_bit = true;
      }

      else if (arch.equals("arm"))
      {
        dir = "linux" + sep + "arm32.so";
        arch_64_bit = false;
      }

      else if (arch.equals("aarch64"))
      {
        dir = "linux" + sep + "aarch64.so";
        arch_64_bit = true;
      }

      else if (arch.equals("sparc"))
        dir = "linux" + sep + "sparc32.so";

      else if (arch.equals("ppc64"))       // From Jvon Barnes
      {
        dir = "linux" + sep + "ppc64.so";
        arch_64_bit = true;
      }

      else if (arch.equals("ppc32"))       // This is not confirmed!
        dir = "linux" + sep + "ppc32.so";

      else if (arch.indexOf("64") != -1)
      {
        dir = "linux" + sep + "linux64.so";
        arch_64_bit = true;
      }

      else if (bits != null && bits.equalsIgnoreCase("64"))
      {
        dir = "linux" + sep + "linux64.so";
        arch_64_bit = true;
      }

      else
        dir = "linux" + sep + "linux32.so";
    }


    else
      common.failure("Undefined support requested for platform: " +
                     System.getProperty("os.arch") +
                     "; contact me at the Oracle Vdbench Forum for support");

    File full = new File(classpath + dir);
    try
    {
      shared_library     = full.getCanonicalPath();
      shared_library_dir = full.getParent() + File.separator;
      //common.ptod("Setting shared library to: " + shared_library);

      /* This is here to deal with VirtualBox not supporting memory mapping:  */
      /* (Apparently only for shared library loading though. DV mmap is fine) */
      if (get_debug(USE_TMP_SHARED_LIBRARY))
      {
        String temp = Utils.CopyFile.copyToTemp(shared_library);
        //common.plog("1Setting shared library to: " + temp);
        System.load(temp);
      }
      else
      {
        //common.plog("2Setting shared library to: " + shared_library);
        System.load(shared_library);
      }



    }

    catch (Throwable t)
    {
      synchronized (ptod_lock)
      {
        if (!full.exists())
        {
          common.ptod("");
          common.ptod("File " + shared_library + " does not exist.");
          common.ptod("This may be an OS that a shared library currently ");
          common.ptod("is not available for. You may have to do your own compile.");
        }
        common.ptod("t: " + t);

        common.ptod("");
        common.ptod("Loading of shared library " + shared_library + " failed.");
        common.ptod("If the error message relates to 32 vs. 64bit processing ");
        common.ptod("please understand that Vdbench supports 32 and 64-bit Java ");
        common.ptod("for Solaris and Linux only at this time. ");
        common.ptod("There also may be issues related to a cpu type not being ");
        common.ptod("acceptable to Vdbench, e.g. MAC PPC vs. X86");
        common.ptod("Contact me at the Oracle Vdbench Forum for support.");
        common.ptod("");
        common.failure("Failure loading shared library");
      }
    }

    return shared_library_dir;
  }


  /**
   * Replace a string within a string.
   * This is a 'replace once'.
   */
  public static String replace_string(String string, String find, String replace)
  {

    if (string.indexOf(find) != -1)
    {
      String one = string.substring(0, string.indexOf(find));
      String two = string.substring(string.indexOf(find) + find.length());
      return one + replace + two;
    }
    else
      return string;
  }


  /**
   * Run vdb init scripts.
   * Execute config.sh from the install directory eg. vdbench/solaris/
   * and also my_config.sh to prevent a reinstall from overlaying user's
   * modified contents of config.sh.
   */
  public static void run_config_scripts()
  {
    long start = System.currentTimeMillis();
    PrintWriter init = null;

    if (get_debug(NO_CONFIG_SCRIPT))
      return;

    /* Only run command when it exists in our own library: */
    String command1 = common.get_shared_lib() + "config.sh";
    String command2 = common.get_shared_lib() + "my_config.sh";
    if (Fget.file_exists(command1) || Fget.file_exists(command2))
    {
      init = Report.createHmtlFile("config.html");
      Report.chModAllReports();
      Report.getSummaryReport().printHtmlLink("Link to config file", "config", "config");
    }

    if (Fget.file_exists(command1))
    {

      /* Before we can run os_command() Native must load the shared libraries: */
      Native x = new Native();

      /* Run config.sh: */
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText(command1);
      ocmd.execute(false);
      String[] stdout = ocmd.getStdout();
      String[] stderr = ocmd.getStderr();

      common.ptod("config.sh command output:\n", init);
      for (int i = 0; i < stdout.length; i++)
        init.println("stdout: " + stdout[i]);
      for (int i = 0; i < stderr.length; i++)
        init.println("stderr: " + stderr[i]);
    }


    /* Only run command when it exists in our own library: */
    if (Fget.file_exists(command2))
    {

      /* Run my_config.sh: */
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText(command2);
      ocmd.execute();
      String[] stdout = ocmd.getStdout();
      String[] stderr = ocmd.getStderr();
      if (stdout.length + stderr.length > 0)
      {
        init.println("");
        common.ptod("my_config.sh command output:\n", init);
      }
      for (int i = 0; i < stdout.length; i++)
        init.println("stdout: " + stdout[i]);
      for (int i = 0; i < stderr.length; i++)
        init.println("stderr: " + stderr[i]);

    }

    if (init != null)
      init.close();


    long elapsed = System.currentTimeMillis() - start;
    if (elapsed > 5000)
      common.ptod("Running 'config.sh' took more than 5 seconds: " + elapsed + "ms.");
  }


  /**
   * Replace String within String.
   * This is a 'replace all'.
   */
  public static String replace(String source, String old_value, String new_value)
  {
    //common.ptod("source: " + source);
    //common.ptod("value: " + value);
    //common.ptod("nval: " + nval);

    while (source.indexOf(old_value) != -1)
    {
      int index = source.indexOf(old_value);
      source = source.substring(0, index) + new_value + source.substring(index+old_value.length());
    }

    //common.ptod("source: " + source);
    return source;
  }


  /**
   * Debugging, write/read object
   */
  public static void serial_out(String fname, Object obj)
  {
    ObjectOutputStream os;
    try
    {
      os = new ObjectOutputStream(new FileOutputStream(fname));
      os.writeObject(obj);
      os.close();
    }
    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public static Object serial_in(String fname)
  {
    if (!Fget.file_exists(fname))
      common.failure("unknown serial_in file: " + fname);
    Object obj  = null;

    try
    {
      ObjectInputStream is = new ObjectInputStream(new FileInputStream(fname));
      obj = is.readObject();
      is.close();
    }
    catch (StreamCorruptedException e)
    {
      //e.printStackTrace(log);
      //e.printStackTrace();
      return null;
    }
    catch (Exception e)
    {
      e.printStackTrace(log_html);
      e.printStackTrace();
      return null;
    }

    return obj;
  }



  private static void doWhere(int lines_wanted, String txt)
  {
    synchronized (ptod_lock)
    {
      String line;
      int lines_done = 0;
      StackTraceElement[] stack = new Throwable().getStackTrace();

      if (!get_debug(PRINT_WIDE_WHERE))
      {
        if (txt != null)
          common.ptod("==> where: " + txt);
        for (int i = 2; i < stack.length && lines_done++ < lines_wanted; i++)
        {
          if (txt == null && lines_done == 1)
            line = "==> where: ";
          else
            line = "           ";
          line += stack[i].toString();
          common.ptod(line);
        }
      }

      else
      {
        StringBuffer buf = new StringBuffer(1024);
        if (txt != null)
          buf.append("==> where: " + txt );
        for (int i = 2; i < stack.length && lines_done++ < lines_wanted; i++)
        {
          if (txt == null && lines_done == 1)
            buf.append(" ==> where: ");
          else
            buf.append(" ");
          buf.append(stack[i].toString());
        }
        common.ptod(buf.toString());
      }
    }
  }

  public static void where(int lines_wanted, String txt)
  {
    doWhere(lines_wanted, txt);
  }

  public static void where()
  {
    doWhere(1, null);
  }

  public static void where(int lines)
  {
    doWhere(lines, null);
  }

  public static void where(String txt)
  {
    doWhere(1, txt);
  }


  public static void printStacktrace()
  {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    String txt = "\n\t\tStack Trace: ";
    for (int i = 0; stack != null && i < stack.length; i++)
      txt += "\n\t\t at " + stack[i].toString();

    common.ptod(txt);
  }
  public static String getStacktrace()
  {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    String txt = "\n\t\tStack Trace: ";
    for (int i = 0; stack != null && i < stack.length; i++)
      txt += "\n\t\t at " + stack[i].toString();

    return txt;
  }

  public static void main(String args[])
  {
    //   long num = Long.parseLong(args[0]);
    //   Date date = new Date(num / 1000);
    //   common.ptod("date: " + date);
    //
    //   DateFormat df = new SimpleDateFormat("EEEE, MMMM dd yyyy, HH:mm:ss.SSS zzz" );
    //   df.setTimeZone(TimeZone.getTimeZone("PST"));
    //   common.ptod("df: " + df.format(num / 1000));

    //  int key_block_size = 512;
    //  int compare  = 4096000;
    //
    //  for (int i = 0; i < 37356972; i++)
    //  {
    //    //long lba = 2000 * 1024*1024l + i * 512;
    //    long lba = i *  key_block_size;
    //    int block = (int) (lba / key_block_size + i);
    //    if (block == compare)
    //      common.ptod("lba:   %,14d %016x %,12d %,d", lba, lba, block, i);
    //  }
    //

    //   String[] list = new String[]
    //   {
    //     "0",
    //     "1f400000",
    //     "3e800000",
    //     "5dc00000",
    //     "7d000000",
    //     "7d03c800",
    //     "9c43c800",
    //     "bb83c800",
    //     "dac3c800",
    //     "dac79000",
    //     "fa079000",
    //     "119479000",
    //     "138879000",
    //     "157c79000",
    //     "177079000",
    //     "3f7079000",
    //     "416479000",
    //     "435879000",
    //     "454c79000",
    //     "474079000"
    //   };
    //
    //   int key_block_size = 512;
    //   for (String str : list)
    //   {
    //     long lba  = Long.parseLong(str, 16);
    //     int block = (int) (lba / key_block_size);
    //     common.ptod("lba:   %,14d %016x %,12d ", lba, lba, block);
    //   }



    //   int key_block_size = 512;
    //
    //   for (int block = 0; block <  37356972; block++)
    //   {
    //     long lba = (long) block * (long) key_block_size;
    //     if (block == 4096000 || block == 4096001)
    //       common.ptod("lba: %,14d %08x %,10d ", lba, lba, block);
    //   }

    //   long value = 0x0123456789abcdefL;
    //
    //   byte [] bytes  = new byte[8];
    //   for (int b = 0; b < 8; b++)
    //   {
    //     bytes[b] = (byte) (value >>> (56 - (b*8)));
    //     common.ptod("bytes[b]: %02x", bytes[b]);
    //   }

    long dedup_block = 10000;
    long relative_dedup_offset  = 77779173376l * 40;
    int  dedup_unit = 8192;
    int  rel_block    = (int) (dedup_block + relative_dedup_offset / dedup_unit);

    common.ptod("dedup_block: %,12d          ", dedup_block);
    common.ptod("relative_dedup_offset: %,12d", relative_dedup_offset);

    common.ptod("rel_block:                  " + rel_block);


  }

  public static void main1(String args[])
  {

    // test sign bit after picking up a long
    //   int  key   = 0xff;
    //   long block = 0xfffffffffL;
    //
    //   long stored = ((long) key) << 56 | block;
    //   common.ptod("key: %02x %08x", key, block);
    //   common.ptod("key: %02x %08x %08x %016x", key, (int) (block >> 32), (int) block, stored);
    //
    //   key   = (int) (stored >>> 56);
    //   block = stored & 0xffffffffffffL;
    //
    //   common.ptod("key: %02x %08x %08x %016x", key, (int) (block >> 32), (int) block, stored);
    //
    //   key   = (int) (stored >>> 56);
    //   block = stored << 8 >>> 8;
    //
    //   common.ptod("key: %02x %08x %08x %016x", key, (int) (block >> 32), (int) block, stored);


    int key    = 0xff;
    int block0 = 0x7;
    int block1 = 0x89abcdef;

    long long_value = make64(block0, block1);
    common.ptod("long_value: %016x", long_value);
    common.ptod("long_value: %016x", addKey(key, long_value));
    common.ptod("long_value: %016x", make64Key(key, block0, block1));

    long_value = make64Key(key, block0, block1);
    common.ptod("key: " + getKey(long_value));

    common.ptod("left32:  %08x ", left32(long_value) );
    common.ptod("right32: %08x ", right32(long_value));

  }


  private static int left32(long long_value)
  {
    return(int) (long_value >>> 32);
  }
  private static int right32(long long_value)
  {
    return(int) long_value;
  }
  private static long make64Key(long key, int left, int right)
  {
    return make64(left, right) | (key << 56);
  }
  private static long make64(int left, int right)
  {
    long long_value = ((long) left) << 32;
    long_value     |= ((long) right) &0xffffffffL;
    return long_value;
  }
  public static int getKey(long long_value)
  {
    return(int) ((long_value >> 56) & 0xff);
  }
  public static long addKey(long key, long long_value)
  {
    return(key << 56) | long_value;
  }

  // from java 1.8 Integer.toUnsignedLong()
  public static long toUnsignedLong(int x)
  {
    return((long) x) & 0xffffffffL;
  }


  public static void main3(String args[]) throws Exception
  {
    long MB             = 1024l * 1024l;
    long GB             = 1024l * 1024l * 1024l;
    long TB             = 1024l * 1024l * 1024l * 1024l;
    long unit           = 4 * 1024l;
    long blocks         = 100 * MB / unit;
    long uniques_needed = 10000; // blocks * 40 / 100;
    long uniques_found  = 0;
    long modulo         = blocks / uniques_needed;


    common.ptod("blocks:         " + blocks);
    common.ptod("uniques_needed: " + uniques_needed);
    common.ptod("modulo:         " + modulo);

    for (long block = 0; block < blocks; block ++)
    {
      if (block % modulo == 0)
      {
        if (block < 20)
        {
          //common.ptod("result: block: %4.0f %40.16f ", block, result);
          common.ptod("block: " + block);
        }
        uniques_found++;
      }
    }
    common.ptod("blocks:         " + blocks);
    common.ptod("uniques_needed: " + uniques_needed);
    common.ptod("uniques_found:  " + uniques_found);



    //   long MB             = 1024l * 1024l;
    //   long GB             = 1024l * 1024l * 1024l;
    //   long TB             = 1024l * 1024l * 1024l * 1024l;
    //   long unit           = 128 * 1024l;
    //   long blocks         = 100 * MB / unit;
    //   long uniques_needed = 360; // blocks * 50 / 100;
    //   long uniques_found  = 0;
    //
    //   double every       = (double) blocks / uniques_needed;
    //   long   delta_every = (long) ((every / 100) * 1000000);
    //   common.ptod("every:       " + every);
    //   common.ptod("delta_every: " + delta_every);
    //
    //   common.ptod("blocks:         " + blocks);
    //   common.ptod("uniques_needed: " + uniques_needed);
    //   common.ptod("blocks / uniques_needed: " + blocks / uniques_needed);
    //
    //   for (long block = 0; block < blocks; block ++)
    //   {
    //     double t1    = (double) block / uniques_needed;
    //     long   t2    = block / uniques_needed;
    //     double delta = t1 - t2;
    //     long   delta_long = (long) (delta * 1000000) ;
    //     //delta_long = delta_long % 100000;
    //
    //     //common.ptod("delta: %08d", (long) (delta * 1000000));
    //
    //     boolean match = delta_long % delta_every == 0;
    //     //if (block < 30)
    //     //if (match)
    //     {
    //       common.ptod("t1: %12.8f %8df %12.8f %5b %08d %08d", t1, t2, delta,  match, delta_long, delta_every);
    //       //common.ptod("block: " + block);
    //     }
    //
    //     if (match)
    //     //if (block % (blocks / uniques_needed) == 0)
    //     {
    //       uniques_found++;
    //     }
    //   }
    //   common.ptod("blocks:         " + blocks);
    //   common.ptod("uniques_needed: " + uniques_needed);
    //   common.ptod("uniques_found:  " + uniques_found);
  }

  /**
   * sizeof
   *
   * Minimum 8 bytes for any even empty instance.
   **/
  public static void sizeof(String args[]) throws Exception
  {
    int loops = 1000000;
    if (args.length > 0)
      loops = Integer.parseInt(args[0]);
    Object[] sink = new Object[loops];

    /* Make sure we have no old garbage: */
    System.gc();
    System.gc();
    double used_at_start = Runtime.getRuntime().totalMemory() -
                           Runtime.getRuntime().freeMemory();


    for (int i = 0; i < loops; i++)
    {
      sink[i] = new FileEntry();
    }


    System.gc();
    System.gc();
    double used_at_end = Runtime.getRuntime().totalMemory() -
                         Runtime.getRuntime().freeMemory();

    common.ptod("used_at_start: " + used_at_start / 1048576.);
    common.ptod("used_at_end:   " + used_at_end / 1048576.);
    common.ptod("used_at_end:   " + (used_at_end - used_at_start));
    common.ptod("estimated size per instance: " + (int) ((used_at_end - used_at_start) / loops));

    /* This code is here to assure that GC can not clean up the sink array yet: */
    long dummy = 0;
    for (int i = 0; i < loops; i++)
      dummy += sink[i].hashCode();
  }


  /**
  * Get the IP address for the current host.
  *
  * There has been a problem once with the system not being able to find its own
  * local IP addres:
  *
  *   The /etc/nsswitch.conf file entry for hosts was:
  * nis [NOTFOUND=return] files
  *
  * This host is not in NIS, so I reordered the entry to
  * files nis [NOTFOUND=return]
  * John.Wieczorek@Sun.COM
  */
  public static String getCurrentIP()
  {
    String current_ip = null;
    try
    {
      current_ip = InetAddress.getLocalHost().getHostAddress();
    }
    catch (UnknownHostException e)
    {
      try
      {
        common.ptod("Can not determine current network IP address: " + InetAddress.getLocalHost());
        common.ptod("Returning instead the host name: " + InetAddress.getLocalHost().getHostName());

        return InetAddress.getLocalHost().getHostName();
      }
      catch (UnknownHostException e2)
      {
        common.where();
        common.failure(e2);
      }
    }

    return current_ip;
  }


  private static String fatal_marker = "*Fatal error*";
  public static boolean isFatal(String line)
  {
    if (line == null)
      return false;
    return(line.indexOf(fatal_marker) != -1);
  }


  public static void notifySlaves()
  {
    /* try to send message to the slaves so that they know this was a 'deliberate' */
    /* termination of the socket: */
    SlaveList.killSlaves();

    // Extra message because there is something fishy that I can not explain
    common.ptod("Debugging message: " + get_stacktrace());

    /* Give the message the chance to get there: */
    common.sleep_some(1000);
  }


  public static String get_stacktrace()
  {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    String txt = "\nStack Trace: ";
    for (int i = 0; stack != null && i < stack.length; i++)
      txt += "\n at " + stack[i].toString();
    return txt;
  }

  public static String get_stacktrace(Exception e)
  {
    StackTraceElement[] stack = e.getStackTrace();
    String txt = "\nStack Trace: ";
    for (int i = 0; stack != null && i < stack.length; i++)
      txt += "\n at " + stack[i].toString();
    return txt;
  }

  public static void main2(String args[]) throws Exception
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("/usr/bin/ls -alF " + args[0]);

    ocmd.execute();
    String[] stdout = ocmd.getStdout();
    if (stdout.length != 1)
    {
      for (int i = 0; i < stdout.length; i++)
        common.ptod("stdout: " + stdout[i]);
      common.ptod("Unexpected length from 'ls' command");
      return;
    }

    String line = stdout[0];

    StringTokenizer st = new StringTokenizer(line);
    if (st.countTokens() != 11)
    {
      common.ptod("line: " + line);
      common.ptod("Unexpected contents from 'ls' command");
      return;
    }

    String device_name = line.substring(line.lastIndexOf(" ") + 1);

    common.ptod("device_name: " + device_name);

    if (!device_name.startsWith("../../devices"))
    {
      common.ptod("device_name: " + device_name);
      common.ptod("Device name does not start with ../../devices");
      return;
    }

    device_name = device_name.substring(13);
    common.ptod("device_name: " + device_name);

    if (!device_name.endsWith(":"))
    {
      common.ptod("device_name: " + device_name);
      common.ptod("Device name does not end with ':'");
      return;
    }

    device_name = device_name.substring(0, device_name.length() -1);
    common.ptod("device_name: " + device_name);



    Vector paths = Fget.read_file_to_vector("/etc/path_to_inst");
    for (int i = 0; i < paths.size(); i++)
    {
      line = (String) paths.elementAt(i);
      if (line.startsWith("#"))
        continue;
      st = new StringTokenizer(line);
      if (st.countTokens() != 3)
        continue;

      //if (line.indexOf("9ec940") == -1)
      //  continue;

      String path   = common.replace(st.nextToken(), "\"", "");
      String number = st.nextToken();
      String driver = common.replace(st.nextToken(), "\"", "");

      //common.ptod("path:        " + path);
      //common.ptod("device_name: " + device_name);

      if (!device_name.equals(path))
        continue;

      common.ptod("path:   " + path);
      common.ptod("number: " + number);
      common.ptod("driver: " + driver);
    }

  }


  // This works only for Java 1.5
  public static void dumpAllStacks()
  {
    ThreadGroup tg = Thread.currentThread().getThreadGroup();
    ptod("tg: " + tg);
    ptod("tg.get_name: " + tg.getName());
    ptod("tg.activeCount: " + tg.activeCount());
    tg.list();

    Thread tl[] = new Thread[Thread.currentThread().activeCount() + 5]; // Fudge factor
    int threads = Thread.currentThread().enumerate(tl);

    for (int i = 0; i < threads; i++)
    {
      ptod("tl            " + tl);
      ptod("tl[]:         " + tl[i]);
      ptod("tl[].isalive: " + tl[i].isAlive());
      ptod("Threadname:   " + tl[i].getName());
      dumpOneStack(tl[i]);
    }
  }
  public static void dumpOneStack(Thread th)
  {
    //common.ptod("dumpOneStack functionality depends on using Java 1.5 and up. ivgnored. ");
    StackTraceElement[] els = th.getStackTrace();

    for (int i = 0; i < els.length; i++)
      ptod("dumpOneStack: " + els[i].getClassName() + " " + els[i].getLineNumber());
  }

  public static long parseSize(String prm)
  {
    if (prm.startsWith("0x"))
      return Long.parseLong(prm.substring(2), 16);

    String parm = prm;
    long multi = 1;
    if (parm.endsWith("k"))
      multi = 1024l;
    else if (parm.endsWith("m"))
      multi = 1024l * 1024l;
    else if (parm.endsWith("g"))
      multi = 1024l * 1024l * 1024l;
    else if (parm.endsWith("t"))
      multi = 1024l * 1024l * 1024l * 1024l;

    if (multi != 1)
      parm = parm.substring(0, parm.length() - 1);

    try
    {
      return Long.parseLong(parm) * multi;
    }
    catch (Exception e)
    {
      common.ptod("Numeric value parsing; invalid value: %s", prm);
      common.failure(e);
    }
    return 0;
  }


  /**
   * Make large numbers look easier to deal with.
   */
  public static String whatSize(double size)
  {
    return whatSizeX(size, 3);
  }

  public static String whatSizeX(double size, int digits)
  {
    double KB = 1024.;
    double MB = 1024. * 1024.;
    double GB = 1024. * 1024. * 1024.;
    double TB = 1024. * 1024. * 1024. * 1024.;
    double PB = 1024. * 1024. * 1024. * 1024. * 1024.;
    String txt;
    String mask = String.format("%%.%df%%s", digits);
    String tail_end = ".000000000000000".substring(0, 1+digits);

    if (size < 100000)
      txt = "" + (int) size;
    else if (size < MB)
      txt = String.format(mask, size / KB, "k");
    else if (size < GB)
      txt = String.format(mask, size / MB, "m");
    else if (size < TB)
      txt = String.format(mask, size / GB, "g");
    else if (size < PB)
      txt = String.format(mask, size / TB, "t");
    else
      txt = String.format(mask, size / PB, "p");

    /* Remove '.000' if this is a 'nice' number: */
    // Can't do that 1.000m may mean it is a 1.0001 result, so not clean.

    return txt;
  }

  public static boolean isNumeric(String txt)
  {
    try
    {
      long number = Long.parseLong(txt);
    }
    catch (NumberFormatException e)
    {
      return false;
    }
    return true;
  }
  public static boolean isDouble(String txt)
  {
    try
    {
      double number = Double.parseDouble(txt);
    }
    catch (NumberFormatException e)
    {
      return false;
    }
    return true;
  }

  /**
   * Interrupt any thread.
   * This lock is needed to avoid anything during a ptod() from being
   * interrupted causing aborts.
   *
   * For some still unbelievable reason, when doing a ptod() AFTER the
   * t.interrupt(), the interrupt is LOST..... ?????
   * There's nothing in Vdbench code that directly could cause this.
   * 09/24/2010
   */
  public static void interruptThread()
  {
    interruptThread(Thread.currentThread());
  }
  public static void interruptThread(Thread t)
  {
    synchronized (ptod_lock)
    {
      t.interrupt();

      //ptod("after: " + Thread.currentThread());
      //ptod("interruptThread2 " + t + " " + t.isInterrupted());
      //ptod("interruptThread3 " + t + " " + t.isInterrupted());
      //ptod("interruptThread4 " + t + " " + t.isInterrupted());
    }
  }
}


