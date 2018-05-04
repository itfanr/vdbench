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

import java.util.Calendar;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;
import java.net.*;
import Utils.Format;
import Utils.ClassPath;
import Utils.Encryptor;
import Utils.Fput;
import Utils.Fget;
import Utils.OS_cmd;




/**
  * The common class contains some general service methods
  *
  * Warning: some calls from code in the Utils package to similary named methods
  * here will NOT actually use the code below!
  * Need to prevent that some day.
  */
public class common
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


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

  static long current_sleep;

  static long last_interval_tod = 0;

  /* Debugging flags. The settings are done in Utils.common() */

  public static int PRINT_OPEN_FLAGS      = 1;
  public static int PRINT_BUCKETS         = 2;
  public static int PRINT_MEMORY          = 3;
  public static int NO_PRINT_FLUSH        = 4;

  public static int DEVXLATE              = 6;
  public static int PRINT_BLOCK_COUNTERS  = 7;
  public static int FIXED_SEED            = 8;
  public static int OLD_FORMAT            = 9;
  public static int PRINT_SIZES           = 10;
  public static int FAST_HEADERS          = 11;
  public static int BURST_OF_ONE          = 12;
  public static int TIMERS                = 13;

  public static int PRINT_IO_COMP         = 15;

  public static int NO_KSTAT              = 17;
  public static int NO_CONFIG_SCRIPT      = 18;
  public static int REPLAY_FOLD           = 19;
  public static int SPIN                  = 20;   /* set hires_tick=1 works just as well */
  public static int PTOD_TO_DISK          = 21;

  public static int DV_UNIQUE_SD          = 23;
  public static int IGNORE_MISSING_REPLAY = 24;
  public static int NO_ADM_ON_CONSOLE     = 25;   // Given to Tim Szeto */

  public static int LONGER_HEARTBEAT      = 27;   // -d27 is documented in blog!
  public static int SHORTER_HEARTBEAT     = 28;
  public static int FILEENTRY_SET_BUSY    = 29;
  public static int PRINT_DATA            = 30;

  public static int DIRECTORY_SET_BUSY    = 32;
  public static int DIRECTORY_CREATED     = 33;
  public static int NO_GUI_WARNING        = 34;
  public static int REPORT_CREATES        = 35;
  public static int FAST_SYNCTIME         = 36;
  public static int FAST_BLOCK_KILL       = 37;
  public static int USE_FORMAT_RATE       = 38;
  public static int LOCAL_JVM             = 39;  // can cause jvm hangs at the end?
  public static int FORCE_KSTAT_ERROR     = 40;
  public static int SCSI_RESET_AT_START   = 41;
  public static int SCSI_RESET_ALL_START  = 42;
  public static int SOCKET_TRAFFIC        = 43;
  public static int SLAVE_LOG_ON_CONSOLE  = 44;
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

  public static int SHORT_INTERVALS1      = 61;
  public static int SHORT_INTERVALS2      = 62;

  public static int USE_TVDBENCH          = 75;
  public static int USE_ANY_JAVA          = 76;
  public static int DEBUG_SPREAD          = 77;


  public static int ORACLE_QUEUES         = 83;
  public static int ORACLE                = 84;
  public static int ORACLE_NO_CONSOLE     = 85;

  public static int PRINT_EACH_IO         = 99;

  /* Data Validation debugging options:                               */
  /* Proper sequence for a forced failed run getting pending writes.  */
  /* -jn -d101, then -jr.                                             */
  public static int DEBUG_DV_CLEANUP      = 101;
  public static int DONT_DUMP_MAPS        = 102;
  public static int FORCE_ERROR_NOWRITE   = 103;
  public static int FORCE_ERROR_NOAFTER   = 104;
  public static int DV_DEBUG_WRITE_ERROR  = 105;   /* Only works with force_error_after=n */
  public static int ACCEPT_DV_NOREADS     = 106;

  public static int COUNT_INSTANCES       = 204;



  private static String shared_library_dir = null;   /* Directory with shared library */


  private static boolean solaris;
  private static boolean windows;
  private static boolean zlinux;
  private static boolean aix;
  private static boolean hp;
  private static boolean linux;
  private static boolean freebsd;
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

    linux   = (System.getProperty("os.name").toLowerCase().startsWith("linux"));

    freebsd = (System.getProperty("os.name").toLowerCase().startsWith("freebsd"));

    mac     = (System.getProperty("os.name").toLowerCase().startsWith("mac"));
  }


  /**
   * We need to blow up this program because of a fatal error.
   * We always keep some spare memory around to help us clean up!
   */
  static byte[] spare_memory = new byte[4*1024*1024];
  /**
   * Generate an exception and print calling stack
   */
  public static void failure(Exception e)
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
      Debug_cmds.ending_command.run_command();

      Native.free_jni_shared_memory();
    }

    /* Exit must be outside of the lock: */
    common.exit(-99);
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
    synchronized(failure_lock)
    {
      if (spare_memory != null)
        spare_memory = null;
      else
      {
        common.ptod("common.failure(): System.exit(-99)");
        exit(-99);
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

      Throwable t = new RuntimeException(txt);

      /* This goes to stderr: */
      t.printStackTrace();

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
      Debug_cmds.ending_command.run_command();

      /* In case there are vdblite processes running, tell them to shut up: */
      /* (jni will just return if there aren't any)                         */
      WG_stats stats = new WG_stats();
      Native.get_one_set_statistics(stats, 0, true);

      Native.free_jni_shared_memory();

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


  /**
   * Preferred method to shut down a JVM.
   *
   *
   */
  public static void exit(int rc)
  {
    Ctrl_c.removeShutdownHook();
    OS_cmd.killAll();

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
    try
    {
      Thread.sleep(usecs / 1000, (int) (usecs % 1000) * 1000);
    }

    catch (InterruptedException x)
    {
      /* Basically ignore this interrupt now, but allow someone else to check */
      /* for interrupted()                                                    */
      Thread.currentThread().interrupt();
    }
  }


  /**
   * Print a timestamp followed by the included string.
   * If string is null, do not print a carriage return.
   */
  public static void ptod(String txt, PrintWriter pw)
  {
    //if (txt.trim().length() == 0)
    //  common.where(8);

    String tod = tod();

    synchronized(ptod_lock)
    {
      ptodFileIfNeeded(txt);

      if (txt != null)
        pw.println(tod + " " + txt);
      else
        pw.print(tod);

      chk_error(pw);

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

        chk_error(common.log_html);
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

  /**
   * Print timestamped line to stdout and logfile
   */
  public static void ptod(String txt)
  {
    /* For debugging: */
    if (txt.startsWith("+") && SlaveJvm.isThisSlave())
    {
      SlaveJvm.sendMessageToConsole(txt);
      return;
    }


    synchronized(ptod_lock)
    {
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


  public static void ptod(Vector vec)
  {
    synchronized (ptod_lock)
    {
      for (int i = 0; i < vec.size(); i++)
        ptod(vec.elementAt(i));
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
      common.ptod("(plog) " + txt);
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
  public static String tod()
  {
    synchronized(locked_df)
    {
      return locked_df.format( new Date() );
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

    chk_error(pw);
    chk_error(stdout);
  }

  public static void print(String text, PrintWriter pw)
  {
    pw.print(text);
    if (pw == stdout)
      common.log_html.print(text);

    chk_error(pw);
    chk_error(stdout);
  }


  /**
   * Set the intervalnumber to 1, return at next one second boundary
   */
  public static void set_interval_start()
  {
    long now  = System.currentTimeMillis();
    long then = ownmath.round_lower(now + 1000, 1000);
    long wait = then - now;
    if (wait > 0)
      sleep_some(wait);
    last_interval_tod = System.currentTimeMillis();
    return;
  }


  /**
   * Wait until a specific synchronized amount of seconds.
   * The target tod of the previous call is taken, and the new interval time
   * is added.
   * Routine returns at that tod.
   */
  public static void wait_interval(long interval)
  {
    long then;
    long now  = System.currentTimeMillis();
    if (get_debug(SHORT_INTERVALS1))
    {
      then = ownmath.round_lower(last_interval_tod + interval * 250, 250);
      memory_usage();
    }
    else if (get_debug(SHORT_INTERVALS2))
    {
      then = ownmath.round_lower(last_interval_tod + interval * 100, 100);
      memory_usage();
    }
    else
      then = ownmath.round_lower(last_interval_tod + interval * 1000, 1000);
    long wait = then - now;
    while (wait > 0)
    {
      now = System.currentTimeMillis();
      wait = then - now;
      if (wait <= 0)
        break;
      //common.ptod("wait: " + Math.min(1000, wait));
      sleep_some(Math.min(1000, wait));
      if (Vdbmain.isWorkloadDone())
        break;
    }
    last_interval_tod = System.currentTimeMillis();
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
      {
        /* Report memory usage: */
        common.ptod("Out of memory: \n\n\t\t modify the vdbench script and increase the '-Xmx512m' value " +
                    "where 512m equals the java heap size requested. ");
        common.ptod("If the error message says 'unable to create new native thread' " +
                    "modify the vdbench script adding '-Xss256k' or lower value for " +
                    "the java thread stack size. \n\n");
        common.ptod("Examples are for Solaris. For other platforms see the Java provider's documentation");

        common.memory_usage();
        VdbCount.listCounters("OutOfMemoryError:");
        out_of_memory = true;
      }

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


  /**
   * Report Java specific memory usage
   */
  public static void memory_usage()
  {
    double free  = (double) Runtime.getRuntime().freeMemory()  / 1048576.0;
    double total = (double) Runtime.getRuntime().totalMemory() / 1048576.0;

    if (common.get_debug(common.PRINT_MEMORY))
      common.ptod(Format.f("Memory total Java heap: %8.3f MB;", total) +
                  Format.f(" Free: %8.3f MB;", free) +
                  Format.f(" Used: %8.3f MB;", total - free));
    else
      common.plog(Format.f("Memory total Java heap: %8.3f MB;", total) +
                  Format.f(" Free: %8.3f MB;", free) +
                  Format.f(" Used: %8.3f MB;", total - free));
  }

  static boolean warned = false;

  /**
   * Error checking for PrintWriter.
   * The only time I have seen an error was when a filesystem was full.
   *
   * Other causes: an outstanding interupt.
   * Maybe remove chkerr????????
   */
  public static void chk_error(PrintWriter pw)
  {
  }
  public static void no_chk_error(PrintWriter pw)
  {
    if (pw == null)
      return;

    /* Check for errors, except for stdout.                                  */
    /* stdout will fail when the Master disappears before the slave find out */
    if (!pw.equals(stdout) && pw.checkError())
    {
      if (!warned)
      {
        String fname = Report.getWriterName(pw);
        {
          // debugging:
          Fput fp = new Fput("stacktrace.txt");
          fp.println(get_stacktrace());
          fp.close();
          Thread.currentThread().dumpStack();

          System.out.println("Possible print error writing to file '" + fname + "'");
          System.out.println("This error has shown up validly when a target file system is full");
          System.out.println("There have however been instances that an internal error ");
          System.out.println("flag was set without any obvious reason.");
          System.out.println("stack: " + get_stacktrace());

          common.log_html.println("Possible print error writing to file '" + fname + "'");
          common.log_html.println("This error has shown up validly when a target file system is full");
          common.log_html.println("There have however been instances that an internal error ");
          common.log_html.println("flag was set without any obvious reason.");
          common.log_html.println(new Date());
          common.log_html.println(get_stacktrace());


          warned = true;
        }

        /*
        if (Thread.currentThread().isInterrupted())
          System.out.println("Interrupt in common()");
        else
        {
          System.out.println("++Unidentified PrintWriter error. Is your file system full?");
          System.exit(999);
        }
        */
      }
    }
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
  public static boolean onFreeBsd()
  {
    return freebsd;
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
      dir = "windows" + sep + "vdbench.dll";

    else if (onSolaris())
    {
      if (arch.equals("amd64"))
        dir = "solx86" + sep + "solx86-64.so";

      else if (arch.equals("x86"))
        dir = "solx86" + sep + "solx86-32.so";

      else if (arch.equals("sparc"))
        dir = "solaris" + sep + "sparc32.so";

      else if (arch.equals("sparcv9"))
        dir = "solaris" + sep + "sparc64.so";

      else common.failure("Unknown system architecture for solaris: " + " " + arch);
    }

    else if (onAix())
    {
      if (bits != null && bits.equals("64"))
        dir = "aix" + sep + "aix-64.so";
      else if (arch.indexOf("64") != -1)
        dir = "aix" + sep + "aix-64.so";
      else
        dir = "aix" + sep + "aix-32.so";
    }

    else if (onMac())
      dir = "mac" + sep + "libvdbench.dylib";

    else if (onHp())
      dir = "hp" + sep + "libvdbench.sl";

    else if (onZLinux())
    {
      if (arch.equals("s390"))
        dir = "linux" + sep + "zlinux32.so";
      else
        dir = "linux" + sep + "zlinux64.so";
    }

    else if (onLinux())
    {
      if (arch.indexOf("64") != -1)
        dir = "linux" + sep + "linux64.so";

      else if (bits != null && bits.equalsIgnoreCase("64"))
        dir = "linux" + sep + "linux64.so";

      else
        dir = "linux" + sep + "linux32.so";
    }

    else if (onFreeBsd())
      dir = "freebsd" + sep + "freebsd64.so";


    else
      common.failure("Undefined support requested for platform: " +
                     System.getProperty("os.arch") +
                     "; contact Henk Vandenbergh (hv@sun.com) for support");

    File full = new File(classpath + dir);
    try
    {
      shared_library     = full.getCanonicalPath();
      shared_library_dir = full.getParent() + File.separator;
      common.plog("Setting shared library to: " + shared_library);
      System.load(shared_library);
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

        common.ptod("");
        common.ptod("Loading of shared library " + shared_library + " failed.");
        common.ptod("If the error message relates to 32 vs. 64bit processing ");
        common.ptod("please understand that Vdbench supports 32 and 64-bit Java ");
        common.ptod("for Solaris and Linux only at this time. ");
        common.ptod("There also may be issues related to a cpu type not being ");
        common.ptod("acceptable to Vdbench, e.g. MAC PPC vs. X86");
        common.ptod("Contact Henk Vandenbergh (hv@sun.com) for support.");
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
  public static String replace(String source, String value, String nval)
  {
    //common.ptod("source: " + source);
    //common.ptod("value: " + value);
    //common.ptod("nval: " + nval);

    while (source.indexOf(value) != -1)
    {
      int index = source.indexOf(value);
      source = source.substring(0, index) + nval + source.substring(index+value.length());
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


  static void xwhere(int lines_wanted, String txt)
  {
    synchronized (ptod_lock)
    {
      int lines_done = 0;
      lines_wanted++;
      StackTraceElement[] stack = new Throwable().getStackTrace();
      String line = "";
      for (int index = 2; index < lines_wanted && index < stack.length; index++)
      {
        if (lines_done++ == 0)
          line = "==> where: ";
        line += stack[index].toString() + ((txt != null) ? ": " + txt : "");
      }
      common.ptod(line);
    }
  }

  static void where(int lines_wanted, String txt)
  {
    synchronized (ptod_lock)
    {
      String line;
      int lines_done = 0;
      lines_wanted++;
      StackTraceElement[] stack = new Throwable().getStackTrace();
      for (int index = 2; index < lines_wanted && index < stack.length; index++)
      {
        if (lines_done++ == 0)
          line = "==> where: ";
        else
          line = "           ";
        line += stack[index].toString() + ((txt != null) ? ": " + txt : "");
        common.ptod(line);
      }
    }
  }

  public static void where()
  {
    where(1+1, null);
  }

  public static void where(int lines)
  {
    where(lines, null);
  }

  public static void where(String txt)
  {
    where(1+1, txt);
  }


  public static void printStacktrace()
  {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    String txt = "\n\t\tStack Trace: ";
    for (int i = 0; stack != null && i < stack.length; i++)
      txt += "\n\t\t at " + stack[i].toString();

    common.ptod(txt);
  }

  public static void main(String args[]) throws Exception
  {
    common.ptod("getCurrentIP: " + getCurrentIP());

    try
    {
      InetAddress local = InetAddress.getLocalHost();
      String current_ip = local.getHostAddress();
      String remote_ip  = InetAddress.getByName(getCurrentIP()).getHostAddress();

      common.ptod("current_ip: " + current_ip);
      common.ptod("remote_ip: " + remote_ip);
      return ;
    }
    catch (UnknownHostException e)
    {
      common.ptod("UnknownHostException: ");
      return;
    }
  }

  public static void main3(String args[]) throws Exception
  {
    Fget fg = new Fget("h:\\vdbench502\\output\\a");
    HashMap map = new HashMap(10240);
    String line = null;

    int lines = 0;
    while ((line = fg.get()) != null)
    {
      //if (lines++ > 10) break;
      //common.ptod("line: " + line);

      String[] split = line.split(" +");

      if (split[2].indexOf("++") != -1)
      {
        String old = (String) map.put(split[4], split[4]);
        if (old != null)
          common.failure("duplicate lba: " + split[4]);
        //common.ptod("added: " + split[4]);
      }
      else
      {
        String old = (String) map.get(split[4]);
        if (old == null)
          common.failure("lba not found: " + split[4]);
        map.remove(split[4]);
        //common.ptod("remov: " + split[4]);
      }
    }

    String[] rest = (String[]) map.keySet().toArray(new String[0]);
    for (int i = 0; i < rest.length; i++)
      common.ptod("leftovers: " + rest[i]);


    fg.close();


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
}


