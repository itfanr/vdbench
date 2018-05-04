package Utils;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import java.text.*;
import java.lang.*;
import java.net.*;  // for inetaddr


/**
 * This 'common' class contains a subset of some standard methods
 * in both Swat dn Vdbench common.java.
 * This class is here to allow reasonable easy portability between
 * 'Utils' package between the two.
 */
public class common
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private static PrintWriter ptod_output = new PrintWriter(System.out, false);
  private static PrintWriter log_html    = null;


  /* Debug flags for all packages, including Utils.                  */
  private static boolean[] debug_flags = new boolean[256];

  public static Object ptod_lock = new Object();

  private static DateFormat df = new SimpleDateFormat( "HH:mm:ss.SSS" );


  public static void ptod()
  {
    synchronized (ptod_lock)
    {
      if (log_html != null)
        log_html.println();
      ptod_output.println(tod());
    }
  }

  public static void ptod(PrintWriter pw)
  {
    synchronized (ptod_lock)
    {
      if (log_html != null)
        log_html.println();
      pw.println(tod());
    }
  }

  public static void ptod(Exception e)
  {
    synchronized (ptod_lock)
    {
      ptod(e.getClass().getName());
      if (log_html != null)
      {
        e.printStackTrace(log_html);
      }
      e.printStackTrace(ptod_output);
    }
  }

  public static void ptod(String txt)
  {
    synchronized (ptod_lock)
    {
      if (log_html != null)
      {
        log_html.println(tod() + " " + txt);
      }
      ptod_output.println(tod() + " " + txt);
    }
  }

  public static String tod()
  {
    return df.format(new Date());
  }

  public static void setPlog(PrintWriter pw)
  {
    log_html = pw;
  }
  public static synchronized void plog(String txt)
  {
    /* plog() output from this Utils.common.plog() is suppressed unless           */
    /* the log_html PrintWriter has been set.                                     */
    /* This is different compared to the way it normally works in Swat or Vdbench */
    if (log_html != null)
    {
      log_html.println(tod() + " " + txt);
      log_html.flush();
    }

    else
      ptod("plog(): " + txt);
  }


  /**
   * We need to blow up this program because of a fatal error.
   * We always keep some spare memory around to help us clean up!
   */
  static byte[] spare_memory = new byte[4*1024*1024];
  static Object failure_lock = new Object();

  public static void failure(Exception e)
  {
    synchronized(failure_lock)
    {

      if (spare_memory != null)
        spare_memory = null;
      else
      {
        common.ptod("common.failure(): System.exit(-99)");
        System.exit(-99);
      }

      /* Put a message on stderr so that we can look for errors: */
      System.err.println();
      System.err.println("Abort requested: " + e.getClass().getName() + " " + e.getMessage());

      common.ptod("common.failure():");
      e.printStackTrace();
      if (log_html != null)
        e.printStackTrace(log_html);

      Fput.closeAll();

      common.ptod("common.failure(): System.exit(-98)");
      System.exit(-98);
    }
  }

  public static void failure(String txt)
  {
    synchronized(failure_lock)
    {
      /* We have some spare memory set aside to help us in recovery situations. */
      /* It also serves as a check to avoid recusrive errors: */
      if (spare_memory != null)
        spare_memory = null;
      else
      {
        common.ptod("common.failure(): Recursive call. System.exit(-97)");
        System.exit(-97);
      }


      /* Put a message on stderr so that we can look for errors: */
      System.err.println();
      System.err.println("Abort requested: " + txt);

      common.ptod("common.failure():");
      Throwable t = new RuntimeException( txt);
      t.printStackTrace();
      if (log_html != null)
        t.printStackTrace(log_html);

      Fput.closeAll();

      common.ptod("common.failure(): System.exit(-96)");
      System.exit(-96);
    }
  }

  static Vector assertit(Vector obj)
  {
    return(Vector) assertit((Object) obj);
  }
  static Object assertit(Object obj)
  {
    if (obj == null)
    {
      int index = 1;
      StackTraceElement[] stack = new Throwable().getStackTrace();
      String caller = stack[index].toString();
      if (caller.indexOf("(") < caller.lastIndexOf(")"))
        caller = caller.substring(caller.indexOf("(") + 1, caller.lastIndexOf(")"));

      common.failure("Assert failed at " + caller);
    }

    return obj;
  }

  /**
   * Check where we're running
   */
  public static boolean onLinux()
  {
    return(System.getProperty("os.name").toLowerCase().startsWith("linux"));
  }
  public static boolean onWindows()
  {
    return(System.getProperty("os.name").toLowerCase().startsWith("windows"));
  }
  public static boolean onSolaris()
  {
    return(System.getProperty("os.name").toLowerCase().startsWith("sunos") ||
           System.getProperty("os.name").toLowerCase().startsWith("solaris"));
  }
  static boolean onAix()
  {
    return(System.getProperty("os.name").toLowerCase().startsWith("aix"));
  }



  /**
   * Replace char within String
   * (parameters reversed between both replace methods!)
   */
  public static String replace(String source, String new_value, char old_value)
  {
    String newsrc = "";
    for (int i = 0; i < source.length(); i++)
    {
      if (source.charAt(i) == old_value)
        newsrc += new_value;
      else
        newsrc += source.charAt(i);
    }
    return newsrc;
  }


  /**
   * Replace String within String.
   * (parameters reversed between both replace methods!)
   */
  public static String replace(String source, String old_value, String new_value)
  {
    int last_index = -1;

    while (source.indexOf(old_value) != -1)
    {
      int index = source.indexOf(old_value);
      if (index == last_index)
        break;
      last_index = index;
      source = source.substring(0, index) + new_value + source.substring(index+old_value.length());
    }

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


  /**
   * Sleep x milliseconds, with or without returning an interrupt
   */
  public static void sleep_some_no_int(long msecs)
  {
    try
    {
      sleep_some(msecs);
    }
    catch (InterruptedException e)
    {
    }
  }
  public static void sleep_some(long msecs) throws InterruptedException
  {

    if (msecs == 0)
      return;

    sleep_some_usecs(msecs * 1000);
  }


  public static void sleep_some_usecs(long usecs) throws InterruptedException
  {

    try
    {
      Thread.sleep(usecs / 1000, (int) (usecs % 1000) * 1000);
    }

    catch (InterruptedException x)
    {
      common.ptod("Interrupted in common.sleep()");
      throw(new InterruptedException());
    }
  }


  static void where(int lines_wanted, String txt)
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


  /**
   * Debugging flags: set
   */
  public static void set_debug(int number)
  {
    if (number >= debug_flags.length)
      common.failure("Requested debug flag setting too large: " + number);

    debug_flags[number] = true;
  }


  /**
   * Debugging flags: query
   */
  public static boolean get_debug(int number)
  {
    return debug_flags[number];
  }


  /**
   * Debugging flags: get string with flags to pass to others
   */
  public static String get_debug_string()
  {
    String data = " ";

    for (int i = 0; i < debug_flags.length; i++)
    {
      if (debug_flags[i])
        data = data + "-d" + i + " ";
    }

    return data;
  }
}





