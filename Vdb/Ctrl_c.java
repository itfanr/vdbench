package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.IOException;
import Utils.OS_cmd;



public class Ctrl_c extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  static Thread  ctrlc_thread = null;


  private static boolean ctrlc_active = false;
  public static Vector stacks = new Vector(256, 0);

  private Ctrl_c()
  {
    setName("Ctrl_c");
  }

  public static void activateShutdownHook()
  {
    Runtime.getRuntime().addShutdownHook(ctrlc_thread = new Ctrl_c());
  }


  public static boolean active()
  {
    return ctrlc_active;
  }

  public static synchronized StackTraceElement[] addStack()
  {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    stacks.addElement(stack);
    return stack;
  }
  public static synchronized void removeStack(StackTraceElement[] nstack)
  {
    stacks.removeElement(nstack);
  }


  /**
   * This thread gets started when the JVM shuts down.
   *
   * This means that if there was NO ctrl-c used, we still display the
   * message.
   */
  public void run()
  {
    ctrlc_active = true;
    System.out.println("CTRL-C requested. vdbench terminating");

    Status.printStatus("Ctrl-C found", null);

    /* When someone hits ctrl-c we want to make sure that he knows that */
    /* there have been some errors:                                     */
    if (ErrorLog.getErrorCount() > 0)
    {
      System.out.println("*");
      System.out.println("*");
      System.out.println("Total Data Validation or I/O error count: " + ErrorLog.getErrorCount());
      System.out.println("See error_log.html");
      System.out.println("*");
      System.out.println("*");
    }

    /* For debugging: if ctrl-c is hit, report some counters: */
    if (Vdbmain.isFwdWorkload())
      Blocked.printAndResetCounters();


    /* This is for debugging, not currently used: */
    for (int i = 0; i < stacks.size(); i++)
    {
      StackTraceElement[] stack = (StackTraceElement[]) stacks.elementAt(i);
      for (int index = 1; index < stack.length; index++)
        System.out.println("==> ctrlc: " + stack[index].toString());
    }

    if (!SlaveJvm.isThisSlave())
      Report.closeAllReports();

    OS_cmd.killAll();

    //common.dump_all_stacks();
  }


  /**
   * Remove CTRL hook.
   * we may already be terminating, so allow for an Exception
   * (java.lang.IllegalStateException: Shutdown in progress)
   */
  public static synchronized void removeShutdownHook()
  {
    if (ctrlc_thread != null)
    {
      try
      {
        Runtime.getRuntime().removeShutdownHook(ctrlc_thread);
      }
      catch (IllegalStateException es)
      {
      }
      ctrlc_thread = null;
    }
  }

  public static void main(String[] args)
  {
    new Ctrl_c();

    System.out.println("Press CTRL-C");

    while (true)
      try
      {
        System.in.read();
      }
      catch (IOException ioe)
      {
        ioe.printStackTrace();
      }
  }
}
