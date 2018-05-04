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
import java.io.IOException;
import Utils.OS_cmd;



public class Ctrl_c extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

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

  public static void removeShutdownHook()
  {
    if (ctrlc_thread != null)
      Runtime.getRuntime().removeShutdownHook(ctrlc_thread);
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
