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

import Utils.OS_cmd;


/**
 * This class is a Shutdown hook.
 *
 * There are two uses:
 * - send a message to both stderr and stdout so that a JVM that is waiting for
 *   it's completion in OS_cmd() gets a message, allowing any outstanding
 *   BufferedReader.readLine() to complete.
 *
 * - Close any open report files. This is cheaper than using flush() for each
 *   single report line written. With many luns and many slaves doing the flush
 *   each time for each line for each interval just gets to be too expensive.
 */
public class Shutdown extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private Shutdown()
  {
    setName("Shutdown");
  }

  public static void activateShutdownHook()
  {
    Runtime.getRuntime().addShutdownHook(new Shutdown());
  }



  public void run()
  {

    if (SlaveJvm.isThisSlave())
    {
      System.out.println(OS_cmd.getShutdownMessage());
      System.err.println(OS_cmd.getShutdownMessage());
    }

    Report.closeAllReports();
  }
}
