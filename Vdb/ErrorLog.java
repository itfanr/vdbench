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

import java.io.PrintWriter;


import java.io.*;
import java.util.*;
import Utils.OS_cmd;
import Utils.CommandOutput;


/**
 * This class handles writing to error_log.html.
 *
 * Messages generated from the master go directly to the file,
 * while messages from the slave are sent to the master using sockets.
 * The master then will write them to file.
 */
public class ErrorLog
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private static PrintWriter pw                = null;
  private static int         errors            = 0;
  private static long        tod_last_dv_error = 0;

  private static String      NOCONSOLE = "noconsole";

  private static String header = "Error log. If there is no data beyond this line " +
                                 "then there were no Data validation or I/O errors.\n";

  public static void create()
  {
    pw = Report.createHmtlFile("errorlog.html", header);
  }

  public static long getLastErrorTod()
  {
    return tod_last_dv_error;
  }

  /**
   * Messages on slave to be sent to master
   */
  public static void sendMessagesToMaster(Vector messages)
  {
    SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, messages);
  }


  /**
   * Messages from slave received on Master
   */
  public static void printMessagesOnLog(String label, Vector messages)
  {
    synchronized (pw)
    {
      for (int i = 0; i < messages.size(); i++)
      {
        common.ptod(label + " " + (String) messages.elementAt(i), pw);
        common.ptod(label + " " + (String) messages.elementAt(i));
        //common.ptod(label + " " + (String) messages.elementAt(i), common.stdout);
      }
    }
  }

  /**
   * Print message on errorlog and on the console.
   * A Message starting with "noconsole" will not go to the console.
   * (Cheap trick, but it works).
   */
  public static void printMessageOnLog(String txt)
  {
    synchronized (pw)
    {
      boolean noconsole = txt.endsWith(NOCONSOLE);
      if (noconsole)
        txt = txt.substring(0, txt.indexOf(NOCONSOLE));
      pw.println(common.tod() + " " + txt);
      if (!noconsole)
        common.ptod(txt, common.stdout);
    }
  }


  /**
   * Send message to the error log only.
   */
  public static void sendMessageToLog(String txt)
  {
    SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, txt + NOCONSOLE);
  }


  /**
   * Send message to both errorlog and console.
   */
  public static void sendMessageToMaster(String txt)
  {
    SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, txt);
  }


  /**
   * Count i/o or DV errors.
   */
  public static void countErrorsOnSlave(String lun, long lba, int xfersize)
  {
    if (Validate.getErrorCommand() == null)
    {
      SlaveJvm.sendMessageToMaster(SocketMessage.COUNT_ERRORS);
      return;
    }

    startErrorCommand(lun, lba, xfersize);
  }

  public static void countErrorsOnMaster()
  {
    /* Terminate upon request: */
    tod_last_dv_error = System.currentTimeMillis();

    errors++;
    if (errors >= Validate.getMaxErrorCount())
      common.failure("'data_error=" + Validate.getMaxErrorCount() + "' requested. Abort after last error.");
  }

  public static int getErrorCount()
  {
    return errors;
  }


  /**
   * Start the optional data_errors="xyz $output $lun $lba $size" command.
   */
  private static synchronized void startErrorCommand(String lun, long lba, int xfersize)
  {
    String cmd = Validate.getErrorCommand();
    if (cmd.startsWith("stop"))
      common.failure("'data_error=stop' requested. Abort after first error");

    /* Start error script, substituting a few fields: */
    cmd = common.replace(cmd, "$output", Validate.getOutput());
    cmd = common.replace(cmd, "$lun",    lun);
    cmd = common.replace(cmd, "$lba",    "" + lba);
    cmd = common.replace(cmd, "$size",   "" + xfersize);
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);
    ocmd.execute();

    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();
    for (int i = 0; i < stdout.length; i++)
      common.ptod("stdout: " + stdout[i]);
    for (int i = 0; i < stderr.length; i++)
      common.ptod("stderr: " + stderr[i]);

    /* Give master a little time to receive the message before we abort: */
    common.sleep_some(1000);

    common.failure("'data_error=" + cmd + "' requested. Abort after first error");
  }
}




