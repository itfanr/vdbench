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

import java.net.*;
import java.text.*;
import java.text.Format.Field;
import java.util.*;

import Utils.CommandOutput;
import Utils.Format;
import Utils.OS_cmd;



/**
 * This class handles the starting of a SlaveJVM, and the waiting for
 * completion so that we know when and if the SlaveJVM completes or fails.
 */
class SlaveStarter extends ThreadControl
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private Slave slave;
  private boolean abort_pending = false;


  public void setSlave(Slave slave)
  {
    this.slave = slave;
  }


  /**
   * Start the slave, locally or remotely
   */
  public void run()
  {
    setName("SlaveStarter " + slave.getName());
    try
    {
      this.setIndependent();
      startSlave();
    }

    /* This catches any other exceptions: */
    catch (Exception e)
    {
      common.failure(e);
    }

    this.removeIndependent();

    common.plog("SlaveStarter terminating: " + slave.getLabel());
  }


  public void startSlave()
  {
    try
    {
      Host host = slave.getHost();
      OS_cmd ocmd = new OS_cmd();
      ocmd.setWaitForJvm();

      ocmd.setOutputMethod(new CommandOutput()
                           {
                             public boolean newLine(String line, String type, boolean more)
                             {
                               String txt = Format.f("%-12s: ", slave.getLabel()) + line;
                               slave.getConsoleLog().println(common.tod() + " " + line);

                               if (!slave.isConnected() && txt.endsWith("Command not found"))
                                 common.failure("Receiving 'command not found' from slave " +
                                                slave.getLabel() + ": " + txt);

                               if (txt.indexOf("common.failure()") != -1)
                                 abort_pending = true;

                               if (common.get_debug(common.SLAVE_LOG_ON_CONSOLE) ||
                                   abort_pending)
                                 common.ptod(txt);

                               return true;
                             }
                           });


      /* In theory using RshDeamon() for more than one JVM on a host      */
      /* should work fine. However, targeting solaris from windows there    */
      /* were some unexplained problems losing sockets etc.                 */
      /* It was easier at this time to just use RshDeamon only upon       */
      /* request (shell=vdbench) when the target is Windows. All other OS's */
      /* have an rsh.                                                       */

      /* For a remote host, if we don't use our RshDeamon() then */
      /* create an rsh/ssh command: */
      if (!slave.isLocalHost() && !host.getShell().equals("vdbench"))
      {
        ocmd.addText(host.getShell());
        ocmd.addText(slave.getSlaveIP());
        ocmd.addText("-l");
        ocmd.addText(host.getUser());

        /* See OS_cmd.isWindowsRsh(): '-n' will eliminate this problem: */
        //ocmd.addText("-n");
      }

      /* Add vdbench script: */
      if (!common.get_debug(common.USE_TVDBENCH))
        ocmd.addQuot(host.getVdbench() + "vdbench");
      else
        ocmd.addQuot(host.getVdbench() + "tvdbench");

      /* Add vdbench main for slave: */
      ocmd.addText("SlaveJvm");

      /* Add the name/ip address of the master: */
      ocmd.addText("-m");
      if (host.getLabel().equals("localhost"))
        ocmd.addText("localhost");
      else
        ocmd.addText(common.getCurrentIP());

      /* Add the name of this slave to serve socket recognition: */
      ocmd.addText("-n");
      ocmd.addText(slave.getName());

      /* Add the slave label: */
      ocmd.addText("-l");
      ocmd.addText(slave.getLabel());

      /* Add port number: */
      ocmd.addText("-p");
      ocmd.addText(SlaveSocket.getMasterPort());

      /* Add debugging flags: */
      ocmd.addText(common.get_debug_string());

      /* Start local or via separate JVM (see note at startJvm()) ? */
      startJvm(ocmd, host);

      /* If we get completion before we expect it then we are in error */
      /* and the whole test fails.                                     */
      if (!slave.may_terminate())
      {
        /* Sleep a little to get some possibly pending messages out: */
        common.sleep_some(500);

        common.ptod("");
        common.ptod("Slave " + slave.getLabel() + " prematurely terminated. ");
        if (slave.abort_msg != null)
        {
          common.ptod("");
          common.ptod("Slave aborted. Abort message received: ");
          common.ptod(slave.abort_msg);
          common.ptod("");
        }
        common.ptod("Look at file " + slave.getConsoleLog().getFileName() +
                    ".html for more information.");
        common.plog("HTML link: <A HREF=\"" + slave.getConsoleLog().getFileName() + ".html\">" +
                    slave.getConsoleLog().getFileName() + ".html</A>");
        common.failure("Slave " + slave.getLabel() + " prematurely terminated. ");
      }
      //else
      //  common.ptod("Slave " + slave.getLabel() + " terminated");

      slave.setTerminated();
    }

    catch (Exception e)
    {
      common.where();
      common.failure(e);
    }
  }


  /**
   * Start Slave.
   *
   * Note:
   * the RSH from MKS Toolkit does not close its stdout/stderr writers correctly
   * or at all, and the RSH command therefore never appears to terminate.
   * I resolved that by renaming the MKS RSH and not using it.
   */
  private void startJvm(OS_cmd ocmd, Host host)
  {

    /* A single local slave can start locally: */
    /* (This is only for debugging, and may no longer work at all) */
    if (SlaveList.runSlaveInsideMaster() && slave.isLocalHost())
    {
      /* Remove the first two parameters from the OS_cmd() command: */
      StringTokenizer st = new StringTokenizer(ocmd.getCmd());
      String[] args = new String[st.countTokens() - 2];
      st.nextToken();
      st.nextToken();
      int i = 0;
      while (st.hasMoreTokens())
        args[i++] = st.nextToken();

      SlaveJvm.main(args);
    }

    else
    {
      /* Start the slave. This thread ends up waiting for completion.  */
      common.ptod("Starting slave: " + ocmd.getCmd());
      if (slave.isLocalHost())
        ocmd.execute(false);

      else if (host.getShell().equals("vdbench"))
        Rsh.issueCommand(slave, ocmd.getCmd());

      else
        ocmd.execute(false);

      return;
    }
  }
}
