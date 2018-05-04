package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.Format;

import Utils.CommandOutput;
import Utils.Fget;
import Utils.OS_cmd;



/**
 * This class handles the starting of a SlaveJVM, and the waiting for
 * completion so that we know when and if the SlaveJVM completes or fails.
 */
class SlaveStarter extends ThreadControl
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

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

    //common.plog("SlaveStarter terminating: " + slave.getLabel());
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
                               //System.out.println("line: " + line);

                               String txt = String.format("%-12s: ", slave.getLabel()) + line;
                               slave.getConsoleLog().println(common.tod() + " " + line);

                               if (!slave.isConnected() && txt.endsWith("Command not found"))
                                 common.failure("Receiving 'command not found' from slave " +
                                                slave.getLabel() + ": " + txt);

                               if (txt.indexOf("common.failure()") != -1)
                                 abort_pending = true;

                               if (common.get_debug(common.SLAVE_LOG_ON_CONSOLE) ||
                                   abort_pending)
                                 common.ptod(txt);

                               //if (txt.indexOf("Debug_cmds():") != -1)
                               //  common.ptod(txt);

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
      {
        String ip = "localhost";

        if (Fget.file_exists("override_ip.txt"))
        {
          ip = Fget.readFileToArray("override_ip.txt")[0];
          common.ptod("Using file 'override_ip.txt': " + ip);
        }
        ocmd.addText(ip);
      }
      else
      {
        String ip = common.getCurrentIP();
        if (ip.equals("127.0.0.1"))
          common.failure("Current system IP address is '%s'. "+
                         "Invalid network definition for multi-host processing.", ip);
        if (Fget.file_exists("override_ip.txt"))
        {
          ip = Fget.readFileToArray("override_ip.txt")[0];
          common.ptod("Using file 'override_ip.txt': " + ip);
        }

        ocmd.addText(ip);
      }

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

      //common.where();
      //ocmd.addText(String.format(" | tee slave.%d.txt", common.getProcessId()));

      /* Start local or via separate JVM (see note at startJvm()) ? */
      startJvm(ocmd, host);
      // do I need a bit to get latest stdout?
      common.sleep_some(500);

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
    /* Start the slave. This thread ends up waiting for completion.  */
    common.ptod("Starting slave: " + ocmd.getCmd());
    if (slave.isLocalHost() && !common.get_debug(common.FAKE_RSH))
      ocmd.execute(false);

    else if (host.getShell().equals("vdbench") || common.get_debug(common.FAKE_RSH))
    {
      if (common.get_debug(common.SLAVE_LOG_ON_CONSOLE))
        common.failure("'-d44' option does not work with vdbench=rsh");

      /* We've got a problem here: any command sent to this socket will be */
      /* blindly executed. Need to remove the './vdbench SlaveJVM' piece.  */
      /* RSH at the other side will add it again.                          */
      String cmd = ocmd.getCmd().substring(ocmd.getCmd().indexOf("SlaveJvm") + 8);
      Rsh.issueCommand(slave, cmd);
    }

    else
      ocmd.execute(false);

    return;
  }
}
