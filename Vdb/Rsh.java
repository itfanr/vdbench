package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.net.*;
import java.util.*;
import Utils.Format;



/**
 * Start remote command.
 * This was written to eliminate problems with Windows openSSH.
 * There were too many problems getting the proper file names
 * back and forth to windows that this was easier.
 */
public class Rsh
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  public  SlaveSocket socket_to_remote;
  private String host;



  public Rsh(String host)
  {
    this.host = host;
  }

  /**
   * Connect to RshDeamon()
   */
  public void connectToRemote()
  {

    Signal signal = new Signal(60);

    /* Connect to the master: */
    while (true)
    {
      try
      {
        socket_to_remote = new SlaveSocket(host, SlaveSocket.getRemotePort());
        socket_to_remote.setSlaveLabel("rsh_to_client");
        socket_to_remote.setSlaveName("rsh_to_client");

        /* Set timeout */
        socket_to_remote.getSocket().setSoTimeout(10 * 60 * 1000);

        common.ptod("Successfully connected to the Vdbench rsh daemon on host " + host);
      }

      /* Not here, keep trying: */
      catch (ConnectException e)
      {
        if (signal.go())
          common.failure("Vdbench rsh connection failure.");
        common.ptod("");
        common.ptod("Trying to connect to the Vdbench rsh daemon on host " + host);
        common.ptod("The Vdbench rsh daemon must be started on each target host.");
        common.ptod("This requires a one-time start of './vdbench rsh' on the target host.");
        common.ptod("Trying this for %d seconds only.", signal.getDuration());
        common.sleep_some(5000);
        continue;
      }

      /* Any errors at this time are fatal: */
      catch (UnknownHostException e)
      {
        common.failure(e);
      }
      catch (IOException e)
      {
        common.failure(e);
      }

      /* Connection made, drop out of connect loop: */
      common.ptod("RSH Connection to " + host + " using port " + SlaveSocket.getRemotePort() + " successful");
      break;

    }
  }


  /**
   * Get messages from RshDeamon()
   */
  private void getRemoteOutput(Slave slave)
  {
    String label = slave.getLabel();

    while (true)
    {
      try
      {
        SocketMessage sm = socket_to_remote.getMessage();

        if (sm.getMessageNum() == SocketMessage.RSH_STDERR_OUTPUT)
        {
          String line = (String) sm.getData();
          slave.getConsoleLog().println(common.tod() + " stderr: " + line);
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_STDOUT_OUTPUT)
        {
          String line = (String) sm.getData();
          slave.getConsoleLog().println(common.tod() + " " + line);

          /* A fatal error message is handled right away: */
          if (common.isFatal(line))
          {
            common.ptod("Fatal error message received from slave " + label);
            common.psum("Fatal error message received from slave " + label);
            common.notifySlaves();
          }
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_COMMAND)
        {
          break;
        }

        else
          common.failure("Unknown socket message: " + sm.getMessageText());
      }

      catch (Exception e)
      {
        common.ptod("Exception in getRemoteOutput(): " + label);
        common.failure(e);
      }
    }

    socket_to_remote.close();
  }

  public void getRemoteSwatOutput()
  {
    String label = "swatlabel";

    while (true)
    {
      try
      {
        SocketMessage sm = socket_to_remote.getMessage();

        if (sm.getMessageNum() == SocketMessage.RSH_STDERR_OUTPUT)
        {
          String line = (String) sm.getData();
          common.ptod(" stderr: " + line);
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_STDOUT_OUTPUT)
        {
          String line = (String) sm.getData();
          common.ptod(" stdout: " + line);
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_COMMAND)
        {
          break;
        }

        else
          common.failure("Unknown socket message: " + sm.getMessageText());
      }

      catch (Exception e)
      {
        common.ptod("Exception in getRemoteOutput(): " + label);
        common.failure(e);
      }
    }

    socket_to_remote.close();
  }



  /**
   * Send a command request to a remote system.
   */
  public static void issueCommand(Slave slave, String cmd)
  {
    Rsh rsh = new Rsh(slave.getSlaveIP());

    /* Connect to the master: */
    rsh.connectToRemote();

    SocketMessage sm = new SocketMessage(SocketMessage.RSH_COMMAND);
    sm.setData(cmd);
    rsh.socket_to_remote.putMessage(sm);

    /* RshDeamon() will send us stdout and stderr back: */
    rsh.getRemoteOutput(slave);

    /* When we come back here the master has told us to nicely shut down */
    /* by returning to use the RSH_COMMAND message:                      */
    rsh.socket_to_remote.close();

  }

  /**
   * Send a command request to a remote system.
   */
  public static void main(String[] args)
  {
    /* Strip off the IP: */
    String ip = args[0];

    /* Concatenate the rest: */
    String command = "";
    for (int i = 1; i < args.length; i++)
      command += args[i] + " ";

    Rsh rsh = new Rsh(ip);

    /* Connect to the master: */
    rsh.connectToRemote();

    SocketMessage sm = new SocketMessage(SocketMessage.RSH_COMMAND);
    sm.setData(command);
    rsh.socket_to_remote.putMessage(sm);

    /* RshDeamon() will send us stdout and stderr back: */
    rsh.getRemoteSwatOutput();

    /* When we come back here the master has told us to nicely shut down */
    /* by returning to use the RSH_COMMAND message:                      */
    rsh.socket_to_remote.close();

  }

  /**
   * This is NOT the main, started by the master to do all the work.
   */
  public static void mainold(String[] args) throws Exception
  {
    //String cmd = "ls -l /var/tmp";
    Rsh rsh = new Rsh("sbm-thor-a");

    String cmd =
    "script                                                         \n" +
    "                                                               \n" +
    "  run('shares');                                               \n" +
    "  projects = list();                                           \n" +
    "                                                               \n" +
    "  printf('%-40s %-10s %-10s\\n', 'SHARE', 'USED', 'AVAILABLE');\n" +
    "                                                               \n" +
    "                                                               \n" +
    "                                                               \n" +
    "  for (i = 0; i < projects.length; i++)                        \n" +
    "  {                                                            \n" +
    "    run('select ' + projects[i]);                              \n" +
    "    shares = list();                                           \n" +
    "                                                               \n" +
    "    for (j = 0; j < shares.length; j++)                        \n" +
    "    {                                                          \n" +
    "      run('select ' + shares[j]);                              \n" +
    "                                                               \n" +
    "      share = projects[i] + '/' + shares[j];                   \n" +
    "      used  = run('get space_data').split(/\\s+/)[3];          \n" +
    "      avail = run('get space_available').split(/\\s+/)[3];     \n" +
    "                                                               \n" +
    "      printf('%-40s %-10s %-10s\\n', share, used, avail);      \n" +
    "      run('cd ..');                                            \n" +
    "    }                                                          \n" +
    "                                                               \n" +
    "  run('cd ..');                                                \n" +
    "  }                                                            \n";

    cmd = "echo \"" + cmd + "\" > /var/tmp/henk ; chmod +x /var/tmp/henk ; /var/tmp/henk";
    common.ptod("cmd: " + cmd);

    /* Connect to the master: */
    rsh.connectToRemote();

    SocketMessage sm = new SocketMessage(SocketMessage.RSH_COMMAND);
    sm.setData(cmd);
    rsh.socket_to_remote.putMessage(sm);

    /* RshDeamon() will send us stdout and stderr back: */
    rsh.testRemoteOutput("xx");

    /* When we come back here the master has told us to nicely shut down */
    /* by returning to use the RSH_COMMAND message:                      */
    rsh.socket_to_remote.close();
  }


  private void testRemoteOutput(String label)
  {
    while (true)
    {
      try
      {
        SocketMessage sm = socket_to_remote.getMessage();

        if (sm.getMessageNum() == SocketMessage.RSH_STDERR_OUTPUT)
        {
          String line = (String) sm.getData();
          common.ptod("stderr: " + line);
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_STDOUT_OUTPUT)
        {
          String line = (String) sm.getData();
          common.ptod("stdout: " + line);

          /* A fatal error message is handled right away: */
          if (common.isFatal(line))
          {
            common.ptod("Fatal error message received from slave " + label);
            common.psum("Fatal error message received from slave " + label);
            common.notifySlaves();
          }
        }

        else if (sm.getMessageNum() == SocketMessage.RSH_COMMAND)
        {
          break;
        }

        else
          common.failure("Unknown socket message: " + sm.getMessageText());
      }

      catch (Exception e)
      {
        common.ptod("Exception in getRemoteOutput(): " + label);
        common.failure(e);
      }
    }

    socket_to_remote.close();
  }

}

