package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

import Utils.ClassPath;
import Utils.CommandOutput;
import Utils.OS_cmd;

/**
 *
 */
public class RshUser extends ThreadControl
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private SlaveSocket socket_to_user;


  public static Vector active_commands = new Vector(32, 0);



  public void setSocket(SlaveSocket socket)
  {
    socket_to_user = socket;
  }


  public void run()
  {
    setName("RshUser");
    try
    {
      processUser();
    }


    /* This catches any other exceptions: */
    catch (Exception e)
    {
      common.failure(e);
    }
  }


  /**
   * Handle whatever we needed coming from a slave
   */
  public void processUser()
  {
    this.setIndependent();

    try
    {

      /* From now on, all messages received from the socket come here: */
      while (true)
      {
        SocketMessage sm = socket_to_user.getMessage();

        /* Clean shutdown? */
        if (sm == null)
          break;

        int msgno = sm.getMessageNum();

        if (msgno == SocketMessage.RSH_COMMAND)
        {
          String cmd = (String) sm.getData();

          /* To prevent abuse of this java socket we now add './vdbench SlaveJvm' back in: */
          cmd = ClassPath.classPath("vdbench") + " SlaveJvm " + cmd;

          common.ptod("Executing command: " + cmd);
          issueCommand(cmd);
          socket_to_user.putMessage(sm);
          socket_to_user.close();
          common.ptod("Completed command: " + cmd);
          break;
        }

        else
          common.failure("unexpected message from rsh user: " + sm.getMessageText());
      }

    }


    catch (Exception e)
    {
      common.failure(e);
    }


    this.removeIndependent();
  }


  /**
   * Start the requested command.
   * Output will be sent back over the socket.
   */
  private void issueCommand(String cmd)
  {
    active_commands.add(cmd);
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             int num = (type.equals("stdout")) ? SocketMessage.RSH_STDOUT_OUTPUT : SocketMessage.RSH_STDERR_OUTPUT;
                             SocketMessage sm = new SocketMessage(num, line);
                             //common.ptod("newLine: " + line);

                             return socket_to_user.putMessage(sm);
                           }
                         });

    ocmd.execute();

    active_commands.remove(cmd);
  }

}

