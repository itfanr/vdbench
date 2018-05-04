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
import java.io.*;
import java.util.*;
import java.text.*;
import Utils.OS_cmd;
import Utils.CommandOutput;

/**
 *
 */
public class RshUser extends ThreadControl
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

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

