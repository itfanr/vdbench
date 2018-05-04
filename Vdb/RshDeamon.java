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
import java.net.*;
import java.io.*;
import Utils.ClassPath;
import Utils.Fget;

/**
 * Remote Shell deamon for vdbench.
 * See notes under Rsh().
 */
class RshDeamon
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  public static void waitForUsers()
  {
    long signaltod = 0;

    /* Open a server socket. This allows clients to find us: */
    ServerSocket serverSocket = null;
    try
    {
      serverSocket = new ServerSocket(SlaveSocket.getRemotePort());
      serverSocket.setSoTimeout(15 * 60 * 1000);
    }
    catch (IOException e)
    {
      System.err.println("Could not listen on port: " + SlaveSocket.getRemotePort());
      common.failure(e);
    }


    /* Continuously accept new connections: */
    while (true)
    {
      /* Every x seconds tell user we're still waiting: */
      if ( (signaltod = common.signal_caller(signaltod, 300 * 1000)) == 0)
        common.ptod("Vdbench rsh deamon: waiting for new users");

      try
      {
        SlaveSocket socket = new SlaveSocket(serverSocket);

        /* We have a new client, hand him off to a separate thread: */
        RshUser ru = new RshUser();
        ru.setSocket(socket);
        socket.setRecovery(false);
        ru.start();
      }

      /* socket.accept() timeout. That's OK, try again: */
      catch (SocketTimeoutException e)
      {
        if (RshUser.active_commands.size() > 0)
        {
          common.ptod("waiting for new RshDeamon request. Current active requests:");
          for (int i = 0; i < RshUser.active_commands.size(); i++)
            common.ptod((String) RshUser.active_commands.elementAt(i));
        }
        continue;
      }

      catch (IOException e)
      {
        common.failure(e);
      }
    }
  }


  /**
   * Get port number from file "portnumbers.txt"
   */
  public static void readPortNumbers()
  {
    int rsh_port = 0;
    int master_port = 0;

    /* Pick up the port number: */
    if (new File(ClassPath.classPath(), "portnumbers.txt").exists())
    {
      Vector lines = Fget.read_file_to_vector(ClassPath.classPath(), "portnumbers.txt");
      for (int i = 0; i < lines.size(); i++)
      {
        String line = (String) lines.elementAt(i);
        line = line.trim().toLowerCase();
        if (line.startsWith("rshdeamonport="))
        {
          try
          {
            rsh_port = Integer.parseInt(line.substring(14));
          }
          catch (Exception e)
          {
            common.ptod("Error parsing 'rshdeamonport=': " + line);
          }
        }

        else if (line.startsWith("masterslaveport="))
        {
          try
          {
            master_port = Integer.parseInt(line.substring(16));
          }
          catch (Exception e)
          {
            common.ptod("Error parsing 'masterslaveport=': " + line);
          }
        }
      }

      if (rsh_port != 0)
        SlaveSocket.setRemotePort(rsh_port);
      if (master_port != 0)
        SlaveSocket.setMasterPort(master_port);
    }

    //common.ptod("rshdeamonport: " + SlaveSocket.getRemotePort());
    //common.ptod("masterslaveport: " + SlaveSocket.getMastersh_port());

    if (SlaveSocket.getRemotePort() == SlaveSocket.getMasterPort())
    {
      common.ptod("rshdeamonport: " + SlaveSocket.getRemotePort());
      common.ptod("masterslaveport: " + SlaveSocket.getMasterPort());
      common.failure("rshdeamonport and masterslaveport must have different port numbers");
    }
  }


  /**
   * This main can run 7*24 and all it does is wait for remote requests
   * to execute command lines.
   */
  public static void main(String[] args)
  {
    readPortNumbers();

    waitForUsers();
  }

}
