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

/**
 * This class waits for slaves to connect to the master
 */
public class ConnectSlaves
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static ServerSocket server_socket_to_slaves = null;

  public static void createSocketToSlaves()
  {
    int port_attempts = 0;
    int max_attempts  = 5;

    /* Open a server socket. This allows clinets to find us: */
    while (true)
    {
      try
      {
        server_socket_to_slaves = new ServerSocket(SlaveSocket.getMasterPort());
        server_socket_to_slaves.setSoTimeout(100);
        break;
      }

      catch (IOException e)
      {
        common.ptod("Unable to listen on port " + SlaveSocket.getMasterPort() +
                    ". Possibly caused by running multiple Vdbench tests concurrently.");
        if (++port_attempts == max_attempts)
          common.failure(e);

        /* Increment port#: */
        SlaveSocket.setMasterPort(SlaveSocket.getMasterPort() + 1);
        SlaveSocket.setRemotePort(SlaveSocket.getRemotePort() + 1);

        common.ptod("Trying again on port " + SlaveSocket.getMasterPort());
      }
    }
  }


  public static void connectToSlaves()
  {
    long signaltod    = 0;

    /* Continuously accept new connections: */
    long start = System.currentTimeMillis();
    while (true)
    {
      /* If we got them all we're done: */
      if (SlaveList.waitForConnections())
        break;

      /* If all slaves have aborted we'll abort here: */
      SlaveList.allDead();

      /* Every x seconds tell user we're still waiting: */
      if ( (signaltod = common.signal_caller(signaltod, 10000)) == 0)
        SlaveList.displayConnectWait();

      if (System.currentTimeMillis() - start > 60*1000)
        common.failure("Terminating attempt to connect to slaves.");

      try
      {
        SlaveSocket socket = new SlaveSocket(server_socket_to_slaves);

        /* We have a new client, hand him off to a separate thread: */
        SlaveOnMaster som = new SlaveOnMaster();
        som.setSocket(socket);
        som.start();


        //SlaveStarter ss = (SlaveStarter) ThreadControl.getIdleThread("SlaveStarter");
        //ss.setSlave(slave);
        //ss.startWorking();
      }

      /* socket.accept() timeout. That's OK, try again: */
      catch (SocketTimeoutException e)
      {
        continue;
      }

      catch (IOException e)
      {
        common.failure(e);
      }
    }
  }

}
