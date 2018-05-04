package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static ServerSocket server_socket_to_slaves = null;

  public static void createSocketToSlaves()
  {
    int port_attempts = 0;
    int max_attempts  = 8;

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
        {
          common.ptod("");
          common.ptod("A total of %d attempts to find an available port has been made. Vdbench is giving up. ",
                      max_attempts);
          common.ptod("");
          common.failure(e);
        }

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
      {
        /* After all connections have been established there no longer is a need to    */
        /* keep the server socket open. This then allows for port numbers to be reused */
        /* Of course, if at some time, if ever, we write code to reconnect after       */
        /* a connection is lost, then we're scre.ed. I can live with thqat for now.    */
        try
        {
          server_socket_to_slaves.close();
        }
        catch (Exception e)
        {
          common.failure(e);
        }

        Status.printStatus("Slaves connected", null);
        break;
      }


      /* If all slaves have aborted we'll abort here: */
      SlaveList.allDead();

      /* Every x seconds tell user we're still waiting: */
      if ( (signaltod = common.signal_caller(signaltod, 10000)) == 0)
        SlaveList.displayConnectWait();

      /* The proliferation of extra slaves requires some extra time to connect, */
      /* so I went from 60 to 120 seconds. */
      if (System.currentTimeMillis() - start > 120*1000)
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
