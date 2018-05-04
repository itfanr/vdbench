package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
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
 * Remote Shell daemon for vdbench.
 * See notes under Rsh().
 */
class RshDeamon
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

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
        common.ptod("Vdbench rsh daemon: waiting for new users");

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
          common.ptod("waiting for new Rshdaemon request. Current active requests:");
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
        if (line.startsWith("rshdaemonport="))
        {
          try
          {
            rsh_port = Integer.parseInt(line.substring(14));
          }
          catch (Exception e)
          {
            common.ptod("Error parsing 'rshdaemonport=': " + line);
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

    //common.ptod("rshdaemonport: " + SlaveSocket.getRemotePort());
    //common.ptod("masterslaveport: " + SlaveSocket.getMastersh_port());

    if (SlaveSocket.getRemotePort() == SlaveSocket.getMasterPort())
    {
      common.ptod("rshdaemonport: " + SlaveSocket.getRemotePort());
      common.ptod("masterslaveport: " + SlaveSocket.getMasterPort());
      common.failure("rshdaemonport and masterslaveport must have different port numbers");
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
