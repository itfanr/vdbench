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

import java.io.*;
import java.net.*;
import java.util.*;
import Vdb.common;

public class GuiServer
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private boolean            server;         /* Either server or client    */
  private boolean            client;
  private Socket             socket  = null; /* Socket for this connection */
  private ObjectInputStream  istream = null; /* Input stream for socket    */
  private ObjectOutputStream ostream = null; /* Output stream for socket   */
  private Object             message;        /* For after get_message()    */
  private ServerSocket       server_sock;


  /**
   * Connect to the requested client
   */
  public void connect_to_client(int port)
  {
    server_sock = null;

    /* Create a server socket: */
    client = true;
    server = false;
    try
    {
      server_sock = new ServerSocket(port);
      //common.ptod("server_sock2: " + server_sock);
    }
    catch (IOException e)
    {
      common.failure(e);
    }


    /* Now get a client to get data from: */
    try
    {
      if (server_sock != null && !server_sock.isClosed())
      {
        socket  = server_sock.accept();
        istream = new ObjectInputStream(socket.getInputStream());
        ostream = new ObjectOutputStream(socket.getOutputStream());
        //common.ptod("istream1: " + istream + " " + this);
      }
    }
    catch (IOException e)
    {
      //common.failure(e);
    }
  }

  /**
   * Close the server socket.
   */
  public void closeSocket()
  {
    try
    {
      if (server_sock != null)
      {
        server_sock.close();
      }

      if (socket != null)
      {
        socket.close();
      }

      if (istream != null)
      {

        istream.close();
        istream = null;
        //common.ptod("istream3: " + istream + " " + this);
      }
      if (ostream != null)
      {
        ostream.close();
        ostream = null;
      }

    }
    catch (IOException e)
    {
      common.failure(e);
    }
  }


  /**
   * Get Object from socket
   */
  public boolean get_message()
  {
    try
    {
      if (istream != null)// && !server_sock.isClosed())
      {
        message = istream.readObject();
      }
    }
    catch (SocketException e)
    {
      //common.ptod("GuiServer.get_message(): Server socket closed");
      return false;
    }
    catch (ClassNotFoundException e)
    {

      common.where();
      //common.failure(e);
      return false;
    }
    catch (IOException e)
    {
      //common.ptod(e);
      return false;
    }
    catch (Exception e)
    {
      common.ptod(e);
      common.where();
      return false;
    }

    return true;
  }


  /**
   * Send Object to socket.
   * Returns 'false' after any error.
   */
  public boolean put_message(Object obj)
  {
    try
    {
      if (ostream != null)
      {
        ostream.reset();
        ostream.writeObject(obj);
        ostream.flush();
      }
    }
    catch (SocketException e)
    {
      return false;
    }
    catch (IOException e)
    {
      common.ptod("common.failure: " + e);
      return false;
    }
    return true;
  }


  /**
   * Write data from vdbench to the GUI chart.
   *
   * The data sent is no longer used. The Socket status however is used if
   * the user wants to CANCEL the run.
   *
   * The data is no longer needed because I removed the KavaChart code
   * that displayed the performance chart.
   *
   */
  public boolean send_interval_data(SdStats intv_stats)
  {
    /* Only send data if needed: */
    if (Vdbmain.gui_port == 0)
      return true;

    /* get all the data we need: */
    Gui_perf_data gp = new Gui_perf_data();
    gp.end_ts        = new Date();
    gp.resp_time     = intv_stats.respTime();
    gp.iops          = intv_stats.rate();
    gp.megabytes     = intv_stats.megabytes();

    put_message(gp);

    /* Wait for confirmation that the data has arrived: */
    if (get_message())
    {
      return true;
    }

    common.ptod("Socket from vdbench to GUI closed. Vdbench terminating");
    common.exit(0);
    return false;

  }





  /**
   * Get message content
   */
  public Object get_data()
  {
    return message;
  }

  /**
   * Slave tries to connect to master
   */
  public void connect_to_server(String server, int port)
  {
    long signaltod = 0;

    while (true)
    {
      try
      {

        if ( (signaltod = common.signal_caller(signaltod, 10000)) == 0)
          common.ptod("Trying to connect to Gui");

        socket = new Socket(server, port);

        ostream = new ObjectOutputStream(socket.getOutputStream());
        istream = new ObjectInputStream(socket.getInputStream());
        //common.ptod("istream2: " + istream + " " + this);

        /* Set timeout: */
        socket.setSoTimeout(1 * 60 * 1000);

        return;
      }

      //are we in the correct JVM??????


      catch (OptionalDataException e)
      {
        common.ptod("connect_to_server status 1");
        common.failure(e);
      }
      catch (UnknownHostException e)
      {
        common.ptod("connect_to_server status 2");
        common.failure(e);
      }
      catch (ConnectException e)
      {
        common.ptod("connect_to_server status 3");
        common.sleep_some(1000);
        continue;
      }
      catch (IOException e)   // Socket disconnected means previous master just left
      {
        common.ptod("connect_to_server status 4");
        //sleep_some_no_int(1000);

        common.ptod("GUI socket closed. Run terminating");
        common.exit(0);
        continue;
      }
    }
  }


  public static void main(String args[])
  {
    /* Test the client portion: */
    if (args[0].equals("client"))
    {
      GuiServer client = new GuiServer();
      client.connect_to_client(10656);

      while (client.get_message())
      {
        common.ptod("from client: " + (String) client.message);
        client.put_message("received: " + (String) client.message);
      }
    }

    /* test the server portion: */
    else
    {
      GuiServer server = new GuiServer();
      server.connect_to_server("localhost", 10656);

      for (int i = 0; i < 10; i++)
      {
        server.put_message("message " + i);
        if (!server.get_message())
        {
          common.where();
          return;
        }
        common.ptod("from server: " + (String) server.message);
      }
    }
  }
}




