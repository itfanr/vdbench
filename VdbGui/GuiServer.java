package VdbGui;

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
    server_sock  = null;

    /* Create a server socket: */
    client = true;
    server = false;
    try
    {
      server_sock = new ServerSocket(port);
    }
    catch (IOException e)
    {
      failure(e);
    }


    /* Now get a client to get data from: */
    try
    {
      if(server_sock != null && !server_sock.isClosed())
      {
        socket  = server_sock.accept();
        istream = new ObjectInputStream(socket.getInputStream());
        ostream = new ObjectOutputStream(socket.getOutputStream());
      }
    }
    catch (IOException e)
    {
      //failure(e);
    }
  }

  /**
   * Close the server socket.
   */
  public void xcloseSocket()
  {
    Vdb.common.ptod("server_sock: " + server_sock);
    Vdb.common.ptod("socket: " + socket);
    try
    {
      if(server_sock != null)
      {
        server_sock.close();
      }

      if(socket != null)
      {
        socket.close();
      }

      if(istream != null)
      {

        istream.close();
        istream = null;
      }
      if(ostream != null)
      {
        ostream.close();
        ostream = null;
      }

    }
    catch(IOException e)
    {
      failure(e);
    }
  }


  /**
   * Get Object from socket
   */
  public boolean get_message()
  {
    try
    {
      if(istream != null && !server_sock.isClosed())
      {
        message = istream.readObject();
      }
    }
    catch (SocketException e)
    {
      //System.err.println("GuiServer.get_message(): Server socket closed");
      return false;
    }
    catch (ClassNotFoundException e)
    {
      //failure(e);
      return false;
    }
    catch (IOException e)
    {
      //failure(e);
      return false;
    }
    catch(Exception e)
    {
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

    //System.err.println("put_message: " + obj);
    try
    {
      if(ostream != null)
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
      System.err.println("failure: " + e);
      return false;
    }
    return true;
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

        if ( (signaltod = signal_caller(signaltod, 10000)) == 0)
          System.err.println("Trying to connect to Gui");

        socket = new Socket(server, port);

        ostream = new ObjectOutputStream(socket.getOutputStream());
        istream = new ObjectInputStream(socket.getInputStream());

        /* Set timeout: */
        socket.setSoTimeout(1 * 60 * 1000);

        return;
      }


      catch (OptionalDataException e)
      {
        System.err.println("connect_to_server status 1");
        failure(e);
      }
      catch (UnknownHostException e)
      {
        System.err.println("connect_to_server status 2");
        failure(e);
      }
      catch (ConnectException e)
      {
        System.err.println("connect_to_server status 3");
        sleep_some_no_int(1000);
        continue;
      }
      catch (IOException e)   // Socket disconnected means previous master just left
      {
        System.err.println("connect_to_server status 4");
        sleep_some_no_int(1000);
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
      client.connect_to_client(4444);

      while (client.get_message())
      {
        System.err.println("from client: " + (String) client.message);
        client.put_message("received: " + (String) client.message);
      }
    }

    /* test the server portion: */
    else
    {
      GuiServer server = new GuiServer();
      server.connect_to_server("localhost", 4444);

      for (int i = 0; i < 10; i++)
      {
        server.put_message("message " + i);
        if (!server.get_message())
          return;
        System.err.println("from server: " + (String) server.message);
      }
    }
  }

  private static void failure(Exception e)
  {
    System.err.println(e);
    e.printStackTrace();
    //Thread.currentThread().dumpStack();
    //System.exit(-1);
  }

  /**
   * Signal caller after n milliseconds.
   * Returns zero if more than 'msecs' time elapsed since the first call.
   */
  private static long signal_caller(long base, long msecs)
  {
    long tod = System.currentTimeMillis();

    /* First call, just set base tod: */
    if (base == 0)
      return tod;

    /* If tod expired, return 0 which is the signal that time expired */
    if (base + msecs < tod)
      return 0;

    return base;
  }


  /**
   * Sleep x milliseconds, with or without returning an interrupt
   */
  public static void sleep_some_no_int(long msecs)
  {
    try
    {
      sleep_some(msecs);
    }
    catch (InterruptedException e)
    {
    }
  }
  public static void sleep_some(long msecs) throws InterruptedException
  {

    if (msecs == 0)
      return;

    sleep_some_usecs(msecs * 1000);
  }


  public static void sleep_some_usecs(long usecs) throws InterruptedException
  {

    try
    {
      Thread.sleep(usecs / 1000, (int) (usecs % 1000) * 1000);
    }

    catch (InterruptedException x)
    {
      System.out.println("Interrupted in common.sleep()");
      throw(new InterruptedException());
    }
  }

}




