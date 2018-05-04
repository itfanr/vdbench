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
import Utils.Format;
import Utils.Fget;
import Utils.ClassPath;


/**
 * This class handles socket communications between master and slave.
 */
class SlaveSocket implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /* You may only see these names in messages during signon */
  private String slave_name  = "signon";
  private String slave_label = "signon";

  private ObjectInputStream  istream;
  private ObjectOutputStream ostream;
  private Socket             socket;
  private boolean            shutdown_in_progress = false;
  private boolean            try_recovery = true;

  private long               last_heartbeat_received = System.currentTimeMillis();

  private static int remote_port = 5560;
  private static int master_port = 5570;

  private static boolean print_put_get = common.get_debug(common.SHOW_SOCKET_MESSAGES);


  /**
   * This instantiation connects from slave to master
   */
  public SlaveSocket(String address, int port) throws UnknownHostException, IOException
  {
    socket = new Socket(address, port);

    /* Set timeout to something that is reasonably guaranteed to be higher than */
    /* the reporting interval:                                                  */
    /* I am suspicious that sotimers popping too often concurrently with        */
    /* a real message arriving has caused some unexplained hangs                */
    socket.setSoTimeout(120 * 1000);

    ostream = new ObjectOutputStream(socket.getOutputStream());
    istream = new ObjectInputStream(socket.getInputStream());
  }


  /**
   * This instantiation accepts a new slave from a server socket
   */
  public SlaveSocket(ServerSocket serversocket) throws IOException
  {
    socket = serversocket.accept();

    ostream = new ObjectOutputStream(socket.getOutputStream());
    istream = new ObjectInputStream(socket.getInputStream());

    ///* Windows (and maybe others) do not cause a readObject() to be interrupted */
    ///* I therefore set the timeout to only one second and manually check for    */
    ///* isInterrupted()                                                          */
    socket.setSoTimeout(120 * 1000);
  }

  public void setSlaveLabel(String label)
  {
    slave_label = label;
  }

  public void setSlaveName(String name)
  {
    slave_name = name;
  }

  public static void setRemotePort(int port)
  {
    remote_port = port;
  }
  public static int getRemotePort()
  {
    return remote_port;
  }

  public static void setMasterPort(int port)
  {
    master_port = port;
  }
  public static int getMasterPort()
  {
    return master_port;
  }

  public Socket getSocket()
  {
    return socket;
  }


  /**
   * Override the default socket recovery setting.
   * This recovery code must still be written!
   */
  public void setRecovery(boolean bool)
  {
    try_recovery = bool;
  }


  public SocketMessage getMessage()
  {
    SocketMessage sm = null;
    while (true)
    {
      try
      {
        sm = (SocketMessage) istream.readObject();
      }

      catch (EOFException e)
      {
        if (shutdown_in_progress)
          return null;
        common.ptod("");
        common.ptod("Receiving unexpected EOFException from slave: " + slave_label);
        common.ptod("This means that this slave terminated prematurely.");

        /* If we abort here right away we don't give SlaveStarter() and  */
        /* OS_cmd() the chance to recognize that this slave disappeared. */
        /* It is cleaner to have them abort, and not do it here.         */
        common.ptod("This thread will go to sleep for 5 seconds to allow ");
        common.ptod("slave termination to be properly recognized.");
        common.ptod("");
        common.sleep_some(5000);

        common.failure(e);
      }
      catch (SocketException e)
      {
        if (shutdown_in_progress)
          return null;

        /* Remove any outstanding interrupt: */
        //Thread.currentThread().interrupted();

        common.ptod("");
        common.ptod("SocketException from slave: " + slave_label);
        common.ptod("Slave " + slave_label + " terminated unexpectedly.");

        Slave slave = SlaveList.findSlave(slave_label);
        common.ptod("Look at file " + slave.getConsoleLog().getFileName() +
                    ".html for more information.");
        common.plog("HTML link: <A HREF=\"" + slave.getConsoleLog().getFileName() + ".html\">" +
                    slave.getConsoleLog().getFileName() + ".html</A>");


        common.failure(e);
      }

      /* We're OK with this: */
      catch (SocketTimeoutException e)
      {
        /* An interrupt is asking for the thread to be stooped: */
        if (Thread.currentThread().isInterrupted())
        {
          return null;
        }

        common.plog("SocketTimeoutException; continuing " + slave_label);
        continue;
      }
      catch (ClassNotFoundException e)
      {
        common.ptod("ClassNotFoundException from slave: " + slave_label);
        common.failure(e);
      }
      catch (IOException e)
      {
        common.ptod("IOException from slave: " + slave_label);
        common.failure(e);
      }

      if (print_put_get || common.get_debug(common.SOCKET_TRAFFIC))
      {
        if (common.get_debug(common.SOCKET_TRAFFIC))
          common.ptod(Format.f("getMessage: %-12s", slave_label) +
                      Format.f(" %3d ", sm.getSeqno()) + sm.getMessageText());
        else
          common.plog(Format.f("getMessage: %-12s", slave_label) +
                      Format.f(" %3d ", sm.getSeqno()) + sm.getMessageText());
      }

      /* Remember when we last heard from this guy: */
      if (sm.getMessageNum() == SocketMessage.HEARTBEAT_MESSAGE)
        last_heartbeat_received = System.currentTimeMillis();

      return sm;
    }
  }



  /**
   * Send a message to the other side of the socket.
   * 'synchronized' is set to prevent more than one writeObject() from
   * being active at the same time, cauing a 'stream active' IOException
   */
  public boolean putMessage(SocketMessage sm)
  {
    Exception error = null;

    synchronized(this)
    {
      while (true)
      {
        sm.setSeqno();

        if (print_put_get || common.get_debug(common.SOCKET_TRAFFIC))
        {
          if (common.get_debug(common.SOCKET_TRAFFIC))
            common.ptod(Format.f("putMessage: %-12s", slave_label) +
                        Format.f(" %3d ", sm.getSeqno()) + sm.getMessageText());
          else
            common.plog(Format.f("putMessage: %-12s", slave_label) +
                        Format.f(" %3d ", sm.getSeqno()) + sm.getMessageText());
        }

        /* Any text message sent to the master also goes on the slave's log: */
        if (sm.getMessageNum() == SocketMessage.CONSOLE_MESSAGE &&
            sm.getData() instanceof String)
          common.plog("Message to master: " + (String) sm.getData());

        try
        {
          ostream.reset();
          ostream.writeObject(sm);
          ostream.flush();
        }

        catch (SocketException e)
        {
          if (!try_recovery)
          {
            common.ptod("SocketException during putMessage(sm). Continuing");
            return false;
          }
          error = e;
        }
        catch (IOException e)
        {
          if (!try_recovery)
          {
            common.ptod("IOException during putMessage(sm). Continuing");
            return false;
          }
          error = e;
        }

        if (error != null)
          break;

        return true;
      }
    }

    /* The call to 'failure' was moved here so that we can call it WITHOUT */
    /* the socket being locked. */
    common.ptod("Exception from slave: " + slave_label);
    Thread.currentThread().dumpStack();
    common.failure(error);

    return false;
  }


  public long getlastHeartBeat()
  {
    return last_heartbeat_received;
  }



  public void setShutdown(boolean bool)
  {
    shutdown_in_progress = bool;
  }
  public boolean isShutdown()
  {
    return shutdown_in_progress;
  }
  public void close()
  {
    try
    {
      if (socket != null)
        socket.close();
    }
    catch (IOException e)
    {
    }

    socket = null;
  }
}


