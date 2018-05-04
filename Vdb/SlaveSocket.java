package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import Utils.*;


/**
 * This class handles socket communications between master and slave.
 */
public class SlaveSocket implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /* You may only see these names in messages during signon */
  private String slave_name  = "signon";
  private String slave_label = "signon";

  private ObjectInputStream  istream;
  private ObjectOutputStream ostream;
  private Socket             socket;
  private boolean            shutdown_in_progress = false;
  private boolean            try_recovery = true;

  private long               last_heartbeat_received = System.currentTimeMillis();

  private static Object bytes_lock = new Object();
  private static long bytes_raw    = 0;
  private static long bytes_zip    = 0;

  public static long shortest_delta = Long.MAX_VALUE;

  private static int remote_port = 5560;
  private static int master_port = 5570;


  private static boolean dont_zip = common.get_debug(common.DONT_ZIP_SOCKET_MSGS);

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
    setBufferSize();

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
    setBufferSize();
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
        sm.receive_time = System.currentTimeMillis();

        if (!dont_zip)
          sm.setData(unCompressObj((byte[]) sm.getData()));

        long delta = sm.receive_time - sm.send_time;
        if (delta < shortest_delta)
        {
          shortest_delta = delta;
          if (common.get_debug(common.SHOW_SOCKET_MESSAGES))
            common.ptod("shortest_delta: " + shortest_delta);
        }

        /* Optional report the message size just received on the master: */
        if (!SlaveJvm.isThisSlave()
            //&& sm.getMessageNum() == SocketMessage.SLAVE_STATISTICS
            && common.get_debug(common.REPORT_MESSAGE_SIZE))
        {
          int    org_size = CompressObject.sizeof(sm);
          int    zip_size = compressObj(sm).length;
          double ratio    = (double) org_size / zip_size;

          synchronized (bytes_lock)
          {
            bytes_raw += org_size;
            bytes_zip += zip_size;
            double ratio2 = (double) bytes_raw / bytes_zip;

            common.ptod("getMessage length: " +
                        SlaveList.getLabelMask() +
                        " %6d %6d %6.2f:1 %,12d %,12d %6.2f:1 %s ",
                        slave_label, org_size, zip_size, ratio,
                        bytes_raw, bytes_zip, ratio2, sm.getMessageText());
          }
        }
      }

      catch (EOFException e)
      {
        if (shutdown_in_progress)
          return null;
        if (Ctrl_c.active())
          return null;
        Vector lines = new Vector(16);
        lines.add("");
        if (!SlaveJvm.isThisSlave())
        {
          lines.add("Receiving unexpected EOFException from slave: " + slave_label);
          lines.add("This means that this slave terminated prematurely.");
        }
        else
        {
          lines.add("Receiving unexpected EOFException from the master");
          lines.add("This means that the master terminated prematurely.");
        }

        /* If we abort here right away we don't give SlaveStarter() and  */
        /* OS_cmd() the chance to recognize that this slave disappeared. */
        /* It is cleaner to have them abort, and not do it here.         */
        lines.add("This thread will go to sleep for 5 seconds to allow ");
        lines.add("slave termination to be properly recognized.");
        lines.add("");
        common.ptod(lines);
        common.sleep_some(5000);

        common.failure(e);
      }
      catch (SocketException e)
      {
        if (shutdown_in_progress)
          return null;

        /* Remove any outstanding interrupt: */
        //Thread.currentThread().interrupted();

        Slave slave = SlaveList.findSlaveLabel(slave_label);
        Vector msgs = new Vector(8);
        msgs.add("");
        msgs.add("SocketException from slave: " + slave_label);
        msgs.add("Slave " + slave_label + " terminated unexpectedly.");
        msgs.add("Look at file " + slave.getConsoleLog().getFileName() +
                 ".html for more information.");
        msgs.add("");
        common.plog("HTML link: <A HREF=\"" + slave.getConsoleLog().getFileName() + ".html\">" +
                    slave.getConsoleLog().getFileName() + ".html</A>");
        common.ptod(msgs);

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

      /* Debugging: */
      long resptime = sm.receive_time - sm.send_time - shortest_delta;
      if (common.get_debug(common.SHOW_SOCKET_MESSAGES) ||
          common.get_debug(common.SOCKET_TRAFFIC))
      {
        if (common.get_debug(common.SOCKET_TRAFFIC))
          common.ptod("getMessage: %-12s %3d %5d %s",
                      slave_label, sm.getSeqno(), resptime, sm.getMessageText());

        else
          common.plog("getMessage: %-12s %3d %5d %s",
                      slave_label, sm.getSeqno(), resptime, sm.getMessageText());
      }

      /* Extra info to help with possible slow socket traffic: */
      // This tells you that it took nnn milliseconds between SENDER and RECEIVER!
      if (resptime > 5000)
        common.plog("Slow getMessage: %-12s %3d %5d %s",
                    slave_label, sm.getSeqno(), resptime, sm.getMessageText());


      /* Remember when we last heard from this guy: */
      if (sm.getMessageNum() == SocketMessage.HEARTBEAT_MESSAGE)
        last_heartbeat_received = System.currentTimeMillis();


      // To figure out the size of the object:
      // vdbench -tf: 26391 bytes for one FSD.
      // three fsds two workloads: 38000
      // three fsds one workloads: 32000
      // In other words: noise.
      // SD: 1536-2048 bytes per SD * WD * slaves

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

    /* Any text message sent to the master also goes on the slave's log. */
    /* We don't need the socket lock to do that: */
    if (sm.getMessageNum() == SocketMessage.CONSOLE_MESSAGE &&
        sm.getData() instanceof String)
      common.plog("Message to master: " + (String) sm.getData());

    synchronized(this)
    {
      while (true)
      {
        sm.setSeqno();

        if (common.get_debug(common.SHOW_SOCKET_MESSAGES) ||
            common.get_debug(common.SOCKET_TRAFFIC))
        {
          if (common.get_debug(common.SOCKET_TRAFFIC))
            common.ptod("putMessage: %-12s %3d       %s",
                        slave_label, sm.getSeqno(), sm.getMessageText());
          else
            common.plog("putMessage: %-12s %3d       %s",
                        slave_label, sm.getSeqno(), sm.getMessageText());
        }

        try
        {
          if (!dont_zip)
            sm.setData(compressObj(sm.getData()));

          long start = System.currentTimeMillis();
          sm.send_time = System.currentTimeMillis();
          ostream.reset();
          ostream.writeObject(sm);
          ostream.flush();
          long end = System.currentTimeMillis();


          // To figure out the size of the object:
          // vdbench -tf: 26391 bytes for one FSD.
          // three fsds two workloads: 38000
          // three fsds one workloads: 32000
          // In other words: noise.
          //String tmpf = Fput.createTempFileName("one", "two");
          //common.serial_out(tmpf, sm);
          //common.ptod("length: %8d %s %6d", new File(tmpf).length(),
          //            sm.getMessageText(), (end - start));
        }

        catch (SocketException e)
        {
          /* If the slave already told us he is aborting, don't bother any more: */
          if (!SlaveJvm.isThisSlave())
          {
            Slave slave = SlaveList.findSlaveLabel(slave_label);
            if (slave.isAborted())
              break;
          }

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
        {
          break;
        }

        return true;
      }
    }

    /* We can't do ptod() above due to deadlock risk:*/
    if (!SlaveJvm.isThisSlave())
    {
      Slave slave = SlaveList.findSlaveLabel(slave_label);
      if (slave.isAborted())
      {
        common.plog("SocketException from aborting slave. That's OK.");
        return false;
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

  public void setlastHeartBeat()
  {
    last_heartbeat_received = System.currentTimeMillis();
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


  /**
   * Set the buffer sizes to a minimum of 48k.
   *
   * This was added as an attempt to fix frequent windows 2008 server socket
   * reset problems. Not sure if it helps, but 8k on windows just feels too
   * small anyway, that with 48k being the solaris default.
   *
   * BTW: the Win2008 problem was caused by SOME Linux system on the network
   * being rebooted, that indirectly (why the heck?) causing all win2008 java
   * sockets to be reset. Go figure!
   */
  private void setBufferSize()
  {
    try
    {
      int BUFFER_SIZE = 48*1024;
      if (socket.getSendBufferSize() < BUFFER_SIZE)
        socket.setSendBufferSize(BUFFER_SIZE);
      if (socket.getReceiveBufferSize() < BUFFER_SIZE)
        socket.setReceiveBufferSize(BUFFER_SIZE);
      //common.ptod("socket.getSendBufferSize():   " + socket.getSendBufferSize());
      //common.ptod("ocket.getReceiveBufferSize(): " + socket.getReceiveBufferSize());
    }
    catch (Exception e)
    {
      common.failure(e);
    }
  }


  private byte[] compressObj(Object obj)
  {
    if (obj == null)
      return null;

    try
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GZIPOutputStream      zos = new GZIPOutputStream(bos);
      ObjectOutputStream    ous = new ObjectOutputStream(zos);

      ous.writeObject(obj);
      zos.finish();
      bos.flush();

      return bos.toByteArray();
    }
    catch (Exception e)
    {
      common.failure(e);
      return null;
    }
  }

  private Object unCompressObj(byte[] array)
  {
    if (array == null)
      return null;

    try
    {
      Object obj = null;

      ByteArrayInputStream bis = new ByteArrayInputStream(array);
      GZIPInputStream      zis = new GZIPInputStream(bis);
      ObjectInputStream    ois = new ObjectInputStream(zis);

      try
      {
        obj = ois.readObject();
      }
      catch (ClassNotFoundException e)
      {
        common.failure(e);
      }

      return obj;
    }
    catch (Exception e)
    {
      common.failure(e);
      return null;
    }
  }
}


