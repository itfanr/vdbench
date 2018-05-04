package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

import Utils.ClassPath;


/**
 * Heartbeat monitor to make sure that a slave or the master does not disappear.
 * The last TOD of any getMessage(HEARTBEAT_MESSAGE) is checked. If it has been
 * too long the program will abort.
 *
 * Heartbeat values are set to 2 minutes.
 * A heartbeat message is sent from the master to all slaves every 30 seconds.
 * The slaves then respond, and on both sides the timestamp of this message is
 * stored.
 * There is always 5 seconds added to each timeout value.
  */
class HeartBeat extends ThreadControl
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static String[] dflt_cmd = new String[]
  {
    ClassPath.classPath("vdbench jstack")
  };
  public static Debug_cmds heartbeat_error = new Debug_cmds().storeCommands(dflt_cmd);

  private static boolean   check_slave_heartbeat;
  private static int       DEFAULT      = 3 * 60;
  private static int       seconds_late = DEFAULT;
  private static HeartBeat beater;

  {
    if (common.get_debug(common.LONGER_HEARTBEAT))
      seconds_late = DEFAULT = 15 * 60;
    else if (common.get_debug(common.SHORTER_HEARTBEAT))
      seconds_late = DEFAULT =      10;
  }


  public HeartBeat(boolean slave)
  {
    this.check_slave_heartbeat = slave;
    if (slave)
      this.setName("Check Slave HeartBeat");
    else
      this.setName("Check Master HeartBeat");
  }


  public void run()
  {
    beater = this;
    this.setIndependent();

    try
    {
      while (true)
      {
        if (check_slave_heartbeat)
        {
          if (!checkSlaveHeartBeat())
          {
            if (heartbeat_error != null)
              heartbeat_error.run_command();
            this.removeIndependent();
            common.memory_usage();
            common.dumpAllStacks();
            common.failure("Heartbeat monitor: One or more slaves did not respond");
          }

          sendSlaveHeartBeat();
        }

        else
        {
          if (!checkMasterHeartBeat())
          {
            if (heartbeat_error != null)
              heartbeat_error.run_command();
            this.removeIndependent();
            common.memory_usage();
            common.dumpAllStacks();
            common.failure("Heartbeat monitor: Master did not respond. Timeout value: " + seconds_late);
          }
        }

        common.sleep_some(seconds_late / 4 * 1000);

        if (Thread.currentThread().isInterrupted())
        {
          this.removeIndependent();
          break;
        }

        /* This was included to isolate a rare problem where it appears the interrupt */
        /* trying to shut this down is lost.                                          */
        /* Mike Berg, after a long 'endcmd="pdm end", though no clear relation        */
        if (SlaveList.shutdown_requested)
        {
          common.ptod("Heartbeat Monitor: shutting down after what may have been a lost interrupt.");
          this.removeIndependent();
          break;
        }
      }
    }

    catch (Exception e)
    {
      common.ptod("Heartbeat monitor died unexpectedly. Run terminated.");
      common.memory_usage();
      common.failure(e);
    }
  }



  /**
   * Check all slaves, see if we have heard from them lately.
   * Whatever values are set, we add 5 seconds fudge factor.
   */
  private static boolean checkSlaveHeartBeat()
  {
    long now        = System.currentTimeMillis();
    boolean missing = false;

    Vector slave_list = SlaveList.getSlaveList();
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      /* If the slave is already gone, don't bother. */
      if (slave.isShutdown())
        continue;

      if (slave.getSocket() != null)
      {
        if (slave.getSocket().getlastHeartBeat() + ((seconds_late + 5) * 1000) < now)
        {
          common.ptod("HeartBeat.checkHeartBeat(): slave " + slave.getLabel() + " has not responded for " +
                      (seconds_late + 5) + " seconds.");
          missing = true;
        }
      }
    }

    return !missing;
  }



  /**
   * Send heartbeat message to each slave.
   */
  private static void sendSlaveHeartBeat()
  {
    Vector slave_list = SlaveList.getSlaveList();
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave        = (Slave) slave_list.elementAt(i);
      SlaveSocket socket = slave.getSocket();

      /* If the slave is already gone or not there yet, don't bother. */
      if (slave.isShutdown())
        continue;

      SocketMessage sm   = new SocketMessage(SocketMessage.HEARTBEAT_MESSAGE);
      socket.putMessage(sm);
    }
  }



  /**
   * Check master, see if we have heard from him lately.
   * Check every 60 seconds. We'll probably have to change that at some time,
   * but for now just 60 seconds.
   */
  private static boolean checkMasterHeartBeat()
  {
    long now = System.currentTimeMillis();

    /* If the slave is already done, don't bother. */
    if (Vdbmain.isWorkloadDone())
      return true;;

    if (SlaveJvm.getMasterSocket() != null)
    {
      if (SlaveJvm.getMasterSocket().getlastHeartBeat() + seconds_late * 1000 < now)
      {
        common.ptod("HeartBeat.checkHeartBeat(): Master has not responded for " +
                    seconds_late + " seconds.");
        return false;
      }
    }

    return true;
  }
}


