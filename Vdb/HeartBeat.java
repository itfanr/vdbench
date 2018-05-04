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


/**
 * Heartbeat monitor to make sure that a slave or the master does not disappear.
 * The last TOD of any getMessage(HEARTBEAT_MESSAGE) is checked. If it has been
 * too long the program will abort.
 *
 * Heartbeat values are set to 2 minutes.
 * A heartbeat message is sent from the master to all slaves every 30 seconds.
 * The slaves then respond, and on both sides the timestamp of this message is
 * stored.
 * There is always 5 seconds added to each value.
  */
class HeartBeat extends ThreadControl
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public static Debug_cmds heartbeat_error = null;

  private static boolean   slave;
  private static int       DEFAULT      = 2 * 60;
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
    this.slave = slave;
    if (slave)
      this.setName("Slave HeartBeat");
    else
      this.setName("Master HeartBeat");
  }


  public void run()
  {
    beater = this;
    this.setIndependent();

    try
    {
      while (true)
      {
        if (slave)
        {
          if (!checkSlaveHeartBeat())
          {
            if (heartbeat_error != null)
              heartbeat_error.run_command();
            this.removeIndependent();
            common.memory_usage();
            VdbCount.listCounters("Heartbeat:");
            common.dumpAllStacks();
            common.failure("Heartbeat monitor: One or more slaves did not respond");
          }

          sendSlaveHeartBeat();
        }

        else
        {
          if (!checkMasterHeartBeat())
          {
            this.removeIndependent();
            common.memory_usage();
            VdbCount.listCounters("Heartbeat:");
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
      }
    }

    catch (Exception e)
    {
      common.ptod("Heartbeat monitor died unexpectedly. Run terminated.");
      common.memory_usage();
      VdbCount.listCounters("Heartbeat:");
      common.failure(e);
    }
  }



  /**
   * Check all slaves, see if we have heard from them lately.
   * Whatever values are set, we add 5 seconds fudge factor.
   */
  public static boolean checkSlaveHeartBeat()
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
  public static void sendSlaveHeartBeat()
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
  public static boolean checkMasterHeartBeat()
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


