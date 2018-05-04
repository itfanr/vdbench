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
import java.text.*;
import java.util.*;

/**
 * Code that handles messages received from the slave.
 */
public class SlaveOnMaster extends ThreadControl
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private SlaveSocket socket_to_slave;
  private Slave       slave;

  public void setSocket(SlaveSocket socket)
  {
    socket_to_slave = socket;
  }


  public void run()
  {

    setName("SlaveOnMaster");

    try
    {
      Thread.currentThread().setPriority( Thread.MAX_PRIORITY );

      processSlave();

      if (slave != null)
      {
        /* Whether this is a clean shutdown or not, set the status: */
        //slave.setShutdown(true);
        common.plog("SlaveOnMaster terminating: " + slave.getLabel());
      }
      else
        common.plog("SlaveOnMaster terminating. No proper slave identification.");
    }


    /* This catches any other exceptions: */
    catch (Exception e)
    {
      common.failure(e);
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * Handle whatever we needed coming from a slave
   */
  public void processSlave()
  {
    this.setIndependent();

    try
    {
      /* Make sure everything is in sync between master and slave: */
      if (!signonSlave())
      {
        socket_to_slave.close();
        return;
      }


      /* This slave is now up and ready. */
      slave.setConnected();


      /* From now on, all messages received from the socket come here: */
      while (true)
      {
        /* In case we immediately need to kill him: */
        if (slave.isAborted())
        {
          SlaveList.killSlaves();
          break;
        }

        SlaveSocket ss   = slave.getSocket();
        SocketMessage sm = ss.getMessage();

        /* Clean shutdown? */
        if (sm == null)
          break;

        int msgno = sm.getMessageNum();

        if (msgno == SocketMessage.WORK_TO_SLAVE)
        {
        }

        else if (msgno == SocketMessage.SLAVE_STATISTICS)
          CollectSlaveStats.receiveStats(slave, (SlaveStats) sm.getData());

        else if (msgno == SocketMessage.SLAVE_ABORTING)
        {
          String txt = (String) sm.getData();
          common.ptod("");
          common.ptod("**********************************************************");
          common.ptod("Slave " + slave.getLabel() + " aborting: " + txt);
          common.ptod("**********************************************************");
          common.ptod("");
          slave.setAborted(txt);

          SlaveList.killSlaves();
          break;
        }

        else if (msgno == SocketMessage.SLAVE_WORK_COMPLETED)
        {
          slave.setWorkDone(true);
          if (SlaveList.allSlaveWorkDone())
            Vdbmain.setWorkloadDone(true);
        }

        else if (msgno == SocketMessage.READY_FOR_MORE_WORK)
        {
          slave.setReadyForMore(true);
        }

        else if (msgno == SocketMessage.SLAVE_REACHED_EOF)
        {
          slave.setSequentialDone(true);
          if (SlaveList.allSequentialDone())
          {
            common.ptod("All sequential workloads on all slaves are done.");
            if (!SD_entry.isTapeTesting())
              common.ptod("This triggers end of run inspite of possibly some "+
                          "non-sequential workloads that are still running.");
            Vdbmain.setWorkloadDone(true);
          }
        }

        else if (msgno == SocketMessage.GET_LUN_INFO_FROM_SLAVE)
          ((InfoFromHost) sm.getData()).receiveInfoFromHost();

        else if (msgno == SocketMessage.SLAVE_READY_TO_GO)
          slave.setReadyToGo(true);

        else if (msgno == SocketMessage.ERROR_MESSAGE)
        {
          if (sm.getData() instanceof String)
            ErrorLog.printMessageOnLog(slave.getLabel() + ": " + (String) sm.getData());
          else
            ErrorLog.printMessagesOnLog(slave.getLabel() + ":", (Vector) sm.getData());
        }

        else if (msgno == SocketMessage.CONSOLE_MESSAGE)
        {
          if (sm.getData() instanceof String)
            common.ptod(slave.getLabel() + ": " + (String) sm.getData());
          else
          {
            synchronized (common.ptod_lock)
            {
              Vector lines = (Vector) sm.getData();
              common.ptod("");
              common.ptod("Message from slave " + slave.getLabel() + ": ");
              for (int i = 0; i < lines.size(); i++)
                common.ptod(lines.elementAt(i));
            }
          }
        }

        else if (msgno == SocketMessage.ADM_MESSAGES)
        {
          Vector lines = (Vector) sm.getData();
          for (int i = 0; i < lines.size(); i++)
            slave.getHost().writeAdmMessagesFile((String) lines.elementAt(i));
        }

        else if (msgno == SocketMessage.COUNT_ERRORS)
          ErrorLog.countErrorsOnMaster();

        /* Heartbeat() takes care of this: */
        else if (msgno == SocketMessage.HEARTBEAT_MESSAGE)
          continue;

        else if (msgno == SocketMessage.CLEAN_SHUTDOWN_COMPLETE)
        {
          slave.setShutdown(true);
          break;
        }

        else if (msgno == SocketMessage.STARTING_FILE_STRUCTURE)
          slave.setStructurePending(true);

        else if (msgno == SocketMessage.ENDING_FILE_STRUCTURE)
          slave.setStructurePending(false);

        else if (msgno == SocketMessage.ANCHOR_SIZES)
          ((AnchorReport) sm.getData()).printNumbers();

        else
          common.failure("unexpected message from slave: " + sm.getMessageText());
      }

    }

    catch (Exception e)
    {
      common.failure(e);
    }

    this.removeIndependent();
  }


  /**
   * Connect to the slave, make sure it is a valid one.
   * This check allws us to get rid of possible stale slaves that stay behind
   * in case there were errors somewhere that did not cause all slaves to
   * bet terminated
   */
  public boolean signonSlave()
  {

    /* Let's make sure that this slave is acceptable: */
    SocketMessage sm = new SocketMessage(SocketMessage.SEND_SIGNON_INFO_TO_MASTER);
    socket_to_slave.putMessage(sm);

    sm = socket_to_slave.getMessage();

    /* Make sure the right message# comes back: */
    if (sm.getMessageNum() != SocketMessage.SEND_SIGNON_INFO_TO_MASTER)
    {
      common.ptod("Killing Slave; wrong message received: " + sm.getMessageNum());
      killSlaveSignonError();
      return false;
    }

    /* Verify the data that we received. If the data is wrong, tell the */
    /* slave to kill itself:                                            */
    if (!(sm.getData() instanceof String[]))
    {
      common.ptod("Killing Slave; no String[] array received");
      killSlaveSignonError();
      return false;
    }

    /* Must have a fixed array size: */
    String data[]     = (String[]) sm.getData();
    String master_ip  = data[0];
    String slave_name = data[1];
    String os_name    = data[2];
    String os_arch    = data[3];
    if (data.length != 4)
    {
      common.ptod("Killing Slave; Only String[] array of " + data.length + " received");
      killSlaveSignonError();
      return false;
    }

    /* We must know the slave: */
    slave = SlaveList.findSlaveName(slave_name);
    if (slave == null)
    {
      common.ptod("Killing slave: invalid slave name: " + slave_name);
      killSlaveSignonError();
      return false;
    }
    socket_to_slave.setSlaveLabel(slave.getLabel());

    /* Remember OS type: */
    slave.getHost().setOS(os_name, os_arch);

    /* Must have the proper current (master) IP address: */
    if (!master_ip.equals("localhost"))
    {
      if (!master_ip.equals(common.getCurrentIP()))
      {
        common.ptod("Killing slave: invalid master IP address: " + master_ip + "; expecting: " + common.getCurrentIP());
        killSlaveSignonError();
        return false;
      }
    }

    /* We're all happy now. Tell slave that signon is successful. */
    sm = new SocketMessage(SocketMessage.SEND_SIGNON_SUCCESSFUL);
    socket_to_slave.putMessage(sm);

    /* As a confirmation, wait for this message to come back: */
    sm = socket_to_slave.getMessage();

    /* Of course, this confirmation must be OK: */
    if (sm.getMessageNum() != SocketMessage.SEND_SIGNON_SUCCESSFUL)
    {
      common.ptod("Successful signon confirmation failed: " + sm.getMessageNum());
      killSlaveSignonError();
      return false;
    }

    /* Now we're this far, it is time to mark this slave as 'connected': */
    slave.setSlaveSocket(socket_to_slave);

    return true;
  }



  /**
   * Tell the slave to go away.
   */
  private void killSlaveSignonError()
  {
    SocketMessage sm = new SocketMessage(SocketMessage.KILL_SLAVE_SIGNON_ERROR);
    socket_to_slave.putMessage(sm);
    common.ptod("Killed slave");
  }
}


class ClockDelta
{
  long start_put;
  long end_get;
  long slave_tod;
}
