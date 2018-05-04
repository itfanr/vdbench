package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

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
        //common.plog("SlaveOnMaster terminating: " + slave.getLabel());
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
      String pid_txt = signonSlave();
      if (pid_txt == null)
      {
        socket_to_slave.close();
        return;
      }


      /* This slave is now up and ready. */
      slave.setConnected(pid_txt);


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
          BoxPrint.printOne("Slave " + slave.getLabel() + " aborting: " + txt);
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
            common.ptod("This triggers end of run inspite of possibly some "+
                        "non-sequential workloads that are still running.");
            //Vdbmain.setWorkloadDone(true);
          }
        }

        else if (msgno == SocketMessage.GET_LUN_INFO_FROM_SLAVE)
          ((InfoFromHost) sm.getData()).receiveInfoFromHost();

        else if (msgno == SocketMessage.SLAVE_READY_TO_GO)
          slave.setReadyToGo(true);

        else if (msgno == SocketMessage.ERROR_MESSAGE)
        {
          if (sm.getData() instanceof String)
            ErrorLog.ptod(slave.getLabel() + ": " + (String) sm.getData());
          else
            ErrorLog.ptodSlave(slave, (Vector) sm.getData());
        }

        else if (msgno == SocketMessage.ERROR_LOG_MESSAGE)
        {
          if (sm.getData() instanceof String)
            ErrorLog.plog(slave.getLabel() + ": " + (String) sm.getData());
          else
            common.failure("ERROR_LOG_MESSAGE tbd");
            //ErrorLog.printMessagesOnLog(slave.getLabel() + ":", (Vector) sm.getData());
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

        else if (msgno == SocketMessage.SUMMARY_MESSAGE)
        {
          common.psum(slave.getLabel() + ": " + (String) sm.getData());
          common.ptod(slave.getLabel() + ": " + (String) sm.getData());
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
      common.ptod(e);
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
  public String signonSlave()
  {

    /* Let's make sure that this slave is acceptable: */
    SocketMessage sm = new SocketMessage(SocketMessage.SEND_SIGNON_INFO_TO_MASTER);
    sm.setData(new Integer(common.getProcessId()));
    socket_to_slave.putMessage(sm);

    sm = socket_to_slave.getMessage();

    /* Make sure the right message# comes back: */
    if (sm.getMessageNum() != SocketMessage.SEND_SIGNON_INFO_TO_MASTER)
    {
      common.ptod("Killing Slave; wrong message received: " + sm.getMessageNum());
      killSlaveSignonError();
      return null;
    }

    /* Verify the data that we received. If the data is wrong, tell the */
    /* slave to kill itself:                                            */
    if (!(sm.getData() instanceof String[]))
    {
      common.ptod("Killing Slave; no String[] array received");
      killSlaveSignonError();
      return null;
    }

    /* Must have a fixed array size: */
    String data[] = (String[]) sm.getData();
    if (data.length != SocketMessage.SIGNON_INFO_SIZE)
    {
      common.ptod("Killing Slave; Only String[] array of " + data.length + " received");
      killSlaveSignonError();
      return null;
    }

    String master_ip  = data[0];
    String slave_name = data[1];
    String os_name    = data[2];
    String os_arch    = data[3];
    String pid_txt    = data[4];
    String utc        = data[5];

    /* We must know the slave: */
    slave = SlaveList.findSlaveName(slave_name);
    if (slave == null)
    {
      common.ptod("Killing slave: invalid slave name: " + slave_name);
      killSlaveSignonError();
      return null;
    }
    socket_to_slave.setSlaveLabel(slave.getLabel());

    /* This is an easier fix that having Vdbench automagically adjust for deltas: */
    /* Of course, if it took this message 30 seconds to get across we have other problems. */
    long delta = Math.abs(System.currentTimeMillis() - Long.parseLong(utc)) + 1;
    if (delta >= 30*1000)
      common.ptod("Clock synchronization warning: slave %s is %d seconds out of sync. This can lead to heartbeat issues.",
                  slave.getLabel(), delta / 1000);

    /* Remember OS type: */
    slave.getHost().setOS(os_name, os_arch);

    /* Must have the proper current (master) IP address: */
    if (!master_ip.equals("localhost"))
    {
      if (!master_ip.equals(common.getCurrentIP()))
      {
        common.ptod("Killing slave: invalid master IP address: " + master_ip + "; expecting: " + common.getCurrentIP());
        killSlaveSignonError();
        return null;
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
      return null;
    }

    /* Now we're this far, it is time to mark this slave as 'connected': */
    slave.setSlaveSocket(socket_to_slave);

    return pid_txt;
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
