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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Vector;

import Oracle.OracleStats;

import Utils.*;



/**
 * Main workhorse for Vdbench. All real work is done here.
 *
 * This program is started in its own JVM, though when there is only one
 * local JVM needed it is started by calling SlaveJvm.main(args)
 */
public class SlaveJvm
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static String master_ip   = null;
  private static String slave_name  = null;
  private static String slave_label = null;

  private static boolean first_slave_on_host = false;
  private static boolean master_aborting;
  private static Semaphore wait_to_run = new Semaphore(0);

  private static SlaveSocket socket_to_master;

  private static Semaphore workload_done_semaphore = new Semaphore(0);
  private static Semaphore master_done_semaphore = new Semaphore(0);
  private static boolean workload_done;

  private static boolean replay   = false;;
  private static String  replay_file;

  private static boolean fwd_workload = false;
  private static boolean this_is_an_active_slave = false;

  private static boolean error_with_kstat = false;

  private static Mount rd_mount;

  private static String master_abort_msg = "Master is aborting. Slave now also terminating";


  public static boolean isThisSlave()
  {
    return this_is_an_active_slave;
  }

  public static boolean isFirstSlaveOnHost()
  {
    return first_slave_on_host;
  }

  public static void setReplay(boolean bool, String fname)
  {
    replay      = bool;
    replay_file = fname;
  }
  public static boolean isReplay()
  {
    return replay;
  }

  public static void setKstatError()
  {
    error_with_kstat = true;
  }
  public static boolean getKstatError()
  {
    return error_with_kstat;
  }

  private static void connectToMaster()
  {
    int timeout = 60;

    /* Connect to the master: */
    while (true)
    {
      try
      {
        socket_to_master = new SlaveSocket(master_ip, SlaveSocket.getMasterPort());
        socket_to_master.setSlaveLabel(slave_label);
        socket_to_master.setSlaveName(slave_name);

        /* Set timeout */
        socket_to_master.getSocket().setSoTimeout(timeout * 1000);

        common.ptod("successfully connected to master " + master_ip);
      }

      /* Not here, keep trying: */
      catch (ConnectException e)
      {
        common.ptod(e);
        common.ptod("Slave ConnectException. ");
        common.failure("It took at least " + timeout +
                       " seconds to connect. SlaveJvm terminated");
      }

      /* Any errors at this time are fatal: */
      catch (UnknownHostException e)
      {
        common.failure(e);
      }
      catch (IOException e)
      {
        common.failure(e);
      }

      /* Connection made, drop out of connect loop: */
      common.ptod("Connection to " + master_ip + " using port " + SlaveSocket.getMasterPort() + " successful");
      break;

    }
  }

  public static SlaveSocket getMasterSocket()
  {
    return socket_to_master;
  }

  public static void sendMessageToConsole(Object txt_or_vector)
  {
    if (!(txt_or_vector instanceof String))
    //  common.plog("sendMessageToConsole: " + txt_or_vector);
    //else
    {
      synchronized (common.ptod_lock)
      {
        Vector txt = (Vector) txt_or_vector;
        for (int i = 0; i < txt.size(); i++)
          common.plog("sendMessageToConsole: " + txt.elementAt(i));
      }
    }

    sendMessageToMaster(SocketMessage.CONSOLE_MESSAGE, txt_or_vector);
  }

  public static void sendMessageToMaster(int msgno)
  {
    sendMessageToMaster(msgno, null);
  }
  public static void sendMessageToMaster(int msgno, Object data)
  {
    SocketMessage sm = new SocketMessage(msgno);
    sm.setData(data);
    socket_to_master.putMessage(sm);
  }

  /**
   * Wait for messages from the master.
   * The message received will determine what needs to be done by this slave.
   */
  private static void getMessagesFromMaster()
  {
    while (true)
    {
      SocketMessage sm = socket_to_master.getMessage();

      if (sm.getMessageNum() == SocketMessage.WORK_TO_SLAVE)
      {
        /* Start the thread that processes the Work list: */
        new SlaveWorker((Work) sm.getData()).start();
      }

      else if (sm.getMessageNum() == SocketMessage.REQUEST_SLAVE_STATISTICS)
      {
        if (common.get_debug(common.ORACLE))
          OracleStats.report(1000);
        CollectSlaveStats.sendStatsToMaster(socket_to_master, sm.getInfo());
        if (common.get_debug(common.PRINT_MEMORY))
        {
          common.memory_usage();
          Native.printMemoryUsage();
        }
      }

      else if (sm.getMessageNum() == SocketMessage.GET_LUN_INFO_FROM_SLAVE)
      {
        InfoFromHost.getInfoForMaster((InfoFromHost) sm.getData());
      }

      else if (sm.getMessageNum() == SocketMessage.SLAVE_GO)
      {
        if (SlaveWorker.work == null)
          common.failure("Received 'SLAVE_GO' message without first receiving Work");

        wait_to_run.release();

        /* Get starting stats to prepare for first interval: */
        if ((common.onSolaris() || common.onWindows()) &&
            SlaveJvm.isFirstSlaveOnHost())
        {
          CpuStats.getNativeCpuStats();
          CollectSlaveStats.getAllKstatData();

          if (common.onSolaris())
            NfsStats.getAllNfsDeltasFromKstat();
        }
      }

      else if (sm.getMessageNum() == SocketMessage.WORKLOAD_DONE)
      {
        SlaveJvm.setWorkloadDone(true);
        SlaveJvm.setMasterDone();
      }

      else if (sm.getMessageNum() == SocketMessage.CLEAN_SHUTDOWN_SLAVE)
      {
        break;
      }

      else if (sm.getMessageNum() == SocketMessage.MASTER_ABORTING)
      {
        master_aborting = true;

        /* Is it really worth it trying to shut down everything nicely? No! */
        common.failure(master_abort_msg);

      }

      else if (sm.getMessageNum() == SocketMessage.HEARTBEAT_MESSAGE)
      {
        SlaveJvm.sendMessageToMaster(SocketMessage.HEARTBEAT_MESSAGE);
      }


      else
        common.failure("Unknown socket message: " + sm.getMessageText());
    }

  }



  /**
   * Scan execution parameters
   */
  private static void scan_args(String args[])
  {

    Getopt g = new Getopt(args, "p:m:l:d:n:", 1);
    if (!g.isOK())
      common.failure("Parameter scan error");

    g.print("SlaveJvm");

    master_ip           = g.get_string('m');
    slave_label         = g.get_string('l');
    slave_name          = g.get_string('n');
    first_slave_on_host = slave_label.endsWith("-0");

    SlaveSocket.setMasterPort(Integer.parseInt(g.get_string('p')));

    Thread.currentThread().setName(slave_label);
  }



  /**
   * Before we can start we must make sure that the master is happy with us.
   */
  private static void signonToMaster()
  {
    /* The first that we expect is a request to sign on: */
    SocketMessage sm = socket_to_master.getMessage();

    /* The master wants to make sure that we are not talking to a stale master: */
    if (sm.getMessageNum() == sm.SEND_SIGNON_INFO_TO_MASTER)
    {
      /* OS is also determined in InfoFromHost.getInfoForMaster(), but it was */
      /* found that I will need the OS earlier. Old code has not been removed */
      String data[] = new String[4];
      data[0] = master_ip;
      data[1] = slave_name;
      data[2] = System.getProperty("os.name");
      data[3] = System.getProperty("os.arch");

      sm.setData(data);
      socket_to_master.putMessage(sm);
    }
    else
      common.failure("Unexpected message number during signon: " + sm.getMessageNum());

    /* The next one is good or bad news: */
    sm = socket_to_master.getMessage();
    if (sm.getMessageNum() == sm.KILL_SLAVE_SIGNON_ERROR)
      common.failure("Signon to master failed");

    if (sm.getMessageNum() != sm.SEND_SIGNON_SUCCESSFUL)
      common.failure("Unexpected message number during signon: " + sm.getMessageNum());

    /* Confirm that we received the successful message: */
    socket_to_master.putMessage(sm);
  }


  /**
   *
   */
  public static void main(String args[])
  {
    /* Shutdown hook to allow Vdbmain to recognize JVM shutdown: */
    Shutdown.activateShutdownHook();

    /* Let the rest of the code know that this is a slave jvm: */
    this_is_an_active_slave = true;

    try
    {
      Thread.currentThread().setPriority( Thread.MAX_PRIORITY );

      /* Get execution parameters:   */
      scan_args(args);

      /* Slaves will NOT have a logfile.html any longer, everything to stdout: */
      if (SlaveList.getSlaveCount() == 0)
      {
        common.log_html =
        common.stdout   = new PrintWriter(System.out, true);
      }


      /* Connect to the master: */
      connectToMaster();

      /* Get information from master, this causes all the work we need to do: */
      signonToMaster();

      /* For debugging purposes it is good to know how we started: */
      //if (!slave_label.startsWith("localhost"))
      //  ClassPath.reportVdbenchScript();

      /* Make sure we stay awake: */
      new HeartBeat(false).start();

      /* The master will tell us what to do. Work will be started inside */
      /* separate threads: */
      getMessagesFromMaster();

      /* When we return here, all is done: */
      //ThreadControl.shutdownAll("SlaveWork"); // in case it did not shut down itself

      //ThreadControl.printActiveThreads();

      common.memory_usage();
      VdbCount.listCounters("End of SlaveJvm");

      /* We're done: */
      if (first_slave_on_host)
        Adm_msgs.copy_varadmmsgs();
      //common.log_html.close();
      //
      //
    }


    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

    Native.free_jni_shared_memory();

    sendMessageToMaster(SocketMessage.CLEAN_SHUTDOWN_COMPLETE);

    Jnl_entry.closeAllMaps();

    /* This shutdown causes OS_cmd() to signal 'done' to master if windows used rsh: */
    if (SlaveList.getSlaveCount() == 0)
      common.exit(0);

  }


  /**
   * Wait for a signal from the master that we can start running.
   *
   * Slave sends SLAVE_READY_TO_GO to the master, and then waits for a semaphore
   * which will be released after receiving SLAVE_GO
   */
  public static void waitToGo()
  {
    try
    {
      sendMessageToMaster(SocketMessage.SLAVE_READY_TO_GO);
      wait_to_run.acquire();
    }
    catch (InterruptedException e)
    {
    }
  }

  /**
   * isWorkloadDone() is used by the slave to determine if the slave workload is
   * done, either decided by the slave itself, or by the master by sending the
   * WORKLOAD_DONE socket message.
   * When the master however sends WORKLOAD_DONE before the slave determines that
   * it is done (things like 'seq eof, format end, etc) it means that there can be
   * some things that the slave still has to do, e.g. drain the current active
   * threads. Once that is done, only then is the slave really finished.
   *
   * At that point the ControlFile can be finally written to disk.
   */
  public static void waitForMasterDone()
  {
    try
    {
      master_done_semaphore.acquire();
    }
    catch (InterruptedException e)
    {
    }
  }
  public static void setMasterDone()
  {
    master_done_semaphore.release();
  }

  public static void setMount(Mount mnt)
  {
    rd_mount = mnt;
  }
  public static Mount getMount()
  {
    return rd_mount;
  }

  public static boolean isWorkloadDone()
  {
    return workload_done;
  }
  public static void setWorkloadDone(boolean bool)
  {
    workload_done = bool;
    if (workload_done)
      workload_done_semaphore.release();

    else
      workload_done_semaphore = new Semaphore(0);
  }
  public static void waitForWorkloadDone()
  {
    try
    {
      workload_done_semaphore.acquire();
    }
    catch (InterruptedException e)
    {
    }
  }

  public static void setWdWorkload(boolean bool)
  {
    fwd_workload = bool;
  }
  public static boolean isWdWorkload()
  {
    if (!SlaveJvm.isThisSlave())
      common.failure("'isWdWorkload' for SlaveJvm requested on master");
    return fwd_workload;
  }
  public static boolean isFwdWorkload()
  {
    if (!SlaveJvm.isThisSlave())
      common.failure("'isFwdWorkload' for SlaveJvm requested on master");
    return !fwd_workload;
  }

  public static String getMasterAbortMessage()
  {
    return master_abort_msg;
  }
  public static String getSlaveLabel()
  {
    return slave_label;
  }
}
