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
import java.io.*;
import java.text.*;
import Utils.ClassPath;
import Utils.Format;

/**
 * This class contains code that keeps track of what slaves we have
 * and in what status they are.
 */
public class SlaveList
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private static Vector slave_list = new Vector(64, 0);


  public static void addSlave(Slave slave)
  {
    slave_list.add(slave);
  }


  /**
   * Find the Slave instance for this specific slave
   */
  public static Slave findSlaveName(String n)
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getName().equals(n))
        return slave;
    }
    return null;
  }
  public static Slave findSlave(String n)
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getLabel().equals(n))
        return slave;
    }
    common.failure("findSlave() Unable to find slave: " + n);
    return null;
  }

  public static int getSlaveCount()
  {
    return slave_list.size();
  }
  public static Vector getSlaveList()
  {
    return slave_list;
  }

  public static String[] getSlaveNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      names.put(slave.getLabel(), slave);
    }

    return (String[]) names.keySet().toArray(new String[0]);
  }




  /**
   * Close all sockets
   */
  public static void closeAllSlaveSockets()
  {
    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      SlaveSocket ss = slave.getSocket();
      common.plog("close socket to " + slave.getLabel());
      ss.close();
    }
  }



  /**
   * Tell all slaves to shut down
   */
  public static void shutdownAllSlaves()
  {
    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      SlaveSocket ss = slave.getSocket();

      /* Remember we did this so that we can accept a socket failure: */
      slave.set_may_terminate();
      ss.setShutdown(true);

      SocketMessage sm = new SocketMessage(SocketMessage.CLEAN_SHUTDOWN_SLAVE);
      ss.putMessage(sm);

      //slave.setSocket(null);
    }
  }



  /**
   * Tell all slaves that the current work is done.
   */
  public static void sendWorkloadDone()
  {
    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      if (slave.getCurrentWork() == null)
        continue;

      SlaveSocket ss = slave.getSocket();

      SocketMessage sm = new SocketMessage(SocketMessage.WORKLOAD_DONE);
      ss.putMessage(sm);
    }
  }



  /**
   * Check to see if all sequential workloads are done
   */
  public static boolean allSequentialDone()
  {
    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      //common.ptod("slave: " + slave.getLabel() + " " + slave.isSequentialDone());
      if (!slave.isSequentialDone())
        return false;
    }

    return true;
  }



  /**
   * Tell all slaves to go away
   */
  public static void killSlaves()
  {
    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      /* If this slave has aborted already, don't bother: */
      if (slave.isAborted())
        continue;

      common.ptod("Slave " + slave.getLabel() + " killed by master");
      slave.setAborted("Killed by master");

      /* Send a message to the slave only if we already have a socket: */
      SlaveSocket ss = slave.getSocket();
      if (ss != null)
      {
        /* Send SD list to slave: */
        SocketMessage sm = new SocketMessage(SocketMessage.MASTER_ABORTING);
        ss.putMessage(sm);

        /* Remember we did this so that we can accept a socket failure: */
        slave.set_may_terminate();
        ss.setShutdown(true);
      }

      else
      {
        /* Remember we did this so that we can accept a socket failure: */
        slave.set_may_terminate();
      }
    }
  }


  public static void allDead()
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      //common.ptod("slave: " + slave.getJVMLabel() + " " +
      //            slave.isAborted() + " " + slave.hasTerminated());

      /* If this slave is still running, exit */
      if (!slave.hasTerminated())
        return;
      if (!slave.isAborted())
        return;
    }

    common.failure("All slaves have aborted. Run cancelled");

  }



  /**
   * Wait until all slaves report 'complete'
   */
  public static void waitForSlaveWorkCompletion()
  {
    boolean waiting;
    Slave slave = null;
    Signal signal = new Signal(30);

    do
    {
      waiting = false;
      for (int i = 0; i < slave_list.size(); i++)
      {
        slave = (Slave) slave_list.elementAt(i);
        //if (slave.getCurrentWork() != null && !slave.isWorkDone() && !slave.isReadyForMore())
        if (slave.getCurrentWork() != null && !slave.isReadyForMore())
        {
          waiting = true;
          break;
        }
      }

      if (waiting)
      {
        common.sleep_some(100);
        if (signal.go())
          common.ptod("SlaveList.waitForSlaveWorkCompletion(): " + slave.getLabel());
      }

    } while (waiting);
  }



  /**
   * Check to see if all slaves are done
   */
  public static boolean allSlaveWorkDone()
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getCurrentWork() != null && !slave.isWorkDone())
        return false;
    }

    return true;
  }


  public static void waitForAllSlavesShutdown()
  {
    Signal signal = new Signal(5);

    while (true)
    {
      boolean waiting = false;
      for (int i = 0; i < slave_list.size(); i++)
      {
        Slave slave = (Slave) slave_list.elementAt(i);
        if (!slave.isShutdown())
        {
          waiting = true;
          common.sleep_some(100);
          if (signal.go())
            common.ptod("Waiting for slave shutdown: " + slave.getLabel());
        }
      }

      if (!waiting)
        return;
    }
  }



  /**
   * Start each slave that has been defined.
   */
  public static void startSlaves()
  {
    // I bet that multi jvm will work for WD workloads also,
    // but it needs to be tested
    if (Validate.isValidate() && Vdbmain.isWdWorkload())
    {
      if (Host.getDefinedHosts().size() > 1 ||
          !((Slave) slave_list.firstElement()).isLocalHost() ||
          slave_list.size() > 1)
        common.failure("Data Validation may only run on the local host "+
                       "with only a single JVM");
    }


    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave     = (Slave) slave_list.elementAt(i);
      SlaveStarter ss = new SlaveStarter();
      slave.setSlaveStarter(ss);
      ss.setSlave(slave);
      ss.start();
    }
  }


  /**
   * This was created because using ssh from windows to solaris did not
   * appear to complete the OS_cmd()
   */
  public static void stopSlaveStarters()
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      SlaveStarter ss = slave.getSlaveStarter();
      ss.interrupt();
    }
  }


  /**
   * Wait for all slaves to be connected.
   */
  public static boolean waitForConnections()
  {
    while (true)
    {
      boolean missing = false;
      for (int i = 0; i < slave_list.size(); i++)
      {
        Slave slave = (Slave) slave_list.elementAt(i);
        if (!slave.isConnected())
          missing = true;

        /* If we have aborted, just exit: */
        //if (slave.isAborted())
        //  return true;
      }

      if (missing)
        return false;
      else
      {
        common.ptod("All slaves are now connected");
        return true;
      }
    }
  }



  /**
   * Tell user which slaves we're waiting for
   */
  public static void displayConnectWait()
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (!slave.isConnected())
        common.ptod("Waiting for slave connection: " + slave.getLabel());
    }
  }


  /**
   *  Add a list of FwgEntry() instances to the slave
   */
  public static void AddFwgsToSlave(Slave slave, Vector fwgs, RD_entry rd, boolean run)
  {
    Work work = slave.getCurrentWork();
    if (work == null)
    {
      work = new Work();
      slave.setCurrentWork(work);
      work.validate_options   = Validate.getOptions();
      work.fwd_rate           = rd.fwd_rate;
      work.pattern_dir        = DV_map.pattern_dir;
      work.format_run         = rd.isThisFormatRun();
      work.format_flags       = rd.format;
      work.force_fsd_cleanup  = Vdbmain.force_fsd_cleanup;
      work.maximum_xfersize   = FileAnchor.getLargestXfersize();
      Validate.setCompression(-1);
      work.distribution       = rd.distribution;
      work.fwgs_for_slave     = new Vector(8, 0);
      work.rd_start_command   = rd.start_cmd;
      work.rd_end_command     = rd.end_cmd;
      work.work_rd_name       = rd.rd_name + " " + rd.current_override.getText();
      work.rd_mount           = rd.rd_mount;
      work.bucket_types       = BucketRanges.getBucketTypes();
    }

    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);

      /* For shared FSDs this list may contains FWGs for a different host: */
      if (fwg.host_name.equals(slave.getHost().getLabel()))
      {
        work.fwgs_for_slave.add(fwg);

        if (run)
          common.plog(Format.f("Sending to %-12s ",  slave.getLabel()) +
                      work.work_rd_name +
                      " anchor=" + fwg.anchor.getAnchorName());
      }
    }


    /* Create a list of Kstat instance names that a slave has to return data for: */
    if (slave.equals(slave.getHost().getFirstSlave()) &&
        slave.getHost().getHostInfo().isSolaris())
      work.instance_pointers = slave.getHost().getHostInfo().getInstancePointers();
  }


  /**
  * The fwd_rate entered represents a TOTAL and must be divided by the
  * number of slaves we have
  */
  public static void adjustSkew()
  {
    /* Count the amount of slaves used: */
    int slaves = 0;
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getCurrentWork() != null)
        slaves++;
    }

    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getCurrentWork() != null)
        slave.getCurrentWork().fwd_rate /= slaves;
    }
  }


  public static String hostsMustmatch(FileAnchor anchor, Vector fwgs_for_anchor)
  {
    FwgEntry fwg0 = (FwgEntry) fwgs_for_anchor.firstElement();
    for (int i = 0; i < fwgs_for_anchor.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_anchor.elementAt(i);

      if (!fwg0.host_name.equals(fwg.host_name))
      {
        common.ptod("fwg0.host_name: " + fwg0.host_name);
        common.ptod("fwg0.host_name: " + fwg0.host_name);
        common.ptod("fwg0:           " + fwg0.getName());
        common.ptod("fwg:            " + fwg.getName());
        common.failure("All fwd= workloads using anchor=" +
                       anchor.getAnchorName() + " must target the same host");
      }
    }

    return fwg0.host_name;
  }


  public static void sendWorkToSlaves(RD_entry rd)
  {
    int count = 0;

    /* Send to the slave: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      /* If there is no work now for this slave, no message: */
      if (slave.getCurrentWork() == null)
        continue;

      /* Slave needs yo know about the others: */
      Work work         = slave.getCurrentWork();
      work.slave_count  = slave_list.size();
      work.slave_number = slave.getSlaveNumber();

      SocketMessage sm = new SocketMessage(SocketMessage.WORK_TO_SLAVE, work);
      slave.setWorkDone(false);
      slave.setReadyToGo(false);
      slave.setReadyForMore(false);
      slave.getSocket().putMessage(sm);
      count ++;
    }

    if (count == 0)
      common.failure("sendWorkToSlaves(): no work sent to any slave for rd=" + rd.rd_name);
  }


  public static void printWorkForSlaves(RD_entry rd)
  {
    if (Vdbmain.isWdWorkload())
    {
      common.plog("SlaveList.printWorkForSlaves() " +
                  (RD_entry.next_rd.use_waiter ? "" : "*"));

      for (int i = 0; i < slave_list.size(); i++)
      {
        Slave slave = (Slave) slave_list.elementAt(i);

        if (slave.getCurrentWork() != null)
        {
          Work work = slave.getCurrentWork();
          for (int k = 0; k < work.wgs_for_slave.size(); k++)
          {
            //common.ptod("slave: " + slave.getLabel());
            common.plog(((WG_entry) work.wgs_for_slave.elementAt(k)).report(rd));
          }
        }
      }
      common.plog("");

    }
  }



  /**
   * Wait for all slaves to be ready to go.
   * If not, display a message every n seconds
   */
  public static void waitForSlavesReadyToGo()
  {
    int  sleepy_time = (common.get_debug(common.FAST_SYNCTIME)) ? 1 : 30;
    Signal signal = new Signal(sleepy_time);

    while (true)
    {
      boolean waiting = false;
      for (int i = 0; i < slave_list.size(); i++)
      {
        Slave slave = (Slave) slave_list.elementAt(i);
        if (slave.getCurrentWork() != null && !slave.isReadyToGo())
        {
          waiting = true;
          common.sleep_some(100);
          if (signal.go())
          {
            String txt = "Waiting for slave synchronization: " + slave.getLabel();
            if (slave.getStructurePending())
              txt += ". Building and validating file structure(s).";
            common.ptod(txt);
          }
        }
      }

      if (!waiting)
        return;
    }
  }

  public static void tellSlavesToGo()
  {
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);
      if (slave.getCurrentWork() != null)
        slave.getSocket().putMessage(new SocketMessage(SocketMessage.SLAVE_GO));
    }
  }


  /**
   * Decide whether we can bypass starting a jVM and run only locally.
   *
   * Note: doing this means that the vdbench script must have enough memory
   * to run a huge workload!
   * And I therefore maybe should not do it once going live!
   *
   * Then of course, we can run the startup script with as little memory
   * as possible, and enter real heap requirements in the parmfile.
   * I'll likely opt to always have the script specify the heap sizes.
   * User just must have enough virtual memory/swap space
   *
   * Of course, we can have two separate scripts:
   * - one for starting of Vdbench and the Gui
   * - one for SlaveJvm.
   * This means I would have to change the install process to cover having
   * two scripts.
   *
   * I have just one script, but the script reacts to the first argument
   * being 'SlaveJvm'.
   *
   * Until final decision is made, never inside of master.
   */
  public static boolean runSlaveInsideMaster()
  {

    if (common.get_debug(common.LOCAL_JVM))
      return true;

    // if (common.get_debug(common.NO_LOCAL_JVM))
    //   return false;
    //
    // if (getSlaveCount() > 1)
    //   return false;
    //
    // /* A single local slave can start locally: */
    // if (((Slave) SlaveList.getSlaveList().firstElement()).isLocalHost())
    //   return true;
    //
    // return false;
    return false;

  }
}






