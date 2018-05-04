package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.text.*;
import java.util.*;

import Utils.*;

/**
 * This class contains code that keeps track of what slaves we have
 * and in what status they are.
 */
public class SlaveList
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static Vector <Slave> slave_list = new Vector(64, 0);
  public  static boolean        shutdown_requested = false;
  private static int            max_label_length   = 0;
  private static String         label_mask         = "%s";

  public static void addSlave(Slave slave)
  {
    slave_list.add(slave);
    max_label_length = Math.max(max_label_length, slave.getLabel().length());
    label_mask = "%-" + max_label_length + "s";
  }

  public static String getLabelMask()
  {
    return label_mask;
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
  public static Slave findSlaveLabel(String n)
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
  public static Vector <Slave> getSlaveList()
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

    return(String[]) names.keySet().toArray(new String[0]);
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
    Status.printStatus("Shutting down slaves", null);
    shutdown_requested = true;

    /* Tell slaves to shut down cleanly: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      SlaveSocket ss = slave.getSocket();

      /* Remember we did this so that we can accept a socket failure: */
      slave.set_may_terminate();
      if (ss != null)
      {
        ss.setShutdown(true);

        SocketMessage sm = new SocketMessage(SocketMessage.CLEAN_SHUTDOWN_SLAVE);
        ss.putMessage(sm);
      }

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
    /* If there are no sequential workloads whatsoever, then return FALSE. */
    boolean any_sequentials = false;
    for (Slave slave : slave_list)
    {
      if (slave.sequentialFilesOnSlave() > 0)
        any_sequentials = true;
    }

    if (!any_sequentials)
      return false;


    /* If any of the slaves that have sequential works is not done yet, return FALSE: */
    for (Slave slave : slave_list)
    {
      if (slave.sequentialFilesOnSlave() > 0 && !slave.isSequentialDone())
        return false;
    }

    /* Yes, we have sequentials and we're all done: */
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
    Signal too_long_signal = new Signal(300);
    Signal message_signal  = new Signal(5);

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
          if (message_signal.go())
            common.ptod("Waiting for slave shutdown: " + slave.getLabel());

          if (too_long_signal.go())
            common.failure("Waited %d seconds for all slaves to shut down. Giving up.",
                           too_long_signal.getDuration());
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
    String[] slave_names = getSlaveNames();
    Arrays.sort(slave_names);
    for (String name : slave_names)
    {
      Slave slave     = findSlaveLabel(name);
      SlaveStarter ss = new SlaveStarter();
      slave.setSlaveStarter(ss);
      ss.setSlave(slave);
      ss.start();

      /* Just a little sleepy time to allow the slaves to get started */
      /* in sequence, even though they really run async:              */
      /* This is for reporting and debugging only.                    */
      common.sleep_some(20);
    }

    Status.printStatus("Starting slaves", null);
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
      common.interruptThread(ss);
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
      work.fwd_rate           = rd.fwd_rate;
      work.format_run         = rd.isThisFormatRun();
      work.format_flags       = rd.format;
      work.force_fsd_cleanup  = Vdbmain.force_fsd_cleanup;
      work.maximum_xfersize   = FileAnchor.getLargestXfersize();
      work.validate_options   = Validate.getOptions();
      work.pattern_options    = Patterns.getOptions();
      Validate.setCompressionRatio(rd.compression_ratio_to_use);
      work.distribution       = rd.distribution;
      work.fwgs_for_slave     = new Vector(8, 0);
      work.rd_start_command   = rd.start_cmd;
      work.rd_end_command     = rd.end_cmd;
      work.work_rd_name       = rd.rd_name + " " + rd.current_override.getText();
      work.rd_mount           = rd.rd_mount;
      work.bucket_types       = BucketRanges.getBucketTypes();
      work.miscellaneous      = MiscParms.getMiscellaneous();
      //if (work.miscellaneous.size() == 0)
      //  common.failure("debugging");
    }

    String slave_mask = "slv=%-" + Slave.max_slave_name  + "s ";
    String fwd_mask   = "fwd=%-" + FwdEntry.max_fwd_name + "s ";
    String fsd_mask   = "fsd=%-" + FsdEntry.max_fsd_name + "s ";

    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);

      /* For shared FSDs this list may contains FWGs for a different host: */
      if (fwg.host_name.equals(slave.getHost().getLabel()))
      {
        work.fwgs_for_slave.add(fwg);

        if (run) //  && common.get_debug(common.DETAIL_SLV_REPORT))
          common.plog(slave_mask + fwd_mask + fsd_mask +
                      "anchor=%s threads=%2d skew=%5.2f operation=%s ",
                      slave.getLabel(),
                      fwg.fwd_used.fwd_name,
                      fwg.fsd_name,
                      fwg.anchor.getAnchorName(),
                      fwg.threads, fwg.skew,
                      Operations.getOperationText(fwg.getOperation()));
      }
    }

    /* Set a flag that allows an old control file to be preserved: */
    if (work != null)
    {
      if (work.format_run)
        work.keep_controlfile = false;
      else
        work.keep_controlfile = Operations.keepControlFile(work.fwgs_for_slave);
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
    WhereWhatWork.checkWorkForSlave0(rd);
    WhereWhatWork.paranoiaDvCheck();
    WhereWhatWork.rememberWhereDvWent();

    int count = 0;

    /* Send to the slave: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      /* If there is no work now for this slave, no message: */
      if (slave.getCurrentWork() == null)
        continue;

      // Note:if there are inactive slaves we have a real problem with slave_count below!

      /* Slave needs yo know about the others: */
      Work work         = slave.getCurrentWork();
      work.slave_count  = slave_list.size();
      work.slave_number = slave.getSlaveNumber();

      /* We are (possibly) sending out concatenated SDs here. */
      /* If we send the IDENTICAL SD out, then we send out the CURRENT lun name */
      /* to each host. ConcatMarkers() may have changed the lun name however. */
      /* We therefore must set the proper lun name just before the SD_entry */
      /* is serialized */
      /* .... */

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


  public static int countSlavesWithWork(RD_entry rd)
  {
    int count = 0;

    /* Send to the slave: */
    for (int i = 0; i < slave_list.size(); i++)
    {
      Slave slave = (Slave) slave_list.elementAt(i);

      /* If there is no work now for this slave, no message: */
      if (slave.getCurrentWork() == null)
        continue;

      count ++;
    }

    return count;
  }




  /**
   * Wait for all slaves to be ready to go.
   * If not, display a message every n seconds
   */
  public static void waitForSlavesReadyToGo()
  {
    int  sleepy_time = (common.get_debug(common.FAST_SYNCTIME)) ? 1 : 30;
    Signal signal    = new Signal(sleepy_time);

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

  /**
   * All slaves are synched, or is that psyched?
   */
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
   * An extra option, allowing artificial synching of concurrent independent
   * Vdbench executions.
   */
  public static void externalSynchronize()
  {
    String SYNC_BASE = "external_synch.";
    String SYNC_NAME = "external_synch.txt";


    if (common.get_debug(common.EXTERNAL_SYNCH))
    {
      /* Create my own sync file: */
      Fput fp = new Fput(SYNC_BASE + Native.getSolarisPids());
      fp.close();

      /* I now just wait for 'n' files to exist: */
      common.pboth("Waiting for external synchronization.");


      Signal signal = new Signal(15*60);
      while (true)
      {
        if (signal.go())
          common.failure("Someone left Vdbench out to dry when using the " +
                         "'EXTERNAL_SYNCH' option, which has a timeout of %d seconds",
                         signal.getDuration());

        if (new File(SYNC_NAME).exists())
        {
          common.pboth("External synchronization complete.");
          break;
        }
        common.sleep_some(100);
      }
    }
  }
}






