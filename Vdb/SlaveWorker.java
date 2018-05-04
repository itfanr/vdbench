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
 * This class does the actual current_work.
 * It runs a slave, and receives his list of Work() instances from the master.
 */
public class SlaveWorker extends ThreadControl
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /* These fields are allowed to be static because there is going to be only */
  /* one active instance of SlaveWorker on any JVM                           */
  public  static Work   work;
  public  static Vector sd_list;
  private static Adm_msgs adm_message_scanner = null;
  private static Vector list_of_io_tasks = new Vector(63, 0);
  public  static long   first_tod;



  public SlaveWorker(Work wrk)
  {
    work = wrk;
    BucketRanges.setBucketTypes(work.bucket_types);
    setName("SlaveWorker");

    SlaveJvm.setWdWorkload(work.wgs_for_slave != null);

    /* Pick up information for this workload: */
    Validate.storeOptions(work.validate_options);
    if (SlaveJvm.isWdWorkload())
    {
      sd_list = work.convertWgListToSdList();
      SlaveJvm.setReplay(work.replay, work.replay_filename);
      ReplayDevice.setDeviceList(work.replay_device_list);
    }

    /* The first worker must start the adm scanner: */
    if (SlaveJvm.isFirstSlaveOnHost() && common.onSolaris() && adm_message_scanner == null)
      (adm_message_scanner = new Adm_msgs()).start();
  }

  public static boolean isAdmRunning()
  {
    return adm_message_scanner != null;
  }


  public void run()
  {
    common.ptod("Beginning of run setup");
    common.ptod("**********************\n\n");

    try
    {
      long tod;


      // debug
      for (int j = 0; work.fwgs_for_slave != null && j < work.fwgs_for_slave.size(); j++)
      {
        FwgEntry fwg = (FwgEntry) work.fwgs_for_slave.elementAt(j);
        //common.ptod("worker fwg: " + fwg.anchor.getAnchorName() + " " + fwg.anchor.exist());
      }

      this.setIndependent();

      /* Reporting and communication between master and slaves is very */
      /* important, so we'll run the real work with the lowest prio:   */
      /* (If we're out of cycles it is a mess anyway, so this is fine) */
      Thread.currentThread().setPriority( Thread.currentThread().MIN_PRIORITY);

      SlaveJvm.setWorkloadDone(false);

      /* Issue possible start command: */
      if (SlaveJvm.isFirstSlaveOnHost())
        work.rd_start_command.run_command();

      // Experiment
      if (false && common.onSolaris())
      {
        Devxlate.clearMnttab();
        String[] mnt = Devxlate.get_mnttab("/ar_system1/small_fsd", false);
        for (int i = 0; mnt != null && i < mnt.length; i++)
          common.ptod("mnt: " + mnt[i]);
        //System.exit(999);
      }

      /* Modify mount command and remount if needed: */
      if (work.rd_mount != null && SlaveJvm.getMount() != null)
        SlaveJvm.getMount().mountIfNeeded(work.rd_mount);


      /* Start of a run. For NFS mounted devices an instance can have changed */
      /* because of a remounts. Rebuild:                                      */
      /* (This must always run in case a previous mount was done              */
      /* because the info collected here is not passed back to the master)    */
      // need to find out why I still have native pointer with -d17???
      if (work.instance_pointers != null && !common.get_debug(common.NO_KSTAT))
        InstancePointer.rebuildNativePointers(work.instance_pointers);

      if (work.wgs_for_slave != null)
        doRegularWorkload();
      else
        doFileSystemWorkload();

      common.memory_usage();
      Native.printMemoryUsage();

      /* We're done. Tell master about that: */
      SlaveJvm.sendMessageToMaster(SocketMessage.SLAVE_WORK_COMPLETED);

      /* Wait for acknowledgment from the master: */
      SlaveJvm.waitForMasterDone();
      if (work.wgs_for_slave == null)
        ControlFile.writeAllControlFiles();

      /* Issue possible end command: */
      if (SlaveJvm.isFirstSlaveOnHost())
        work.rd_end_command.run_command();

      SlaveJvm.sendMessageToMaster(SocketMessage.READY_FOR_MORE_WORK);
      common.ptod("End of run");
      common.ptod("**********\n\n");

      this.removeIndependent();
    }

    /* This catches any other exceptions: */
    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }



  /**
   * Execute a normal SD/WD workload
   */
  public void doRegularWorkload()
  {
    /* Workloads must be properly numbered to allow proper identification by JNI code */
    WG_entry.setWorkloadNumbers(work.wgs_for_slave);

    /* Pick up openflags= from WD=: */
    WG_entry.overrideOpenflags(work.wgs_for_slave);

    /* Allocate Cmd table: */
    Cmd_entry.cmd_create_pool();

    /* Start or recover journal if requested.  */
    Jnl_entry.recoverSDJournalsIfNeeded(sd_list);

    /* Data validation may need some tables: */
    DV_map.allocateSDMapsIfNeeded(sd_list);

    /* Allocate fifos: */
    WG_entry.alloc_fifos(work);

    /* Open all files: */
    SD_entry.sd_open_all_files(sd_list);

    /* With journaling, dump all maps, new and old recovered maps: */
    Jnl_entry.dumpAllMaps();

    /* Pass context to jni: */
    WG_context.setup_jni_context(work.wgs_for_slave);

    /* Start Workload Generators: */
    WG_entry.wg_start_sun_tasks(work.wgs_for_slave);

    /* Start Waiter task(s): */
    common.plog("work.use_waiter: " + work.use_waiter);
    if (work.use_waiter)
      new WT_task(new Task_num("WT_task")).start();

    DV_map.create_patterns((int) work.maximum_xfersize);

    /* Start IO tasks: */
    StartIoThreads(work);

    /* All tasks are started. Synchronize them all and then make them begin: */
    Task_num.task_wait_start_complete();

    /* Wait for permission to run: */
    SlaveJvm.waitToGo();

    /* Tell WG_task threads to go ahead and start working: */
    Task_num.task_run_all();

    /* Now wait for the 'workload done' signal: */
    SlaveJvm.waitForWorkloadDone();

    Task_num.interrupt_tasks("LogWriter");
    Task_num.interrupt_tasks("Oracle");
    Task_num.interrupt_tasks("WG_task");
    Task_num.interrupt_tasks("WT_task");
    //Task_num.interrupt_tasks("IO_task");
    Task_num.interrupt_tasks("ReplayRun");
    sendInterrupts();

    int timed_out = Task_num.task_wait_all();
    if (timed_out > 0)
      common.failure("Shutdown took more than " + timed_out + " minutes; Run aborted");
    Task_num.task_list_clear();

    SD_entry.sd_close_all_files(sd_list);

    /* Cleanup maps and journals: */
    DV_map.dv_set_all_unbusy(sd_list);

    Jnl_entry.dumpAllMaps();
    DV_map.printCounters();

    WG_entry.free_fifos(work);
  }


  public void doFileSystemWorkload()
  {
    Native.alloc_jni_shared_memory(false, common.get_shared_lib());
    DV_map.create_patterns((int) work.maximum_xfersize);

    FwgRun.startFwg(work);

    /* All tasks are started. Synchronize them all and then make them begin: */
    Task_num.task_wait_start_complete();

    /* Wait for permission to run: */
    SlaveJvm.waitToGo();

    Task_num.task_run_all();

    /* Now wait for all tasks to complete: */
    SlaveJvm.waitForWorkloadDone();
    Task_num.interrupt_tasks("FwgWaiter");

    /* May never be done. See comments in Task_num.interrupt_tasks() */
    //Task_num.interrupt_tasks("FwgThread");

    int timed_out = Task_num.task_wait_all();
    if (timed_out > 0)
      common.failure("Shutdown took more than " + timed_out + " minutes; Run aborted");
    Task_num.task_list_clear();

    /* Dump all possible journals */
    Jnl_entry.dumpAllMaps();

    /* Report the final counters. The counters reported on logfile do NOT */
    /* include the work done between the returning of counters for the */
    /* last interval and the run shutdown: */
    Blocked.printCountersToLog();

    DV_map.printCounters();
    FwgWaiter.clearStuff();
  }


  /**
  * Start an IO_task for each SD, for each thread requested .
  */
  static void StartIoThreads(Work work)
  {
    list_of_io_tasks.removeAllElements();

    /* For each active SD entry, create one IO entry per thread: */
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);

      /* Create one IO_task per thread requested: */
      for (int j = 0; j < work.getThreadsUsed(sd); j++)
      {
        IO_task task = new IO_task(new Task_num("IO_task " + sd.lun), sd, j);
        list_of_io_tasks.add(task);
        task.start();
        //common.ptod("list_of_io_tasks.size(): " + list_of_io_tasks.size());

        /* Every n tasks sleep a little to avoid having too many tasks */
        /* in startup at the same time:                                 */
        int sleep_at = 32;
        if (i % sleep_at == sleep_at - 1)
          common.sleep_some(10);
      }
      common.ptod("Started " +  work.getThreadsUsed(sd) + " i/o threads for " + sd.sd_name);
    }

    common.plog("Started a total of " + list_of_io_tasks.size() + " i/o threads");
  }

  /**
   * In the past interrupts were sent using Task_num.interrupt_tasks().
   * Starting with Solaris 11, CIFS, (or maybe earlier but we never ran into
   * this writing against file system files), an outstanding i/o that was
   * interrupted would return an ENOENT error status.
   * To avoid this problem we now ONLY send an interrupt to the threads
   * that do NOT have an i/o active.
   * The threads that are waiting in the FIFO will continue to receive an
   * interrupt.
   *
   * Note: there is no need for synchroniztion here. In IO_task the count
   * is only increased AFTER we check for workload_done which has just been
   * set.
   */
  private static void sendInterrupts()
  {
    int sent = 0;
    for (int i = 0; i < list_of_io_tasks.size(); i++)
    {
      IO_task task = (IO_task) list_of_io_tasks.elementAt(i);
      if (task.getCurrentMultiCount() == 0)
      {
        task.interrupt();
        sent++;
      }
    }

    common.ptod("Sent " + sent + "/" + list_of_io_tasks.size() +
                " interrupts to waiting IO_task threads");
  }

  public static int getSequentialCount()
  {
    return work.sequential_files;
  }
  public static void setSequentialCount(int count)
  {
    work.sequential_files = count;
  }

  public static SD_entry findSd(String name)
  {
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);
      if (sd.sd_name.equals(name))
        return sd;
    }
    common.failure("Unable to find sd=" + name);
    return null;
  }


  public static boolean canWeExpectFileDeletes()
  {
    if (work.format_run)
      return false;

    Vector fwgs = work.fwgs_for_slave;

    /* Go through all FWGs looking for file deletes: */
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (fwg.getOperation() == Operations.DELETE)
        return true;
    }

    return false;
  }
  public static boolean canWeExpectFileCreates()
  {
    Vector fwgs = work.fwgs_for_slave;

    /* Go through all FWGs looking for file deletes: */
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (fwg.getOperation() == Operations.CREATE)
        return true;
    }

    return false;
  }

  public static boolean canWeExpectDirectoryCreates()
  {
    Vector fwgs = work.fwgs_for_slave;

    /* Go through all FWGs looking for file deletes: */
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (fwg.getOperation() == Operations.MKDIR || work.format_run)
        return true;
    }

    return false;
  }

  public static boolean canWeExpectDirectoryDeletes()
  {
    /* During the 'format' of a run that has RMDIR included the RMDIR shows */
    /* up in the fwgs_for_slave list. Though it would be cleaner to not     */
    /* have the RMDIR in there, it was easier to bypass this check and      */
    /* always return false                                                  */
    if (work.format_run)
      return false;

    Vector fwgs = work.fwgs_for_slave;

    /* Go through all FWGs looking for file deletes: */
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (fwg.getOperation() == Operations.RMDIR && !fwg.getShutdown())
        return true;
    }

    return false;
  }


  /**
   *  Data Validation allows (for now?) only one fixed xfersize in a run
   */
  public static int obsolete_getDataValidationXfersize()
  {
    Vector fwgs = work.fwgs_for_slave;

    /* Go through all FWGs looking for single transfersizes */
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (fwg.xfersizes.length != 1)
      {
        String msg = "Data Validation allows only one fixed xfersize";
        SlaveJvm.sendMessageToConsole(msg);
        common.failure(msg);
      }
    }

    /* Now compare the transfer sizes: */
    double xfersize = -1;
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      if (xfersize == -1)
        xfersize = fwg.xfersizes[0];
      else if (fwg.xfersizes[0] != xfersize)
      {
        String msg = "Data Validation allows only one fixed xfersize";
        SlaveJvm.sendMessageToConsole(msg);
        common.failure(msg);
      }
    }

    return(int) xfersize;
  }
}
