package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

import User.UserDeviceInfo;

import Utils.Fput;
import Utils.OS_cmd;


/**
 * This class does the actual current_work.
 * It runs a slave, and receives his list of Work() instances from the master.
 */
public class SlaveWorker extends ThreadControl
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /* These fields are allowed to be static because there is going to be only */
  /* one active instance of SlaveWorker on any JVM                           */
  public  static Work   work;
  public  static Vector <SD_entry> sd_list;
  private static Adm_msgs adm_message_scanner = null;
  private static Vector <IO_task> list_of_io_tasks = new Vector(63, 0);
  public  static long   first_tod;



  public SlaveWorker(Work wrk)
  {
    work = wrk;
    BucketRanges.setBucketTypes(work.bucket_types);
    setName("SlaveWorker");

    ThreadMonitor.clear();

    SlaveJvm.setWdWorkload(work.wgs_for_slave != null);

    /* Pick up information for this workload: */
    Validate.storeOptions(work.validate_options);
    Patterns.storeOptions(work.pattern_options);
    if (SlaveJvm.isWdWorkload())
    {
      sd_list = work.convertWgListToSdList();
      ReplayInfo.setInfo(work.replay_info);
    }

    /* Cope ALL MISC parms here: */
    MiscParms.setMiscellaneous(work.miscellaneous);

    /* The first worker must start the adm scanner: */
    Adm_msgs.parseMessageOptions();
    if (Adm_msgs.scanMessages())
    {
      if (SlaveJvm.isFirstSlaveOnHost() && common.onSolaris() && adm_message_scanner == null)
        (adm_message_scanner = new Adm_msgs()).start();
      else if (SlaveJvm.isFirstSlaveOnHost() && common.onLinux() && adm_message_scanner == null)
        (adm_message_scanner = new Adm_msgs()).start();
    }

  }

  public static boolean isAdmRunning()
  {
    return adm_message_scanner != null;
  }

  public static boolean sharedThreads()
  {
    return Validate.sdConcatenation() && work.threads_from_rd != 0;
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
    /* Create indexes to allow JNI to accumulate statistics: */
    JniIndex.createIndexes(work);

    /* Pick up openflags= from WD=: */
    WG_entry.overrideOpenflags(work.wgs_for_slave);

    /* Allocate Cmd table: */
    Cmd_entry.cmd_create_pool();

    /* Start or recover journal if requested.  */
    Jnl_entry.recoverSDJournalsIfNeeded(sd_list);

    //if (Validate.isJournalRecovery())
    //{
    //  common.where(8);
    //  return;
    //}

    /* Data validation may need some tables: */
    if (Validate.isValidate())
    {
      DV_map.allocateSDMaps(sd_list);

      /* Some stuff may only exist on the slave, too expensive to pass it from the master: */
      if (Validate.isDedup())
        Dedup.slaveLevelSetup(sd_list);
    }

    /* Allocate fifos: */
    WG_entry.alloc_fifos(work);

    /* Open all files: */
    SD_entry.openAllSds();

    /* With journaling, dump all maps, new and old recovered maps: */

    /* This should not be done now, is should be done after the complete journal      */
    /* re-read has been done. (Unless that is skipped? That's OK too since a re-read  */
    /* is still started, but it only stops after just ONE read.                       */
    /* The benefit of postponing this is that if there are problems during the        */
    /* re-read we still have an in-tact journal.                                      */
    /*                                                                                */
    /* There is an other very important reason:                                       */
    /* If the VerifyPending code decides to change a key to a previous value because  */
    /* the write was never done, the journal ai this time contains the wrong key.     */
    /* If the run gets killed before the journal is rewritten, this wrong key         */
    /* will later on report a false corruption.                                       */
    /* The maps therefore should only be rewritten at the end of rd=journal_recovery. */
    if (!work.work_rd_name.startsWith(Jnl_entry.RECOVERY_RUN_NAME))
      Jnl_entry.dumpAllMaps(false);

    /* Pass context to jni: */
    WG_context.setup_jni_context(work.wgs_for_slave);

    /* For UserClass code, clear all devices because we're starting from scratch: */
    UserDeviceInfo.clearDeviceList();

    /* Start Workload Generators: */
    WG_entry.wg_start_sun_tasks(work.wgs_for_slave);

    /* Start Waiter task(s): */
    //common.plog("work.use_waiter: " + work.use_waiter);
    if (work.use_waiter)
      new WT_task(new Task_num("WT_task")).start();

    Patterns.createPattern((int) work.maximum_xfersize);

    /* Start IO tasks: */
    StartIoThreads(work);


    /* All tasks are started. Synchronize them all and then make them begin: */
    Task_num.task_wait_start_complete();

    if (common.onSolaris())
      bindCpus();

    /* Wait for permission to run: */
    SlaveJvm.waitToGo();

    /* Tell WG_task threads to go ahead and start working: */
    ShowLba.openTrace();
    Task_num.task_run_all();

    /* Now wait for the 'workload done' signal: */
    SlaveJvm.waitForWorkloadDone();

    Task_num.interrupt_tasks("WG_task");
    Task_num.interrupt_tasks("WT_task");
    sendInterrupts();

    int timed_out = Task_num.task_wait_all();
    if (timed_out > 0)
      common.failure("Shutdown took more than " + timed_out + " minutes; Run aborted");
    Task_num.task_list_clear();

    /* Note that these closes can take a bit if flush() is implied! */
    SD_entry.closeAllSds();
    ShowLba.closeTrace();

    /* Cleanup maps and journals: */
    if (Validate.isValidate())
      DV_map.dv_set_all_unbusy(sd_list);

    if (Validate.isRealValidate())
    {
      Jnl_entry.dumpAllMaps(true);
      DV_map.printCounters();
    }

    if (Dedup.isDedup())
      Dedup.reportAllSdCounters();

    WG_entry.free_fifos(work);
  }


  public void doFileSystemWorkload()
  {
    Native.allocSharedMemory();
    Patterns.createPattern((int) work.maximum_xfersize);

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

    /* Dump all possible journals. Don't dump them if there was any error. */
    /* In that way a journal recovery will be able to continue re-checking */
    /* those blocks that he already knows of that are bad.                 */
    if (!DV_map.anyBadSectorsFound())
      Jnl_entry.dumpAllMaps(true);

    /* Report the final counters. The counters reported on logfile do NOT */
    /* include the work done between the returning of counters for the */
    /* last interval and the run shutdown: */
    Blocked.printCountersToLog();

    DV_map.printCounters();
    FwgWaiter.clearStuff();

    if (Dedup.isDedup())
      Dedup.reportAllFsdCounters();

    FwgRun.endOfRun(work);

  }


  /**
  * Start an IO_task for each SD, for each thread requested .
  */
  static void StartIoThreads(Work work)
  {
    if (sharedThreads())
      startSharedThreads(work);
    else
      startSdThreads(work);
  }


  private static void startSdThreads(Work work)
  {
    list_of_io_tasks.removeAllElements();

    /* For each active (real or concat) SD entry, create one IO_task per thread: */
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);

      /* Create one IO_task per thread requested: */
      int threads_started = 0;
      for (int j = 0; j < work.getThreadsForSlave(sd.sd_name); j++)
      {
        /* The SD passed here is the real SD: */
        Task_num tn  = new Task_num("IO_task " + sd.lun);
        IO_task task = new IO_task(tn, sd);
        list_of_io_tasks.add(task);
        task.setStreamContext(work.getStreamForSlave(sd.sd_name, j), j);
        task.start();
        threads_started++;

        /* Every n tasks sleep a little to avoid having too many tasks */
        /* in startup at the same time:                                 */
        int sleep_at = 32;
        if (i % sleep_at == sleep_at - 1)
          common.sleep_some(10);
      }

      common.ptod("Started %2d i/o threads for %s",  threads_started, sd.sd_name);
    }

    common.plog("Started a total of %2d i/o threads.", list_of_io_tasks.size());
  }


  private static void startSharedThreads(Work work)
  {
    list_of_io_tasks.removeAllElements();

    /* Create one IO_task per thread requested: */
    for (int j = 0; j < work.threads_from_rd; j++)
    {
      Task_num tn  = new Task_num("IO_task shared thread " + j);
      IO_task task = new IO_task(tn, WG_entry.getSharedFifo(), work.maximum_xfersize);
      list_of_io_tasks.add(task);

      // sidestep streamcontext for now
      //task.setStreamContext(work.getStreamForSlave(sd.sd_name, j), j);
      task.start();

      /* Every n tasks sleep a little to avoid having too many tasks */
      /* in startup at the same time:                                 */
      int sleep_at = 32;
      if (j % sleep_at == sleep_at - 1)
        common.sleep_some(10);
    }

    common.plog("Started a total of " + list_of_io_tasks.size() + " shared i/o threads");

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
   * Note: there is no need for synchronization here. In IO_task the count
   * is only increased AFTER we check for workload_done which has just been
   * set.
   */
  private static void sendInterrupts()
  {
    int sent = 0;
    for (int i = 0; i < list_of_io_tasks.size(); i++)
    {
      IO_task task = (IO_task) list_of_io_tasks.elementAt(i);
      if (task.getActiveCount() == 0)
      {
        common.interruptThread(task);
        sent++;
      }
    }

    common.ptod("Sent %d interrupts to %d waiting IO_task threads",
                sent, list_of_io_tasks.size());
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

  /**
   * Bind IO_task LWPs to a set.
   * First delete the first 10 sets, and then create set1 using provided cpu
   * numbers.
   *
   * 10/18/10: Decided NOT to document and announce this. This stuff was
   * added for a one-time experiment running against ramdisk. to measure very
   * low iops, e.g. 1. This is not available on nonSolaris anyway, so this
   * therefore goes against Vdbench running on any platform.
   */
  private static void bindCpus()
  {
    int[] psrset = Validate.getPsrset();

    /* Exit if we don't need this: */
    if (psrset.length == 0)
      return;

    //if (!common.get_debug(common.USE_PSRSET))
    //{
    //  SlaveJvm.sendMessageToConsole("Bypassing psrset request; missing -d79");
    //  SlaveJvm.sendMessageToConsole("This is during debugging only");
    //  return;
    //}

    /* Start by deleting 10 sets: */
    for (int i = 1; i < 10; i++)
    {
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText(String.format("/usr/sbin/psrset -d %d", i));
      ocmd.execute();
      //ocmd.printStderr();
      ocmd.printStdout();
    }

    /* Create set1: */
    String cpus = "";
    for (int i = 0; i < psrset.length; i++)
      cpus += psrset[i] + " ";
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("/usr/sbin/psrset -c " + cpus);
    ocmd.execute();
    ocmd.printStderr();
    ocmd.printStdout();
    if (ocmd.getStderr().length > 0)
      common.failure("'%s' failed", ocmd.getCmd());

    /* Now bind all IO_task threads to this set: */
    String lwps = "";
    int pid = 0;
    for (int i = 0; i < list_of_io_tasks.size(); i++)
    {
      IO_task task = list_of_io_tasks.elementAt(i);
      lwps += ((int) task.getPids()) + ",";
      pid = (int) (task.getPids() >> 32);
    }

    ocmd = new OS_cmd();
    ocmd.addText("/usr/sbin/psrset -b 1 " + pid + "/" + lwps);
    ocmd.execute();
    ocmd.printStderr();
    ocmd.printStdout();
    if (ocmd.getStderr().length > 0)
      common.failure("'%s' failed", ocmd.getCmd());
  }
}

