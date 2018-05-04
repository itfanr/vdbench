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

import Utils.Format;
import java.util.*;
import Oracle.LogWriter;
import Oracle.OracleBlock;


/**
 * This class contains the code for the IO task.
 */
public class IO_task extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  FifoList wt_to_iot;    /* FIFO list from WT to IOT                          */
  SD_entry sd;           /* SD for this thread                                */
  boolean  windows = common.onWindows();
  int      threadno = -1;
  long win_handle = -1;  /* Handle because windows allows only one            */
                         /* outstanding to a file.                            */
                         /* We should double check if this is still the case! */

  Task_num tn;

  long file_handle = 0;        /* File handle for this thread.                */
                               /* Windows allocates its own for each thread   */


  long   write_buffer;         /* Native write buffer for this task */
  long   read_buffer;          /* Native read buffer for this task  */

  private int ios_currently_in_multi = 0;

  static boolean print_io_comp = common.get_debug(common.PRINT_IO_COMP);

  int       max_cmd_burst = 32;
  Cmd_entry cmd_array[];
  long      cmd_fast[];

  long allocated_buffer_size;

  private Random dedup_randomizer = null;
  private long sets_written = 0;
  private long uniques_written = 0;


  /* This must stay in synch with vdbjni.c!! */
  private static int CMD_READ_FLAG  = 0;
  private static int CMD_RAND       = 1;
  private static int CMD_HIT        = 2;
  private static int CMD_LBA        = 3;
  private static int CMD_XFERSIZE   = 4;
  private static int DV_KEY         = 5;
  private static int CMD_FHANDLE    = 6;
  private static int CMD_BUFFER     = 7;
  private static int WG_NUM         = 8;
  private static int CMD_FAST_LONGS = 9;

  private static boolean debug_write = common.get_debug(common.DV_DEBUG_WRITE_ERROR);

  /**
   * Set up the run parameters for the IO task.
   */
  public IO_task(Task_num tn_in, SD_entry sd_in, int thread)
  {
    sd        = sd_in;
    wt_to_iot = sd.wt_to_iot;
    tn        = tn_in;
    tn.task_set_start_pending();
    threadno = thread;

    /* Force a unique seed for each dedup randomizer: */
    dedup_randomizer = new Random(System.currentTimeMillis() * tn.task_number * 1000);

    setName("IO_task " + sd.sd_name);
  }

  /**
   * Handle i/o errors.
   *
   * ******************************
   * This is called from vdbjni.c *
   * ******************************
   *
   * I/O done using Native.read and Native.write only return a
   * negative value to indicate an error, but there is currently no
   * feedback to possible errno.h or GetLastError().
   */
  public static synchronized void io_error_report(long flg,
                                                  long fhandle,
                                                  long lba,
                                                  long xfersize,
                                                  long errno)
  {
    boolean read = (flg != 0);
    DV_map     dv_map = null;
    String     lun    = null;
    SD_entry   sd     = null;
    ActiveFile afe    = null;

    /* Using the handle, find the SD: */
    Object obj = File_handles.findHandle(fhandle);
    if (obj instanceof SD_entry)
    {
      sd     = (SD_entry) obj;
      dv_map = sd.getDvMap();
      lun    = sd.lun;
    }

    else if (obj instanceof ActiveFile)
    {
      afe    = (ActiveFile) obj;
      dv_map = afe.getFileEntry().getAnchor().getDVMap();
      lun    = afe.getFileEntry().getName();
    }

    else
      common.failure("io_error_report(): unexpected file handle returned: " + obj);



    /* Starting solaris 11 there was a problem with Vdbench interrupting */
    /* IO_task and therefore causing an interrupted i/o to end with      */
    /* ENOENT, but also the sendMessage below to be interrupted.         */
    // Resolved by not sending interrupts to a thread with an active i/o
    //if (SlaveJvm.isWorkloadDone())
    //{
    //  common.plog("Possible i/o error after run completion. May just be caused by interrupt().");
    //  return;
    //}

    /* Send error message to master: */
    // DO NOT CHANGE, SEE dvpOST()
    String txt = String.format("%s op: %-6s lun: %-30s lba: %12d 0x%08X xfer: %8d errno: %s ",
                               common.tod(),
                               (read) ? "read" : "write",
                               lun,
                               lba,
                               lba,
                               xfersize,
                               Errno.xlate_errno(errno));
    ErrorLog.sendMessageToMaster(txt);

    /* Keep track of the amount of errors: */
    ErrorLog.countErrorsOnSlave(lun, lba, (int) xfersize);

    /* When data validation is active, mark the block bad: */
    if (Validate.isValidate())
    {
      try
      {
        if (obj instanceof SD_entry)
          dv_map.dv_set_unbusy(lba, DV_map.DV_ERROR, read);
        else
        {
          // Marking as 'error' is now done in Bad_sector()
          //long file_start_lba = afe.getFileEntry().getFileStartLba();
          //dv_map.dv_set(lba - file_start_lba, DV_map.DV_ERROR);
        }
      }

      catch (Exception e)
      {
        common.failure(e);
      }
    }
  }


  /**
   * Execute the i/o request(s).
   * It can be a read, a write, or a write that was changed to a read by
   * Data Validation so that the data can be validated before we rewrite it.
   */
  private void io_do_cmds(int burst)
  {
    /* Some last minute stuff:                                            */
    /* - Store the file handle to be used                                 */
    /* - Do some checking                                                 */
    for (int i = 0; i < burst; i++)
    {
      Cmd_entry cmd = cmd_array[i];

      /* If this is an EOF signal, remove this i/o from the burst and quit */
      if (cmd.delta_tod == Long.MAX_VALUE)
      {
        /* Whatever comes behind this entry must be ignored: */
        if (--burst == 0)
          return;
        break;
      }

      /* At this point we may never touch lba=0 because that is the vtoc: */
      if (cmd.cmd_lba == 0)
      {
        cmd.cmd_print(sd, "lba0");
        common.failure("We may never access LBA=0; Internal error");
      }

      /* Last double check for lba errors: (obsolete if we don't do concatenation)*/
      if ( (cmd.cmd_lba + cmd.cmd_xfersize) > sd.end_lba)
        common.failure("seek too high: " + cmd.cmd_wg.wd_name + " " +
                       Format.f("%d",  cmd.cmd_lba )     + " / " +
                       cmd.cmd_xfersize + " / " +
                       Format.f("%d",  sd.end_lba ));

      if (cmd.cmd_xfersize > allocated_buffer_size)
        common.failure("Trying to put " + cmd.cmd_xfersize + " bytes into a " +
                       allocated_buffer_size + " byte buffer");
    }

    /* JNI runs much faster with arrays than with objects: */
    cmd_array_xlate(burst);

    /* Last minute stuff in 'fast' array: */
    for (int i = 0; i < burst; i++)
    {
      /* I/O needs a handle: */
      int offset = CMD_FAST_LONGS * i;
      if (windows)
        cmd_fast[ offset + CMD_FHANDLE ] = sd.win_handles[threadno];
      else
        cmd_fast[ offset + CMD_FHANDLE ] = sd.fhandle;

      /* Store buffer addresses: */
      if (cmd_array[i].cmd_read_flag)
        cmd_fast[ offset + CMD_BUFFER  ] = read_buffer;
      else
        cmd_fast[ offset + CMD_BUFFER  ] = write_buffer;

      /* Dedup experiment: */
      if (!cmd_array[i].cmd_read_flag && Validate.getDedupRate() > 0)
      {
        int unique_block = dedup_randomizer.nextInt(100);
        int unique_value = dedup_randomizer.nextInt(Integer.MAX_VALUE);
        int sets_value   = dedup_randomizer.nextInt(Validate.getDedupSets());
        int place        = (int) (cmd_fast[CMD_XFERSIZE] / 4 - 1) *
                           Validate.getDedupPlace() / 100;
        int[] pattern    = DV_map.get_pattern(0);

        /* This code must be locked since for this experiment we overlay the */
        /* contents of a pattern that should be fixed:                       */
        synchronized (pattern)
        {
          //common.ptod("burst: " + burst);
          //common.ptod("unique_block: " + unique_block + " " + SlaveWorker.work.dedup_rate);
          //common.ptod(Format.f("unique_value: %08x ", unique_value) + sd.sd_name );
          if (unique_block < Validate.getDedupRate())
          {
            uniques_written++;
            pattern[place] = unique_value;
            //common.ptod("sd=" + sd.sd_name + Format.f(" unique_value: %08x", unique_value));
          }

          else
          {
            sets_written++;
            pattern = DV_map.get_pattern(1);
            pattern[place] = sets_value;
            //common.ptod("sets_value: " + sets_value);
          }

          Native.array_to_buffer(pattern, write_buffer);
        }
      }
    }

    /* Data Validation stuff: */
    if (Validate.isValidate())
      dv_startup(cmd_array[0], burst);

    /* Go do all the io requested in this array: */
    ios_currently_in_multi += burst;
    //if (debug_pendings > 500)
    Native.multiJniCalls(cmd_fast, burst);
    ios_currently_in_multi -= burst;
    //common.ptod("ios_currently_in_multi2: " + ios_currently_in_multi + " " + burst);

    /* Now for all the post-io stuff: */
    for (int i = 0; i < burst; i++)
    {
      int offset = CMD_FAST_LONGS * i;
      Cmd_entry cmd = cmd_array[i];

      /* For debugging: */
      if (print_io_comp)
        cmd.cmd_print_resp(sd, "print_io_comp");

      /* Data validation stuff: */
      if (Validate.isValidate() && sd.getDvMap() != null)
        dv_cleanup();

      /* Lower on-the-way count to keep track of sequential eof: */
      cmd.cmd_wg.subtract_io(cmd);

      if (cmd.oblocks != null)
      {
        /* If this was a Logwriter completion, notify him: */
        if (cmd.oblocks instanceof Vector)
          LogWriter.notifyLogWriteComplete((Vector) cmd.oblocks);

        /* If this was any other completion, notify user: */
        else if (cmd.oblocks instanceof OracleBlock)
          ((OracleBlock) cmd.oblocks).operationComplete();

        else
          common.failure("Invalid oblocks value: " + cmd.oblocks);
      }
      else if (common.get_debug(common.ORACLE))
      {
        Trace.print();
        common.failure("cmd.oblocks should not be null: readflag: " + cmd.cmd_read_flag + " " + cmd.cmd_lba);
      }

      /* Clear to catch the failure above: */
      cmd.oblocks = null;
    }
  }



  /**
   * Prepare an operation for Data validation
   */
  private void dv_startup(Cmd_entry cmd, int burst)
  {
    if (burst != 1)
      common.failure("Invalid burst count for DV: " + burst);

    /* Write journal record if needed: */
    if (!cmd.cmd_read_flag && Validate.isJournaling())
      sd.getJournal().writeBeforeImage(cmd);

    if (cmd.cmd_read_flag)
      DV_map.key_reads[cmd.dv_key]++;
    else
      DV_map.key_writes[cmd.dv_key]++;
  }


  /**
   * Data validation: after a read, validate. After a reread before a write, write.
   */
  private static int debug_writes = 1000;
  private static int debug_skips  = 10;
  private boolean forced_short_write = false;
  private final static boolean debug_clean = common.get_debug(common.DEBUG_DV_CLEANUP);
  private void dv_cleanup()
  {
    Cmd_entry cmd = cmd_array[0];

    if (forced_short_write)
      common.failure("forced a short write");

    /* If the block is now in error we know that the last io failed,  */
    /* and we do not have to do any cleanup (done in io_error_report) */
    /* (For instance, if preread failed, do not start the write).     */
    if (sd.getDvMap().dv_get(cmd.cmd_lba) == DV_map.DV_ERROR)
      return;

    /* Write journal record if needed: */
    if (!debug_clean)
    {
      if (!cmd.cmd_read_flag && Validate.isJournaling())
        sd.getJournal().writeAfterImage(cmd);
    }
    else
    {
      /* For journal writes, write the first 'n' After images, */
      /* but then skip some After images writes: */
      if (!cmd.cmd_read_flag && Validate.isJournaling())
      {
        common.ptod("debug_writes: " + debug_writes + " " + debug_skips);
        if (debug_writes-- > 0)
          sd.getJournal().writeAfterImage(cmd);
        else
        {
          if (debug_skips-- <= 0)
            common.failure("Debugging Journaling. Limit reached");
          return;
        }
      }
    }


    /* Store timestamp of last successful read/write: */
    sd.getDvMap().save_timestamp(cmd.cmd_lba, cmd.cmd_read_flag);

    /* If this was a DV pre-read, change read to write and start write: */
    if (cmd.dv_pre_read)
    {
      cmd.cmd_read_flag = false;
      cmd.dv_pre_read   = false;
      cmd.dv_key        = DV_map.dv_increment(cmd.dv_key);

      /* Recursive call!! */
      cmd.cmd_wg.add_io(cmd);
      io_do_cmds(1);
    }

    /* Optional read after write? */
    else if (Validate.isImmediateRead() && !cmd.cmd_read_flag)
    {
      cmd.cmd_read_flag = true;

      /* Recursive call!! */
      cmd.cmd_wg.add_io(cmd);
      io_do_cmds(1);

      /* if the block now is bad it means we had a failure in immediate read: */
      if (sd.getDvMap().dv_get(cmd.cmd_lba) == DV_map.DV_ERROR)
      {
        ErrorLog.sendMessageToMaster(common.tod() + " Data Validation error " +
                                     "during immediate re-read. lun=" + sd.lun +
                                     Format.f(" lba 0x%016X", cmd.cmd_lba));
      }
    }

    /* At this point the lba is still marked busy                            */
    else
    {
      try
      {
        sd.getDvMap().dv_set_unbusy(cmd.cmd_lba, cmd.dv_key, cmd.cmd_read_flag);
      }
      catch (Exception e)
      {
        common.ptod("cmd.cmd_lba: " + cmd.cmd_lba);
        common.ptod("cmd.dv_key: " + cmd.dv_key);
        common.ptod("cmd.cmd_read_flag: " + cmd.cmd_read_flag);
        cmd.cmd_print(sd, "exception");
        common.failure(e);
      }
    }
  }


  /**
   * IO task: start i/o's, wait for completion, and maitain statistics
   * Block is read or written. If the read was for a data validation write,
   * validate the data from the read and start the write.
   */
  public void run()
  {
    long rc;

    long prev_lba = -1;
    //common.plog("Starting IO_task " + tn.task_name);


    /* The 'sd' pointer in IO_entry is the same as the sd pointer in       */
    /* the Cmd_entry. This is a leftover from when we did SD concatenation */
    /* Some day we should clean up the code.                               */

    try
    {
      /* Reporting and communication between master and slaves is very */
      /* important, so we'll run the real work with the lowest prio:   */
      /* (If we're out of cycles it is a mess anyway, so this is fine) */
      Thread.currentThread().setPriority( Thread.currentThread().MIN_PRIORITY);

      initialize();

      /* Process until all i/o requests have been completed: */
      int burst = 0;
      while (true)
      {
        try
        {
          /* Eventhough the code logic works to have an interrupt request      */
          /* tell IO_task to terminate (Tasknum.interrupt_task()) there        */
          /* have been situations where it took three interrupt requests       */
          /* before it fell through to here. Maybe there are some areas where  */
          /* the first interrupts are reset that I have not been able to find. */
          /* The simplest solution is to just check workload_done.             */
          if (SlaveJvm.isWorkloadDone())
            break;

          burst = wt_to_iot.getArray(cmd_array);

          if (SlaveJvm.isWorkloadDone())
            break;

          //common.ptod("cmd_array[0].cmd_readflag: " + cmd_array[0].cmd_read_flag);

          /* Do scsi resets if requested: */
          if (sd.scsi_bus_reset != 0 ||
              sd.scsi_lun_reset != 0)
            sd.scsi_reset();


          /* Execute a regular i/o request: */
          io_do_cmds(burst);
        }
        catch (InterruptedException e)
        {
          break;
        }
      }

      /* When we get to this point, a pending interrupt may be set that can   */
      /* cause problems down below. The interrupt is requested by             */
      /* SlaveWorker: Task_num.interrupt_tasks("IO_task"), but the code above */
      /* may have fallen through already because of isWorkloadDone().         */
      /* The call to interrupt_tasks("IO_task") is still needed though        */
      /* because this thread could be waiting inside of the fifo() code.      */
      this.interrupted();

      Native.freeBuffer(allocated_buffer_size, read_buffer);
      if (read_buffer != write_buffer)
        Native.freeBuffer(allocated_buffer_size, write_buffer);

      /* See above! */
      this.interrupted();

      if (Validate.getDedupRate() > 0)
      {
        double totals = (uniques_written + sets_written);
        common.ptod(sd.sd_name +
                    Format.f(" total blocks: %8d;", totals) +
                    Format.f(" sets%%: %5.2f;", ((double) sets_written * 100 / totals)) +
                    Format.f(" unique%%: %5.2f",+ ((double) uniques_written * 100 / totals)));
      }

      tn.task_set_terminating(0);
      //common.plog("Ended IO_task " + tn.task_name);
    }


    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

  }



  /**
   * Translate array of CMD_entry's to an array of longs.
   *
   * This array business is done to save cpu cycles during high iops when
   * there's always a load of ios waiting in the fifos.
   * This especially eliminates JNI call overhead by calling JNI only once
   * for ten ios instead of for each i/o.
   *
   * The consequence is however that the i/o order as they arrive from
   * WG_task is no longer honored (except for sequential, where the LBA is
   * determined in JNI.
   * e.g. WG_task sends i/o as blocks 5,10,15,20
   * If each IO_task gets a burst of TWO, one IO_task does 5 and 10, and
   * the other 15 and 20. Resulted order likely: 5,15,10,20
   * However since nowhere in existing code (except sequential) there is
   * any dependency to execution order, this is fine.
   *
   * Replay and DV however do only a burst of one, therefore guaranteeing
   * the order.
   *
   */
  public void cmd_array_xlate(int burst)
  {
    int i = 0;
    int offset = 0;

    /* For debugging, try to see if we need to force an error: */
    boolean attempt_force = Validate.attemptForcedError();

    try
    {
      for (i = 0; i < burst; i++)
      {
        Cmd_entry cmd = cmd_array[i];
        offset = CMD_FAST_LONGS * i;
        cmd_fast[ offset + CMD_READ_FLAG ] = (cmd.cmd_read_flag) ? 1 : 0;
        cmd_fast[ offset + CMD_HIT       ] = (cmd.cmd_hit) ? 1 : 0;

        /* DV and replay always specify specific seek address.       */
        /* If cmd_rand is set to 0 the sequential lba is set in JNI. */
        if (Validate.isValidate() || SlaveJvm.isReplay())
        {
          cmd_fast[ offset + CMD_RAND    ] = 1;
          if (sd.isTapeDrive())
            cmd_fast[ offset + CMD_RAND  ] = 2;
        }

        else
        {
          /* Random: 1, Sequential: 0, Seq eof: 2 */
          if (cmd.cmd_rand)
            cmd_fast[ offset + CMD_RAND] = 1;
          else if (cmd.cmd_wg.seekpct >= 0)
            cmd_fast[ offset + CMD_RAND] = 0;
          else
            cmd_fast[ offset + CMD_RAND] = 2;
        }

        cmd_fast[ offset + CMD_LBA       ] = cmd.cmd_lba;
        cmd_fast[ offset + CMD_XFERSIZE  ] = cmd.cmd_xfersize;
        cmd_fast[ offset + DV_KEY        ] = cmd.dv_key;
        cmd_fast[ offset + WG_NUM        ] = cmd.cmd_wg.workload_number;

        /* Force a DV error after 'n' reads: */
        if (Validate.isValidate() && (cmd.cmd_read_flag || cmd.dv_pre_read) && !debug_write)
        {
          if (attempt_force && Validate.forceError(sd, null, cmd.cmd_lba, cmd.cmd_xfersize))
            cmd_fast[ offset + DV_KEY    ] = DV_map.DV_ERROR;  // 127: block is in error
        }

        /* Force a short block write after 'n' ios: */
        if (Validate.isValidate() && debug_write && !cmd.cmd_read_flag)
        {
          if (attempt_force && Validate.forceError(sd, null, cmd.cmd_lba, cmd.cmd_xfersize))
          {
            forced_short_write = true;
            cmd_fast[ offset + CMD_XFERSIZE  ] /= 2;
            cmd.cmd_print(cmd.sd_ptr, "forced");
            SlaveJvm.sendMessageToConsole("forced 'DV_DEBUG_WRITE_ERROR' write error to lba: " +
                                          String.format("%08x", cmd_fast[ offset + CMD_LBA ]));
          }
        }

        //if (!cmd.cmd_read_flag)
        //  cmd_fast[ offset + CMD_XFERSIZE  ] = 1024;
      }

    }

    catch (Exception e)
    {
      common.where(16);
      common.ptod("burst: " + burst);
      common.ptod("i:                " + i);
      common.ptod("offset:           " + offset);
      common.ptod("burst:            " + burst);
      common.ptod("cmd_array.length: " + cmd_array.length);
      common.ptod("cmd_fast.length:  " + cmd_fast.length);
      common.failure(e);
    }
  }


  private void initialize()
  {
    if (common.get_debug(common.BURST_OF_ONE)  ||
        common.get_debug(common.ORACLE)        ||
        common.get_debug(common.PRINT_EACH_IO) ||
        SlaveJvm.isReplay()                    ||
        sd.isTapeDrive())
      max_cmd_burst = 1;

    /* Allocate Java read and write buffers only for Data Validation: */
    if (Validate.isValidate())
    {
      /* Data Validation requires us to send io one at the time to jni: */
      max_cmd_burst = 1;

      allocated_buffer_size = SlaveWorker.work.maximum_xfersize;
      read_buffer  = Native.allocBuffer(allocated_buffer_size);
      write_buffer = Native.allocBuffer(allocated_buffer_size);
    }

    else if (SlaveJvm.isReplay())
    {
      allocated_buffer_size = ReplayDevice.getMaxXfersize();
      read_buffer  =
      write_buffer = Native.allocBuffer(allocated_buffer_size);
    }

    else
    {
      allocated_buffer_size = SlaveWorker.work.maximum_xfersize;
      read_buffer  =
      write_buffer = Native.allocBuffer(allocated_buffer_size);
    }

    /* Copy the default data pattern to the native i/o buffer. */
    /* This also includes the compression pattern if needed:   */
    int[] pattern = DV_map.get_pattern(0);
    if (allocated_buffer_size < pattern.length * 4)
      common.failure("Buffer size smaller than data pattern buffer: " +
                     allocated_buffer_size + "/" + (pattern.length * 4));

    Native.array_to_buffer(pattern, write_buffer);

    /* Allocate io burst tables for jni: */
    cmd_array = new Cmd_entry[max_cmd_burst];
    cmd_fast  = new long[max_cmd_burst * CMD_FAST_LONGS];

    /* Communicate status of task: */
    tn.task_set_start_complete();
    tn.task_set_running();

    //common.ptod("Starting IO_task " + tn.task_name);
  }

  public int getCurrentMultiCount()
  {
    return ios_currently_in_multi;
  }


  public static void main(String[] args)
  {
    Random[] rand = new Random[100];

    for (int i = 0; i < rand.length; i++)
    {
      rand[i] = new Random(i);
      common.ptod("rand: " + rand[i].nextInt(1000));
    }
  }
}

