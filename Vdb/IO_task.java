package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

import User.UserCmd;

import Utils.Format;
import Utils.Fput;


/**
 * This class contains the code for the IO task.
 * An IO_task can be SD based, e.g. sd=sd1,threads=8,
 * or it can be WD based, e.g. wd=wd1,threads=8 for concatenated SDs.
 */
public class IO_task extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private SD_entry sd = null;       /* SD for this thread               */

  private FifoList get_from_fifo;   /* FIFO list from WT to IOT         */

  private Task_num tn;

  private int      allocated_buffer_size;
  private long     write_buffer;    /* Native write buffer for this task */
  private long     read_buffer;     /* Native read buffer for this task  */

  private int      ios_currently_active = 0;


  private Cmd_entry cmd_array[] = new Cmd_entry[1];;

  private long      pid_and_lwp;

  private KeyMap    key_map = null;


  private long rc = 0;   /* Return of last excuted read or write. All error       */
                         /* reporting is done by calls to io_error_report from JNI*/
  private int     data_flag;
  private boolean compression_only;

  private UserCmd usercmd;   /* For User API. */

  private StreamContext stream_context = null;


  private ThreadMonitor tmonitor = null;


  private Timers gettime = new Timers("IO_task getArray");
  private Timers iotime  = new Timers("IO_task iotime");

  private int    key_blocksize   = 0;
  private int    max_sd_xfersize = 0;

  /* To be used for translation of concatenated LBA to REAL lba: */
  private SD_entry        concat_search_key = new SD_entry();
  private ConcatLbaSearch search_method     = new ConcatLbaSearch();



  private static boolean print_io_comp     = common.get_debug(common.PRINT_IO_COMP);
  private static boolean fake_trace        = false;

  private static boolean remove_after_error = Validate.removeAfterError();



  /**
   * Set up the run parameters for the IO task that references an SD, either a
   * real SD or a concatenated SD
   */
  public IO_task(Task_num tn_in, SD_entry sd_in)
  {
    sd                = sd_in;
    get_from_fifo     = sd.fifo_to_iot;
    tn                = tn_in;
    data_flag         = Validate.createDataFlag();
    compression_only  = ((data_flag & Validate.FLAG_COMPRESSION) != 0) ;

    max_sd_xfersize   = sd.getMaxSdXfersize();

    if (Validate.isRealValidate())
      key_blocksize = sd.dv_map.getKeyBlockSize();
    else if (Validate.isValidateForDedup())
      key_blocksize = Dedup.getDedupUnit();

    setName(tn.task_name);
    tn.task_set_start_pending();

    fake_trace = Validate.showLba();
  }



  /**
   * For concatenated SDs sharing all rd threads=
   */
  public IO_task(Task_num tn_in, FifoList fifolist, int max_xfer)
  {
    sd                = null;
    get_from_fifo     = fifolist;
    tn                = tn_in;
    data_flag         = Validate.createDataFlag();
    compression_only  = ((data_flag & Validate.FLAG_COMPRESSION) != 0) ;

    max_sd_xfersize   = max_xfer;
    key_blocksize     = max_xfer;
    if (Dedup.isDedup())
      key_blocksize = Dedup.getDedupUnit();

    setName(tn.task_name);
    tn.task_set_start_pending();

    fake_trace = Validate.showLba();
  }


  /**
   * Performance enhancement filling write buffers: If the write buffer address
   * is zero it will be replaced by an address inside of the data pattern
   * buffer. This eliminates the need to copy gigabuckets of data from the
   * pattern buffer to the write buffer, removing a huge amount of cpu cycles.
   *
   * This of course can ONLY be done when Data Validation or Dedup is not
   * involved.
   *
   * BTW: yes, the 'prevent_dedup' call done in JNI copying into the pattern
   * buffer will lose maybe a very slight level of accuracy as far as the
   * no-dedup change, but likely is so little that this will just be
   * noise.
   *
   * The 'slight' turns out to be more than I like when running with multiple
   * (lotsa) threads, so I decided to remove this for now.
   * A better solution in the future maybe is to have the write done from the
   * WRITE buffer after the write buffer has been initialized?
   * TBD!!
   */
  private boolean doWeNeedWriteBuffer()
  {
    /* Without writes, this is easy: */
    if (sd != null && !sd.isOpenForWrite())
      return false;

    /* Validate and Dedup MUST have a write buffer: */
    if (Validate.isRealValidate() || Validate.isValidateForDedup())
      return true;

    /* The rest should all be fine, allowing me to write from the pattern buffer: */
    //return false;

    /* See above notes */
    return true;
  }

  /**
   * Handle i/o errors.
   *
   * ******************************
   * This is called from vdb_dv.c *
   * ******************************
   *
   */
  public static synchronized void io_error_report(long flg,
                                                  long fhandle,
                                                  long file_lba,
                                                  long xfersize,
                                                  long errno,
                                                  long buffer)
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
      sd.sd_error_count++;
      dv_map = sd.dv_map;
      lun    = sd.lun;

      /* A request to ignore the device after an i/o error? */
      if (remove_after_error)
        ErrorLog.ptod("'data_errors=remove_device' requested for sd=" +
                      sd.sd_name + " No longer issuing i/o against this SD");
    }

    else if (obj instanceof ActiveFile)
    {
      afe    = (ActiveFile) obj;
      dv_map = afe.getFileEntry().getAnchor().getDVMap();
      lun    = afe.getFileEntry().getFullName();
    }

    else
    {
      lun = (String) obj;
      common.ptod("errno: " + errno);
      common.ptod("lun: " + lun);
      common.ptod("Non-SD or FSD related i/o error: " + Errno.xlate_errno(errno));
      common.ptod("Unknown file handle. Continuing");
      //common.failure("io_error_report(): unexpected file handle returned: " + obj);
    }

    /* New DV reporting is only done AFTER 60003 has been received. */
    /* If journal recovery finds that the block is fine, great.     */
    if (errno == 60003)
    {
      //common.where(16);
      if (sd != null)
      {
        if (BadDataBlock.reportBadDataBlock(sd, file_lba, 0, buffer))
          return;
      }
      else
      {
        if (BadDataBlock.reportBadDataBlock(afe, file_lba, afe.getFileEntry().getFileStartLba(), buffer))
          return;
      }
    }


    /* Send error message to master: */
    // DvPost may become obsolete
    // DO NOT CHANGE, SEE DVPost()
    String txt = String.format("%s op: %-6s lun: %-30s lba: %12d 0x%08X xfer: %8d errno: %s ",
                               common.tod(),
                               (read) ? "read" : "write",
                               lun,
                               file_lba,
                               file_lba,
                               xfersize,
                               Errno.xlate_errno(errno));
    ErrorLog.ptod(txt);

    /* Keep track of the amount of errors (code will abort if needed): */
    ErrorLog.countErrorsOnSlave(lun, file_lba, (int) xfersize);

    /* When data validation is active, mark the block bad: */
    if (Validate.isValidate())
    {
      try
      {
        /* This data block has an error. The question that can be asked is:   */
        /* If this is a corruption, shall I only mark the key block in error  */
        /* or shall I mark the whole data block in error.                     */
        /* Non-corruption errors of course need the WHOLE block to be marked. */
        /* Solution for now: always the whole data block.                     */
        if (obj instanceof SD_entry)
        {
          KeyMap key_map = new KeyMap(0l, sd.getKeyBlockSize(),
                                      SlaveWorker.work.maximum_xfersize);
          key_map.markDataBlockBad(dv_map, file_lba);
        }
      }

      catch (Exception e)
      {
        common.failure(e);
      }
    }
  }



  /**
   * IO task: start i/o's, wait for completion, and maintain statistics
   * Block is read or written. If the read was for a data validation write,
   * validate the data from the read and start the write.
   */
  public void run()
  {

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

          gettime.before();
          burst  = get_from_fifo.getArray(cmd_array);
          tmonitor.add1();
          gettime.after();

          if (SlaveJvm.isWorkloadDone())
            break;

          if (cmd_array[0].delta_tod == Long.MAX_VALUE)
            break;

          /* A request to ignore the device after an i/o error? */
          if (remove_after_error && sd.sd_error_count > 0)
            continue;

          // To test skip_after_io_error
          //if (remove_after_error && sd.sd_name.equals("sd1"))
          //  io_error_report(1, cmd_array[0].sd_ptr.fhandle, 123, 4096, 555);

          /* Do scsi resets if requested: */
          if (cmd_array[0].sd_ptr.scsi_bus_reset != 0 ||
              cmd_array[0].sd_ptr.scsi_lun_reset != 0)
            cmd_array[0].sd_ptr.scsi_reset();

          iotime.before();
          if (!processSingleIO(cmd_array[0]))
            break;
          iotime.after();
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
      Thread.interrupted();
      if (read_buffer != 0)
        Native.freeBuffer(allocated_buffer_size, read_buffer);
      if (write_buffer != 0)
        Native.freeBuffer(allocated_buffer_size, write_buffer);

      /* See above! */
      Thread.interrupted();

      gettime.print();
      iotime.print();

      tn.task_set_terminating(0);
      //common.ptod("Ended IO_task " + tn.task_name);

    }


    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * Issue a single i/o.
   *
   * Errors are reported via JNI using io_error_report().
   */
  private boolean processSingleIO(Cmd_entry cmd)
  {
    /* Optional user processing needed? */
    if (cmd.cmd_wg.user_class != null)
    {
      usercmd = new UserCmd(cmd);
      cmd.cmd_wg.user_class.preIO(usercmd);
      usercmd.updateCommand();
      if (cmd.cmd_lba == 0 && !cmd.cmd_read_flag && !cmd.sd_ptr.canWeUseBlockZero())
        common.failure("Attempting to read or write to lba 0");
    }

    /* When requested, allow override of lba for sequential streaming: */
    if (stream_context != null)
    {
      cmd.cmd_lba = stream_context.getNextSequentialLba(cmd);
      if (cmd.cmd_lba < 0)
      {
        cmd.cmd_wg.sequentials_lower();
        return false;
      }
    }

    /* If we are working with a concatenated SD now determine the proper */
    /* real SD to use with it's real lba:                                */
    if (cmd.sd_ptr.concatenated_sd)
    {
      cmd.concat_lba = cmd.cmd_lba;
      cmd.concat_sd  = cmd.sd_ptr;
      WG_entry wg    = cmd.cmd_wg;
      sd = ConcatSds.translateConcatToRealSd(cmd, concat_search_key,
                                             search_method,
                                             wg.access_block_zero);

      /* Paranoia check: */
      if (cmd.cmd_lba == 0 && !cmd.cmd_read_flag && !cmd.sd_ptr.canWeUseBlockZero())
        common.failure("Attempting to write to lba 0");
    }


    /* Now go do the i/o: ('while (true)' just for break) */
    while (true)
    {
      /* Keep track of outstanding i/o to prevent interrupts during shutdown: */
      ios_currently_active++;

      /* Get the easy stuff out of the way first,      */
      /* stuff without real data buffer manipulations. */

      /* Simple straight-forward read:                 */
      if (cmd.cmd_read_flag && !Validate.isRealValidate())
      {
        rc = Native.readFile(cmd.sd_ptr.fhandle, cmd.cmd_lba,
                             cmd.cmd_xfersize,   read_buffer, cmd.jni_index);
        break;
      }


      /* A write from a pattern file, pattern is already in write buffer: */
      if (!cmd.cmd_read_flag && Patterns.usePatternFile())
      {
        rc = Native.writeFile(cmd.sd_ptr.fhandle, cmd.cmd_lba,
                              cmd.cmd_xfersize,   write_buffer, cmd.jni_index);
        break;
      }


      /* All the rest below requires serious data buffer work and is done here: */

      /* Tell KeyMap about this data block: */
      if (!key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map))
      {
        /* One or more key blocks of this data block are in error. */
        /* We should never get here since checks are done earlier: */
        common.failure("Unexpected DV_ERROR block found.");
      }

      //cmd.cmd_print("hot stuff?");

      /* The only problem I have here is that we always write from the BEGINNING */
      /* of the write buffer, e.g. if storage compresses on 128 chunks, an 8k write
      /* here always writes the same 8k. */

      // But I only have a 16 buffer, meaning I do have one consecutive write buffer pattern
      // That would require to allocate a full size*2 write buffer.
      // TBD

      //  if (compression_only)
      //  {
      //    long offset = cmd.cmd_lba % 8192;
      //    common.ptod("offset: " + offset);
      //    common.ptod("write_buffer: " + write_buffer);
      //    rc = Native.noDedupAndWrite(cmd.sd_ptr.fhandle, cmd.cmd_lba,
      //                                cmd.cmd_xfersize, write_buffer + offset, cmd.jni_index);
      //    break;
      //  }



      ioWithDataPatterns(cmd);

      break;
    }


    /* Keep track of outstanding i/o to prevent interrupt during shutdown: */
    ios_currently_active--;


    if (cmd.cmd_wg.user_class != null)
      cmd.cmd_wg.user_class.postIO(usercmd);

    /* For sequential stuff we need to keep track of i/o count: */
    cmd.cmd_wg.subtract_io(cmd);

    if (print_io_comp)
      cmd.cmd_print("print_io_comp2");

    /* For debugging, see ShowLba.java: */
    if (fake_trace)
      ShowLba.writeRecord(cmd);

    return true;
  }


  /**
   * I/O done for anything that requires a specific data pattern to be written, or
   * a data pattern to be compared for Data Validation.
   */
  private void ioWithDataPatterns(Cmd_entry cmd)
  {
    if (Dedup.isDedup())
      sd.dedup.dedupSdBlock(key_map, cmd);
    else
      key_map.setSdCompressionOnlyOffset(cmd.cmd_lba);

    /* If block is already in error, return.  We will never touch that block again. */
    if (Validate.isRealValidate() && !key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map))
      return;


    /* Just a DV read? (No other reads will come here) */
    if (cmd.cmd_read_flag)
    {
      // cleanup:
      if (!Validate.isValidateForDedup() && !Validate.isRealValidate())
        common.failure("This simple read call should never make it here!");


      if (cmd.type_of_dv_read != Validate.FLAG_PENDING_READ)
        cmd.type_of_dv_read = Validate.FLAG_NORMAL_READ;

      readAndValidateBlock(cmd);

      if (cmd.type_of_dv_read == Validate.FLAG_PENDING_READ)
        saveLastTod(Timestamp.PENDING_READ);
      else if (cmd.type_of_dv_read == Validate.FLAG_PENDING_REREAD)
        saveLastTod(Timestamp.PENDING_REREAD);
      else
        saveLastTod(Timestamp.READ_ONLY);


      /* We must unbusy the map, but during journal recovery the key may have changed: */
      if (Validate.isRealValidate())
      {
        if (cmd.type_of_dv_read == Validate.FLAG_PENDING_READ)
        {
          if (!key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map))
            common.failure("Unexpected key block state for lba 0x%08x", cmd.cmd_lba);
        }
        key_map.storeKeys();
      }
      return;
    }


    /* This is a write, read it first, except when we use DV for Dedup only: */
    if (Validate.isRealValidate() && key_map.anyDataToCompare() && !Validate.isNoPreRead())
    {
      cmd.type_of_dv_read = Validate.FLAG_PRE_READ;
      readAndValidateBlock(cmd);
      saveLastTod(Timestamp.PRE_READ);
      if (rc != 0)
      {
        key_map.storeKeys();
        return;
      }
    }

    /* Do a write with required data patterns, including DV: */
    patternWrite(cmd);
    saveLastTod(Timestamp.WRITE);

    /* After a write (and maybe journal write) is done update the keys in the DV_map: */
    if (Validate.isRealValidate() || Validate.isValidateForDedup())
    {
      /* Skip read-immediate if the write failed: */
      if (rc == 0 && Validate.isImmediateRead())
      {
        cmd.type_of_dv_read = Validate.FLAG_READ_IMMEDIATE;
        readAndValidateBlock(cmd);
        saveLastTod(Timestamp.READ_IMMED);
      }

      /* storeKeys() also unbusies the blocks: */
      key_map.storeKeys();
      if (sd.dedup != null)
        sd.dedup.countDedup(key_map, sd.dv_map);
    }
  }


  /**
   * Read a block and Validate its contents.
   */
  private void readAndValidateBlock(Cmd_entry cmd)
  {
    /* This is a Data Validation read, but if we don't know anything about */
    /* this block only read and forget about the contents:                 */
    if (!key_map.anyDataToCompare())
    {
      rc = Native.readFile(cmd.sd_ptr.fhandle, cmd.cmd_lba,
                           cmd.cmd_xfersize,   read_buffer, cmd.jni_index);
      return;
    }


    if (cmd.type_of_dv_read == Validate.FLAG_PENDING_READ)
    {
      if (HelpDebug.doAfterCount("corruptDuringPendingRead"))
        HelpDebug.corruptBlock(cmd.sd_ptr.fhandle, (int) cmd.cmd_xfersize, cmd.cmd_lba);
    }

    /* This call reads and validates 'n' key blocks: */
    rc = Native.multiKeyReadAndValidateBlock(cmd.sd_ptr.fhandle,
                                             data_flag | cmd.type_of_dv_read,
                                             0,
                                             cmd.cmd_lba,
                                             (int) cmd.cmd_xfersize,
                                             read_buffer,
                                             key_map.getKeyCount(),
                                             key_map.getKeys(),
                                             key_map.getCompressions(),
                                             key_map.getDedupsets(),
                                             cmd.sd_ptr.sd_name8,
                                             cmd.jni_index);

    if (cmd.type_of_dv_read == Validate.FLAG_PENDING_READ && rc == 0)
    {
      ErrorLog.plog("Pending key block at lba 0x%08x key 0x%02x is OK.",
                    cmd.cmd_lba, sd.dv_map.dv_get(cmd.cmd_lba) & 0x7f);
      common.ptod("Pending key block at lba 0x%08x key 0x%02x is OK.",
                  cmd.cmd_lba, sd.dv_map.dv_get(cmd.cmd_lba) & 0x7f);
    }



    /* If this was a corruption of a journal recovery pending read we must reread */
    /* the block and check again if the key had changed:                          */
    else if (cmd.type_of_dv_read == Validate.FLAG_PENDING_READ && rc == 60003)
    {
      //for (int i = 0; i < key_map.getKeyCount(); i++)
      //{
      //  common.ptod("key_map1: %08x %02x", cmd.cmd_lba, key_map.getKeys()[i]);
      //}

      /* BadKeyBlock can have modified the key. Get the new value: */
      /* If the block is now in error, or now unknown, skip:       */
      if (!key_map.storeDataBlockInfo(cmd.cmd_lba, cmd.cmd_xfersize, cmd.sd_ptr.dv_map) ||
          !key_map.anyDataToCompare())
      {
        rc = 0;
        return;
      }

      /* A changed key also requires a changed dedupset: */
      if (Dedup.isDedup())
        cmd.sd_ptr.dedup.dedupSdBlock(key_map, cmd);


      //for (int i = 0; i < key_map.getKeyCount(); i++)
      //{
      //  common.ptod("key_map2: %08x %02x", cmd.cmd_lba, key_map.getKeys()[i]);
      //}

      /* If BadKeyBlock told us to read the key block again, but now with */
      /* different key, do so:                                            */
      Byte flag = sd.dv_map.journal.before_map.pending_map.get(cmd.cmd_lba);
      if (flag == null)
        common.failure("Invalid pending map state");
      int pending_flag = flag & 0xff;
      if (pending_flag == DV_map.PENDING_KEY_REREAD)
      {
        // this one no longer makes sense to me ???
        if (HelpDebug.doAfterCount("corruptAfterPendingRead"))
          HelpDebug.corruptBlock(cmd.sd_ptr.fhandle, (int) cmd.cmd_xfersize, cmd.cmd_lba);

        /* Read the block again, corruptions will be thrown out by BadSector: */
        cmd.type_of_dv_read = Validate.FLAG_PENDING_REREAD;
        rc = Native.multiKeyReadAndValidateBlock(cmd.sd_ptr.fhandle,
                                                 data_flag | cmd.type_of_dv_read,
                                                 0, cmd.cmd_lba,
                                                 (int) cmd.cmd_xfersize,
                                                 read_buffer,
                                                 key_map.getKeyCount(),
                                                 key_map.getKeys(),
                                                 key_map.getCompressions(),
                                                 key_map.getDedupsets(),
                                                 cmd.sd_ptr.sd_name8,
                                                 cmd.jni_index);
      }
    }

    if (rc == 0)
      key_map.countRawReadAndValidates(cmd.sd_ptr, cmd.cmd_lba);
  }



  /**
   * Write with required data patterns, including DV.
   */
  private void patternWrite(Cmd_entry cmd)
  {
    /* Increment the keys so that we can write:      */
    /* (A block with an error will NOT be rewritten) */
    if (Validate.isValidate() && !key_map.incrementKeys())
    {
      common.where();
      return;
    }

    /* Write pre-keys to Journal file: */
    if (Validate.isJournaling())
      key_map.writeBeforeJournalImage();


    // long set = key_map.getDedupsets()[0];
    // //if (Dedup.isDuplicate(set))
    //   common.ptod("multiKeyWrite: %4d  %016x %08x %s", Dedup.getSet(set),
    //               set, cmd.cmd_lba, Dedup.xlate(set));

    /* If we do 'compression only' pass a zero buffer.              */
    /* This will inside of JNI be translated to writing from the    */
    /* data pattern buffer, avoiding constant copies to the buffer. */
    //long buffer = (!Validate.isValidate() && compression_only) ? 0 : write_buffer;

    // It appeared that with lots of threads I'd get too much Dedup.
    // however, even removing buffer=0 I still get the same problem
    // And this is also with compratio=n
    // in other words: something's wrong, big time!!
    //buffer = write_buffer;

    /* Note that even though this is called 'multi key', there may be only ONE: */
    long tod = (Validate.isRealValidate()) ? System.currentTimeMillis() : 0;
    rc = 0;


    //  for (int i = 0; i < key_map.getKeyCount(); i++)
    //  {
    //    common.ptod("key_map2: keys: %2d  %,16d %02x %016x", key_map.getKeyCount(),
    //                cmd.cmd_lba, key_map.getKeys()[i], key_map.getDedupsets()[i]);
    //  }

    /* When this triggers during a write of a duplicate, nobody will notice! */
    if (!HelpDebug.doAfterCount("skipWriteAfter"))
    {
      rc  = Native.multiKeyFillAndWriteBlock(cmd.sd_ptr.fhandle,
                                             tod,
                                             data_flag,               // int
                                             0,                       // file_start_lba
                                             cmd.cmd_lba,             // file_lba
                                             (int) cmd.cmd_xfersize,  // data_length
                                             key_map.pattern_lba,
                                             key_map.pattern_length,
                                             write_buffer,
                                             key_map.getKeyCount(),
                                             key_map.getKeys(),
                                             key_map.getCompressions(),
                                             key_map.getDedupsets(),
                                             cmd.sd_ptr.sd_name8,
                                             cmd.jni_index);
    }

    if (HelpDebug.doAfterCount("corruptAfterWrite"))
      HelpDebug.corruptBlock(cmd.sd_ptr.fhandle, (int) cmd.cmd_xfersize, cmd.cmd_lba);

    if (HelpDebug.doForLba("corruptLbaAfterWrite", cmd.cmd_lba, cmd.cmd_xfersize))
    {
      HelpDebug.corruptBlock(cmd.sd_ptr.fhandle, (int) cmd.cmd_xfersize, cmd.cmd_lba);
      common.failure("corruptLbaAfterWrite");
    }

    /* When the write failed writeAfterJournalImage() will skip the journal write: */
    /* Write post-keys to Journal: */
    if (Validate.isJournaling())
      key_map.writeAfterJournalImage();

    /* Count the number of key blocks written: */
    key_map.countRawWrites(cmd.sd_ptr, cmd.cmd_lba);
  }



  public long getPids()
  {
    return pid_and_lwp;
  }
  public void setStreamContext(StreamContext sc, int thread)
  {
    stream_context = sc;
    if (stream_context != null)
      common.ptod("StreamContext for thread=%02d; %s", thread, sc);
  }

  /**
  *
  */
  private void initialize()
  {
    /* Though technically there may not always be a need for a KeyMap, */
    /* it allows for more common code use:                             */
    if (Validate.isRealValidate() || Validate.isValidateForDedup())
    {
      if (ReplayInfo.isReplay())
        key_map = new KeyMap(0l, key_blocksize, ReplayDevice.getAllDevicesMaxXfersize());
      else
        key_map = new KeyMap(0l, key_blocksize, max_sd_xfersize);
    }

    else if (ReplayInfo.isReplay() && Validate.isValidateForDedup())
      key_map = new KeyMap(0l, key_blocksize, ReplayDevice.getAllDevicesMaxXfersize());

    else
      key_map = new KeyMap();

    if (common.onSolaris())
      pid_and_lwp = Native.getSolarisPids();

    /* Always allocate a read buffer. Write buffer only when needed: */
    allocated_buffer_size = max_sd_xfersize;

    /* Dedup, when not using multiples of dedupunit needs extra space:     */
    /* By doing TWICE the max xfersize we make sure the buffer is at least */
    /* a multiple of the dedupunit.                                        */
    if (Dedup.isDedup())
      allocated_buffer_size *= 2;
    //if (compression_only)
    //  allocated_buffer_size *= 2;

    if (ReplayInfo.isReplay())
    {
      allocated_buffer_size = ReplayDevice.getAllDevicesMaxXfersize();

      /* With Dedup we can get some crazy data patterns trying to align the */
      /* current xfersize with dedupunit. Just be generous here:            */
      if (Dedup.isDedup())
        allocated_buffer_size *=2;
    }

    read_buffer = Native.allocBuffer((int) allocated_buffer_size);
    if (doWeNeedWriteBuffer())
    {
      write_buffer = Native.allocBuffer((int) allocated_buffer_size);
      Patterns.storeStartingSdPattern(write_buffer, (int) allocated_buffer_size);
    }

    tmonitor = new ThreadMonitor("IO_task", (sd != null) ? sd.sd_name : "shared", null);

    /* Communicate status of task: */
    tn.task_set_start_complete();
    tn.waitForMasterGo();

    //common.ptod("Starting IO_task " + tn.task_name);
  }


  // This could be replaced by just having a boolean set when we are waiting for fifo?
  public int getActiveCount()
  {
    return ios_currently_active;
  }

  /**
   * Optionally save the last tod this block was read/written.
   * This of course is ONLY for Data Validation.
   */
  private void saveLastTod(long type)
  {
    if (!Validate.isStoreTime())
      return;
    if (rc == 0)
      key_map.saveTimestamp(type);
  }

  public static void main(String[] args)
  {
  }
}

