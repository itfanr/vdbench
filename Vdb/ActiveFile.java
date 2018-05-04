package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.HashMap;
import java.util.Random;

/**
 * This class contains information for a file that is currently opened
 * for use.
 */
class ActiveFile
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private FileEntry  fe             = null;
  private FileAnchor anchor         = null;
  private FwgEntry   active_fwg     = null;
  private FwgThread  calling_thread = null;
  private FwdStats   active_stats   = null;

  public  int        xfersize       = 0;
  private int        prev_xfer      = 0;
  public  long       next_lba       = 0;
  private long       file_start_lba = 0;
  private long       fhandle        = 0;
  private long       high_write_lba = 0;

  private long       blocks_done    = 0;
  private long       bytes_done     = 0;
  private long       bytes_to_do    = Long.MAX_VALUE;
  private long       blocks_to_do   = Long.MAX_VALUE;
  public  boolean    done_enough    = false;

  private long       native_read_buffer  = 0;
  private long       native_write_buffer = 0;

  private String     full_name      = null;
  private KeyMap     key_map        = null;

  private boolean    open_for_read;

  private int        data_flag;


  /* This does not need to be a static randomizer. Small tests show no big difference. */
  /* So just leave it. */
  private static     Random seek_rand = new Random();

  private static boolean print_open_flags = common.get_debug(common.PRINT_OPEN_FLAGS);


  /**
   * Create an instance for a file that we're going to do things with.
   */
  public ActiveFile(FileEntry  fe_in, FwgEntry fwg_in, long rbuf, long wbuf)
  {
    fe                  = fe_in;
    anchor              = fe.getAnchor();
    active_fwg          = fwg_in;
    calling_thread      = (FwgThread) Thread.currentThread();
    active_stats        = calling_thread.per_thread_stats;
    full_name           = fe.getFullName();
    file_start_lba      = fe.getFileStartLba();
    native_read_buffer  = rbuf;
    native_write_buffer = wbuf;
    xfersize            = 0;
    prev_xfer           = 0;
    next_lba            = 0;
    blocks_done         = 0;
    bytes_done          = 0;
    data_flag           = Validate.createDataFlag();

    /* For 'stopafter=nn', set the block or byte count we can do: */
    if (!SlaveWorker.work.format_run)
    {
      /* By default stopafter is set to 100 AND the file size.                 */
      /* This prevents an 8k file to have his one block read 100 times, unless */
      /* specifically requested.                                               */
      if (fwg_in.stopafter == Long.MAX_VALUE)
      {
        bytes_to_do  = fe.getReqSize();
        blocks_to_do = fwg_in.stopafter;
      }

      /* For 'stopafter=nn', set the block or byte count we can do: */
      else if (fwg_in.stopafter < 0)
        bytes_to_do = fe.getReqSize() * (fwg_in.stopafter * -1) / 100;
      else if (fwg_in.stopafter > 0)
        blocks_to_do = fwg_in.stopafter;

      //common.ptod("bytes_to_do: " + bytes_to_do);
      //common.ptod("blocks_to_do: " + blocks_to_do);
    }
  }


  public FileEntry getFileEntry()
  {
    return fe;
  }
  public KeyMap getKeyMap()
  {
    return key_map;
  }
  public FwgEntry getFwg()
  {
    return active_fwg;
  }
  public long getHandle()
  {
    return fhandle;
  }
  public FileAnchor getAnchor()
  {
    return anchor;
  }

  /**
   * Open a file for processing.
   *
   * Note: open/close gives 8 getattrs and 7 access
   */
  protected void openFile(boolean read)
  {
    /* Remember why we opened: */
    open_for_read = read;

    if (fhandle !=0)
      common.failure("openfile(): Trying to open file that is already open: %s",
                     full_name);

    if (!fe.isBusy())
      common.failure("openfile(): Trying to open file that is not marked busy: %s" ,
                     full_name);

    /* Since AR can lose the contents of a file if during the destaging from */
    /* its cache the file system runs 'out of quota', we need to check the */
    /* file size each time we open it. Do it at read only: */
    // For now do not abort.
    if (open_for_read)
    {
      File fptr = new File(full_name);
      if (fptr.exists() && fptr.length() != fe.getCurrentSize())
        common.ptod("openFile(): invalid file size. Expected: " +
                    FileAnchor.whatSize(fe.getCurrentSize()) +
                    "; found: " + FileAnchor.whatSize(new File(full_name).length()));
    }

    if (print_open_flags)
      common.ptod("openFile  flags: %s %s", active_fwg.open_flags, full_name);

    /* Now open the file: */
    if ((fhandle = Native.openFile(full_name, active_fwg.open_flags, (read) ? 0 : 1)) < 0)
    {
      //common.memory_usage();
      Native.printMemoryUsage();

      if (common.get_debug(common.DIRECTORY_CREATED))
        anchor.printFileStatus();

      /* The logic needed to allow data_errors=nn for an open failure is more */
      /* than I am willing to handle right now. TBD                           */
      common.failure("open failed for " + full_name);
    }

    /* If the file is empty, or we don't know the size yet, don't clear cache */
    /* (If the file exists and is not empty we SHOULD have the size already,  */
    /* but just playing it safe here)                                         */
    if (fe.getCurrentSize() > 0 && active_fwg.open_flags.isOther(OpenFlags.SOL_CLEAR_CACHE))
    {
      int rc = Native.eraseFileSystemCache(fhandle, fe.getCurrentSize());
      if (rc != 0)
        common.failure("Native.eraseFileSystemCache() failed: " + rc);
    }

    /* Keys are always needed: */
    if (Validate.isRealValidate() || Dedup.isDedup())
      key_map = anchor.allocateKeyMap(file_start_lba);
    else
      key_map = new KeyMap();

    fe.setOpened();
    File_handles.addHandle(fhandle, this);

    /* When using 'stopafter' we start at the current file size: */
    /* (This simulates an append)                                */
    if (!SlaveWorker.work.format_run && active_fwg.sequential_io && active_fwg.stopafter != Long.MAX_VALUE)
    {
      if (fe.getCurrentSize() != fe.getReqSize())
        next_lba = fe.getCurrentSize();
      else
      {
        if (fe.getLastLba() >= fe.getCurrentSize())
          next_lba = 0;
        else
          next_lba = fe.getLastLba();
      }
      //common.ptod("fe.getCurrentSize(): " + fe.getCurrentSize() + " " + fe.getReqSize() + " " + next_lba);
    }
  }


  /**
   * Close file.
   * This CAN include a file handle close if file was used for reads or writes.
   *
   * closeFile() returns 'null' to accomodate clearing using 'afe = closeFile()'
   */
  public static void conditionalCloseFile(ActiveFile af)
  {
    if (af == null)
      return;
    if (af.fhandle != 0)
      af.closeFile();
  }


  public ActiveFile closeFile()
  {
    return closeFile(false);
  }
  public ActiveFile closeFile(boolean delete)
  {
    if (fhandle == 0)
      common.failure("closing handle for a file that is not open: " + full_name);

    /* Remove the file handle BEFORE closing the file.                   */
    /* This prevents an other thread from reusing the same handle        */
    /* before I have the chance to remove it from the File_handles list: */
    File_handles.remove(fhandle);

    if (print_open_flags)
      common.ptod("closeFile flags: %s %s", active_fwg.open_flags, full_name);

    long start = Native.get_simple_tod();
    long rc = Native.closeFile(fhandle, active_fwg.open_flags);
    if (rc != 0)
      fileError("close failure", rc);
    active_fwg.blocked.count(Blocked.FILE_CLOSES);
    active_stats.count(Operations.CLOSE, start);

    fhandle = 0;

    /* Remember the file size for the next operations: */
    fe.setCurrentSize(new File(full_name).length());

    /* If the file did not exist yet, mark it existent: */
    if (!fe.exists())
    {
      fe.setExists(true);
      fe.getParent().countFiles(+1, fe);
      anchor.countExistingFiles(+1, fe);
      active_fwg.blocked.count(Blocked.FILE_CREATES);
    }

    /* Need to remember for sequential 'stopafter' how far we've come: */
    fe.setLastLba(next_lba + xfersize);

    /* If needed, delete the file right after closing it but before unlock: */
    if (delete)
      fe.deleteFile(active_fwg);

    fe.setUnBusy();
    return null;
  }



  /**
   * Get the next sequential lba.
   * If this is for a journal recovery only return those blocks in a file
   * that have anything written to it. The result can be that a file gets
   * not read at all!
   * It means that the file is opened without any reason! Should fix that,
   * though DV and Journal recovery does not have to be really fast.
   *
   * If any piece of the block portion is in error the whole block will be
   * skipped.
   */
  public boolean setNextSequentialRead()
  {
    return setNextSequentialLba(true);
  }
  public boolean setNextSequentialWrite()
  {
    return setNextSequentialLba(false);
  }
  private boolean setNextSequentialLba(boolean read)
  {
    long max_lba = (read) ? fe.getCurrentSize() : fe.getReqSize();

    while (true)
    {
      /* Add previous xfersize to lba: */
      next_lba += prev_xfer;

      /* If there is no room whatsoever, we're done: */
      if (!doesBlockOrShorterBlockFit())
      {
        /* At eof after pending writes, clear the flag: */
        if (fe.pending_writes)
          fe.pending_writes = false;

        return false;
      }

      /* If there is no room left return: */
      if (next_lba >= max_lba)
        return false;

      prev_xfer = xfersize;


      /* Get the list of keys. Used for both read and writes.             */
      /* if ANY portion of the block is bad, we'll skip it and try again: */
      if (!key_map.storeDataBlockInfo(next_lba, xfersize, anchor.getDVMap()))
      {
        calling_thread.block(Blocked.SKIP_BAD_BLOCKS);
        continue;
      }

      /* If this file has pending writes from journal recovery           */
      /* read the blocks as found in the pending map:                    */
      /* BadDataBlock then will decide whether this block is good or bad */
      if (fe.pending_writes)
      {
        HashMap pending_lbas = anchor.pending_file_lba_map.get(fe);

        /* Block not pending, skip: */
        if (pending_lbas.get(next_lba) == null)
          continue;

        ErrorLog.plog("Verify pending write for %s lba 0x%08x", full_name,next_lba );
      }

      return true;
    }
  }


  public boolean setNextRandomLba()
  {
    long max_lba = (open_for_read) ? fe.getCurrentSize() : fe.getReqSize();
    int attempts = 0;

    /* If the file is too small to handle this block, return lba 0: */
    if (xfersize > max_lba)
    {
      next_lba = 0;

      /* Shorten xfersize if needed: */
      doesBlockOrShorterBlockFit();

      /* Get the list of keys. used for both read and writes: */
      if (!key_map.storeDataBlockInfo(next_lba, xfersize, anchor.getDVMap()))
      {
        calling_thread.block(Blocked.SKIP_BAD_BLOCKS);
        return false;
      }

      return true;
    }


    while (true)
    {
      /* If we tried this 100 times, then just forget about this file: */
      if (attempts++ > 100)
      {
        calling_thread.block(Blocked.SKIP_BAD_FILE);
        return false;
      }

      /* Generate LBA on an xfersize boundary: */
      long blocks = max_lba / xfersize;
      blocks     *= seek_rand.nextDouble();
      next_lba    = blocks * xfersize;

      if (next_lba < 0)
        common.failure("setNextRandomLba(): negative lba: " +
                       next_lba + " " + max_lba + " " + xfersize + " " + attempts);

      /* Shorten xfersize if needed: */
      doesBlockOrShorterBlockFit();

      /* Get the list of keys to see if we have bad blocks: */
      if (key_map.storeDataBlockInfo(next_lba, xfersize, anchor.getDVMap()))
        return true;

      calling_thread.block(Blocked.SKIP_BAD_BLOCKS);
    }
  }


  private boolean doesBlockOrShorterBlockFit()
  {
    long max_lba = (open_for_read) ? fe.getCurrentSize() : fe.getReqSize();

    /* If the next block does not fit anymore then adjust xfersize: */
    if (next_lba + xfersize >= max_lba)
      xfersize = (int) (max_lba - next_lba);

    /* If there is no room whatsoever, we're done: */
    if (xfersize == 0)
      return false;

    return true;
  }


  /**
   * Without DV, just write the block.
   * With DV, if any of the key blocks have a nonzero key, read the block
   * and validate.
   */

  protected void writeBlock()
  {
    if (native_write_buffer == 0)
      common.failure("No write buffer available");

    if (xfersize == 0)
      common.failure("zero xfersize for %s %d %d %d",
                     full_name, fe.getFileStartLba(), next_lba, fe.getCurrentSize());

    if (HelpDebug.doAfterCount("simulate_fsd_write_error"))
    {
      fileError("Debugging forced error", 7777);
      return;
    }

    /* Keep track of last byte written. This is needed if we want to   */
    /* reread this block before the file is closed (only at close time */
    /* is the current size of the file stored)                         */
    high_write_lba = Math.max(high_write_lba, next_lba + xfersize);

    if (Validate.isRealValidate() || Validate.isValidateForDedup())
    {
      /* There was a need to do Data Validation without re-reading before   */
      /* each new write. The added reads changed a workload so much that an */
      /* existing error never showed up.                                    */
      if (!Validate.isNoPreRead())
      {
        /* If there are any valid key blocks, read and validate the block first: */
        /* (A block with an error will NOT be reread)                            */
        if (key_map.anyDataToCompare())
        {
          readAndValidate(Validate.FLAG_PRE_READ);
          key_map.saveTimestamp(Timestamp.PRE_READ);
        }
      }

      /* Write the block with Data Valition keys: */
      dataValidationWrite();
      key_map.saveTimestamp(Timestamp.WRITE);

      /* Make sure the data is OK right now? */
      if (Validate.isImmediateRead())
      {
        readAndValidate(Validate.FLAG_READ_IMMEDIATE);
        key_map.saveTimestamp(Timestamp.READ_IMMED);
      }
    }

    /* Without DV/Dedup just write the block: */
    else
    {
      /* Compression only? writeWithPattern() uses a 'fake' KeyMap: */
      if (Validate.isCompression())
      {
        writeWithPattern();
      }

      /* No compression pattern; just a 'regular' write: */
      else
      {
        //common.failure("This should be obsolete");
        long tod = Native.get_simple_tod();
        long rc  = Native.noDedupAndWrite(fhandle, next_lba, xfersize,
                                          native_write_buffer, -1);
        FwdStats.countXfer(Operations.WRITE, tod, xfersize);
        blocks_done ++;
        bytes_done  += xfersize;

        if (rc != 0)
        {
          fileError("Write error", rc);
          return;
        }

      }
    }
  }


  /**
   * Write after storing a data pattern.
   */
  private boolean writeWithPattern()
  {
    if (Dedup.isDedup())
      anchor.dedup.dedupFsdBlock(this);

    else if (Validate.isCompression())
      key_map.setFsdCompressionOnlyOffset(this);

    if (key_map.pattern_length == 0)
      common.failure("zero pattern length");

    long start_tod = Native.get_simple_tod();
    long tod       = (Validate.isRealValidate()) ? System.currentTimeMillis() : 0;
    long rc  = Native.multiKeyFillAndWriteBlock(fhandle,
                                                tod,
                                                data_flag,
                                                file_start_lba,
                                                next_lba,
                                                xfersize,
                                                key_map.pattern_lba,
                                                key_map.pattern_length,
                                                native_write_buffer,
                                                key_map.getKeyCount(),
                                                key_map.getKeys(),
                                                key_map.getCompressions(),
                                                key_map.getDedupsets(),
                                                anchor.fsd_name_8bytes, -1);
    if (rc != 0)
    {
      fileError("Write error", rc);
      return false;
    }

    if (HelpDebug.doAfterCount("corruptAfterWrite"))
    {
      common.ptod("next_lba: %08x", next_lba);
      HelpDebug.corruptBlock(fhandle, xfersize, next_lba);
      common.failure("corruptAfterWrite");
    }


    if (anchor.dedup != null)
      anchor.dedup.countDedup(key_map, anchor.getValidationMap());

    FwdStats.countXfer(Operations.WRITE, start_tod, xfersize);
    blocks_done   ++;
    bytes_done += xfersize;

    return true;
  }


  /**
   * Normal write. No changed to data pattern needed at this time, though it may
   * have just been overlaid with an LFSR pattern.
   * This write is for anything without DV, Dedup or Compression.
   */
  private void obsolete_writeBuffer()
  {
    long tod = Native.get_simple_tod();
    common.failure("This should be obsolete");
    long rc  = Native.noDedupAndWrite(fhandle, next_lba, xfersize,
                                      native_write_buffer, -1);

    FwdStats.countXfer(Operations.WRITE, tod, xfersize);
    blocks_done   ++;
    bytes_done += xfersize;

    if (rc != 0)
      fileError("WriteBuffer error", rc);
  }


  private void readAndValidate(int type_of_dv_read)
  {
    if (Dedup.isDedup())
      anchor.dedup.dedupFsdBlock(this);
    else if (Validate.isCompression())
      key_map.setFsdCompressionOnlyOffset(this);

    if (native_read_buffer == 0)
      common.failure("Read buffer is missing");

    long tod = Native.get_simple_tod();
    long rc  = Native.multiKeyReadAndValidateBlock(fhandle,
                                                   data_flag | type_of_dv_read,
                                                   file_start_lba,
                                                   next_lba,
                                                   xfersize,
                                                   native_read_buffer,
                                                   key_map.getKeyCount(),
                                                   key_map.getKeys(),
                                                   key_map.getCompressions(),
                                                   key_map.getDedupsets(),
                                                   anchor.fsd_name_8bytes, -1);

    /* A corruption reported during journal recovery 'read pending write' */
    /* will be checked again: */
    if (rc == 60003 && type_of_dv_read == Validate.FLAG_PENDING_READ)
    {
      DV_map dv_map = anchor.getValidationMap();
      Byte flag = dv_map.journal.before_map.pending_map.get(file_start_lba + next_lba);
      if (flag == null)
        common.failure("Invalid pending map state");

      /* Async BadKeyBlock may have asked to reread the block since it changed the key: */
      int pending_flag = flag & 0xff;
      if (pending_flag == DV_map.PENDING_KEY_REREAD)
      {
        /* Asynchronously BadSector may have reset the keys, so get them again: */
        if (!key_map.storeDataBlockInfo(next_lba, xfersize, anchor.getDVMap()))
          common.failure("Block should not be in error");

        ErrorLog.plog("Re-checking file %s lba 0x%08x because of PENDING_KEY_REREAD",
                      full_name, next_lba);

        if (HelpDebug.doAfterCount("corruptAfterPendingRead"))
          HelpDebug.corruptBlock(fhandle, xfersize, next_lba);

        rc = Native.multiKeyReadAndValidateBlock(fhandle,
                                                 data_flag | Validate.FLAG_PENDING_REREAD,
                                                 file_start_lba,
                                                 next_lba,
                                                 xfersize,
                                                 native_read_buffer,
                                                 key_map.getKeyCount(),
                                                 key_map.getKeys(),
                                                 key_map.getCompressions(),
                                                 key_map.getDedupsets(),
                                                 anchor.fsd_name_8bytes, -1);
        if (rc == 0)
          ErrorLog.plog("Re-checking file %s lba 0x%08x successful",
                        full_name, next_lba);
      }
    }


    FwdStats.countXfer(Operations.READ, tod, xfersize);

    /* Count the number of key blocks read and validated: */
    key_map.countFileReadAndValidates(fe, next_lba);

    /* The data is 100%, but for debugging we can mess it up again: */
    if (HelpDebug.doAfterCount("forceFsdCorruptions"))
      HelpDebug.forceFsdCorruptions(fhandle,
                                    data_flag | type_of_dv_read,
                                    file_start_lba,
                                    next_lba,
                                    xfersize,
                                    native_read_buffer,
                                    active_fwg.getMaxXfersize(),
                                    key_map,
                                    anchor.fsd_name_8bytes);

    /* Any real i/o error, not DV error will cause an abort: */
    if (rc != 0 && rc != 60003)
      fileError("readAndValidate error", rc);
  }


  /**
   * Write a block containing Data Validation key values.
   */
  private void dataValidationWrite()
  {
    if (Dedup.isDedup())
      anchor.dedup.dedupFsdBlock(this);

    if (Validate.isRealValidate() || Validate.isValidateForDedup())
    {
      /* Increment the keys so that we can write:      */
      /* (A block with an error will NOT be rewritten) */
      if (!key_map.incrementKeys())
      {
        calling_thread.block(Blocked.SKIP_WRITE);
        return;
      }
    }

    /* Write pre-keys to Journal file: */
    if (Validate.isJournaling())
      key_map.writeBeforeJournalImage();

    if (!writeWithPattern())
      return;

    /* Write post-keys to Journal: */
    if (Validate.isJournaling())
      key_map.writeAfterJournalImage();

    /* Count the number of key blocks written: */
    key_map.countFileWrites(fe, next_lba);

    /* After a write is done update the keys in the DV_map: */
    key_map.storeKeys();
  }


  /**
   * Error using a file.
   * Currently called for read or write i/o error and a close error.
   *
   * The objective was to also include 'open' errors, but the logic to ignore
   * the file and look for an other one was something I did not want to deal
   * with (yet?).
   */
  private void fileError(String label, long rc)
  {
    String txt = "";
    txt += label + " using file " + full_name;
    txt += "\nError:         " + Errno.xlate_errno(rc);
    txt += "\nlba:           " + next_lba;
    txt += "\nxfersize:      " + xfersize;
    txt += "\nblocks_done:   " + blocks_done;
    txt += "\nbytes_done:    " + bytes_done;
    txt += "\nopen_for_read: " + open_for_read;
    txt += "\nfhandle:       " + fhandle;

    /* Keep track of the amount of errors (code will abort if needed): */
    common.ptod(txt);

    /* Give some time for the above ptod() to arrive on the master before */
    /* a possible abort: */
    common.sleep_some(100);
    ErrorLog.countErrorsOnSlave(full_name, next_lba, (int) xfersize);

  }


  /**
   * Without DV, just read the block.
   * With DV, read and validate.
   * If we are reading a block whose keys are all zeros, the block will not be
   * compared at all.
   */
  protected void readBlock()
  {
    if (native_read_buffer == 0)
      common.failure("No read buffer available");

    /* If the file is not full we better have just written the */
    /* block and the close() has not happened yet: */
    if (next_lba + xfersize > fe.getCurrentSize() &&
        next_lba + xfersize > high_write_lba)
      common.failure("Trying to read beyond EOF: %s 0x%08x (%d) %d %d",
                     full_name, next_lba, next_lba, fe.getCurrentSize(),
                     fe.getReqSize());

    if (Validate.isRealValidate())
    {
      if (fe.pending_writes)
        readAndValidate(Validate.FLAG_PENDING_READ);
      else
        readAndValidate(Validate.FLAG_NORMAL_READ);
      key_map.saveTimestamp(Timestamp.READ_ONLY);
    }

    else
    {
      long tod = Native.get_simple_tod();
      long rc  = Native.readFile(fhandle, next_lba, xfersize,
                                 native_read_buffer);
      FwdStats.countXfer(Operations.READ, tod, xfersize);

      if (rc != 0)
        fileError("Read error", rc);
    }

    blocks_done ++;
    bytes_done  += xfersize;
  }


  /**
   * Determine if we've done enough i/o.
   */
  public void checkEnough()
  {
    if (bytes_done >= bytes_to_do)
      done_enough = true;

    else if (blocks_done >= blocks_to_do)
      done_enough = true;

    //common.ptod("bytes_done: " + bytes_done + " " +
    //            bytes_to_do + " " + blocks_done + " " + blocks_to_do + " " + done_enough);
  }
}
