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

import java.util.Random;
import java.io.*;

/**
 * This class contains information for a file that is currently opened
 * for use.
 */
class ActiveFile extends VdbObject
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private FileEntry  fe             = null;
  private FwgEntry   active_fwg     = null;
  private FwgThread  calling_thread = null;
  private FwdStats   active_stats   = null;

  public  int        xfersize       = 0;
  public  int        prev_xfer      = 0;
  public  long       next_lba       = 0;
  private long       file_start_lba = 0;
  public  long       fhandle        = 0;
  public  long       blocks_done    = 0;
  public  long       bytes_done     = 0;
  public  long       bytes_to_do    = Long.MAX_VALUE;
  public  long       blocks_to_do   = Long.MAX_VALUE;
  public  boolean    done_enough    = false;

  private long       native_buffer  = 0;

  private String     parents        = null;
  private String     full_name      = null;
  private KeyMap     key_map        = null;

  private boolean    open_for_read;

  private static     Random seek_rand = new Random(0); // should this get a different seed?
  private static     boolean force_error_nowrite = common.get_debug(common.FORCE_ERROR_NOWRITE);
  private static     boolean force_error_noafter = common.get_debug(common.FORCE_ERROR_NOAFTER);

  private static boolean print_open_flags = common.get_debug(common.PRINT_OPEN_FLAGS);


  /**
   * Create an instance for a file that we're going to do things with.
   */
  public ActiveFile(FileEntry  fe_in, FwgEntry fwg_in, long buffer)
  {
    fe             = fe_in;
    active_fwg     = fwg_in;
    calling_thread = (FwgThread) Thread.currentThread();
    active_stats   = calling_thread.per_thread_stats;
    parents        = fe.getParentName();
    full_name      = fe.getName();
    file_start_lba = fe.getFileStartLba();
    native_buffer  = buffer;
    xfersize      = 0;
    prev_xfer     = 0;
    next_lba      = 0;
    blocks_done   = 0;
    bytes_done    = 0;

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
      common.failure("openfile(): Trying to open file that is already open");

    if (!fe.isBusy())
      common.failure("openfile(): Trying to open file that is not marked busy");

    /* Since AR can lose the contents of a file if during the destaging from */
    /* its cache the file system runs 'out of quota', we need to check the */
    /* file size each time we open it. Do it at read only: */
    // For now do not abort.
    if (open_for_read)
    {
      if (new File(full_name).length() != fe.getCurrentSize())
        common.ptod("openFile(): invalid file size. Expected: " +
                    FileAnchor.whatSize(fe.getCurrentSize()) +
                    "; found: " + FileAnchor.whatSize(new File(full_name).length()));
    }

    if (print_open_flags)
      common.ptod("openFile  flags: %s %s", active_fwg.open_flags, fe.getName());

    /* Determine how to open the file. SOL_CLEAR_CACHE requires 'open for write' */
    int open_for = (read) ? 0 : 1;
    if (active_fwg.open_flags.isOther(OpenFlags.SOL_CLEAR_CACHE))
      open_for = 1;

    /* Now open the file: */
    if ((fhandle = Native.openFile(full_name, active_fwg.open_flags, open_for)) < 0)
    {
      common.memory_usage();
      Native.printMemoryUsage();
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

    /* Data Validation keys need to be there if needed: */
    if (Validate.isValidate())
      key_map = fe.getAnchor().allocateKeyMap(file_start_lba);

    fe.setOpened();
    File_handles.addHandle(fhandle, this);

    /* When using 'stopafter' we start at the current file size: */
    /* (This simulates an append)                                */
    if (!SlaveWorker.work.format_run && active_fwg.sequential_io && active_fwg.stopafter != 0)
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
      common.failure("closing handle for a file that is not open: " + fe.getName());

    /* Remove the file handle BEFORE closing the file.                   */
    /* This prevents an other thread from reusing the same handle        */
    /* before I have the chance to remove it from the File_handles list: */
    File_handles.remove(fhandle);

    if (print_open_flags)
      common.ptod("closeFile flags: %s %s", active_fwg.open_flags, fe.getName());

    long start = Native.get_simple_tod();
    long rc = Native.closeFile(fhandle, active_fwg.open_flags);
    if (rc != 0)
      common.failure("File close failed: rc=" + rc + " " + full_name);
    active_fwg.blocked.count(Blocked.FILE_CLOSES);
    active_stats.count(Operations.CLOSE, start);

    fhandle = 0;

    /* Remember the file size for the next operations: */
    fe.setCurrentSize(new File(fe.getName()).length());

    /* If the file did not exist yet, mark it existent: */
    if (!fe.exists())
    {
      fe.setExists(true);
      fe.getParent().countFiles(+1, fe);
      fe.getAnchor().countExistingFiles(+1, fe);
    }

    /* Need to remember for sequential 'stopafter' how far we've come: */
    fe.setLastLba(next_lba + xfersize);

    /* If needed, delete the file right after closing it but befor unlock: */
    if (delete)
      fe.deleteFile(active_fwg);

    fe.setBusy(false);
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
        return false;

      /* If there is no room left return: */
      if (next_lba >= max_lba)
        return false;

      prev_xfer = xfersize;

      if (!Validate.isValidate())
        return true;

      /* Get the list of keys. Used for both read and writes: */
      if (key_map.getKeysFromMap(next_lba, xfersize))
      {
        /* During journal recover skip block if no data there: */
        if (Validate.isJournalRecoveryActive() && !key_map.preReadNeeded())
          continue;

        return true;
      }

      calling_thread.block(Blocked.SKIP_BAD_BLOCKS);
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

      if (Validate.isValidate())
      {
        /* Get the list of keys. used for both read and writes: */
        if (!key_map.getKeysFromMap(next_lba, xfersize))
        {
          calling_thread.block(Blocked.SKIP_BAD_BLOCKS);
          return false;
        }
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
      next_lba = blocks * xfersize;

      if (next_lba < 0)
        common.failure("setNextRandomLba(): negative lba: " +
                       next_lba + " " + max_lba + " " + xfersize);

      /* Shorten xfersize if needed: */
      doesBlockOrShorterBlockFit();

      if (!Validate.isValidate())
        return true;

      /* Get the list of keys to see if we have bad blocks: */
      if (key_map.getKeysFromMap(next_lba, xfersize))
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
    if (xfersize == 0)
      common.failure("zero xfersize for " + fe.getName() +
                     " lba: " + fe.getFileStartLba() + "/" + next_lba);

    /* Without DV just write the block: */
    if (!Validate.isValidate())
    {
      normalWrite();
      return;
    }

    /* There was a need to do Data Validation without re-reading before   */
    /* each new write. The added reads changed a workload so much that an */
    /* existing error never showed up. Beware that if we do that we will  */
    /* miss any possible lost writes!                                     */
    if (!Validate.isNoPreRead())
    {
      /* If there are any valid key blocks, read and validate the block first: */
      /* (A block with an error will NOT be reread) */
      preReadAndValidate();
    }

    /* Write the block with Data Valition keys: */
    dataValidationWrite();
  }


  /**
   * Normal write. No Data Validation active.
   */
  private void normalWrite()
  {
    long tod = Native.get_simple_tod();
    long rc = Native.writeFile(fhandle, next_lba, xfersize, native_buffer);
    if (rc != 0)
      writeError(rc);

    FwdStats.countXfer(Operations.WRITE, tod, xfersize);
    blocks_done   ++;
    bytes_done += xfersize;
  }


  /**
   * Data validation pre-read and validate.
   * A block, once marker in error, will never get to this point.
   */
  private void preReadAndValidate()
  {
    /* Get the DV keys: */
    int[] keys      = key_map.getKeys();
    int   key_count = key_map.getKeyCount();

    if (key_map.preReadNeeded())
    {
      //for (int i = 0; i < key_count; i++)
      //  common.ptod("preReadAndValidate: lba: %08x %2d", next_lba, keys[i]);

      //for (int i = 0; i < keys.length; i++)
      //  common.ptod("preReadAndValidate: " + i + " " + keys[i]);
      long tod = Native.get_simple_tod();
      long rc  = Native.readAndValidateBlock(fhandle,
                                             file_start_lba,
                                             next_lba,
                                             xfersize,
                                             native_buffer,
                                             key_count, keys,
                                             active_fwg.anchor.fsd_name_8bytes);

      if (rc != 0)
        writeError(rc);
      FwdStats.countXfer(Operations.READ, tod, xfersize);
      //blocks_done ++;
      //bytes_done += xfersize;

      /* Count the number of key blocks read and validated: */
      key_map.countReadAndValidates();

      /* Store timestamp of last successful read: */
      key_map.saveTimestamp(true);
    }
  }


  /**
   * Write a block containing Data Validation key vales.
   */
  private void dataValidationWrite()
  {
    long rc = 0;

    /* For debugging, try to see if we need to force an error: */
    boolean force_error   = false;
    if (Validate.attemptForcedError())
      force_error = Validate.forceError(null, fe, next_lba, xfersize);

    /* Increment the keys so that we can write:      */
    /* (A block with an error will NOT be rewritten) */
    if (!key_map.incrementKeys())
    {
      calling_thread.block(Blocked.SKIP_WRITE);
      return;
    }


    /* Get the DV keys: */
    int[] keys      = key_map.getKeys();
    int   key_count = key_map.getKeyCount();

    /* Write pre-keys to Journal file: */
    if (Validate.isJournaling())
      key_map.writeBeforeJournalImage();

    long tod = Native.get_simple_tod();
    if (force_error && !SlaveWorker.work.format_run)
    {
      String txt = String.format("Forced error: bypassed write. file=%s "+
                                 "lba=0x%08x, xfersize=%d, key=0x%02x",
                                 fe.getName(), next_lba , xfersize, keys[0]);
      ErrorLog.sendMessageToMaster(txt);
    }

    else
    {
      rc  = Native.fillAndWriteBlock(fhandle,
                                     file_start_lba,
                                     next_lba,
                                     xfersize,
                                     native_buffer,
                                     key_count, keys,
                                     active_fwg.anchor.fsd_name_8bytes);
    }

    /* Write post-keys to Journal: */
    if (Validate.isJournaling())
    {
      if (force_error && !SlaveWorker.work.format_run && force_error_noafter)
        common.failure("Forced error: bypassed journal after. Lba: " + file_start_lba);
      else
        key_map.writeAfterJournalImage();
    }

    if (rc != 0)
      writeError(rc);
    FwdStats.countXfer(Operations.WRITE, tod, xfersize);
    blocks_done   ++;
    bytes_done += xfersize;

    /* Count the number of key blocks written: */
    key_map.countWrites();

    /* After a write is done update the keys in the DV_map: */
    key_map.storeKeys();

    /* Store timestamp of last successful write: */
    key_map.saveTimestamp(false);

    /* Make sure the data is OK right now? */
    if (Validate.isImmediateRead())
    {
      readBlock();
      key_map.saveTimestamp(true);
    }
  }


  private void writeError(long rc)
  {
    String txt = "";
    txt += "Error writing file " + fe.getName();
    txt += "\nError:          " + Errno.xlate_errno(rc);
    txt += "\nlba:            " + next_lba;
    txt += "\nxfersize:       " + xfersize;
    txt += "\nblocks_done:    " + blocks_done;
    txt += "\nbytes_done:     " + bytes_done;
    txt += "\nopen_for_read: " + open_for_read;
    //ErrorLog.sendMessageToMaster(txt);
    common.failure(txt);
  }


  /**
   * Without DV, just read the block.
   * With DV, read and validated.
   */
  protected void readBlock()
  {
    if (next_lba + xfersize > fe.getCurrentSize())
      common.failure(String.format("Trying to read beyond EOF: %s 0x%08x (%d) %d %d",
                                   fe.getName(), next_lba, next_lba, fe.getCurrentSize(),
                                   fe.getReqSize()));

    if (!Validate.isValidate())
    {
      long tod = Native.get_simple_tod();
      long rc  = Native.readFile(fhandle, next_lba, xfersize, native_buffer);
      if (rc != 0)
        readError(rc);
      FwdStats.countXfer(Operations.READ, tod, xfersize);
      blocks_done   ++;
      bytes_done += xfersize;
    }

    else
    {
      int[] keys      = key_map.getKeys();
      int   key_count = key_map.getKeyCount();
      long  tod       = Native.get_simple_tod();
      long  rc = Native.readAndValidateBlock(fhandle,
                                             file_start_lba,
                                             next_lba,
                                             xfersize,
                                             native_buffer,
                                             key_count,
                                             key_map.getKeys(),
                                             active_fwg.anchor.fsd_name_8bytes);

      if (rc != 0)
        readError(rc);
      FwdStats.countXfer(Operations.READ, tod, xfersize);
      blocks_done ++;
      bytes_done += xfersize;

      /* Count the number of key blocks read and validated: */
      key_map.countReadAndValidates();

      /* Store timestamp of last successful read: */
      key_map.saveTimestamp(true);
    }
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

  private void readError(long rc)
  {
    String txt = "";
    txt += "Error reading file " + fe.getName();
    txt += "\nError: " + Errno.xlate_errno(rc);
    txt += "\nlba:      " + next_lba;
    txt += "\nxfersize: " + xfersize;
    common.failure(txt);
  }
}
