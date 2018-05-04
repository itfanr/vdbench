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

import java.io.*;
import java.util.*;
import Utils.Format;

/**
 * This class contains all data needed for a specific file name
 */
class FileEntry extends VdbObject implements Comparable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private Directory parent          = null;
  private long      req_file_size   = 0;       /* Requested file size      */
  private long      cur_file_size   = 0;       /* Current file size        */
  private int       file_no         = 0;
  private int       file_no_in_list = 0;
  private boolean   file_opened     = false;

  private boolean   file_exists     = false;
  private boolean   file_busy       = false;
  private boolean   bad_file        = false;   /* Any i/o error occurred   */
  private boolean   format_complete = false;
  private boolean   marked_busy     = false;
  private boolean   parent_busy     = false;

  private long      file_start_lba  = 0;       /* Used for Data Validation */
  private long      bad_bytes       = 0;       /* These two could be ints */
  private long      bad_blocks      = 0;

  private long      last_lba_used   = 0;



  private static boolean debug = common.get_debug(common.FILEENTRY_SET_BUSY);

  private static long queries = 0;
  private static long found   = 0;

  public FileEntry()
  {
  }

  /**
  * - parent: Parent Directory
  * - no:     relative file# within directory, starting with one.
  * - size:   requested file size
  * - lba:    relatiove LBA for Data Validation
  * - seqno:  relative file# within the file_list
  */
  public FileEntry(Directory parent_dir, int no, long size, long lba, int seqno)
  {
    parent          = parent_dir;
    file_no         = no;
    req_file_size   = size;
    file_start_lba  = lba;
    file_no_in_list = seqno;

    /* When we have a delete pending in a format run we don't need to */
    /* really understand the current status of the files since they */
    /* will be deleted anyway. */
    if (getAnchor().isDeletePending())
      return;

    /* We need to know if the file is already there:                      */
    /* This is needed so that we can make the proper decision to know if */
    /* either all files are there, or they are all gone.                  */
    if (parent.exist())
    {
      /* Can we take a shortcut? */
      if (getAnchor().getControlFile().hasFileStatus())
      {
        /**
         * -1: file does not exist.
         * -2: file exists and is full
         * nn: file exists with length 'nn'
         */
        long control_size = getAnchor().getControlFile().getFileSize(file_no_in_list, req_file_size);
        //common.ptod("cur_file_size: " + cur_file_size + " " + file_no_in_list);

        /* Does file exist? */
        if (control_size != -1)
        {
          file_exists = true;
          parent.countFiles(+1, this);
          cur_file_size = control_size;
          if (cur_file_size == req_file_size)
            getAnchor().countFullFiles(+1, this);
        }
      }

      /* there is no shortcut from ControlFile: use the file system to get status: */
      else
      {
        File file_ptr = new File(getName());
        file_exists   = parent.hasFile(getFileName());
        if (++queries % 1000000 == 0)
        {
          common.ptod("FileEntry queries: " + queries + " " + found + " " +
                      (found * 100 / queries) );

          // new FileEntrys are always created in directory order.
          // Would it be advisible to do a File.list() to avoid overhead?
        }

        if (file_exists)
        {
          /* Can not use setCurrentSize() for the first call: */
          cur_file_size = file_ptr.length();
          if (cur_file_size == req_file_size)
            getAnchor().countFullFiles(+1, this);

          parent.countFiles(+1, this);
          found++;
        }
      }
    }

    if (exists())
      getAnchor().countExistingFiles(+1, this);

    //common.ptod("Created FileEntry: " + getName() + " " + file_exists);
  }


  /**
   * Set file busy. If already busy, return false.
   */
  public synchronized boolean setBusy(boolean bool)
  {
    if (debug)
      common.ptod("FileEntry.setBusy: " + getName() + " " + file_busy + " ===> " + bool);

    if (bool && file_busy)
      return false;

    else if (!bool && !file_busy)
      common.failure("FileEntry.setBusy(false): entry not busy: " + getName());

    file_busy = bool;
    marked_busy = bool;

    return true;
  }
  public synchronized boolean setParentBusy(boolean bool)
  {
    boolean rc = parent.setBusy(bool);
    if (rc)
      parent_busy = bool;

    return rc;
  }

  public synchronized void cleanup()
  {
    if (marked_busy)
      setBusy(false);
    if (parent_busy)
    {
      parent.setBusy(false);
      parent_busy = false;
    }
  }

  public boolean isBusy()
  {
    return file_busy;
  }

  public boolean exists()
  {
    return file_exists;
  }
  public void setExists(boolean bool)
  {
    if (bool && file_exists)
      common.failure("setExists(): file already exists: " + getName());
    if (!bool && !file_exists)
      common.failure("setExists(): file already does not exist: " + getName());

    file_exists = bool;
  }

  public boolean isFull()
  {
    boolean rc = req_file_size == cur_file_size;
    //common.ptod("isFull(): " + getName() + " " + rc);
    return rc;
  }

  public void setOpened()
  {
    file_opened = true;
  }

  public boolean getOpened()
  {
    return file_opened;
  }

  public void setCurrentSize(long size)
  {
    /* If the size stays unchanged, just leave: */
    if (size == cur_file_size)
      return;

    /* If the new size is the required size, count it as full: */
    if (size == req_file_size)
      getAnchor().countFullFiles(+1, this);

    /* If the file used to be full, remove it from the count: */
    else if (cur_file_size == req_file_size)
      getAnchor().countFullFiles(-1, this);

    cur_file_size = size;
  }
  public long getCurrentSize()
  {
    return cur_file_size;
  }

  public void setBlockBad(int xfersize)
  {
    bad_bytes += xfersize;
    bad_blocks++;
    //common.ptod("bad_bytes: " + bad_bytes + " " + bad_blocks);

    if (bad_bytes > getCurrentSize() / 100)
    {
      String txt = "setBlockBad(): more than 1% of the file is marked bad. "+
                   "File no longer will be used: " + getName();
      ErrorLog.sendMessageToMaster(txt);
      setBadFile();
    }
    else if (bad_blocks > 100)
    {
      String txt = "setBlockBad(): more than 100 bad blocks in the file are marked bad. "+
                   "File no longer will be used: " + getName();
      ErrorLog.sendMessageToMaster(txt);
      setBadFile();
    }
  }
  private void setBadFile()
  {
    bad_file = true;
    ErrorLog.sendMessageToMaster("File marked bad: " + getName());
  }
  public boolean isBadFile()
  {
    //common.ptod("isBadFile: " + bad_file + " " + getName());
    return bad_file;
  }

  public long getReqSize()
  {
    return req_file_size;
  }
  public int getFileNoInList()
  {
    return file_no_in_list;
  }

  public void setFormatComplete(boolean bool)
  {
    format_complete = bool;
  }
  public boolean isFormatComplete()
  {
    return format_complete;
  }

  private static long count = 0;
  public String getName()
  {
    //if (++count % 1000 == 0)
    //{
    //  common.ptod("Debugging counter: " + count);
    //  common.where(8);
    //}

    /*
    String label = "" + req_file_size;
    if (req_file_size % GB == 0)
      label = (req_file_size / GB) + "g";

    else if (req_file_size % MB == 0)
      label = (req_file_size / MB) + "m";

    else if (req_file_size % KB == 0)
      label = (req_file_size / KB) + "k";
      */

    String fname = getFileName();

    return parent.getFullName() + fname;
  }

  public String getFileName()
  {
    /* Try to avoid the price of using Format.f(): */
    if (file_no < 10)
      return "vdb_f000" + file_no + ".file";
    else if (file_no < 100)
      return "vdb_f00" + file_no + ".file";
    else if (file_no < 1000)
      return "vdb_f0" + file_no + ".file";
    else
      return "vdb_f" + file_no + ".file";
  }

  public Directory getParent()
  {
    return parent;
  }
  public String getParentName()
  {
    return parent.getFullName();
  }
  public FileAnchor getAnchor()
  {
    return getParent().getAnchor();
  }
  public long getFileStartLba()
  {
    return file_start_lba;
  }

  public void setLastLba(long lba)
  {
    last_lba_used = lba;
  }
  public long getLastLba()
  {
    return last_lba_used;
  }


  public void deleteFile(FwgEntry fwg)
  {
    long start = Native.get_simple_tod();
    File file_ptr = new File(getName());
    if (!file_ptr.delete())
      common.failure("unable to delete file " + getName());
    FwdStats.count(Operations.DELETE, start);

    parent.countFiles(-1, this);
    setExists(false);
    setCurrentSize(0);

    fwg.blocked.count(Blocked.FILE_DELETES);
    getAnchor().countExistingFiles(-1, this);

    if (Validate.isValidate())
      getAnchor().allocateKeyMap(file_start_lba).clearMapForFile(req_file_size);
    //common.ptod("deleted: " + getName());
  }


  public int compareTo(Object obj)
  {
    FileEntry fe = (FileEntry) obj;
    return(int) getName().compareTo(fe.getName());
  }
  public String toString()
  {
    return "FileEntry: " + getName() + " busy: " + isBusy();
  }
}
