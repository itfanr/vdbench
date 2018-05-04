package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.Format;
import Utils.Fput;

/**
 * This class contains all data needed for a specific file name
 */
public class FileEntry implements Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Directory parent          = null;
  private long      req_file_size   = 0;       /* Requested file size      */
  private long      cur_file_size   = 0;       /* Current file size        */
  private int       file_no         = 0;
  private int       file_no_in_list = 0;       /* Used for copy/move       */
  private int       use_count       = 0;
  private boolean   file_opened     = false;
  private boolean   file_selected   = false;

  private boolean   file_exists     = false;
  private boolean   file_busy       = false;
  private boolean   bad_file        = false;   /* Any i/o error occurred   */
  private boolean   format_complete = false;
  private boolean   marked_busy     = false;
  private boolean   parent_busy     = false;
  private boolean   file_copied     = false;

  public  boolean   pending_writes  = false;   /* During journal recovery  */

  private long      file_start_lba  = 0;       /* Used for Data Validation */
  private int       bad_bytes       = 0;
  private char      bad_blocks      = 0;

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

      else if (getAnchor().format_complete_used)
      {
        file_exists   = true;
        cur_file_size = req_file_size;
        getAnchor().countFullFiles(+1, this);
        parent.countFiles(+1, this);
      }

      /* there is no shortcut from ControlFile: use the file system to get status: */
      else
      {
        File file_ptr = new File(getFullName());
        file_exists   = parent.hasFile(getShortName());
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
  public synchronized boolean setFileBusy()
  {
    if (debug)
      common.ptod("setFileBusy: " + getFullName() + " " + file_busy);

    /* The old fashioned way: just one user: */
    if (!getAnchor().fileSharing())
    {
      if (file_busy)
        return false;
      file_busy = true;
      return true;
    }

    /* Sharing files, keep track of use count: */
    use_count++;
    file_busy = true;
    return true;
  }

  /**
   * Set Exclusive busy, no oher users.
   * In spite of allowing file sharing, some times we don't allow it, like when
   * building the file tables or when we want to delete a file.
   *
   * (What happens when we share --creating-- of a file?
   * Should technically work, but is rediculous.
   * Oh well, the user asks for it.
   */
  public synchronized boolean setFileBusyExc()
  {
    if (debug)
      common.ptod("setFileBusyExc: " + getFullName() + " " + file_busy);
    //common.ptod("getAnchor().fileSharing(): " + getAnchor().fileSharing());

    if (file_busy)
      return false;
    file_busy = true;
    use_count = 1;
    return true;
  }

  public synchronized void setUnBusy()
  {
    if (debug)
      common.ptod("setUnBusy:   " + getFullName() + " " + file_busy);

    if (!file_busy)
      common.failure("setUnBusy(false): entry not busy: " + getFullName());

    /* The old fashioned way: just one user: */
    if (!getAnchor().fileSharing())
    {
      file_busy = false;
      if (parent_busy)
      {
        parent.setBusy(false);
        parent_busy = false;
      }
    }

    else
    {
      /* Sharing file, keep track of use count: */
      if (--use_count == 0)
      {
        file_busy = false;
        if (parent_busy)
        {
          parent.setBusy(false);
          parent_busy = false;
        }
      }
    }
  }

  private synchronized boolean obsolete_setBusy(boolean bool)
  {
    if (debug)
      common.ptod("FileEntry.setBusy: " + getFullName() + " " + file_busy + " ===> " + bool);

    if (bool && file_busy)
      return false;

    else if (!bool && !file_busy)
      common.failure("FileEntry.setBusy(false): entry not busy: " + getFullName());

    file_busy   = bool;
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
    // 4/8/11: 'marked_busy' is clearly obsolete!
    //if (marked_busy)
    setUnBusy();
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
      common.failure("setExists(): file already exists: " + getFullName());
    if (!bool && !file_exists)
      common.failure("setExists(): file already does not exist: " + getFullName());

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

  public void setSelected()
  {
    file_selected = true;
  }

  public boolean isSelected()
  {
    return file_selected;
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

    if (bad_file)
      return;

    if (bad_bytes > getReqSize() / 100)
    {
      String txt = "setBlockBad(): more than 1%% of the file is marked bad. "+
                   "File no longer will be used: " + getFullName();
      ErrorLog.ptod(txt);
      setBadFile();
    }
    else if (bad_blocks > 100)
    {
      String txt = "setBlockBad(): more than 100 bad blocks in the file are marked bad. "+
                   "File no longer will be used: " + getFullName();
      ErrorLog.ptod(txt);
      setBadFile();
    }
  }
  private void setBadFile()
  {
    bad_file = true;
    ErrorLog.ptod("File marked bad: " + getFullName());
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
  public String getFullName()
  {
    return parent.getFullName() + getShortName();
  }


  public String getShortName()
  {
    String name;
    if (!common.get_debug(common.LONGER_FILENAME))
      name = String.format("vdb_f%04d.file", file_no);
    else
      name = String.format("vdb_f%04d.%04d.file", file_no, file_no_in_list);

    //common.ptod("name: " + name);
    return name;
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

  public boolean hasBeenCopied()
  {
    return file_copied;
  }
  public void setCopied(boolean bool)
  {
    file_copied = bool;
  }


  /**
   * Windows has a problem with quick delete/create:
   * http://stackoverflow.com/questions/3764072/c-win32-how-to-wait-for-a-pending-delete-to-complete
   *
   * The result is that at create time the delete may not have been completed
   * yet.
   * Oh well. Adding a 5 ms sleep after the delete went around the problem, but
   * I do not make that a permanent change.
   * But let's get real: who is going to notice? Sleep is in. NOT!
   *
   * This problem of course shows up with fileio=(seq,delete)
   */
  public void deleteFile(FwgEntry fwg)
  {
    long start = Native.get_simple_tod();
    File file_ptr = new File(getFullName());
    if (!file_ptr.delete())
      common.failure("unable to delete file " + getFullName());
    FwdStats.count(Operations.DELETE, start);
    //if (common.onWindows())
    //  common.sleep_some(5);

    parent.countFiles(-1, this);
    setExists(false);
    setCurrentSize(0);

    fwg.blocked.count(Blocked.FILE_DELETES);
    getAnchor().countExistingFiles(-1, this);

    if (Validate.isValidate())
      getAnchor().allocateKeyMap(file_start_lba).clearMapForFile(req_file_size);
    if (debug)
      common.ptod("deleted: " + getFullName());
  }


  public int compareTo(Object obj)
  {
    FileEntry fe = (FileEntry) obj;
    return(int) getFullName().compareTo(fe.getFullName());
  }
  public String toString()
  {
    return "FileEntry: " + getFullName() + " busy: " + isBusy();
  }

  public static void main(String[] args)
  {
    int loop = Integer.parseInt(args[0]) * 1000;
    for (int i = 0; i < loop; i++)
    {
      Fput fp = new Fput("w:/temp/file1");
      fp.close();
      new File(fp.getName()).delete();
    }

  }
}
