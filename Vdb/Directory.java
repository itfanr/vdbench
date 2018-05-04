package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;


/**
 * This class contains information about the status of a directory.
 *
 * The first Directory instance also immediately creates all his directory
 * children, up to 'depth' * 'width'.
 *
 * Directory names are reconstructed using depth and width information.
 */
class Directory implements Serializable, Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private   FileAnchor  anchor;
  private   Directory   parent   = null; /* Either Directory() or FileAnchor */
  private   Directory[] children = null;

  private   int         width    = 0;      // could be char, but is 32767 big enough?
  private   int         depth    = 0;
  protected int         files_in_dir = 0;

  private   boolean     dir_busy       = false;
  private   boolean     dir_exists     = false;
  private   boolean     files_checked  = false;
  private   boolean     this_is_anchor = false;



  // Debugging fields. Search through code to find uses.
  //public    String       last_busy = null;
  //public    FwgThread   busy_owner;



  /* List of directory names, only used during initial creation */
  /* of all Directory instance for a specific FileAnchor()      */
  private static Vector <Directory> temp_dir_list  = null;

  private static boolean debug  = common.get_debug(common.DIRECTORY_SET_BUSY);
  private static boolean debugc = common.get_debug(common.DIRECTORY_CREATED);

  public String debugging = null;

  public Directory()
  {
    this_is_anchor = true;
  }

  /**
   * Create a Directory instance.
   * The parent is either an other Directory, or it is a String, naming the
   * anchor directory.
   */
  public Directory(Directory parent,  int current_width,
                   int current_depth, FileAnchor anchor, int max_width, int max_depth)
  {
    this.anchor  = anchor;
    this.parent  = parent;
    this.width   = current_width;
    this.depth   = current_depth;

    temp_dir_list.add(this);
    //common.ptod("added dir.getFullName(): " + getFullName() + " parent exists: " + parent.exist());
    //common.ptod("creating fptr(): " + getFullName() + " " + exists + " parent: " + parent.exist());

    if (depth < max_depth)
    {
      children = new Directory[max_width];
      for (int w = 0; w < max_width; w++)
        children[w] = new Directory(this, w, depth + 1, anchor, max_width, max_depth);

      //common.ptod("xx: " + getFullName());
    }
    else
    {
      children = null;
      if (temp_dir_list.size() % 1000000 == 0)
        common.ptod("Initializing directory structure for %s: %,d",
                    anchor.getAnchorName(), temp_dir_list.size());

      for (int i = 99999990; i < temp_dir_list.size(); i++)
      {
        Directory dir = (Directory) temp_dir_list.elementAt(i);
        common.ptod("yy: " + dir.getFullName());
        if (dir.getFullName().equals(this.getFullName()))
          common.ptod("no: " + this.getFullName());
      }
    }
  }


  private File getDirPtr()
  {
    return new File(buildFullName());
  }

  /**
   * Does directory have any files?
   *
   * Used during the creation of FileEntry instances to eliminate the need to
   * keep checking to see if the file exists. This saves a lot of startup time.
   *
   * This all depends on the FileEntry instances being created in directory order!
   */
  private static HashMap   last_map;
  private static Directory last_checked_dir;
  public boolean hasFile(String fname)
  {
    if (last_checked_dir != this)
    {
      if (files_checked)
        common.failure("Recursive call to hasFile(): " + getFullName() + " " + fname);
      files_checked    = true;
      last_checked_dir = this;
      String[] list = getDirPtr().list();
      last_map      = new HashMap(list.length * 2);
      //common.ptod("list.length: " + list.length + " " + getFullName() + " " + fname);
      for (int i = 0; i < list.length; i++)
      {
        //common.ptod("list[i]: " + list[i]);
        last_map.put(list[i], list[i]);
      }
    }

    if (last_map.size() == 0)
    {
      //common.where();
      return false;
    }
    boolean rc = last_map.get(fname) != null;

    //common.ptod("fname: " + fname + " " + rc);
    return rc;
  }

  public int getDepth()
  {
    return depth;
  }
  public int getWidth()
  {
    return width;
  }

  public boolean exist()
  {
    //common.ptod("exists:    " + dir_exists + " " + getFullName());
    //common.where(8);
    return dir_exists;
  }
  public void setExists(boolean bool)
  {
    //common.ptod("setExists: " + bool + " " + getFullName());
    //common.where(8);
    dir_exists = bool;
  }

  /**
   * Count the amount of files in this directory.
   */
  public synchronized int countFiles(int c, FileEntry fe)
  {
    files_in_dir += c;

    //if (fe != null)
    //  common.ptod("countFiles: " + fe.getName() + " " + files_in_dir + " " + c);

    if (files_in_dir < 0)
      common.failure("negative file count");

    return files_in_dir;
  }

  /**
   * Set directory busy. If already busy, return false.
   *
   * This busy flag is ONLY for mkdir and rmdir.
   *
   * Just in case someone is waiting for this directory to become available
   * again (mkdir of parents), notify all waiters.
   */
  public synchronized boolean setBusy(boolean bool)
  {
    /* There is never a need to have the anchor itself marked busy */
    /* since it can never go away. So we just fake it:             */
    // Wrong: this will allow multiple threads to create all children
    // of ???
    if (this_is_anchor)
      return true;

    /* Debugging code, to track who last locked this Directory: */
    //if (debugc && bool && !dir_busy)
    //  last_busy = common.get_stacktrace();

    //if (getFullName().contains("vdb.4_1.dir"))
    //{
    //  common.ptod("vdb.4_1.dir: " + getFullName() + " " + dir_busy + " ===> " + bool);
    //  common.where(4);
    //}


    if (debug)
      common.ptod("Directory.setBusy: " + getFullName() + " " + dir_busy + " ===> " + bool);

    if (bool && dir_busy)
    {
      //common.ptod("busy_owner: " + " " + getFullName()+ " " +
      //            busy_owner.getName() + " " + busy_owner.tn.task_number);
      return false;
    }

    else if (!bool && !dir_busy)
      common.failure("Directory.setBusy(false): entry not busy");

    dir_busy = bool;

    /* Just in case someone is waiting for this directory: */
    if (!bool)
      this.notifyAll();

    //if (bool)
    //  busy_owner = (FwgThread) Thread.currentThread();
    //else
    //  busy_owner = null;

    return true;
  }
  public synchronized boolean isBusy()
  {
    return dir_busy;
  }
  public boolean isBusyNoSync()
  {
    return dir_busy;
  }

  /**
   * Each FileAnchor gets a list of directories. Since the list being created
   * here in this class is static, we must make sure that the list gets cleared
   * and picked up between new FileAnchor() instances.
   */
  public static void clearStaticDirectoryList()
  {
    temp_dir_list = new Vector(1024, 0);
    //common.ptod("Clearing directory structure");
  }

  /**
   * Return a list of directory names.
   */
  public static Vector <Directory> getTempDirectoryList()
  {
    return temp_dir_list;
  }


  public Directory getParent()
  {
    return parent;
  }

  /**
   * Return a text representation of the complete parent structure.
   *
   * This method was created to eliminate the need to possibly store thousands
   * of directory names in memory.
   * However, because of debugging statements everywhere this method is called
   * so often that it may impact cpu overhead.
   * I therefore decided to store the directory name anyway.
   * Remember, it is the directory name. I am still not storing the file name
   * which could cost us much more for instance if we have lots of files in the
   * final directory.
   */
  private static int once = 0;
  public String getFullName()
  {
    return buildFullName();
  }
  public String buildFullName()
  {
    String name = "";
    Directory dir = this;
    while (true)
    {
      name = dir.getDirName() + File.separator + name;
      if (dir.parent instanceof FileAnchor)
      {
        FileAnchor anchor = (FileAnchor) dir.parent;
        //common.ptod("anchor.getDirName(): " + anchor.getAnchorName());
        name = anchor.getAnchorName() + File.separator + name;
        return name;
      }
      else
        dir = (Directory) dir.parent;
    }
  }

  public Directory[] getChildren()
  {
    return children;
  }

  public boolean anyExistingChildren()
  {
    if (children == null)
      return false;

    for (int i = 0; i < children.length; i++)
    {
      if (children[i].exist())
        return true;
    }

    return false;
  }

  public String getDirName()
  {
    String name = String.format("vdb.%d_%d.dir", depth, width+1);

    return name;
  }


  /**
   * Get a list of parents in reverse order.
   * This allows us to do a forward search to see if the parents exist.
   */
  public Vector getReverseParentList()
  {
    Vector list = new Vector(16, 0);
    Directory dir = this;
    while (true)
    {
      list.insertElementAt(dir, 0);
      if (dir.parent instanceof FileAnchor)
        break;
      dir = (Directory) dir.parent;
    }

    return list;
  }


  public FileAnchor getAnchor()
  {
    return anchor;
  }



  public synchronized boolean createDir()
  {
    //common.ptod("createDir1: " + getFullName() + " " + ((FwgThread) Thread.currentThread()).tn.task_number);

    File      dir_ptr = getDirPtr();
    FwgThread fwt     = ((FwgThread) Thread.currentThread());

    if (dir_exists)
      common.failure("Creating directory that already exists: " +
                     dir_ptr.exists() + " " + getFullName() + " " +
                     ((FwgThread) Thread.currentThread()).tn.task_number);

    /* For shared, before we even try to create directory, see if someone else did: */
    if (fwt.fwg.shared && dir_ptr.exists())
    {
      setExists(true);
      getAnchor().countExistingDirectories(+1);
      fwt.fwg.blocked.count(Blocked.DIR_CREATE_SHARED);
      return false;
    }

    /* Now create the directory: */
    long start = Native.get_simple_tod();
    if (!dir_ptr.mkdir())
    {
      /* If we failed when using a shared FSD, this may be OK.          */
      /* On Linux it appears that though the directory has already been */
      /* created by a different host and this mkdir therefore fails,    */
      /* the current OS still does not know that the directory exists,  */
      /* with the exists() below then failing.                          */
      /* First give this system a bit of time to sync up:               */
      if (fwt.fwg.shared)
      {
        Signal signal = new Signal(5);
        while (!dir_ptr.exists())
        {
          common.sleep_some(10);
          fwt.fwg.blocked.count(Blocked.DIR_WAIT_SHARED);
          if (signal.go())
            break;
        }

        if (dir_ptr.exists())
        {
          setExists(true);
          getAnchor().countExistingDirectories(+1);
          fwt.fwg.blocked.count(Blocked.DIR_CREATE_SHARED);
          return false;
        }
      }

      common.ptod("dir.dir_exists:   " + dir_exists);
      common.ptod("dir_ptr.exists(): " + dir_ptr.exists());
      common.ptod("file.exists():    " + new File(getFullName()).exists());
      Blocked.printCountersToLog();
      common.failure("Unable to create directory: " + getFullName());
    }
    FwdStats.count(Operations.MKDIR, start);

    setExists(true);
    //common.ptod("createDir2: " + getFullName() + " " + ((FwgThread) Thread.currentThread()).tn.task_number);

    //if (debugc)
    //  common.ptod("Created directory: " + getFullName());

    getAnchor().countExistingDirectories(+1);
    return true;
  }



  public synchronized void deleteDir(FwgEntry fwg)
  {
    if (!dir_exists)
      common.failure("Deleting directory that does not exist");

    if (files_in_dir != 0)
      common.failure("Deleting directory that is not empty");

    long start = Native.get_simple_tod();
    if (!getDirPtr().delete())
    {
      common.ptod("dir.exists:       " + dir_exists);
      common.ptod("dir_ptr.exists(): " + getDirPtr().exists());
      common.ptod("file.exists():    " + new File(getFullName()).exists());
      common.ptod("anyExistingChildren: " + anyExistingChildren());
      common.failure("Unable to delete directory: " + getFullName());
    }
    FwdStats.count(Operations.RMDIR, start);
    fwg.blocked.count(Blocked.DIRECTORY_DELETES);

    setExists(false);

    //if (debugc)
    //  common.ptod("Deleted directory: " + getFullName());

    getAnchor().countExistingDirectories(-1);
  }

  /**
   * Sort the directory list by depth and width.
   * This forces directory parents to the top of the list.
   */
  public int compareTo(Object obj)
  {
    Directory dir = (Directory) obj;

    if (depth != dir.depth)
      return(depth - dir.depth);
    return width - dir.width;
  }


  /**
   * Check the status of the directories.
   * This is done by either going to the information obtained from ControlFile(),
   * or just go to the directory itself.
   */
  public static void setDirectoryStatus(Vector dir_list)
  {
    for (int i = 0; i < dir_list.size(); i++)
    {
      Directory dir = (Directory) dir_list.elementAt(i);

      /* If the parent does not exist then the child's not there either: */
      /* This saves time looking for directories that don't exist: */
      if (dir.getAnchor().format_complete_used || dir.parent.exist())
      {
        /* Can we avoid reading the directory? */
        if (dir.anchor.getControlFile().hasDirStatus())
        {
          if (dir.anchor.getControlFile().getDirStatus(i))
          {
            dir.setExists(true);
            dir.getAnchor().countExistingDirectories(+1);
          }
        }
        else
        {
          if (dir.getAnchor().format_complete_used || dir.getDirPtr().exists())
          {
            dir.setExists(true);
            dir.getAnchor().countExistingDirectories(+1);
          }
        }
      }
    }
  }
}

