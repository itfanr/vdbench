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
import java.lang.*;
import java.util.*;

import Utils.*;

/**
 * This class contains all data needed for a specific directory
 */
public class FileAnchor extends Directory implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private boolean build_structure      = true;
  private String  anchor_name;
  private String  windows_anchor_name;
  public  String  fsd_name_active;
  public  String  fsd_name_8bytes;
  private DV_map  validation_map       = null;
  public  String  jnl_file_name;
  private boolean reported_this_anchor = false;

  public  int      depth      = -1;
  public  int      width      = -1;
  public  int      files      = -1;
  public  String   dist       = "bottom";
  public  double[] filesizes  = null;
  public  long     total_size = -1;
  public  long     working_set;

  private boolean  delete_pending = false;

  private long     delete_dir_count;
  private long     delete_file_count;
  private long     delete_start;

  private ControlFile control_file;

  public  int    total_directories  = 0;  /* Amount of directories in anchor  */
  public  int    maximum_file_count = 0;  /* Amount of files in anchor        */
  public  long   est_max_byte_count = 0;  /* An estimate of the above         */

  private int    existing_dirs      = 0;  /* Amount of created directories    */
  private int    existing_files     = 0;  /* Amount of created files          */
  private int    full_file_count    = 0;  /* Amount of files that are full    */


  private int    round_robin_files = 0;
  private int    round_robin_dirs = 0;

  private Vector dir_list  = null;        /* List of Directory instances      */

  // Maybe some day change these to ArrayList?
  private Vector file_list = null;        /* List of FileEntry instances      */
  private Vector use_list  = null;        /* List of (totalsize=)             */
  private Vector wss_list  = null;        /* Same, only for active WSS        */

  public  long   bytes_in_file_list = 0;  /* What we have in file_list        */
  private long   bytes_in_use_list  = 0;  /* What we have in use_list         */
  private long   bytes_in_wss_list  = 0;  /* What we have in wss_list         */

  private Random file_select_randomizer = new Random();
  private Random dir_select_randomizer  = new Random();

  public  Vector  devxlate_list = null;   /* list of disks for this anchor    */

  private Random file_size_randomizer = null;  /* Must have fixed seed        */

  private Vector  xfersizes_used    = new Vector(8);
  private Vector  filesizes_used    = new Vector(8);
  private int     dv_key_size       = 0; /* Greatest common divisor of DV xfer*/
  private int     dv_max_xfer_size  = 0; /* Maximum DV xfersize               */

  public  FormatCounter mkdir_threads_running;
  public  FormatCounter create_threads_running;



  private static int    dv_largest_xfer_size  = 0;  /* Largest in whole run   */
  private static Vector anchor_list  = new Vector(8);

  private static double KB = 1024.;
  private static double MB = 1024. * 1024.;
  private static double GB = 1024. * 1024. * 1024.;
  private static double TB = 1024. * 1024. * 1024. * 1024.;
  private static double PB = 1024. * 1024. * 1024. * 1024. * 1024.;

  {
    if (common.get_debug(common.ANCHOR_FIXED_SEED))
    {
      file_select_randomizer = new Random(0);
      dir_select_randomizer  = new Random(0);
    }
  };



  public static Vector getAnchorList()
  {
    return anchor_list;
  }

  /**
   * Allocate a new unique Directory instance.
   * If there already is one with this directory (anchor) name then return
   * one that was allocated previously.
   */
  public static FileAnchor newFileAnchor(FsdEntry fsd)
  {
    common.plog("Creating anchor for " + fsd.dirname + ", fsd=" + fsd.name);

    /* If we already have this anchor, just return the old one: */
    for (int i = 0; i < anchor_list.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchor_list.elementAt(i);
      if (anchor.anchor_name.equals(fsd.dirname))
        return anchor;
    }

    FileAnchor anchor    = new FileAnchor();
    anchor.anchor_name   = fsd.dirname;
    anchor.jnl_file_name = fsd.jnl_file_name;
    anchor.dist          = fsd.dist;
    anchor_list.addElement(anchor);

    /* For DV we need to know the name of the active FSD: */
    if (Validate.isValidate())
    {
      anchor.fsd_name_8bytes = (fsd.name + "        ").substring(0,8);
      anchor.fsd_name_active = fsd.name;
    }

    /* Create anchor directory instance if it is not there: */
    // code may be obsolete since THIS runs on the master.
    anchor.dir_ptr = new File(anchor.anchor_name);
    anchor.setExists(true);

    /* Create a (possible) unix2windows anchor name: */
    anchor.windows_anchor_name = Work.unix2Windows(null, anchor.anchor_name);

    return anchor;
  }



  /**
   * Prepare an Anchor for use.
   */
  public void initializeFileAnchor(FwgEntry fwg)
  {
    /* Randomizer must have fixed seed. In that way when we expand the */
    /* file list we get the same sizes as before. */
    file_size_randomizer = new Random(0); /* Always fixed seed! */

    /* Store the working set size:                                      */
    /* (Can it happen that different FSDs using the same anchor can use */
    /* different wss sizes? Should maybe check for this somehwere)      */
    working_set = fwg.working_set;

    /* This directory must exist. I don't want to be responsible for creating */
    /* a huge amount of file structure in the wrong directory! */
    if (!Fget.dir_exists(anchor_name))
      common.failure("Anchor directory name does not exist: " + anchor_name);

    /* If the parameters changed we are not allowed to have any files or directories */
    /* in the old anchor: */
    if (depth != fwg.depth ||
        width != fwg.width ||
        files != fwg.files ||
        !dist.equals(fwg.dist))
    {

      common.ptod("Old depth: " + depth + " new depth: " + fwg.depth);
      common.ptod("Old width: " + width + " new width: " + fwg.width);
      common.ptod("Old files: " + files + " new files: " + fwg.files);
      common.ptod("Old dist:  " + dist  + " new dist:  " + fwg.dist);
      common.ptod("existing_dirs: " + existing_dirs);
      common.ptod("existing_files: " + existing_files);
      if (existing_dirs + existing_files != 0)
      {
        common.ptod("");
        common.ptod("anchor=" + anchor_name);
        common.ptod("Changing directory structure while there are still existing directories and/or files");
        common.ptod("Old depth: " + depth + " new depth: " + fwg.depth);
        common.ptod("Old width: " + width + " new width: " + fwg.width);
        common.ptod("Old files: " + files + " new files: " + fwg.files);
        common.ptod("Old dist:  " + dist  + " new dist:  " + fwg.dist);
        common.ptod("existing_dirs: " + existing_dirs);
        common.ptod("existing_files: " + existing_files);
        common.ptod("totalsize: " + total_size);
        common.ptod("wg.totalsize: " + fwg.total_size);
        common.failure("Invalid parameter setting, or, insert a 'clean' workload");
      }

      build_structure = true;
    }

    /* If we alreay have a complete structure, exit: */
    if (!build_structure)
    {
      common.ptod("Reusing existing FileAnchor structure.");
      return;
    }


    calculateStructureSize(fwg, false);
    build_structure = false;


    /* Read the control file in the anchor directory to make sure */
    /* that the file structure is the same:                       */
    control_file = new ControlFile(this);
    if (control_file.exists() && !isDeletePending())
    {
      /* Format, except for a restart, ignores the control file: */
      // 5/13/09: this prevented a changed structure from being recognized
      //if (!SlaveWorker.work.format_run && !SlaveWorker.work.format_flags.format_restart)
      if (!fwg.shared)
        control_file.readControlFile(fwg);
    }

    /* Create directory names: */
    dir_list = createDirectoryList(anchor_name, width, depth);

    for (int i = 999999990; i < dir_list.size(); i++)
    {
      Directory dir = (Directory) dir_list.elementAt(i);
      common.ptod("dir_list: " + dir.getFullName());
    }

    /* Create list of file names: */
    createFileList(this, dir_list, fwg);

    /* Create a list with a subset of these file for totalsize= purposes: */
    createUsedList();

    /* Create a list with a subset of these file for WSS= purposes: */
    createWorkingSetList();

    VdbCount.listCounters("initializeFileAnchor");

    /* Data Validation may want to get some maps: */
    allocateDVMapIfNeeded();

    /* Write all the filenames to the anchor directory.                     */
    /* We can use this later to verify that FWD parameters were not changed */
    /* possibly causing strange performance problems because of old stuff   */
    /* left behind in the directories:                                      */
    if (!fwg.shared)
      control_file.writeControlFile(fwg.shared, true);

    if (existing_dirs > 0)
    {
      common.ptod("During anchor creation for anchor=" + getAnchorName() +
                  " there were " +
                  existing_dirs + " directories and " +
                  existing_files + " files");
    }

    /* Once we're this far we no longer need the detailed status info. */
    /* GC can clean this up now.                                       */
    control_file.clearStatus();
  }


  /**
   * Create list of directories.
   * The list is sorted at then end using the length of the directory name.
   * This assures that the parents of a directory are always on top.
   */
  public Vector createDirectoryList(String anchor_name, int width, int depth)
  {
    Directory.clearDirectoryList(width, depth);

    for (int w = 0; w < width; w++)
      new Directory(this, w, 1, this);

    common.ptod("Completed the creation of the directory list: " +
                Directory.getDirectoryList().size() + " directories");

    Vector new_list = Directory.getDirectoryList();

    long start = System.currentTimeMillis();
    Collections.sort(new_list);
    long seconds  = (System.currentTimeMillis() - start) / 1000;
    if (seconds > 10)
      common.ptod("Directory name sort took " + seconds + " seconds.");

    /* Determine the status (exists yes/no): */
    if (!isDeletePending())
      Directory.setDirectoryStatus(new_list);

    //for (int i = 0; i < new_list.size(); i++)
    //  common.ptod("new_list: " + ((Directory) new_list.elementAt(i)).getFullName());

    return new_list;
  }


  /**
   * Create list of FileEntry instances.
   * Files are created in order for each directory
   * Note: remember that the directory list is sorted on LENGHT of the directory
   * name, this to assure that the parents are always at the beginning.
   */
  private void createFileList(FileAnchor anchor, Vector dirlist, FwgEntry fwg)
  {
    Signal signal = new Signal(30);
    file_list = new Vector(maximum_file_count);
    int     created_file_count  = 0;
    int     relative_file_count = 0;
    int     slave_number        = SlaveWorker.work.slave_number;
    int     slave_count         = SlaveWorker.work.slave_count;

    /* Preallocate some memory to eliminate too much GC activiy: */
    checkMemory();

    /* Go through each directory and create all files for that directory: */
    for (int i = 0; i < dirlist.size(); i++)
    {
      Directory dir = (Directory) dirlist.elementAt(i);

      /* Only create files if there are no child directories: */
      if (dist.equals("bottom") && dir.getChildren() != null)
        continue;

      /* Create all files for this directory: */
      for (int j = 0; j < files; j++)
      {
        /* The file size must be obtained BEFORE we make the shared decision: */
        /* This is needed to keep the file sizes the same when we change the  */
        /* amount of clients. (because of possible randomizer call)           */
        long file_size = getFileSize();

        /* On a shared FSD each slave gets only every n-th file: */
        if (fwg.shared && relative_file_count++ % slave_count != slave_number)
          continue;

        /* Create the FileEntry: */
        FileEntry fe = new FileEntry(dir, j+1, file_size, bytes_in_file_list, file_list.size());
        //common.ptod("fe: " + fe.getName() + " " + file_size);
        file_list.add(fe);
        created_file_count++;

        if (created_file_count % 1000 == 0 && signal.go())
        {
          common.ptod(Format.f("Generated %8d file names; total anchor size: ",
                               created_file_count) + whatSize(bytes_in_file_list));
          SlaveJvm.sendMessageToConsole("Continuing the creation of internal "+
                                        "file structure for anchor=" +
                                        getAnchorName() + ": " + created_file_count + " files.");
        }

        bytes_in_file_list += file_size;
      }
    }

    common.ptod(Format.f("Generated %8d file names; total anchor size: ", created_file_count) +
                whatSize(bytes_in_file_list));

    for (int i = Integer.MAX_VALUE; i < file_list.size(); i++)
    {
      FileEntry fe = (FileEntry) file_list.elementAt(i);
      common.ptod("fe1: " + fe.getName());
    }
  }


  /**
   * Create a list containing a random subset of the files in file_list up
   * to 'totalsize=' bytes.
   * This allows us to start with the creation of a smaller subset of the files
   * and then grow/shrink that subset later.
   */
  private void createUsedList()
  {
    use_list = null;
    if (total_size == 0 || total_size == Long.MAX_VALUE)
      return;

    if (total_size != Long.MAX_VALUE && total_size > bytes_in_file_list)
      common.failure("A totalsize has been requested that is "+
                     "larger than the currently defined size for this anchor. "+
                     "\n\t\ttotalsize=" + whatSize(total_size) + " anchor size: " +
                     whatSize(bytes_in_file_list));

    // obsolete
    for (int i = Integer.MAX_VALUE; i < file_list.size(); i++)
    {
      FileEntry fe = (FileEntry) file_list.elementAt(i);
      if (fe.isBusy())
        common.failure("still files busy: " + fe.getName());
    }

    /* Randomly select files, up to the total working set size: */
    /* (Fixed seed) */
    bytes_in_use_list = 0;
    long busy_blocks  = 0;
    int  files_found  = 0;
    existing_files    = 0;
    full_file_count   = 0;
    Random subset_random = new Random(0);
    while (bytes_in_use_list < total_size)
    {
      int number = (int) (subset_random.nextDouble() * file_list.size());
      FileEntry fe = (FileEntry) file_list.elementAt(number);
      if (fe == null)
        common.failure("Unable to create a totalsize= subset for anchor=" + getAnchorName());

      /* Set busy to prevent the file from being selected twice: */
      if (!fe.setBusy(true))
      {
        /* Loop protection. More than 100,000 consecutive busies: */
        if (busy_blocks++ > 100000)
          common.failure("Unable to create a totalsize= subset for anchor="+
                         getAnchorName() + "; Too many busy blocks. " +
                         whatSize(bytes_in_use_list) + " " + files_found);
        continue;
      }

      /* Use this file for the subset. Keep it busy to prevent reuse: */
      bytes_in_use_list += fe.getReqSize();
      files_found++;
      busy_blocks = 0;

      /* Keep track of how many files in this list exist: */
      if (fe.exists())
        existing_files++;
      if (fe.isFull())
        full_file_count++;
    }

    /* Allocate an estimated Vector size: */
    int list_size = (int) (total_size / bytes_in_file_list * file_list.size());
    Vector subset_list = new Vector(list_size);

    /* Now pick up any busy FileEntry and put it in the new list. */
    /* (This eliminates the need to do a sort on the file names), */
    /* (the original list is already in the proper order)         */
    for (int i = 0; i < file_list.size(); i++)
    {
      FileEntry fe = (FileEntry) file_list.elementAt(i);
      if (fe.isBusy())
      {
        fe.setBusy(false);
        subset_list.add(fe);
      }
    }

    for (int i = Integer.MAX_VALUE; i < subset_list.size(); i++)
    {
      FileEntry fe = (FileEntry) subset_list.elementAt(i);
      common.ptod("fe2: " + fe.getName());
    }

    String txt = "Created totalsize=" + whatSize(total_size) +
                 " subset for anchor=" + getAnchorName() +
                 " using " + subset_list.size() +
                 " of " + file_list.size() + " files.";
    SlaveJvm.sendMessageToConsole(txt);

    if (subset_list.size() == 0)
      common.failure("Subset creation failed. Zero files found.");

    use_list = subset_list;
  }





  /**
   * Create a list containing a subset of the files in either file_list or
   * used_list allowing us to work within a certain amount of file system space,
   * therefore controlling the working set size.
   */
  private void createWorkingSetList()
  {
    /* Format always ignores the working set size since that is not a real workload */
    wss_list = null;

    // why not when format? What if I only want to format a subset?
    if (working_set == 0)// || SlaveWorker.work.format_run)
      return;

    Vector list_to_use   = (use_list == null) ? file_list : use_list;
    long   bytes_in_list = (use_list == null) ? bytes_in_file_list : bytes_in_use_list;

    if (working_set > bytes_in_list)
      common.failure("A working set size has been requested that is larger than "+
                     "\n\t\tthe currently defined total size for this anchor. "+
                     "wss=" + whatSize(working_set) + " totalsize=" + whatSize(bytes_in_list));

    /* Randomly select files, up to the total working set size: */
    /* (Fixed seed) */
    bytes_in_wss_list = 0;
    long busy_blocks  = 0;
    existing_files    = 0;
    full_file_count   = 0;
    Random subset_random = new Random(0);
    while (bytes_in_wss_list < working_set)
    {
      int number = (int) (subset_random.nextDouble() * list_to_use.size());
      FileEntry fe = (FileEntry) list_to_use.elementAt(number);
      if (fe == null)
        common.failure("Unable to create a working set size (wss) subset.");

      /* Set busy to prevent the file from being selected twice: */
      if (!fe.setBusy(true))
      {
        /* Loop protection: */
        if (busy_blocks++ > 1000000)
          common.failure("Unable to create a working set size (wss) subset.");
        continue;
      }

      /* Use this file for the subset. Keep it busy to prevent reuse: */
      bytes_in_wss_list += fe.getReqSize();

      /* Keep track of how many files in this list exist: */
      if (fe.exists())
        existing_files++;
      if (fe.isFull())
        full_file_count++;
    }

    /* Allocate an estimated Vector size: */
    int list_size = (int) (working_set / bytes_in_list * list_to_use.size());
    Vector subset_list = new Vector(list_size);


    /* Now pick up any busy FileEntry and put it in the new list. */
    /* (This eliminates the need to do a sort on the file names)  */
    for (int i = 0; i < list_to_use.size(); i++)
    {
      FileEntry fe = (FileEntry) list_to_use.elementAt(i);
      if (fe.isBusy())
      {
        fe.setBusy(false);
        subset_list.add(fe);
      }
    }

    String txt = "Created workingset=" + whatSize(working_set) +
                 " subset for anchor=" + getAnchorName() +
                 " using " + subset_list.size() +
                 " of " + list_to_use.size() + " files.";
    SlaveJvm.sendMessageToConsole(txt);

    if (subset_list.size() == 0)
      common.failure("Subset creation failed. Zero files found.");

    wss_list = subset_list;


    for (int i = Integer.MAX_VALUE; i < wss_list.size(); i++)
    {
      FileEntry fe = (FileEntry) wss_list.elementAt(i);
      common.ptod("fe3: " + fe.getName());
    }
  }


  public void startRoundRobin()
  {
    round_robin_files =
    round_robin_dirs  = 0;
  }

  public String getAnchorName()
  {
    return anchor_name;
  }
  public static FileAnchor findAnchor(String a)
  {
    Vector anchors = FileAnchor.getAnchorList();
    int width = 0;
    for (int i = 0; i < anchors.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchors.elementAt(i);
      if (anchor.getAnchorName().equals(a))
        return anchor;
    }

    common.ptod("anchors.size(): " + anchors.size());
    for (int i = 0; i < anchors.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchors.elementAt(i);
      common.ptod("anchor.getAnchorName(): " + anchor.getAnchorName());
    }
    common.failure("Unable to find anchor: " + a + "<<<");

    return null;
  }



  /**
   * (Recursively) read directory.
   * While the directories are read, files are immediately deleted.
   * Once the files in a directory are deleted, the directory itself is deleted.
   */
  private void getRecursiveDirList(FwgEntry fwg)
  {
    Signal signal = new Signal(30);
    long start = System.currentTimeMillis();
    readDirsAndDelete(fwg, anchor_name, signal);

    long end = System.currentTimeMillis();
    if ((end - start) > 5000)
      common.ptod("getRecursiveDirList(): it took " + ((end - start) / 1000) +
                  " seconds to list the directory.");
  }


  /**
   * Get a directory listing and while doing that, delete all files, followed
   * by the delete of the directory.
   */
  private void readDirsAndDelete(FwgEntry fwg,
                                 String parent,
                                 Signal signal)
  {
    int invalids = 0;

    /* Go through a list of all files and directories of this parent: */
    String[] dirlist = new File(parent).list();
    for (int i = 0; dirlist != null && i < dirlist.length; i++)
    {
      String name = dirlist[i];
      //common.ptod("name: " + parent + " " + name);

      /* Debugging: */
      // There was an instance where for some reason when trying to delete the
      // directory there were still lots of files in it????
      if (!name.startsWith("vdb") && !name.endsWith("file") && !name.endsWith("dir"))
      {
        if (invalids++ < 25)
          common.ptod("readDirsAndDelete(): Invalid file name found and not deleted: " + parent + " " + name);
        if (invalids == 25)
          common.ptod("Only the first 25 file/directory names have been reported_this_anchor");
      }

      /* Only return our own directory and file names. If any other files */
      /* are left in a directory the directory delete will fail anyway if */
      /* other files are left.                                            */
      if (!name.startsWith("vdb"))
        continue;

      /* Leave the control file around a little bit, but remove debug */
      /* backup copies:                                               */
      if (name.endsWith(ControlFile.CONTROL_FILE))
        continue;
      if (name.indexOf(ControlFile.CONTROL_FILE) == -1)
      {
        if (!name.endsWith(".dir") && !name.endsWith(".file"))
          continue;
      }

      /* If this is a file, delete it: */
      if (name.endsWith("file"))
      {
        long begin_delete = Native.get_simple_tod();
        if (!new File(parent, name).delete())
        {
          if (!fwg.shared)
            common.failure("Unable to delete file: " + name);
          else
            fwg.blocked.count(Blocked.FILE_DELETE_SHARED);
        }
        else
        {
          FwdStats.count(Operations.DELETE, begin_delete);
          fwg.blocked.count(Blocked.FILE_DELETES);
          if (++delete_file_count % 5000 == 0 && signal.go())
            SlaveJvm.sendMessageToConsole("anchor=" + anchor_name +
                                          " deleted " + delete_file_count + " files; " +
                                          delete_file_count * 1000 / (System.currentTimeMillis() - delete_start) + "/sec; ");
        }
        continue ;
      }

      /* Directories get a recursive call before they are deleted: */
      readDirsAndDelete(fwg, new File(parent, name).getAbsolutePath(), signal);

      /* Once back here the directory should be empry and can be deleted: */
      long begin_delete = Native.get_simple_tod();
      if (!new File(parent, name).delete())
      {
        if (!fwg.shared)
        {
          common.ptod("Unable to delete directory: " + new File(parent, name).getAbsolutePath());
          common.ptod("Are there any files left that were not created by Vdbench?");
          common.failure("Unable to delete directory: " + parent + File.separator + name);
        }
        else
          fwg.blocked.count(Blocked.DIR_DELETE_SHARED);
      }

      delete_dir_count++;
      FwdStats.count(Operations.RMDIR, begin_delete);
      fwg.blocked.count(Blocked.DIRECTORY_DELETES);

    }
  }


  /**
   * Delete all old directories and files.
   * Since this method is synchronized it means that if multiple threads
   * are trying to do this they will be locked out.
   */
  public synchronized void cleanupOldFiles(FwgEntry fwg)
  {
    /* While a thread was waiting (synchronized abvove) did delete complete? */
    if (!isDeletePending())
      return;

    if (!exist())
      return;

    deleteOldStuff(fwg);

    setDeletePending(false);

    if (getDVMap() != null)
      getDVMap().eraseMap();
  }


  /**
   * Recursively list the anchor directory.
   * As soon as you find a file, delete it. Directories are stored in a Vector
   * so that they can be deleted later.
   */
  public void deleteOldStuff(FwgEntry fwg)
  {

    /* Start the recusrive directory search: */
    Signal signal     = new Signal(30);
    delete_file_count = 0;
    delete_dir_count  = 0;
    delete_start      = System.currentTimeMillis();
    readDirsAndDelete(fwg, anchor_name, signal);
    double elapsed    = (System.currentTimeMillis() - delete_start) / 1000.;

    if (elapsed > 5)
      common.ptod("deleteOldStuff(): it took " + elapsed +
                  " seconds to delete all old files and directories.");
    getRecursiveDirList(fwg);

    if (delete_file_count > 0)
    {
      double per_sec = (elapsed == 0) ? 0 : delete_file_count / elapsed;
      SlaveJvm.sendMessageToConsole("anchor=" + this.anchor_name + " deleted " +
                                    delete_file_count + " files; " +
                                    (int) per_sec + "/sec");
    }

    if (delete_dir_count > 0)
    {
      double per_sec = (elapsed == 0) ? 0 : delete_dir_count / elapsed;
      SlaveJvm.sendMessageToConsole("anchor=" + this.anchor_name + " deleted " +
                                    delete_dir_count + " directories; " +
                                    (int) per_sec + "/sec");
    }

    /* Finally, clean up the control file: */
    File fptr = new File(getAnchorName(), ControlFile.CONTROL_FILE);
    if (fptr.exists())
    {
      if (!fptr.delete())
        common.failure("Unable to delete control file: " + fptr.getAbsolutePath());
    }

    existing_dirs = 0;
  }



  /**
   * Find a file.
   * Just pick any file, whether it is busy or idle.
   *
   * Even a busy file can be returned because the caller may decide he
   * wants to wait for something, and we can't do that inside of the
   * synchronized lock.
   */
  public synchronized FileEntry getFile(boolean select_random)
  {
    Vector list_to_use;
    if (wss_list != null)
      list_to_use = wss_list;
    else if (use_list != null)
      list_to_use = use_list;
    else
      list_to_use = file_list;

    if (select_random)
    {
      int file_number = file_select_randomizer.nextInt(list_to_use.size());
      FileEntry fe = (FileEntry) list_to_use.elementAt(file_number);
      return fe;
    }

    /* Did we pass through roundrobin for journal recovery? */
    //common.ptod("round_robin_files: " + list_to_use.size() + " " + round_robin_files );
    if (round_robin_files >= list_to_use.size())
    {
      if (Validate.isJournalRecoveryActive())
        return null;
      if (SlaveWorker.work.format_run)
        return null;
    }

    /* Sequential scanning of the file list: */
    /* Round-robin over the whole list: */
    if (round_robin_files >= list_to_use.size())
    {
      round_robin_files = 0;
    }

    FileEntry fe = (FileEntry) list_to_use.elementAt(round_robin_files++);
    //common.ptod("fe: " + fe);
    return fe;
  }

  public synchronized boolean anyFilesToFormat()
  {
    Vector list_to_use;
    if (wss_list != null)
      list_to_use = wss_list;
    else if (use_list != null)
      list_to_use = use_list;
    else
      list_to_use = file_list;

    /* Did we pass through roundrobin? */
    if (round_robin_files >= list_to_use.size())
      return false;
    else
      return true;
  }



  public synchronized Directory getDir(boolean select_random, boolean format)
  {
    //common.where(8);
    Directory dir = null;

    if (select_random)
    {
      int dir_number = dir_select_randomizer.nextInt(dir_list.size());
      dir = (Directory) dir_list.elementAt(dir_number);
    }

    else
    {
      /* Sequential scanning of the directory list, Round-robin over the  */
      /* whole list. For format only return level 1 because as soon as    */
      /* a level 1 is picked all other levels will be immediately created */
      /* by createChildren()                                              */
      do
      {
        if (round_robin_dirs >= dir_list.size())
        {
          round_robin_dirs = 0;
          if (format)
            return null;
        }

        dir = (Directory) dir_list.elementAt(round_robin_dirs++);
      } while (dir.getDepth() != 1);
    }

    //common.ptod("getDir: " + dir.buildFullName() + " " + dir.getDepth());
    return dir;
  }



  public synchronized boolean moreDirsToFormat()
  {
    if (round_robin_dirs >= dir_list.size())
      return false;
    else
      return true;
  }



  /**
   * Get the kstat data for the anchor.
   */
  public void getKstatForAnchor()
  {
    /* Do this only once: (why?) */
    if (devxlate_list == null)
    {
      /* get_device_info() depends on this being a complete file name, add 'tmp': */
      devxlate_list = Devxlate.get_device_info(anchor_name + File.separator + "tmp");
    }

    //if (devxlate_list != null)
    //  Devxlate.set_kstat_active(devxlate_list);
  }


  /**
   * Count the amount of existing files. This count is maintained for the SUBSET
   * of the file list if that is used.
   * The list is either file_list, use_list, or wss_list
   */
  public synchronized long countExistingFiles(int c, FileEntry fe)
  {
    existing_files += c;

    if (existing_files < 0)
      common.failure("negative file count");

    if (false)
    {
      if (c > 0)
        common.ptod("countExistingFiles: " + " + " + existing_files + " " + fe.getName());
      else
        common.ptod("countExistingFiles: " + " - " + existing_files + " " + fe.getName());
    }

    return existing_files;
  }

  /**
   * Count the amount of full files. This count is maintained for the SUBSET
   * of the file list if that is used.
   * The list is either file_list, use_list, or wss_list
   */
  public synchronized void countFullFiles(int c, FileEntry fe)
  {
    full_file_count += c;

    if (full_file_count < 0)
      common.failure("negative file count for " + fe.getName() + " " + fe.getCurrentSize());

    if (false)
    {
      if (c > 0)
        common.ptod("countFullFiles: " + " + " + full_file_count + " " + fe.getName());
      else
        common.ptod("countFullFiles: " + " - " + full_file_count + " " + fe.getName());
    }
  }

  public synchronized long countExistingDirectories(int c)
  {
    existing_dirs += c;
    return existing_dirs;

  }

  public synchronized long anyMoreDirectories()
  {
    if (total_directories - existing_dirs < 0)
    {
      common.ptod("total_directories: " + total_directories);
      common.ptod("existing_dirs: " + existing_dirs);
      common.failure("reached negative directory count");
    }

    //common.ptod("total_directories: " + total_directories + " " + existing_dirs);
    //common.ptod("existing_dirs: " + existing_dirs);
    return total_directories - existing_dirs;
  }
  public synchronized long anyMoreFilesToCreate()
  {
    long ret = getFileCount() - existing_files;

    //common.ptod("anyMoreFilesToCreate: " + anchor_name +
    //            " getFileCount(): " + getFileCount() +
    //            " maximum_file_count: " + maximum_file_count +
    //            " existing_files: " + existing_files + " ret: " + ret);

    if (ret < 0)
    {
      common.ptod("anyMoreFilesToCreate: " + anchor_name +
                  " getFileCount(): " + getFileCount() +
                  " maximum_file_count: " + maximum_file_count +
                  " existing_files: " + existing_files + " ret: " + ret);
      common.failure("reached negative file count");
    }

    return ret;
  }
  public int getExistingFileCount()
  {
    return existing_files;
  }
  public long getExistingDirCount()
  {
    return existing_dirs;
  }


  public static void printCounters()
  {
    if (anchor_list.size() > 0)
    {
      common.plog("Anchor counters:");
      for (int i = 0; i < anchor_list.size(); i++)
      {
        FileAnchor anchor = (FileAnchor) anchor_list.elementAt(i);
        common.plog("anhdor=" + anchor.anchor_name);
        common.plog("existing_dirs: " + anchor.existing_dirs + " of " + anchor.total_directories);
        common.plog("existing_files:       " + anchor.existing_files       + " of " + anchor.maximum_file_count);
      }
    }
  }


  public long calculateStructureSize(FwgEntry fwg, boolean print)
  {
    /* Begin with picking up everything that we need from the FwgEntry: */
    depth           = fwg.depth;
    width           = fwg.width;
    files           = fwg.files;
    dist            = fwg.dist;
    total_size      = fwg.total_size;
    working_set     = fwg.working_set;
    existing_dirs   = 0;
    existing_files  = 0;

    long heap_needed = 0;

    Vector txt = new Vector(8);
    //if (print)
    //  txt.add("Creating new FileAnchor for directory '" + anchor_name + "'.");

    /* Calculate number of directories: */
    total_directories = 0;
    for (int i = 0; i < depth; i++)
    {
      total_directories += (long) (Math.pow(width, i + 1));
      if (total_directories < 0)
      {
        total_directories = 0;
        for (i = 0; i < depth; i++)
        {
          total_directories += (long) (Math.pow(width, i + 1));
          common.ptod("total_directories: " + total_directories);
          if (total_directories < 0)
          {
            common.failure("64-bit overflow trying to calculate the amount of directories. "+
                           "\n\tAre you requesting too many directories?");
          }
        }
      }
    }

    /* Calculate number of files: */
    if (dist.equals("bottom"))
      maximum_file_count = (int) (Math.pow(width, depth)) * files;
    else
      maximum_file_count = (int) total_directories * files;

    long MAX = (!common.get_debug(common.SMALL_FILE_COUNT))? 32* (int) 1024*1024 : 100;
    if (maximum_file_count > MAX)
    {
      txt.add("");
      txt.add("New FileAnchor for directory '" + anchor_name + "'.");
      txt.add("depth=" + depth);
      txt.add("width=" + width);
      txt.add("files=" + files);
      txt.add("dist="  + dist);
      txt.add("There will be " +
              total_directories + " directories and " +
              maximum_file_count +
              " files under this anchor.");
      txt.add("Sorry, code currently supports only a maximum of " + MAX + " files.");
      txt.add("This is just an experimental limit to figure out how we're doing.");
      txt.add("If you need this changed, give me a call. Henk 303 272 9089 (x59089)");

      if (SlaveJvm.isThisSlave())
        SlaveJvm.sendMessageToConsole(txt);
      else
        common.ptod(txt);

      common.failure("Too many files");
    }

    /* Estimate total file size: */
    est_max_byte_count = 0;
    if (fwg.filesizes.length == 1)
      est_max_byte_count = (long) (fwg.filesizes[0] * maximum_file_count);

    else if (fwg.filesizes.length == 2 && fwg.filesizes[1] == 0)
      est_max_byte_count = (long) (fwg.filesizes[0] * maximum_file_count);

    else
    {
      for (int i = 0; i < fwg.filesizes.length; i+=2)
      {
        est_max_byte_count += (maximum_file_count * fwg.filesizes[i+1] / 100) * fwg.filesizes[i];
        //common.ptod("maximum_file_count: " + maximum_file_count);
        //common.ptod("fwg.filesizes[i+1]: " + whatSize(fwg.filesizes[i]) + " " + fwg.filesizes[i+1]);
        //common.ptod("est_max_byte_count: " + whatSize(est_max_byte_count));
      }
    }

    /* Code here reports the size of an anchor only once, though it is possible */
    /* for an anchor to change using a different FSD: */
    if (print && ! reported_this_anchor)
    {
      reported_this_anchor = true;
      txt.add("anchor=" + getAnchorName() + ": there will be " +
              total_directories + " directories and a maximum of " +
              maximum_file_count + " files under this anchor.");

      txt.add("Estimated maximum size for this anchor: " +
              whatSize(est_max_byte_count));

      //if (total_size != 0 && total_size > est_max_byte_count)
      //  common.failure("Requested totalsize=" + whatSize(total_size) +
      //              " less than estimated maximum size for this anchor: " +
      //              whatSize(est_max_byte_count));

      long entries = total_directories + maximum_file_count;
      heap_needed += entries * 64;
      if (heap_needed > 100*1024*1024)
        txt.add(Format.f("Estimated amount of Java heap space needed: %8d", entries) +
                Format.f(" (files + directories) * 64 bytes = %8d bytes, or ", (entries * 64)) +
                whatSize(entries * 64));

      txt.add("");
      common.ptod(txt);
    }

    if (total_size != Long.MAX_VALUE && total_size > est_max_byte_count)
    {
      common.ptod("rd=" + RD_entry.next_rd.rd_name);
      common.failure("fwd=" + fwg.getName() + ",fsd=" + fwg.fsd_name +
                     ": The requested totalsize=" + whatSize(total_size) +
                     " is greater than the estimated total anchor size of " +
                     whatSize(est_max_byte_count));
    }

    if (working_set > est_max_byte_count)
      common.failure("fwd=" + fwg.getName() + ",fsd=" + fwg.fsd_name +
                     ": The requested workingset=" + whatSize(working_set) +
                     " is greater than the estimated total anchor size of " +
                     whatSize(est_max_byte_count));

    if (fwg.total_size > 0 && fwg.working_set > fwg.total_size)
    {
      common.ptod("fwg.total_size:  " + whatSize(fwg.total_size));
      common.ptod("fwg.working_set: " + whatSize(fwg.working_set));
      common.failure("fwd=" + fwg.getName() + ",fsd=" + fwg.fsd_name +
                     ": The requested workingset=" + whatSize(fwg.working_set) +
                     " is greater than the estimated totalsize=" +
                     whatSize(fwg.total_size));
    }

    return heap_needed;
  }



  public static void reportCalculatedMemorySizes(Vector rd_list)
  {
    long heap_needed = 0;
    HashMap reported = new HashMap(32);

    for (int j = 0; j < rd_list.size(); j++)
    {
      RD_entry rd = (RD_entry) rd_list.elementAt(j);

      for (int i = 0; i < rd.fwgs_for_rd.size(); i++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(i);
        if (reported.put(fwg.anchor, fwg.anchor) != null)
          continue;
        heap_needed += fwg.anchor.calculateStructureSize(fwg, false);
      }
    }

    if (heap_needed > 100*1024*1024)
    {
      common.ptod("Estimate of the total amount of memory needed for all anchors: " +
                  heap_needed + " bytes, " +
                  Format.f("%.3fmb", (heap_needed / 1048576.)));
      common.ptod("This will be spread over the requested JVMs.");
      common.ptod("If needed increase the -Xmx values in your vdbench script.");
      common.ptod("");
    }
  }

  public Vector getFileList()
  {
    return file_list;
  }
  public int getFileCount()
  {
    Vector list_to_use;
    if (wss_list != null)
      list_to_use = wss_list;
    else if (use_list != null)
      list_to_use = use_list;
    else
      list_to_use = file_list;

    return list_to_use.size();
  }
  public int getFullFileCount()
  {
    return full_file_count;
  }
  public boolean allFilesFull()
  {
    return getFullFileCount() == getFileCount();
  }

  public FileEntry getRelativeFile(int no)
  {
    return(FileEntry) file_list.elementAt(no);
  }


  /**
   * Get a file size from our distribution table.
   */
  private long getFileSize()
  {
    if (filesizes.length == 1)
      return(long) filesizes[0];

    if (filesizes.length == 2 && filesizes[1] == 0)
      return calculateVarSize();

    int pct = file_size_randomizer.nextInt(100);
    int cumpct = 0;
    int i;

    for (i = 0; i < filesizes.length; i+=2)
    {
      cumpct += filesizes[i+1];
      if (pct < cumpct)
        break;
    }

    long size = (long) filesizes[i];

    return size;
  }


  private long calculateVarSize()
  {
    int mb10  =  10 * 1024 * 1024;
    int mb1   =   1 * 1024 * 1024;
    int kb100 = 100 * 1024;
    int kb10  =  10 * 1024;
    int kb1   =   1 * 1024;

    int wanted = (int) filesizes[0];
    //common.ptod("wanted: " + wanted);
    int half   = wanted / 2;
    int rand   = file_size_randomizer.nextInt(wanted);
    float result = rand + half;


    if (result >= mb10)
      wanted = Math.round(result / mb1) * mb1;
    else if (wanted >= mb1)
      wanted = Math.round(result / kb100) * kb100;
    else if (wanted >= kb100)
      wanted = Math.round(result / kb10) * kb10;
    else
      wanted = Math.round(result / kb1) * kb1;

    //common.ptod("returned: " + wanted);
    return wanted;
  }

  /**
  * Allocate data validation map for this anchor.
  */
  public void allocateDVMapIfNeeded()
  {
    if (!Validate.isValidate())
      return;

    /* Do we already have a map for this anchor? */
    validation_map = DV_map.findMap(anchor_name);
    if (validation_map != null)
    {
      /* If we reuse a map, the blocksize needs to be the same: */
      if (validation_map.getBlockSize() != dv_key_size)
      {
        common.ptod("");
        common.ptod(anchor_name + ": " + Format.f("Key block size from previous run: %7d",
                                                  validation_map.getBlockSize()));
        common.ptod(anchor_name + ": " + Format.f("Key block size for this run:      %7d", dv_key_size));
        common.ptod("Data validation xfersize changed. Data validation map for lun will be cleared");

        if (Validate.isJournaling())
          common.failure("Data Validation with journaling requires that data transfer sizes used are identical");

        /* Delete the old map: */
        DV_map.removeMap(anchor_name);
        validation_map = null;
        System.gc();
      }
    }


    /* If we don't have a map, allocate: */
    if (validation_map == null)
    {
      validation_map = DV_map.allocateMap(fsd_name_active, bytes_in_file_list, dv_key_size);

      /* Journaling requires journal files: */
      if (Validate.isJournaling())
      {
        if (validation_map.journal == null)
        {
          /* Allocate/open journal files: */
          validation_map.journal = new Jnl_entry(jnl_file_name, fsd_name_active);
          validation_map.journal.storeMap(validation_map);

          /* Do we need to recoverOneMap existing journals: */
          if (Validate.isJournalRecoveryActive())
          {
            validation_map.journal.recovery_anchor = this;
            validation_map = validation_map.journal.recoverOneMap(fsd_name_active,
                                                                  bytes_in_file_list,
                                                                  getAnchorName());
            /* The key block size to be used comes from the journal: */
            dv_key_size = validation_map.getBlockSize();
          }

          else
          {
            /* Dump the map now so that we know we'll have enough space for        */
            /* at least the maps. (Journal records of course is a different story) */
            validation_map.journal.dumpOneMap(validation_map);
          }
        }
      }
    }
  }


  /**
   * Xfersizes for Data Validation must be multiples of the shortest, so we keep
   * track of them.
   */
  public void trackXfersizes(double[] sizes)
  {
    //common.ptod("trackXfersizes: " + anchor_name);
    /* Look at all new sizes: */
    top: for (int i = 0; i < sizes.length; i+=2)
    {
      int size = (int) sizes[i];

      /* If the new size is not in the list yet, add: */
      for (int j = 0; j < xfersizes_used.size(); j++)
      {
        Integer old = (Integer) xfersizes_used.elementAt(j);
        if (old.intValue() == size)
          continue top;
      }

      xfersizes_used.add(new Integer(size));
      dv_largest_xfer_size = Math.max(dv_largest_xfer_size, size);
      //common.ptod("trackXfersizes(): " + size);
    }
  }


  public void trackFileSizes(double[] sizes)
  {
    /* Look at all new sizes: */
    top: for (int i = 0; i < sizes.length; i+=2)
    {
      int size = (int) sizes[i];

      /* If the new size is not in the list yet, add: */
      for (int j = 0; j < filesizes_used.size(); j++)
      {
        Integer old = (Integer) filesizes_used.elementAt(j);
        if (old.intValue() == size)
          continue top;
      }

      filesizes_used.add(new Integer(size));
    }
  }


  /**
   * Largest xfersize used for this anchor.
   */
  public int getMaxXfersize()
  {
    return dv_max_xfer_size;
  }

  /**
   * Largest xfersize in the whole Vdbench run.
   * Used to create data pattern buffers.
   */
  public static int getLargestXfersize()
  {
    return dv_largest_xfer_size;
  }

  /**
   * Determine the lowest xfersize used to serve as the length for each
   * Data Validation key.
   */
  public void calculateKeyBlockSize()
  {
    /* First sort the list of xfersizes: */
    Integer[] sizes = (Integer[]) xfersizes_used.toArray(new Integer[0]);
    Arrays.sort(sizes);

    /* For journal recovery we can run into this: */
    if (sizes.length == 0)
      common.failure("No 'xfersize=' parameters found for anchor=" + anchor_name +
                     ". Are you sure this anchor is used?");

    /* All xfersizes must be a  multiple of the first: */
    dv_key_size      = sizes[0].intValue();
    dv_max_xfer_size = sizes[sizes.length-1].intValue();
    for (int i = 0; i < xfersizes_used.size(); i++)
    {
      int next = ((Integer) xfersizes_used.elementAt(i)).intValue();
      if (next % dv_key_size != 0)
      {
        common.ptod("During Data validation all data transfer sizes used for ");
        common.ptod("an FWD must be a multiple of the lowest xfersize.");
        common.ptod("(A format run may have added a transfer size of 64k).");

        for (int j = 0; j < xfersizes_used.size(); j++)
        {
          next = ((Integer) xfersizes_used.elementAt(j)).intValue();
          common.ptod("Xfersize used in parameter file: " + next);
        }

        common.failure("Xfersize error");
      }
    }
  }


  /**
  * Data validation requires the file sizes to also be multiples of the
  * transfer sizes.
  */
  public void matchFileAndXfersizes()
  {
    if (!Validate.isValidate())
      return;

    /* Sort the list of xfersizes and get the shortest size: */
    Integer[] sizes = (Integer[]) xfersizes_used.toArray(new Integer[0]);
    Arrays.sort(sizes);

    int shortest_xfer = sizes[0].intValue();

    /* All file sizes must be a  multiple of the shortest xfersize: */
    for (int i = 0; i < filesizes_used.size(); i++)
    {
      int next = ((Integer) filesizes_used.elementAt(i)).intValue();
      if (next % shortest_xfer != 0)
      {
        common.ptod("During Data validation all file sizes used for ");
        common.ptod("an FWD must be a multiple of the lowest xfersize.");
        common.ptod("(A format run may have added a transfer size of 64k).");

        common.ptod("Lowest xfersize: " + shortest_xfer);
        for (int j = 0; j < filesizes_used.size(); j++)
        {
          next = ((Integer) filesizes_used.elementAt(j)).intValue();
          common.ptod("File size used in parameter file: " + next);
        }

        common.failure("File size error");
      }
    }
  }

  public KeyMap allocateKeyMap(long logical_lba)
  {
    return new KeyMap(validation_map, logical_lba, dv_key_size, dv_max_xfer_size);
  }

  public DV_map getDVMap()
  {
    return validation_map;
  }

  public DV_map getValidationMap()
  {
    return validation_map;
  }

  public ControlFile getControlFile()
  {
    return control_file;
  }

  /**
   * This must be done to make sure that GC can clean things up.
   * FileAnchor points to FileAnchor vv.
   */
  public void clearControlFile()
  {
    control_file = null;
  }

  /**
   * A trick to prevent Java just wasting all its time allocating instances
   * doing loads of GC's every time when ultimately it will run out of memory
   * anyway.
   * What I do here is allocate byte arrays worth one million FileEntry instances
   * of 64 bytes each. When we run out of memory now at least we'll know it while
   * otherwise Java just keeps on trying and trying.
   *
   * Java will keep running GC each time he runs out of memory and when he
   * can't findMap any he'll increase the current heap size in small
   * portions. Once this increment is used up again he'll run GC again.
   * Over and over.
   *
   * Note: when having multiple anchors in a slave the code here may not
   * discover that there can be memory problems. That would require me to
   * do this for all anchors at the same time.
   */
  private void checkMemory()
  {
    int ESTIMATED_FILENTRY_SIZE = 64;
    int CHUNK = 1*1000*1000;
    int loop  = (int) maximum_file_count / CHUNK;
    byte[][] arrays = new byte[loop][];

    // Could it be that the previous lists are not GC'ed?
    common.memory_usage();
    System.gc();
    System.gc();
    System.gc();
    for (int i = 9990; i < 12; i++)
    {
      common.memory_usage();
      common.ptod("sleeping 5 seconds to see if finalize() gets to complete");
      common.sleep_some(5000);
      System.gc();
      VdbCount.listCounters("CheckMemory");
    }
    common.ptod("checkMemory()");
    common.memory_usage();
    VdbCount.listCounters("CheckMemory");

    /* If we have more than CHUNK entries, 'pre-allocate' the memory. */
    /* If we have less than CHUNK entries, don't bother (loop=0)      */
    try
    {
      for (int i = 0; i < loop; i++)
      {
        arrays[i] = new byte[ CHUNK * ESTIMATED_FILENTRY_SIZE];
      }
    }

    catch (OutOfMemoryError e)
    {
      common.memory_usage();
      common.where();
      VdbCount.listCounters("OutOfMemoryError");
      SlaveJvm.sendMessageToConsole("Pre-allocation check for memory needs for "+
                                    "files failed.");
      SlaveJvm.sendMessageToConsole("Each file needs about " + ESTIMATED_FILENTRY_SIZE +
                                    " bytes of memory for " + maximum_file_count + " files."+
                                    " Increase Java heap space.");
      SlaveJvm.sendMessageToConsole("Increase -Xmx value in Vdbench startup script"+
                                    " for the Vdbench.SlaveJvm start.");
      arrays = null;
      common.failure("Not enough memory. See the message above.");
    }

    /* This will let GC know the (temp) memory is no longer needed: */
    arrays = null;
    System.gc();
  }

  public void setDeletePending(boolean bool)
  {
    delete_pending = bool;
  }
  public boolean isDeletePending()
  {
    return delete_pending;
  }


  public void swapAnchorName()
  {
    anchor_name = windows_anchor_name;
  }

  public static String whatSize(double size)
  {
    if (size < KB)
      return "" + size;

    String txt;
    if (size < MB)
      txt = Format.f("%.3fk", size / KB);
    else if (size < GB)
      txt = Format.f("%.3fm", size / MB);
    else if (size < TB)
      txt = Format.f("%.3fg", size / GB);
    else if (size < PB)
      txt = Format.f("%.3ft", size / TB);
    else
      txt = Format.f("%.3fp", size / PB);

    String front = txt.substring(0, txt.length() - 5);
    String tail  = txt.substring(txt.length() - 5);
    if (tail.startsWith(".000"))
      txt = front + tail.substring(4);

    //common.ptod("front: " + front);
    //common.ptod("tail: " + tail);

    return txt;
  }

  public static String whatSize1(double size)
  {
    if (size < KB)
      return "" + size;
    else if (size < MB)
      return Format.f("%.1fk", size / KB);
    else if (size < GB)
      return Format.f("%.1fm", size / MB);
    else if (size < TB)
      return Format.f("%.1fg", size / GB);
    else if (size < PB)
      return Format.f("%.1ft", size / TB);
    else
      return Format.f("%.1fp", size / PB);
  }

  public void reportSizes(long existing_files,
                          long existing_dirs,
                          long existing_bytes,
                          long files_opened,
                          long size_opened)
  {
    AnchorReport ar = new AnchorReport(getAnchorName(),

                                       getFileList().size(),
                                       getDirectoryList().size(),
                                       bytes_in_file_list,

                                       (use_list != null) ? use_list.size() : 0,
                                       (use_list != null) ? getDirectoryList().size() : 0,
                                       (use_list != null) ? bytes_in_use_list : 0,

                                       (wss_list != null) ? wss_list.size() : 0,
                                       (wss_list != null) ? getDirectoryList().size() : 0,
                                       (wss_list != null) ? bytes_in_wss_list : 0,

                                       existing_files,
                                       existing_dirs,
                                       existing_bytes,

                                       files_opened,
                                       size_opened);
  }

  public static void main2(String[] args)
  {
    Random file_size_randomizer = new Random(0);
    double[] filesizes = new double[] { 100*1024*1024, 0};
    long total = 0;
    int LOOP = Integer.parseInt(args[0]);

    int mb10  =  10 * 1024 * 1024;
    int mb1   =   1 * 1024 * 1024;
    int kb100 = 100 * 1024;
    int kb10  =  10 * 1024;
    int kb1   =   1 * 1024;


    for (int i = 0; i < LOOP; i++)
    {
      int wanted = (int) filesizes[0];
      //common.ptod("wanted: " + wanted);
      int half   = wanted / 2;
      int rand   = file_size_randomizer.nextInt(wanted);
      float result = rand + half;

      if (result >= mb10)
        wanted = Math.round(result / mb1) * mb1;
      else if (wanted >= mb1)
        wanted = Math.round(result / kb100) * kb100;
      else if (wanted >= kb100)
        wanted = Math.round(result / kb10) * kb10;
      else
        wanted = Math.round(result / kb1) * kb1;

      //common.ptod("wanted: " + Format.f("%12.3f", result / mb1) + Format.f(" %12d", (int) result) + Format.f(" %12d", wanted));
      total += (long) wanted;

    }
    common.ptod("Qverage: " + (int) filesizes[0] + " " + (int) (total/LOOP));
  }


  public static void main(String[] args)
  {
    FileAnchor anchor = new FileAnchor();
    anchor.filesizes  = new double[] { 100*1024*1024, 0};
    int LOOP = Integer.parseInt(args[0]);
    anchor.file_size_randomizer = new Random(0);

    long total = 0;


    for (int i = 0; i < LOOP; i++)
    {
      total += anchor.calculateVarSize();
    }

    common.ptod("Qverage: " + (int) anchor.filesizes[0] + " " + (int) (total/LOOP));
  }
}










