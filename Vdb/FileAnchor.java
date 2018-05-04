package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.lang.*;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.*;

/**
 * This class contains all data needed for a specific directory
 */
public class FileAnchor extends Directory implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private boolean build_structure      = true;
  private String  anchor_name;
  private String  windows_anchor_name;
  public  String  fsd_name_active;
  public  String  fsd_name_8bytes;

  public  String  time_initialized = null;

  private DV_map  dv_map       = null;
  private DedupBitMap dedup_bitmap;
  public  String  jnl_dir_name;
  private boolean reported_this_anchor = false;

  private boolean file_sharing = false;

  public  int      depth      = -1;
  public  int      width      = -1;
  public  int      files      = -1;
  public  String   dist       = "bottom";
  public  double[] filesizes  = null;
  public  long     total_size  = -1;
  public  long     working_set;
  public  long     relative_dedup_offset = 0;

  private boolean  delete_pending = false;
  private boolean  once_message_sent = false;

  public  boolean  format_complete_used = false;

  private long     delete_dir_count;
  private long     delete_file_count;
  private long     delete_start;

  private ControlFile control_file;

  public  int    total_directories  = 0;  /* Amount of directories in anchor  */
  public  int    maximum_file_count = 0;  /* Amount of files in anchor        */

  private int    existing_dirs      = 0;  /* Amount of created directories    */
  private int    existing_files     = 0;  /* Amount of created files          */
  private int    full_file_count    = 0;  /* Amount of files that are full    */


  private int    round_robin_files = 0;
  private int    round_robin_dirs = 0;

  private Vector dir_list  = null;        /* List of Directory instances      */

  // Maybe some day change these to ArrayList?
  private Vector <FileEntry> file_list = null; /* List of FileEntry instances */
  private Vector use_list  = null;             /* List of (totalsize=)        */
  private Vector wss_list  = null;             /* Same, only for active WSS   */

  public  long   bytes_in_file_list = 0;  /* What we have in file_list        */
  private long   bytes_in_use_list  = 0;  /* What we have in use_list         */
  private long   bytes_in_wss_list  = 0;  /* What we have in wss_list         */

  private Random file_select_randomizer = new Random();
  private Random dir_select_randomizer  = new Random();

  public  Vector  devxlate_list = null;   /* list of disks for this anchor    */

  private Random file_size_randomizer = null;  /* Must have fixed seed        */

  private HashMap xfersizes_map    = new HashMap(8);
  private HashMap filesizes_used   = new HashMap(8);
  private int     key_block_size    = 0; /* Greatest common divisor of DV xfer*/
  private int     dv_max_xfer_size  = 0; /* Maximum DV xfersize               */

  public  FormatCounter mkdir_threads_running;
  public  FormatCounter create_threads_running;

  public  int    random_files_touched = 0;

  public  int    last_format_pct = -1;

  public  boolean create_rw_log = false;
  public  Fput    rw_log = null;

  public HashMap <Long, BadDataBlock>  bad_data_map     = null;

  public  Dedup   dedup = null;

  /* This map contains file names and key block lbas for pending writes */
  /* found during journal recovery:                                     */
  public HashMap   <FileEntry, HashMap <Long, Long> > pending_file_lba_map = null;
  public ArrayList <FileEntry>                        pending_files        = null;

  private static HashMap <String, Fput> open_rwlog_map = new HashMap(8);

  private static int    dv_largest_xfer_size  = 0;  /* Largest in whole run   */
  private static Vector <FileAnchor> anchor_list  = new Vector(8);

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



  public static Vector <FileAnchor> getAnchorList()
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
    common.plog("Creating anchor for " + fsd.dirname + ",fsd=" + fsd.name);

    /* If we already have this anchor, just return the old one: */
    for (int i = 0; i < anchor_list.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchor_list.elementAt(i);
      if (anchor.anchor_name.equals(fsd.dirname))
        return anchor;
    }

    FileAnchor anchor    = new FileAnchor();
    anchor.anchor_name   = fsd.dirname;
    anchor.jnl_dir_name  = fsd.jnl_dir_name;
    anchor.dist          = fsd.dist;
    anchor.create_rw_log = fsd.create_rw_log;
    anchor_list.addElement(anchor);

    /* For DV we need to know the name of the active FSD: */
    anchor.fsd_name_8bytes = (fsd.name + "        ").substring(0,8);
    anchor.fsd_name_active = fsd.name;

    /* Create anchor directory instance if it is not there: */
    // code may be obsolete since THIS runs on the master.
    //anchor.dir_ptr = new File(anchor.anchor_name);
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
    time_initialized = common.tod();

    /* Randomizer must have fixed seed. In that way when we expand the */
    /* file list we get the same sizes as before. */
    file_size_randomizer = new Random(0); /* Always fixed seed! */

    file_sharing = fwg.file_sharing;

    dedup = fwg.dedup;

    //matchFileAndXfersizes();

    random_files_touched = 0;

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

    /* If we already have a complete structure, exit: */
    /* Note: reusing structure is ONLY wthin one single RD, not across RDs! */
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
    dir_list = createDirectoryList(anchor_name);

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
    if (!SlaveWorker.work.format_run)
      createWorkingSetList();

    /* Data Validation may want to get some maps: */
    if (Validate.isValidate())
      allocateDVMap();

    matchFileAndXfersizes();

    /* Write all the filenames to the anchor directory.                     */
    /* We can use this later to verify that FWD parameters were not changed */
    /* possibly causing strange performance problems because of old stuff   */
    /* left behind in the directories:                                      */
    //common.ptod("SlaveWorker.work.keep_controlfile: " + SlaveWorker.work.keep_controlfile);
    if (!fwg.shared)
    {
      /* If we don't do deletes or creates, keep the current control file content: */
      if (!SlaveWorker.work.keep_controlfile)
        control_file.writeControlFile(fwg.shared, true);
      else
      {
        common.ptod("No deletes and creates. Control file not cleared.");
      }
    }

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


    /* We may need to do a little setup work here: */
    if (common.get_debug(common.CREATE_READ_WRITE_LOG))
      setupReadWriteLog(fwg.fsd_name);
  }


  /**
   * Create list of directories.
   * The list is sorted at the end using the length of the directory name. This
   * assures that the parents of a directory are always on top.
   */
  public Vector createDirectoryList(String anchor_name)
  {
    Directory.clearStaticDirectoryList();

    /* Preallocate some memory to eliminate too much GC activiy: */
    //checkMemory();
    if (SlaveWorker.work.format_flags != null)
    {
      format_complete_used = SlaveWorker.work.format_flags.format_complete;
      if (format_complete_used)
      {
        common.ptod("");
        common.ptod("'format=(no,complete)' has been used. Directory and file status "+
                    "will not be verified.");
        common.ptod("Results unpredictable if the file structure is NOT complete.");
        common.ptod("");
      }
    }


    /* Create all directories recursively: */
    for (int w = 0; w < width; w++)
      new Directory(this, w, 1, this, width, depth);

    common.ptod("Completed the creation of the directory list for %s: %,d directories.",
                anchor_name, Directory.getTempDirectoryList().size());

    /* Get the temporary directory list. Clear it right sway for GC: */
    Vector new_list = Directory.getTempDirectoryList();
    Directory.clearStaticDirectoryList();

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
   * Calculate total size of this FSD, using the real file structure as defined
   * by the user.
   */
  public void calculateFsdSize()
  {
    file_size_randomizer = new Random(0); /* Always fixed seed! */

    int dedupunit        = Dedup.getDedupUnit();
    Directory.clearStaticDirectoryList();

    /* Create all directories recursively: */
    for (int w = 0; w < width; w++)
      new Directory(this, w, 1, this, width, depth);

    /* Get the temporary directory list. Clear it right away for GC: */
    Vector <Directory> tmp_list = Directory.getTempDirectoryList();
    Directory.clearStaticDirectoryList();

    /* When we have a huge amount, display message when it takes too long: */
    long start = System.currentTimeMillis();
    Collections.sort(tmp_list);
    long seconds  = (System.currentTimeMillis() - start) / 1000;
    if (seconds > 10)
      common.ptod("Directory name sort took " + seconds + " seconds.");


    /* Scan through the directories and pick up each file size: */
    bytes_in_file_list = 0;
    for (Directory dir : tmp_list)
    {
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
        // we need total size here.
        //if (fwg.shared && relative_file_count++ % slave_count != slave_number)
        //  continue;

        bytes_in_file_list += file_size;
      }
    }

    /* The randomizer must be reset to guarantee proper file size selection: */
    file_size_randomizer = new Random(0); /* Always fixed seed! */
  }


  /**
   * Create list of FileEntry instances.
   * Files are created in order for each directory
   * Note: remember that the directory list is sorted on LENGHT of the directory
   * name, this to assure that the parents are always at the beginning.
   */
  private void createFileList(FileAnchor anchor, Vector dirlist, FwgEntry fwg)
  {
    /* For very large structures, clean up memory BEFORE: */
    file_list = null;
    //GcTracker.gc();

    Signal signal = new Signal(2);
    file_list     = new Vector(maximum_file_count);
    int     created_file_count  = 0;
    int     relative_file_count = 0;
    int     slave_number        = SlaveWorker.work.slave_number;
    int     slave_count         = SlaveWorker.work.slave_count;
    int     dedupunit           = Dedup.getDedupUnit();
    boolean dedup               = Dedup.isDedup();

    /* Preallocate some memory to eliminate too much GC activiy: */
    //checkMemory();

    /* Go through each directory and create all files for that directory: */
    bytes_in_file_list = 0;
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
        FileEntry fe = new FileEntry(dir, j+1, file_size,
                                     (dedup) ? bytes_in_file_list : bytes_in_file_list,
                                     file_list.size());
        //common.ptod("fe: " + fe.getFullName() + " " + file_size);
        file_list.add(fe);
        created_file_count++;

        if (created_file_count % (10 * 1000 * 1000l) == 0) // && signal.go())
        {
          //common.memory_usage();
          common.ptod("Generated %,d file names; total anchor size: %s",
                      created_file_count, whatSize(bytes_in_file_list));
          SlaveJvm.sendMessageToConsole("Continuing the creation of internal "+
                                        "file structure for anchor=%s: %,d files.",
                                        getAnchorName(), created_file_count);
        }

        bytes_in_file_list += file_size;
      }
    }

    common.ptod("Generated %,d file names; total anchor size: %s (%,d)",
                created_file_count, whatSize(bytes_in_file_list), bytes_in_file_list);
    if (signal.getAge() > 30)
      SlaveJvm.sendMessageToConsole("Completing the creation of internal "+
                                    "file structure for anchor=%s: %,d files.",
                                    getAnchorName(), created_file_count);


    if (file_list.size() == 0)
      common.failure("No files available for this slave. Did you ask for more " +
                     "slaves than files?");

    if (common.get_debug(common.CREATE_FILE_LIST))
    {
      int maxlen = 0;
      for (FileEntry fe : file_list)
        maxlen = Math.max(maxlen, fe.getFullName().length());
      String mask = "%-" + fwg.fsd_name.length() + "s %-" + maxlen + "s";

      String hdr = String.format(mask + " %12s %8s %12s %12s",
                                 "FSD", "file name", "size", "size", "loglba_start", "loglba_end+1");
      common.ptod("");
      common.ptod(hdr);

      for (FileEntry fe : file_list)
      {
        String txt = String.format(mask + " %12d %8s %12x %12x",
                                   fwg.fsd_name,
                                   fe.getFullName(),
                                   fe.getReqSize(),
                                   common.whatSizeX(fe.getReqSize(), 1),
                                   fe.getFileStartLba(),
                                   fe.getFileStartLba() + fe.getReqSize());
        common.ptod(txt);
      }
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
        common.failure("still files busy: " + fe.getFullName());
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
      if (!fe.setFileBusyExc())
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
        fe.setUnBusy();
        subset_list.add(fe);
      }
    }

    for (int i = Integer.MAX_VALUE; i < subset_list.size(); i++)
    {
      FileEntry fe = (FileEntry) subset_list.elementAt(i);
      common.ptod("fe2: " + fe.getFullName());
    }

    SlaveJvm.sendMessageToConsole("Created totalsize=%6s using %,12d of %,12d files for anchor=%s",
                                  whatSize1(total_size),
                                  subset_list.size(),
                                  file_list.size(),
                                  getAnchorName());

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
      if (!fe.setFileBusyExc())
      {
        /* Loop protection: */
        if (busy_blocks++ > 1000000)
          common.failure("Unable to create a working set size (wss) subset.");
        continue;
      }

      /* Use this file for the subset. Keep it busy to prevent reuse: */
      busy_blocks = 0;
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
        fe.setUnBusy();
        subset_list.add(fe);
      }
    }

    //String txt = "Created workingset=" + whatSize(working_set) +
    //             " subset for anchor=" + getAnchorName() +
    //             " using " + subset_list.size() +
    //             " of " + list_to_use.size() + " files.";
    SlaveJvm.sendMessageToConsole("Created workingset=%6s using %,12d of %,12d files for anchor=%s",
                                  whatSize1(working_set),
                                  subset_list.size(),
                                  list_to_use.size(),
                                  getAnchorName());
    //SlaveJvm.sendMessageToConsole(txt);

    if (subset_list.size() == 0)
      common.failure("Subset creation failed. Zero files found.");

    wss_list = subset_list;


    for (int i = Integer.MAX_VALUE; i < wss_list.size(); i++)
    {
      FileEntry fe = (FileEntry) wss_list.elementAt(i);
      common.ptod("fe3: " + fe.getFullName());
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
    File[] dirptrs = new File(parent).listFiles();

    /* This is likely caused by the use of 'shared=yes' with an other slave */
    /* deleting this directory.                                             */
    if (dirptrs == null)
    {
      common.ptod("readDirsAndDelete(): directory not found and ignored: " + parent);
      return;
    }

    for (File dirptr : dirptrs)
    {
      String name = dirptr.getName();
      //common.ptod("name: " + parent + " " + name);

      if (name.endsWith(InfoFromHost.NO_DISMOUNT_FILE))
        continue;

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
        if (!name.endsWith(".dir") &&
            (!name.endsWith(".file") && !name.endsWith(".file.gz")))
          continue;
      }

      /* If this is a file, delete it: */
      if (name.endsWith("file"))
      {
        long begin_delete = Native.get_simple_tod();
        if (!dirptr.delete())
        {
          if (!fwg.shared)
            common.failure("Unable to delete file: " + dirptr.getAbsolutePath());
          else
            fwg.blocked.count(Blocked.FILE_DELETE_SHARED);
        }
        else
        {

          FwdStats.count(Operations.DELETE, begin_delete);
          fwg.blocked.count(Blocked.FILE_DELETES);
          if (++delete_file_count % 5000 == 0 && signal.go())
            SlaveJvm.sendMessageToConsole("anchor=%s deleted %,d files; %,d/sec",
                                          anchor_name, delete_file_count,
                                          delete_file_count * 1000 / (System.currentTimeMillis() - delete_start));
        }
        continue;
      }

      /* Directories get a recursive call before they are deleted: */
      readDirsAndDelete(fwg, dirptr.getAbsolutePath(), signal);

      /* Once back here the directory should be empty and can be deleted: */
      long begin_delete = Native.get_simple_tod();
      if (!dirptr.delete())
      {
        if (!fwg.shared)
        {
          common.ptod("");
          common.ptod("Unable to delete directory: " + dirptr.getAbsolutePath());
          common.ptod("Are there any files left that were not created by Vdbench?");
          common.failure("Unable to delete directory: " + parent + File.separator + name +
                         ". \n\t\tAre there any files left that were not created by Vdbench?");
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

    SlaveJvm.sendMessageToConsole("Starting cleanup for anchor=" + this.anchor_name);

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

    //String asize = "n/a";
    //try
    //{
    //  asize = common.whatSizeX(new File(anchor_name).getUsableSpace(), 1);
    //}
    //catch (Exception e)
    //{
    //}
    //SlaveJvm.sendMessageToConsole("anchor=" + this.anchor_name +
    //                              " Estimated available file system size: " + asize);
    //SlaveJvm.sendMessageToConsole("anchor=" + this.anchor_name +
    //                              " Estimated maximum anchor file size:   " +
    //                              common.whatSizeX(est_max_byte_count, 1));

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
  private int[] select_counters = null;
  public synchronized FileEntry getFile(FwgEntry fwg)
  {
    boolean debug2 = false;

    if (debug2) common.ptod("getFile0 start: ");
    Vector list_to_use;
    if (wss_list != null)
      list_to_use = wss_list;
    else if (use_list != null)
      list_to_use = use_list;
    else
      list_to_use = file_list;

    /* During journal recovery we first have to read all pending blocks: */
    if (Validate.isJournalRecovery() && pending_files != null )
    {
      if (pending_files.size() > 0)
      {
        FileEntry fe = pending_files.get(0);

        /* 'pending_writes' flag will be reset at seq eof: */
        fe.pending_writes = true;

        /* Remove this file from 'pending files to read': */
        pending_files.remove(fe);
        if (debug2) common.ptod("getFile1: " + fe);
        common.ptod("this file has pending writes: " + fe);
        return fe;
      }


      /* When the last file has been picked up, either by this thread      */
      /* or by an other thread, clear the pending_files map.               */
      /* The pending lba map must stick around, since we can have an other */
      /* thread still needing it.                                          */
      if (pending_files.size() == 0)
      {
        /* Not completely 'complete', an other thread may still be reading. */
        ErrorLog.plog("Verifying of pending writes for fsd=%s complete", fsd_name_active);
        pending_files        = null;
        //pending_file_lba_map = null;
      }
    }


    if (fwg.select_random)
    {
      /* If fileselect=once is given, try until we've touched all files: */
      /* (With a huge amount of files this may loop a bit!)              */
      if (fwg.select_once)
      {
        while (true)
        {
          if (random_files_touched >= getFileCount())
          {
            if (!once_message_sent)
              SlaveJvm.sendMessageToSummary("Reached 'fileselect=once' for anchor %s", anchor_name);
            once_message_sent = true;

            /* See note below under 'sequential' */
            if (debug2) common.ptod("getFile2: null");
            return null;
          }

          /* distPoisson() is NEVER used for 'once': */
          int file_number = file_select_randomizer.nextInt(list_to_use.size());
          FileEntry fe = (FileEntry) list_to_use.elementAt(file_number);
          if (!fe.isSelected())
          {
            fe.setSelected();
            random_files_touched ++;
            if (debug2) common.ptod("getFile3: " + fe);
            return fe;
          }
        }
      }

      /* Normal random or skewed file selection: */
      int file_number;
      if (fwg.poisson_skew == 0)
        file_number = file_select_randomizer.nextInt(list_to_use.size());
      else
        file_number = (int) ownmath.distPoisson(list_to_use.size(), fwg.poisson_skew);

      // Don't remove
      // debugging: creation a simple distribution chart of selected files.
      if (false)
      {
        if (select_counters == null)
          select_counters = new int[ list_to_use.size() ];

        if (select_counters != null)
          select_counters[ file_number ]++;
      }


      FileEntry fe = (FileEntry) list_to_use.get(file_number);
      if (debug2) common.ptod("getFile4: " + fe);

      return fe;
    }

    /* Did we pass through roundrobin for journal recovery? */
    //common.ptod("round_robin_files: " + list_to_use.size() + " " + round_robin_files );
    if (round_robin_files >= list_to_use.size())
    {
      if (Validate.isJournalRecoveryActive())
      {
        if (debug2) common.ptod("getFile5: null");
        return null;
      }
      if (SlaveWorker.work.format_run)
      {
        if (debug2) common.ptod("getFile6: null");
        return null;
      }
    }

    /* Sequential scanning of the file list: */
    /* Round-robin over the whole list: */
    if (round_robin_files >= list_to_use.size())
    {
      if (fwg.select_once)
      {
        if (!once_message_sent)
          SlaveJvm.sendMessageToSummary("Reached 'fileselect=once' for anchor %s", anchor_name);
        once_message_sent = true;

        /* There is NO check as there is with SDs that when there are other */
        /* workloads besides 'once', we still terminate after the last of   */
        /* the 'once' runs are done.                                        */
        /* That is OK with me.                                              */
        /* Also see Task_num.checkAllInTermination()                        */

        if (debug2) common.ptod("getFile7: null");
        return null;
      }
      round_robin_files = 0;
    }

    FileEntry fe = (FileEntry) list_to_use.elementAt(round_robin_files++);
    if (debug2) common.ptod("getFile8: " + fe);
    return fe;
  }


  /**
   * See if we have to reached the end of going through our list of files in
   * round-robin mode. This signifies 'done with format' Ugly!
   */
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
      /* The result is that when a caller gets the (depth == 1) directory */
      /* we are not doing round-robin. That should be fixed some day.     */
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

    //common.ptod("getDir: rr %6d  sz %3d fmt %b rnd %b %s %d", round_robin_dirs, dir_list.size(),
    //            format, select_random, dir.buildFullName(), dir.getDepth());

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
        common.ptod("countExistingFiles: " + " + " + existing_files + " " + fe.getFullName());
      else
        common.ptod("countExistingFiles: " + " - " + existing_files + " " + fe.getFullName());
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
      common.failure("negative file count for " + fe.getFullName() + " " + fe.getCurrentSize());

    if (false)
    {
      if (c > 0)
        common.ptod("countFullFiles: " + " + " + full_file_count + " " + fe.getFullName());
      else
        common.ptod("countFullFiles: " + " - " + full_file_count + " " + fe.getFullName());
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
        common.plog("anchor=" + anchor.anchor_name);
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
          common.ptod("total_directories: %12d 0x%08x", total_directories, total_directories);
          if (total_directories < 0)
          {
            common.failure("32-bit overflow trying to calculate the amount of directories. "+
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

    /* Determine how many files we'll allow per FSD: */
    long MAX32 =  32l * 1024l * 1024l;
    long MAX64 = 128l * 1024l * 1024l;
    long MAX   = MAX32;

    if (common.running64Bit() || Vdbmain.simulate)
      MAX = MAX64;
    if (common.get_debug(common.SMALL_FILE_COUNT))
      MAX = 100;
    if (maximum_file_count > MAX)
    {
      txt.add("");
      txt.add("New FileAnchor for directory '" + anchor_name + "'.");
      txt.add("depth=" + depth);
      txt.add("width=" + width);
      txt.add("files=" + files);
      txt.add("dist="  + dist);
      txt.add(String.format("There will be %,12d directories and %,12d files under this anchor.",
                            total_directories, maximum_file_count));
      txt.add("Vdbench code currently supports only a maximum of " + MAX + " files.");
      txt.add("To work around this you can specify multiple smaller FSDs spread");
      txt.add("out over multiple JVMs.");

      if (!common.running64Bit())
        txt.add("Vdbench code for 64bit java supports " + MAX64 + " files.");


      if (SlaveJvm.isThisSlave())
        SlaveJvm.sendMessageToConsole(txt);
      else
        common.ptod(txt);

      common.failure("Too many files");
    }


    /* Code here reports the size of an anchor only once, though it is possible */
    /* for an anchor to change using a different FSD: */
    //reported_this_anchor = true;
    //txt.add(String.format("Estimate: anchor=%s: dirs: %,12d; files: %,12d; bytes: %10s (%,d)",
    //                      getAnchorName(), total_directories, maximum_file_count,
    //                      common.whatSize(est_max_byte_count), est_max_byte_count));
    calculateFsdSize();
    txt.add(String.format("Anchor size: anchor=%s: dirs: %,12d; files: %,12d; bytes: %10s (%,d)",
                          getAnchorName(), total_directories, maximum_file_count,
                          common.whatSize(bytes_in_file_list), bytes_in_file_list));

    String asize = "n/a";
    String fsize = "n/a";
    //try
    //{
    //  if (Fget.dir_exists(anchor_name))
    //  {
    //    fsize = common.whatSizeX(new File(anchor_name).getTotalSpace(), 1);
    //    asize = common.whatSizeX(new File(anchor_name).getUsableSpace(), 1);
    //  }
    //  else
    //  {
    //    String parent = new File(anchor_name).getParentFile().getAbsolutePath();
    //    fsize = common.whatSizeX(new File(parent).getTotalSpace(), 1);
    //    asize = common.whatSizeX(new File(parent).getUsableSpace(), 1);
    //  }
    //}
    //catch (Exception e)
    //{
    //}
    //txt.add("Estimated maximum size for this anchor: " + common.whatSize(est_max_byte_count) +
    //        "; estimated file system size: " + fsize +
    //        "; estimated available size (before possible anchor delete): " + asize);

    //if (total_size != 0 && total_size > est_max_byte_count)
    //  common.failure("Requested totalsize=" + whatSize(total_size) +
    //              " less than estimated maximum size for this anchor: " +
    //              whatSize(est_max_byte_count));

    long entries = total_directories + maximum_file_count;
    heap_needed += entries * 100;
    //if (heap_needed > 100*1024*1024)
    //
    //  txt.add(String.format("Estimated amount of Java heap space needed: %,12d "+
    //                        "(files + directories) * 100 bytes = %,12d bytes (%s)",
    //                        entries, entries * 100, whatSize(entries * 100)));
    //
    //txt.add("");

    /* Report only once: */
    if (print && !reported_this_anchor)
    {
      common.ptod(txt);
      reported_this_anchor = true;
    }

    if (total_size != Long.MAX_VALUE && total_size > bytes_in_file_list)
    {
      common.ptod("rd=" + RD_entry.next_rd.rd_name);
      common.failure("fwd=" + fwg.getName() + ",fsd=" + fwg.fsd_name +
                     ": The requested totalsize=" + whatSize(total_size) +
                     " is greater than the estimated total anchor size of " +
                     whatSize(bytes_in_file_list));
    }

    if (working_set > bytes_in_file_list)
      common.failure("fwd=" + fwg.getName() + ",fsd=" + fwg.fsd_name +
                     ": The requested workingset=" + whatSize(working_set) +
                     " is greater than the estimated total anchor size of " +
                     whatSize(bytes_in_file_list));

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

    //if (heap_needed > 100*1024*1024)
    //{
    //  common.ptod("*");
    //  common.ptod("Estimate of the total amount of memory needed for all anchors: %,d bytes or %s",
    //              heap_needed, common.whatSize(heap_needed));
    //  common.ptod("This will be spread over the requested JVMs.");
    //  common.ptod("If needed increase the -Xmx values in your vdbench script.");
    //  common.ptod("*");
    //}
  }

  public Vector <FileEntry> getFileList()
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

  /**
   * Synchronizing to make sure both fields are in sync.
   */
  public synchronized boolean allFilesFull()
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
  public void allocateDVMap()
  {
    //if (jnl_dir_name == null)
    //  common.failure("unknown journal");

    /* Do we already have a map for this anchor? */
    dv_map = DV_map.findExistingMap(fsd_name_active);
    if (dv_map != null)
    {
      /* If we reuse a map, the blocksize needs to be the same: */
      if (dv_map.getKeyBlockSize() != key_block_size)
      {
        common.ptod("");
        common.ptod(anchor_name + ": " + Format.f("Key block size from previous run: %7d",
                                                  dv_map.getKeyBlockSize()));
        common.ptod(anchor_name + ": " + Format.f("Key block size for this run:      %7d", key_block_size));
        common.ptod("Data validation xfersize changed. Data validation map for lun will be cleared");

        if (Validate.isJournaling())
          common.failure("Data Validation with journaling requires that data transfer sizes used are identical");

        /* Delete the old map: */
        DV_map.removeMap(anchor_name);
        dv_map = null;
        GcTracker.gc();
      }
    }


    /* If we don't have a map, allocate: */
    if (dv_map != null)
      dv_map.setDedup(dedup);

    else
    {
      dv_map = DV_map.allocateMap(jnl_dir_name, fsd_name_active,
                                  bytes_in_file_list, key_block_size);
      dv_map.setDedup(dedup);

      /* Journaling requires journal files: */
      if (Validate.isJournaling())
      {
        if (dv_map.journal == null)
        {
          /* Allocate/open journal files: */
          dv_map.journal = new Jnl_entry(fsd_name_active, jnl_dir_name, "fsd");
          dv_map.journal.storeMap(dv_map);

          /* Do we need to recoverOneMap existing journals: */
          if (Validate.isJournalRecoveryActive())
          {
            dv_map.journal.recovery_anchor = this;
            dv_map = dv_map.journal.recoverOneMap(jnl_dir_name,
                                                  fsd_name_active,
                                                  bytes_in_file_list,
                                                  getAnchorName());
            /* The key block size to be used comes from the journal: */
            key_block_size = dv_map.getKeyBlockSize();
          }

          else
          {
            /* Dump the map now so that we know we'll have enough space for        */
            /* at least the maps. (Journal records of course is a different story) */
            dv_map.journal.dumpOneMap(dv_map);
          }
        }
      }
    }

    /* Need Dedup bit map for this fsd? */
    if (Dedup.isDedup())
    {
      dedup_bitmap = DedupBitMap.findUniqueBitmap("fsd=" + fsd_name_active);
      if (dedup_bitmap == null)
      {
        dedup_bitmap = new DedupBitMap();
        dedup_bitmap.createMapForUniques(dedup, bytes_in_file_list, "fsd=" + fsd_name_active);
        DedupBitMap.addUniqueBitmap(dedup_bitmap, "fsd=" + fsd_name_active);
      }
    }
  }


  /**
   * Xfersizes for Data Validation must be multiples of the shortest, so we keep
   * track of them.
   */
  public void trackXfersizes(double[] sizes)
  {
    /* Look at all new sizes: */
    for (int i = 0; i < sizes.length; i+=2)
    {
      int size = (int) sizes[i];

      /* No xfersize used in the fwd=? */
      if (size == -1)
        continue;

      /* Add it to the list: */
      dv_largest_xfer_size = Math.max(dv_largest_xfer_size, size);
      Object ret = xfersizes_map.put(new Integer(size), null);
      if (ret == null)
      {
        //common.ptod("trackXfersizes: added " + size);
        //common.where(8);
      }
    }
  }


  public void trackFileSizes(double[] sizes)
  {
    /* Look at all new sizes: */
    for (int i = 0; i < sizes.length; i+=2)
    {
      filesizes_used.put(new Long((long) sizes[i]), null);
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
   *
   * At this time, this is done for the COMPLETE vdbench execution. It may be that
   * we can do it for each run, but I am not sure.
   */
  public void calculateKeyBlockSize()
  {
    String type = (Validate.isRealValidate()) ? "Data validation" : "Dedup";

    /* With dedup the dedupunit size must be included in the xfersizes: */
    // we don't have dedup yet!!!!
    //if (Dedup.isDedup())
    //  trackXfersizes(new double[] { dedup.getDedupUnit()});


    /* Sort the list of xfersizes: */
    Integer[] sizes = (Integer[]) xfersizes_map.keySet().toArray(new Integer[0]);
    Arrays.sort(sizes);

    /* For journal recovery we can run into this: */
    if (sizes.length == 0)
      common.failure("No 'xfersize=' parameters found for anchor=" + anchor_name +
                     ". Are you sure this anchor is used?");


    /* For Dedup without DV we already have the proper size: */
    if (Validate.isValidateForDedup())
    {
      // shortcut: no support for DSD-level Dedup:
      //key_block_size = dedup.getDedupUnit();
      key_block_size = Dedup.dedup_default.getDedupUnit();
      dv_max_xfer_size = sizes[sizes.length-1].intValue();
      return;
    }


    /* All xfersizes must be a  multiple of the first: */
    if (Dedup.isDedup())
      key_block_size = dedup.getDedupUnit();
    else
      key_block_size = sizes[0].intValue();

    /* All xfersizes must be a  multiple of the first: */
    dv_max_xfer_size = sizes[sizes.length-1].intValue();
    for (int i = 0; i < sizes.length; i++)
    {
      int next = sizes[i].intValue();
      if (next % key_block_size != 0)
      {
        common.ptod("During " + type + " all data transfer sizes used for ");
        common.ptod("an FSD must be a multiple of the lowest xfersize.");
        common.ptod("(A format run may have added a transfer size of 128k).");
        common.ptod("(You may override the format xfersize using 'fwd=format,xfersize=nnn').   ");
        if (Dedup.isDedup())
          common.ptod("(Dedup may have added dedupunit=nnn)");

        for (int j = 0; j < sizes.length; j++)
        {
          next = sizes[j].intValue();
          common.ptod("Xfersize used in parameter file: " + next);
        }

        common.failure("Xfersize error");
      }
    }
    //common.ptod("key_block_size: " + key_block_size);
    //common.ptod("dv_max_xfer_size: " + dv_max_xfer_size);
  }


  /**
  * Data validation and/or Dedup requires the file sizes to also be multiples of
  * the transfer sizes.
  *
  * I can't really think WHY this is an 'always' requirement, since file system
  * i/o handles reading/writing the last incomplete block.
  *
  * For DV and Dedup, yes. There it needs to be a multiple of the dedupunit, or
  * of the smallest xfersize (which is how DV determines it)
  */
  public void matchFileAndXfersizes()
  {
    if (!Validate.isValidate() && !Validate.isValidateForDedup())
      return;

    Integer[] xfersizes = (Integer[]) xfersizes_map.keySet().toArray(new Integer[0]);
    Arrays.sort(xfersizes);

    /* Get all the file sizes: */
    Long[] filesizes = (Long[]) filesizes_used.keySet().toArray(new Long[0]);

    int which_xfer = (Validate.isDedup()) ? dedup.getDedupUnit() : dv_map.getKeyBlockSize();

    /* All file sizes must be a  multiple of the shortest xfersize: */
    // Is this only because of dsim?
    for (int i = 0; i < filesizes.length; i++)
    {
      long next = filesizes[i].longValue();
      if (next % which_xfer != 0)
      {
        BoxPrint box = new BoxPrint();
        common.ptod("");
        box.add("During Data validation all file sizes used for ");
        box.add("an FSD must be a multiple of the lowest xfersize.");
        box.add("(A format run may have added a transfer size of 128k).");
        box.add("Data Validation Key block size or dedupunit: " + which_xfer);
        box.add("");

        for (int j = 0; j < filesizes.length; j++)
        {
          next = filesizes[j].longValue();
          if (next % which_xfer != 0)
            box.add("Mismatched file size: %,12d", next);
        }
        box.print();

        common.failure("File size error");
      }
    }
  }

  public KeyMap allocateKeyMap(long file_start_lba)
  {
    KeyMap keymap = new KeyMap(file_start_lba, key_block_size, dv_max_xfer_size);

    return keymap;
  }

  public DV_map getDVMap()
  {
    return dv_map;
  }
  public DedupBitMap getDedupBitMap()
  {
    return dedup_bitmap;
  }

  public DV_map getValidationMap()
  {
    return dv_map;
  }

  public ControlFile getControlFile()
  {
    return control_file;
  }

  public Vector <Directory> getDirList()
  {
    return dir_list;
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
   * of 'nn'  bytes each. When we run out of memory now at least we'll know it
   * while otherwise Java just keeps on trying and trying.
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
    int ESTIMATED_FILENTRY_SIZE = 100;
    int CHUNK = 1 * 1024 * 1024;
    int loop  = (int) ( total_directories + maximum_file_count) / CHUNK;
    byte[][] arrays = new byte[loop][];

    // Could it be that the previous lists are not GC'ed?
    common.ptod("checkMemory()");
    common.memory_usage();
    GcTracker.gc();
    common.memory_usage();

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
      SlaveJvm.sendMessageToConsole("Pre-allocation check for memory needs for "+
                                    "files failed.");
      SlaveJvm.sendMessageToConsole("Each file needs about %d bytes of memory for %,d files."+
                                    " Increase Java heap space.",
                                    ESTIMATED_FILENTRY_SIZE, maximum_file_count);

      SlaveJvm.sendMessageToConsole("Increase -Xmx value in Vdbench startup script"+
                                    " for the Vdbench.SlaveJvm start.");
      arrays = null;
      common.failure("Not enough memory. See the message above.");
    }

    /* This will let GC know the (temp) memory is no longer needed: */
    arrays = null;
    GcTracker.gc();
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
      return "" + (int) size;

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

    /* Remove '.000' if this is a 'nice' number: */
    String front = txt.substring(0, txt.length() - 5);
    String tail  = txt.substring(txt.length() - 5);
    if (tail.startsWith(".000"))
      txt = front + tail.substring(4);

    return txt;
  }

  public static String whatSize1(double size)
  {
    if (size < 10 * KB)
      return "" + (long) size;
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
                                       dir_list.size(),
                                       bytes_in_file_list,

                                       (use_list != null) ? use_list.size() : 0,
                                       (use_list != null) ? dir_list.size() : 0,
                                       (use_list != null) ? bytes_in_use_list : 0,

                                       (wss_list != null) ? wss_list.size() : 0,
                                       (wss_list != null) ? dir_list.size() : 0,
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


  /**
   * Return current file sharing status.
   * Must come from some better place later on.
   */
  public boolean fileSharing()
  {
    return file_sharing;
  }


  public static void printAnchorStatus()
  {
    Vector anchors = FileAnchor.getAnchorList();
    int width = 0;
    for (FileAnchor anchor : getAnchorList())
      anchor.printFileStatus();
  }

  public void printFileStatus()
  {
    Vector msg = new Vector(16);

    for (Directory dir : getDirList())
    {
      if (dir.isBusyNoSync())
      {
        msg.add(String.format("dir=%s,busy=%b", dir.getFullName(), dir.isBusyNoSync()));
        //msg.add(dir.last_busy);
      }
    }

    for (FileEntry fe : getFileList())
    {
      if (fe.isBusy())
        msg.add(String.format("file=%s,busy=%b", fe.getFullName(), fe.isBusy()));
    }

    common.ptod(msg);
  }


  // debugging:
  public void endOfRun()
  {
    String stars          = "**************************************************" +
                            "**************************************************";

    if (select_counters == null)
      return;

    /* Find highest count and total: */
    long total = 0;
    long max_count = 0;
    for (int i = 0; i < select_counters.length; i++)
    {
      int counter = select_counters[i];
      total      += counter;
      max_count   = Math.max(max_count, counter);
    }

    common.ptod("Random file selection counters for anchor=%s", anchor_name);
    common.ptod("Total file selection count: %,d", total);

    double cum_pct = 0;
    long   cum_count = 0;
    for (int i = 0; i < select_counters.length && i < 100; i++)
    {
      int counter = select_counters[i];
      cum_count += counter;
      double pct  = counter * 100. / total;
      cum_pct += pct;

      double chars = counter * 100 / max_count;
      String st    = stars.substring(0, (int) (chars * stars.length() / 100.));

      String txt = String.format("[%3d]: %8d %8d %5.2f%%  %6.2f%% %s", i, counter,
                                 cum_count,
                                 pct, cum_pct, st);
      System.out.println(txt);

    }

    select_counters = null;
  }


  /**
   * Extra debugging: create a log of all block reads and writes.
   * Requirements:
   * - debug=107
   * - data validation active
   * - use fsd=xxx,...,log=yes parameter
   */
  public void setupReadWriteLog(String fsd)
  {
    if (!Validate.isRealValidate())
      common.failure("Requesting read/write log while not using Data Validation");

    if (!create_rw_log)
      return;

    SimpleDateFormat df_log = new SimpleDateFormat( "MMddyy-HH:mm:ss.SSS zzz" );
    String temp  = System.getProperty("java.io.tmpdir");

    synchronized (open_rwlog_map)
    {
      /* A format run: delete both the file list and the log file: */
      if (SlaveWorker.work.format_run)
      {
        /* First clean up any old already active log: */
        Fput old_log = open_rwlog_map.get(fsd);
        if (old_log != null)
        {
          old_log.close();
          open_rwlog_map.remove(fsd);
          rw_log = null;
        }

        /* (re)create the file list: */
        String fname  = new File(temp, fsd + ".files").getAbsolutePath();
        rw_log = new Fput(fname);
        rw_log.println("* ");
        rw_log.println("* Column description:");
        rw_log.println("* ");
        rw_log.println("* Column 1: File sequence number, unique per FSD");
        rw_log.println("* Column 2: File name");
        rw_log.println("* Column 3: Requested file size");
        rw_log.println("* Column 4+5: Logical file start lba, decimal and hex");
        rw_log.println("* Column 6+7: Logical file end lba, decimal and hex");
        rw_log.println("* ");
        rw_log.println("* This file is only created during a format.");
        rw_log.println("* ");
        rw_log.println("* This file was created at " + df_log.format(new Date()));
        rw_log.println("* ");

        int max_len = 0;
        for (FileEntry fe : file_list)
          max_len = Math.max(max_len, fe.getFullName().length());

        for (FileEntry fe : file_list)
        {
          rw_log.println("%7d %-" + max_len + "s %,9d %,12d (%012x) %,12d (%012x)",
                         fe.getFileNoInList(),
                         fe.getFullName(),
                         fe.getReqSize(),
                         fe.getFileStartLba(),
                         fe.getFileStartLba(),
                         fe.getFileStartLba() + fe.getReqSize() - 1,
                         fe.getFileStartLba() + fe.getReqSize() - 1);
        }
        rw_log.close();

        /* Create the log file: */
        fname = new File(temp, fsd + ".log").getAbsolutePath();
        rw_log = new Fput(fname);
        open_rwlog_map.put(fsd, rw_log);

        //06172015-09:35:43.948 w      3  3      2654208     34111488
        rw_log.println("* ");
        rw_log.println("* Column description:");
        rw_log.println("* ");
        rw_log.println("* Following data is per 'key block size', the smallest xfersize during a test.");
        rw_log.println("* ");
        rw_log.println("* Column 1: Timestamp: HH:mm:ss.SSS");
        rw_log.println("* ");
        rw_log.println("* Column 2: Read or write");
        rw_log.println("* ");
        rw_log.println("* Column 3: Data Validation key just read or written");
        rw_log.println("* ");
        rw_log.println("* (The following two fields next to each other to accomodate 'grep file# lba')");
        rw_log.println("* Column 4: File number, see 'fsdX.files'");
        rw_log.println("* Column 5: Logical byte address of 'key block'");
        rw_log.println("* ");
        rw_log.println("* Column 6: Relative logical byte address of 'key block' within FSD");
        rw_log.println("* ");
        rw_log.println("* ONLY when the file ends with 'Log properly closed' can we be assured this file is complete");
        rw_log.println("* and nothing has been left behind in either java buffers or file system cache.");
        rw_log.println("* There may be multiple occurrences of 'Log properly closed'.");
        rw_log.println("* The log file is cleared during a format.");
        rw_log.println("* ");
        rw_log.println("* This file was created at " + df_log.format(new Date()));
        rw_log.println("* ");
      }

      else
      {
        /* If we already/still have it open, reuse: */
        if ((rw_log = open_rwlog_map.get(fsd)) != null)
        {
          rw_log.println("* This file was continued at " + df_log.format(new Date()));
          return;
        }

        String fname = new File(temp, fsd + ".log").getAbsolutePath();
        rw_log = new Fput(fname, true);
        open_rwlog_map.put(fsd, rw_log);
        rw_log.println("* This file was reopened at " + df_log.format(new Date()));
      }
    }
  }

  /**
   * Close all possible read/write log files at exit or failure:
   */
  public static void closeAllLogs()
  {
    synchronized (open_rwlog_map)
    {
      SimpleDateFormat df_log = new SimpleDateFormat( "MMddyy-HH:mm:ss.SSS zzz" );
      for (String fsd : open_rwlog_map.keySet())
      {
        Fput fp = open_rwlog_map.get(fsd);
        fp.println("* Log properly closed at " + df_log.format(new Date()));
        fp.close();
      }

      open_rwlog_map.clear();
    }
  }
  public static void flushAllLogs()
  {
    synchronized (open_rwlog_map)
    {
      for (Fput fp : open_rwlog_map.values())
        fp.flush();
    }
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











