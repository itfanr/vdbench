package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;


/**
 * Journal entry. Maintains information about the last block written.
 * An attempt has been made to make it so that we can change the unit
 * of data that is written to and read from the journal can be changed
 * from the current 512 bytes to anything larger.
 * It might not be so easy as I hoped for.
 *
 * The journal and map files each have a copy of the Data Validation map as
 * it existed at the begin of the (previous) run.
 * The map file will only be used if it is determined that the last write
 * to the map on the journal file did not complete. In that case, the map is
 * read from it's backup map file.
 *
 * After the copy of the map, the journal has records with:
 * - 4 bytes eye catcher
 * - 4 bytes with a count of how many 8 byte journal entries are stored.
 * n-8 byte entries with:
 *              8 bits with Data Validation key of next write
 *                If key is zero it means that this journal entry describes
 *                the AFTER image of the record and the write was complete.
 *              8 bits Reserved for future use
 *             48 bits with relative record# (lba / xfersize).
 *             Optional an extra 8 bytes (-d111): timestamp in MS.
 *
 *
 * Taking a few extra bits/bytes from the reserved bits:
 *  32 bits * 512 bytes per block =     1 TB
 *  40 bits                       =   256 TB
 *  48 bits                       = 65536 TB = 64PB
 *  Actually, I don't have to worry about sign bit so I already have TWICE this.
 *
 *      for (int i = 8; i < 56; i+=8)
 *      {
 *        long result = (long) Math.pow(2, i) * 512;
 *        common.ptod("Result: %2d %,24d %5s", i, result, FileAnchor.whatSize(result))
 *      }
 *
 *
 *             possible enhancement: instead of always writing a before and
 *             after journal record, flag the before entry in the record
 *             if it still is in the journal record. In that way we usually
 *             will need only one journal entry per write.
 *
 * After a journal is completely recovered we write the map again to the
 * files, and delete the old journal records by starting with an EOF record.
 * In that way we don't need to apply journal records from a previous
 * run.
 *
 * Possible enhancement: maybe every x writes, rewrite the map if there are
 * no i/o's outstanding. That makes for shorter journals.
 *
 *
 *
 * 10/23/2014: attempted to translate this stuff to use NewDirectByteBuffer,
 *             but could not get JNI to compile. Tried only Windows
 *
 * 10/23/2014: added -d111, allowing a timestamp to be added to each journal
 *             entry.
 *
 * 01/19/2015: switching to accommodate 48 bits worth of blocks.
 *
 */
public class Jnl_entry
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /* One entry per SD_entry or FileAnchor                                     */

  private String  jnl_name;
  private String  sd_or_fsd;


  private DV_map current_map;
  private long   jnl_handle;          /* File pointer to Journal file         */
  private long   map_handle;          /* File pointer to map file             */
  private long   jnl_offset;          /* Current journal file offset in bytes */

  private String jnl_dir_name;
  private String map_file_name;       /* File name for map file (.map)        */
  public  String jnl_file_name;       /* File name for jnl file (.jnl)        */

  private int    jnl_array[] = new int[ 1024 / 4 ];
  private int    jnl_index   = 0;     /* Ptr next free journal entry          */

  private long   jnl_native_buffer;  /* Buffer for JNI                        */

  private OpenFlags open_flags = new OpenFlags();     /* Flags for fast/slow read+write access */
  public  FileAnchor recovery_anchor = null;

  private long   dump_journal_tod;

  public  DV_map before_map = null;

  private long   journal_max;
  private HashMap <Long, Integer> pending_writes = null;

  /* Note: these values can be overridden to allow for a larger journal entry */
  /* size, containing a timestamp.                                            */
  /* A journal created with this will have array[7] set in the MAP header.    */
  private static boolean JOURNAL_ADD_TIMESTAMP = false;
  private static int JNL_IO_SIZE     = 512;
  private static int JNL_IO_SIZE_EOF = 1024;
  private static int JNL_ENTSIZE     = 8;  /* word0: if 0: write complete                 */
                                           /*        if 127, write failed DV_map.DV_ERROR */
                                           /*         else: before write; new key         */
                                           /* optional:                                   */
                                           /* word1: lba / xfersize (key block#)          */
                                           /* word2+3: System.currentTimeMilis()          */

  public  static int JNL_ENT_INTS    = JNL_ENTSIZE / 4; /* Number of ints per entry */

  private static int MAP_HDR_SIZE    = 32; /* 32 bytes header on each map record  */
  public  static int MAP_SKIP_HDR    = MAP_HDR_SIZE / 4;

  public  static int JNL_HDR_SIZE    = 16; /* 16 bytes header on each journal record  */
  public  static int JNL_SKIP_HDR    = 4;

  public  static int JNL_ENTRIES     = (JNL_IO_SIZE - JNL_HDR_SIZE) / JNL_ENTSIZE;
  /* Number of 8-byte entries in journal rec */

  static
  {
    overrideConstants();
  };



  public  static int MAP_EYE_CATCHER = 0x4d41502e; /* 'JNL.' for map records */
  public  static int JNL_EYE_CATCHER = 0x4a4e4c2e; /* 'MAP.' for jnl record  */

  static String RECOVERY_RUN_NAME = "journal_recovery";



  /**
   * Create journals for an SD.
   */
  Jnl_entry(String name, String jnl_dir, String typ)
  {
    sd_or_fsd    = typ;
    jnl_name     = name;
    jnl_dir_name = jnl_dir;

    /* In case journal=(max=nn) is used, keep currently active i/o in a map    */
    /* This then allows 'before' journal records to be written after the flush */
    if ((journal_max = Validate.getMaxJournal()) != Long.MAX_VALUE)
      pending_writes = new HashMap(1024);

    common.ptod("Allocating a new Journal file for %s=%s", sd_or_fsd, jnl_name);
    if (jnl_dir_name != null)
    {
      /* If the journal file is on a raw device the MAp file stays in default: */
      if (isRawJournal(jnl_dir_name))
      {
        map_file_name = jnl_name + ".map";
        jnl_file_name = jnl_dir_name;
      }

      else
      {
        map_file_name = jnl_dir_name + File.separator + jnl_name + ".map";
        jnl_file_name = jnl_dir_name + File.separator + jnl_name + ".jnl";
      }
    }

    else
    {
      jnl_dir_name  = ".";
      map_file_name = jnl_name + ".map";
      jnl_file_name = jnl_name + ".jnl";
    }

    map_file_name = new File(map_file_name).getAbsolutePath();
    jnl_file_name = new File(jnl_file_name).getAbsolutePath();

    jnl_native_buffer = Native.allocBuffer(JNL_IO_SIZE_EOF);

    /* Delete the old files first to possibly shorten them: */
    if (!Validate.isJournalRecovery() || Validate.isJournalRecovered() )
    {
      new File(map_file_name).delete();
      if (!isRawJournal(jnl_file_name))
      new File(jnl_file_name).delete();
    }

    /* Open the journal and map files here (use 'fast' access): */
    openFiles(true);
  }


  public static void overrideConstants()
  {
    JOURNAL_ADD_TIMESTAMP = common.get_debug(common.JOURNAL_ADD_TIMESTAMP);
    if (!JOURNAL_ADD_TIMESTAMP)
      return;

    JNL_ENTSIZE  = 16;
    JNL_ENT_INTS = JNL_ENTSIZE / 4;
    JNL_ENTRIES  = (JNL_IO_SIZE - JNL_HDR_SIZE) / JNL_ENTSIZE;
  }

  protected void finalize() throws Throwable
  {
    try
    {
      if (jnl_handle != 0)
      {
        File_handles.remove(jnl_handle);
        File_handles.remove(map_handle);
        Native.closeFile(jnl_handle);
        Native.closeFile(map_handle);
        Native.freeBuffer(JNL_IO_SIZE_EOF, jnl_native_buffer);
      }
    }
    finally
    {
      super.finalize();
    }
  }


  public static void closeAllMaps()
  {
    for (DV_map map : DV_map.getAllMaps())
    {
      Jnl_entry jnl = map.journal;
      if (jnl != null)
      {
        File_handles.remove(jnl.jnl_handle);
        File_handles.remove(jnl.map_handle);
        Native.closeFile(jnl.jnl_handle);
        Native.closeFile(jnl.map_handle);
        Native.freeBuffer(JNL_IO_SIZE_EOF, jnl.jnl_native_buffer);
      }
    }
  }

  public void storeMap(DV_map map)
  {
    current_map = map;
  }


  /**
   * Insert a special RD in the beginning that takes care of all journal
   * recoveries and the re-reading of all data.
   *
   * This no longer is a 'special' operation, it is just a seek=eof.
   * This can be done because of the decision to start treating a seek=eof read
   * workload during Data Validation as a 'why bother reading blocks that we
   * don't know anything about' workload.
   * See WG_task.createNormalIO()
   */
  public static void setupSDJournalRecoveryRun()
  {
    /* WD entry, one for each SD: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      WD_entry wd = new WD_entry();
      Vdbmain.wd_list.add(wd);

      wd.wd_name       = Jnl_entry.RECOVERY_RUN_NAME + "_" + sd.sd_name;
      wd.wd_sd_name    = sd.sd_name;
      wd.sd_names      = new String[] { sd.sd_name};
      wd.setSkew(0);
      wd.lowrange      = -1;
      wd.highrange     = -1;
      wd.seekpct       = -1;   //Jnl_entry.RECOVERY_READ; // Sequential until EOF */
      wd.rhpct         = 0;
      wd.whpct         = 0;
      wd.readpct       = 100;
      wd.xf_table      = new double[0];// { 513}; // see map.determineJnlRecoveryXfersize();
    }

    /* Allocate RD_entry and insert it at the front of the RD list: */
    RD_entry rd = new RD_entry();
    Vdbmain.rd_list.insertElementAt(rd, 0);

    rd.rd_name = Jnl_entry.RECOVERY_RUN_NAME;
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.distribution = 2;
    rd.iorate     = RD_entry.MAX_RATE;
    rd.iorate_req = RD_entry.MAX_RATE;

    // will we still need this? Yes, it needs to be optional
    // common.where(8);
    // double[] threads = new double[] { 1 };
    // new For_loop("forthreads",  threads, rd.for_list);

    /* Force single threaded recovery. If any errors show up with */
    /* multi-threading the async reporting just becomes too ugly. */
    // Leave asis, this may impact performance of journal recovery too much.
    // Alternative: have user specify threads=1
    //double[] threads = new double[] { 1 };
    //new For_loop("forthreads",  threads, rd.for_list);

    rd.wd_names = new String[ 1 ];
    rd.wd_names[ 0 ] = Jnl_entry.RECOVERY_RUN_NAME + "*";
  }



  public static void setupFsdJournalRecoveryRun()
  {
    FwdEntry fwd = new FwdEntry();
    FwdEntry.getFwdList().add(fwd);
    fwd.fwd_name      = Jnl_entry.RECOVERY_RUN_NAME;
    fwd.fsd_names     = new String[] { "*"};
    fwd.setOperation(Operations.READ);
    fwd.sequential_io = true;
    fwd.select_random = false;
    fwd.xfersizes     = new double[] { 128*1024};
    fwd.threads       = 8;

    /* Overrides: */
    if (FwdEntry.recovery_fwd != null)
    {
      // this xfersize does not work, is overridden again.
      fwd.xfersizes = FwdEntry.recovery_fwd.xfersizes;
      fwd.threads   = FwdEntry.recovery_fwd.threads;
    }

    /* Allocate RD_entry and insert it at the front of the RD list: */
    RD_entry rd = new RD_entry();
    Vdbmain.rd_list.insertElementAt(rd, 0);

    rd.rd_name      = Jnl_entry.RECOVERY_RUN_NAME;
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.distribution = 2;
    rd.fwd_rate     = RD_entry.MAX_RATE;

    if (RD_entry.recovery_rd != null)
    {
      rd.setInterval(RD_entry.recovery_rd.getInterval());
      rd.fwd_rate = RD_entry.recovery_rd.fwd_rate;
    }

    rd.fwd_names = new String[ ] { Jnl_entry.RECOVERY_RUN_NAME} ;
  }


  /**
   * Open the journal and map files.
   * If needed they will be closed and re-opened to allow for faster flush
   * of journal reads and writes:
   *
   * Some day I'll change this again to have separate opens for the normal
   * before/after writes and for the rest, but for now this work.
   * Don't fix it when it's not broken!
   */
  private void openFiles(boolean fast)
  {
    /* Set the default flags. (no flags) */
    OpenFlags default_flags = null;
    if (common.onSolaris())
      default_flags = new OpenFlags(new String[] { "o_sync"}, null);
    else if (common.onLinux())
      default_flags = new OpenFlags(new String[] { "o_sync"}, null);
    else if (common.onWindows())
      default_flags = new OpenFlags(new String[] { "directio"}, null);
    else if (common.onAix())
      default_flags = new OpenFlags(new String[] { "directio"}, null);
    else
      common.failure("Synchronous i/o options needed for Journaling currently " +
                     "only known for Solaris, Linux, AIX, and Windows. "+
                     "Contact me at the Oracle Vdbench Forum.");


    /* Decide what the open flags should be:: */
    OpenFlags new_open_flags = default_flags;
    if (fast || !Validate.isJournalFlush())
      new_open_flags = new OpenFlags();

    /* If the files are already open and open flags don't change: exit */
    if (jnl_handle != 0 && open_flags.equals(new_open_flags))
      return;

    /* If the files are open but flags must be changed: close */
    if (jnl_handle != 0)
    {
      /* Handles have to be removed BEFORE the close. An other thread may otherwise */
      /* reuse this handle and addHandle it back to the list, causing duplicates. */
      File_handles.remove(jnl_handle);
      File_handles.remove(map_handle);

      Native.closeFile(jnl_handle);
      Native.closeFile(map_handle);
    }

    /* Save the flags: */
    open_flags = new_open_flags;

/*

- Journal: ????   http://man7.org/linux/man-pages/man2/open.2.html
 The O_DIRECT flag on its own makes an effort
              to transfer data synchronously, but does not give the
              guarantees of the O_SYNC flag that data and necessary metadata
              are transferred.  To guarantee synchronous I/O, O_SYNC must be
              used in addition to O_DIRECT.  See NOTES below for further
              discussion.

*/

    /*                                   */
    /*                                   */
    /* open_flags == 0 means BUFFERED IO */
    /* open_flags != 0 means DIRECT IO   */
    /*                                   */
    /*                                   */
    /* Now go open the files:            */
    //common.ptod("open_flags: " + open_flags + " " + jnl_file_name);
    if ( (jnl_handle = Native.openFile(jnl_file_name, open_flags, 1)) == -1)
      common.failure("Open failed for '" + jnl_file_name + "'");

    if ( (map_handle = Native.openFile(map_file_name, open_flags, 1)) == -1)
      common.failure("Open failed for '" + map_file_name + "'");

    File_handles.addHandle(jnl_handle, jnl_file_name );
    File_handles.addHandle(map_handle, map_file_name);
  }


  /**
   * Dump all journal files for all SDs or FSDs.
   */
  public static void dumpAllMaps(boolean end_of_run)
  {
    if (!Validate.isJournaling())
      return;

    if (common.get_debug(common.DONT_DUMP_MAPS))
    {
      /* For file system formatting: */
      if (!SlaveWorker.work.format_run)
      {
        plog("dumpAllMaps(): request ignored due to 'DONT_DUMP_MAPS'");
        return;
      }

      plog("dumpAllMaps(): 'DONT_DUMP_MAPS' request ignored because this is a format run");
    }

    /* This debugging call does not make sense: skip dump but also skip setAllUnbusy? */
    if (!SlaveWorker.work.format_run)
    {
      if (end_of_run && HelpDebug.hasRequest("dumpAllMaps"))
      {
        plog("dumpAllMaps(): request ignored due to 'HelpDebug request'");
        return;
      }
    }


    for (DV_map map : DV_map.getAllMaps())
    {
      if (map.journal != null)
      {
        map.journal.dumpOneMap(map);
        map.setAllUnBusy();
      }
    }
  }


  public void dumpOneMap(DV_map map)
  {
    /* Locking is needed to allow dumping after 'max_journal': */
    synchronized (map)
    {
      /* Remember when: */
      dump_journal_tod = System.currentTimeMillis();

      /* Possibly re-open the files with fast read+write access: */
      openFiles(true);

      /* Should this be done in reverse order, first the backup.map file */
      /* and then .jnl in case the jnl dump fails? */

      plog("Writing Data Validation map to " + File_handles.getFileName(jnl_handle));
      jnl_dump_map(jnl_handle, map, jnl_file_name);

      plog("Writing Data Validation map to " + File_handles.getFileName(map_handle));
      jnl_dump_map(map_handle, map, map_file_name);

      /* Possibly re-open the files with slow read+write access: */
      openFiles(false);

      /* For the next go-around our index must start clean again: */
      jnl_index = 0;
    }
  }


  /**
   * Read one physical record from Journal
   */
  public static void jnl_read(long  handle,
                              long  seek,
                              long  length,
                              long  buffer,
                              int[] array,
                              int   eye_catcher) throws Exception
  {
    //common.ptod("jnl_read handle: " + handle + " seek: " + seek + " buffer: " + buffer);
    long rc = Native.readFile(handle, seek, length, buffer);
    if (rc != 0)
      throw new Exception("Journal read failed: "+ Errno.xlate_errno(rc));
    Native.buffer_to_array(array, buffer, 512);

    if (array[0] != eye_catcher)
    {
      String fname = (String) File_handles.getFileName(handle);
      common.ptod("Journal file lba: 0x%08x", seek);
      if (eye_catcher == MAP_EYE_CATCHER && array[0] == JNL_EYE_CATCHER)
        common.failure("Expecting MAP record but receiving JNL record from %s", fname);
      if (eye_catcher == JNL_EYE_CATCHER && array[0] == MAP_EYE_CATCHER)
        common.failure("Expecting JNL record but receiving MAP record from %s", fname);
      if (eye_catcher == MAP_EYE_CATCHER)
        common.failure("Expecting MAP record but receiving unknown 0x%08x record from %s", array[0], fname);
      if (eye_catcher == JNL_EYE_CATCHER)
        common.failure("Expecting JNL record but receiving unknown 0x%08x record from %s", array[0], fname);
      common.failure("Receiving unknown 0x%08x record", array[0]);
    }
  }


  /**
   * Write one physical record to journal
   */
  public static void jnl_write(long handle, long seek, long length, long buffer)
  {
    // debugging:
    if (false)
    {
      int[] array = new int[128];
      Native.buffer_to_array(array, buffer, 512);
      if (array[0] == MAP_EYE_CATCHER)
        common.ptod("Writing map record to lba 0x%08x - 0x%08x", seek, seek + length - 1);
      else if (array[0] == JNL_EYE_CATCHER)
        common.ptod("Writing jnl record to lba 0x%08x - 0x%08x", seek,  seek + length - 1);
      else
        common.ptod("Writing unknown record 0x%08x to lba 0x%08x", array[0], seek);
      //  common.where(8);
    }

    long rc = Native.writeFile(handle, seek, length, buffer);
    if (rc != 0)
      common.failure("Journal write failed: " + Errno.xlate_errno(rc));
  }

  /**
   * Write journal record to the journal file
   * Write record, if this is the last entry in the record, write also
   * an extra 512 bytes record with an entry count of zero.
   *
   * A consideration was made that it might be possible to add some code
   * down here that would dump the maps once the journal file reaches a
   * certain threshold, e.g. 10MB and then start writing the new journal
   * records right after the map again.
   * This would prevent the journal file from getting too large.
   *
   * However, since the applyJournal() method depends on first getting a
   * BEFORE record and then an AFTER record it means that we can not have
   * any outstanding i/o when we do this, or re-evaluate the
   * applyJournal().
   *
   * //  ??? double check above!
   *
   * Since the 'journal file too long' issue has never been brought up it
   * just does not seem worth the effort.
   * Remember, 20MB of journal file equals 20*1024*1024/(512/16) ios, or
   * 655,360 ios. That's quite a bit, except of course for file system
   * workloads.
   */
  private void writeJournalRecord()
  {
    /* First store entry count at begin of buffer: */
    jnl_array[0] = JNL_EYE_CATCHER;
    jnl_array[1] = jnl_index;
    jnl_array[2] = (int) jnl_offset / 512;
    jnl_array[3] = (int) dump_journal_tod;  // last 32 bits of tod.

    /* No EOF record needed?: */
    if (jnl_index < JNL_ENTRIES)
    {
      /* It turns out that each time I copy 1024bytes, not 512! */
      /* It turns out that each time I copy 1024bytes, not 512! */
      /* It turns out that each time I copy 1024bytes, not 512! */
      /* It turns out that each time I copy 1024bytes, not 512! */
      Native.arrayToBuffer(jnl_array, jnl_native_buffer);
      jnl_write(jnl_handle, jnl_offset, 512, jnl_native_buffer);
    }
    else
    {
      /* Add EOF record: */
      jnl_array[128+0] = JNL_EYE_CATCHER;
      jnl_array[128+1] = 0;
      jnl_array[128+2] = (int) jnl_offset / 512;
      jnl_array[128+3] = (int) dump_journal_tod;

      Native.arrayToBuffer(jnl_array, jnl_native_buffer);
      jnl_write(jnl_handle, jnl_offset, 1024, jnl_native_buffer);

      jnl_offset += 512;
      jnl_index   = 0;

      if (jnl_offset % (100*1024*1024l) == 0)
        common.ptod("Journal file " + jnl_file_name + " is now " +
                    (jnl_offset / (1*1024*1024)) + "mb");
    }
  }

  /**
   * Store a journal entry, and optionally write it out.
   * Journal record will be written when the journal block is full, or
   * when the 'last' flag is true.
   * (The 'last' flag is to make sure we don't do a sync write for each
   * KeyMap.key_block_size)
   *
   * When the key is zero it implies an AFTER image.
   */
  public synchronized void writeJournalEntry(int key, long key_block, boolean last)
  {
    if (jnl_index == JNL_ENTRIES)
      common.failure("Invalid jnl_index: " + jnl_index);

    /* Note that key may have bit0 set to identify 'recursive call' from below  */

    long store = ((long) key) << 56 | key_block;
    jnl_array [ JNL_SKIP_HDR + jnl_index * JNL_ENT_INTS + 0 ] = left32(store);
    jnl_array [ JNL_SKIP_HDR + jnl_index * JNL_ENT_INTS + 1 ] = right32(store);

    if (JOURNAL_ADD_TIMESTAMP)
    {
      long tod = System.currentTimeMillis();
      jnl_array [ JNL_SKIP_HDR + jnl_index * JNL_ENT_INTS + 2 ] = left32(tod);
      jnl_array [ JNL_SKIP_HDR + jnl_index * JNL_ENT_INTS + 3 ] = right32(tod);
    }

    jnl_index++;

    if (last || jnl_index >= JNL_ENTRIES)
      writeJournalRecord();

    //if (key != 0)
    //  common.ptod("before: lba %08x key %02x", key_block * current_map.getBlockSize(), key);
    //else
    //  common.ptod("after:  lba %08x key %02x", key_block * current_map.getBlockSize(), key);


    /* Temporarily store pending i/o request for journaling.                         */
    /*                                                                               */
    /* When using the 'journal_max=' parameter the journal file will be flushed      */
    /* every journal_max=xxx bytes.                                                  */
    /*                                                                               */
    /* Because of that the journal file will not have a record of writes that were   */
    /* outstanding at that time. If Vdbench or the OS shuts down then with any of    */
    /* these writes still pending a corruption may not be recognized.                */
    /*                                                                               */
    /* It is therefore, when journal_max is used, that we keep track of these        */
    /* pending writes in a small HashMap, dumping that HashMap back into the journal */
    /* the moment that the flush has completed.                                      */

    /* This all still happens under the current lock.                                */
    if (pending_writes == null || (key & 0x80000000) != 0 )
      return;


    /* 'Before' journal record: */
    if (key != 0)
    {
      if (pending_writes.put(key_block, key) != null)
        common.failure("Received duplicate 'i/o pending' status for block: %,d key: %d", key_block, key);
    }

    /* 'After' journal record: */
    else
    {
      if (pending_writes.remove(key_block) == null)
        common.failure("Pending write removing unknown block: %,d", key_block);
    }

    /* Optionally rewrite maps while maps are locked.           */
    /* This eliminates the journal becoming too huge, though it */
    /* may slow down iops while the maps are being dumped.      */

    if (jnl_offset < journal_max)
      return;

    /* Note: if Vdbench or the OS shuts down HERE we may have an issue? */
    plog("'journal_max' reached. Clearing journal file for %s", sd_or_fsd);
    dumpOneMap(current_map);


    /* While still under the current Jnl_entry lock, rewrite BEFORE entries: */
    Long[] blocks = pending_writes.keySet().toArray(new Long[0]);
    for (int i = 0; i < blocks.length; i++)
    {
      long    block      = blocks[i];
      boolean last_block = (i == blocks.length - 1);
      writeJournalEntry(pending_writes.get(block) | 0x80000000, block, last_block);
    }
  }


  /**
   * Dump Data Validation map to file
   * Layout: first sector: controlinfo
   *         rest: all data from map
   *
   * If upon reading of this map we find a '-1' as second eye catcher it
   * means that we never completed the complete map dump. This then means
   * we can not use this map and must go to the backup copy of the map
   * which resides in the '.map' file.
   */
  public void jnl_dump_map(long handle, DV_map map, String label)
  {
    Elapsed elapsed = new Elapsed("jnl_dump_map of " + label, 500*1000);

    /* Write control record saying "dump in progress": */
    jnl_array[0] = MAP_EYE_CATCHER;
    jnl_array[1] = -1;

    jnl_array[2] = left32(map.key_blocks);
    jnl_array[3] = right32(map.key_blocks);

    jnl_array[4] = map.getKeyBlockSize();

    jnl_array[5] = left32(dump_journal_tod);
    jnl_array[6] = right32(dump_journal_tod);

    jnl_array[7] = (JOURNAL_ADD_TIMESTAMP) ? 1 : 0;
    jnl_array[8] = 0;

    Native.arrayToBuffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, 0, 512, jnl_native_buffer);



    /* Start a thread for each one GB of journal: */
    ArrayList <Thread> threads = new ArrayList(8);
    long key_blocks_per_gb = 1024*1024*1024l / 480 * 480;
    for (long start_keyblk = 0; start_keyblk < map.key_blocks; start_keyblk += key_blocks_per_gb)
    {
      long  thread_blocks = Math.min(key_blocks_per_gb, (map.key_blocks - start_keyblk));
      common.ptod("Starting JournalThread for key block %,16d to %,16d",
                  start_keyblk, start_keyblk + thread_blocks);
      JournalThread rest = new JournalThread(this,
                                             map,
                                             handle,
                                             start_keyblk,
                                             thread_blocks,
                                             map.getKeyBlockSize(),
                                             false);
      rest.start();
      threads.add(rest);
    }

    /* Wait for them: */
    JournalThread.waitForList(threads);

    /* Set offset to the start of the journal records: */
    jnl_offset = 512;
    jnl_offset += (map.key_blocks + 511) / 480 * 512;

    //common.ptod("jnl_offset: 0x%08x", jnl_offset);


    /* Write EOF record, meaning no journal records on file: */
    jnl_array[0] = JNL_EYE_CATCHER;
    jnl_array[1] = 0;                       // 0 means EOF
    jnl_array[2] = (int) jnl_offset / 512;
    jnl_array[3] = right32(dump_journal_tod);
    Native.arrayToBuffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, jnl_offset, 512, jnl_native_buffer);

    /* Write control record again, now saying "dump completed": */
    jnl_array[0] = MAP_EYE_CATCHER;
    jnl_array[1] = MAP_EYE_CATCHER;

    jnl_array[2] = left32(map.key_blocks);
    jnl_array[3] = right32(map.key_blocks);

    jnl_array[4] = map.getKeyBlockSize();

    jnl_array[5] = left32(dump_journal_tod);
    jnl_array[6] = right32(dump_journal_tod);

    jnl_array[7] = (JOURNAL_ADD_TIMESTAMP) ? 1 : 0;
    jnl_array[8] = 0;

    Native.arrayToBuffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, 0, 512, jnl_native_buffer);

    elapsed.end(5);
  }


  /**
   * Some methods to assist with 4-8 bit manipulation, including the pain with
   * the sign bit of the right-most 32 bits.
   */
  private static int left32(long long_value)
  {
    return(int) (long_value >>> 32);
  }
  private static int right32(long long_value)
  {
    return(int) long_value;
  }
  private static long not_used_make64Key(long key, int left, int right)
  {
    return make64(left, right) | (key << 56);
  }
  public  static long make64(int left, int right)
  {
    long long_value = ((long) left) << 32;
    long_value     |= ((long) right) &0xffffffffL;
    return long_value;
  }
  public static int getKey(long long_value)
  {
    return(int) ((long_value >> 56) & 0xff);
  }
  public static long getBlock(long long_value)
  {
    return long_value & 0xffffffffffffL;
  }



  /**
   * Restore Data Validation map from file
   * Layout: first sector: controlinfo
   *         rest: all data from map
   */
  private DV_map restoreMap(String jnl_dir_name, long handle, String name, long max_lba, String lun)
  {
    Elapsed elapsed = new Elapsed("DV_map.restoreMap of " + name, 500*1000);

    /* Read controlinfo: */
    try
    {
      jnl_read(handle, 0, 512, jnl_native_buffer, jnl_array, MAP_EYE_CATCHER);
    }
    catch (Exception e)
    {
      common.ptod("Error while restoring the journal map. Journal likely is corrupted");
      common.ptod(e);
      common.failure(e);
    }

    /* Get map length and xfersize: */
    long key_blocks  = make64(jnl_array[2], jnl_array[3]);
    int  key_blksize = jnl_array[4];

    /* First word must always have eye catcher: */
    if (jnl_array[0] != MAP_EYE_CATCHER)
    {
      for (int i = 0; i < 8; i++)
        common.ptod(Format.f("block: %08x", jnl_array[i]));
      common.failure("Invalid contents on journal or map file: " +
                     Format.f("%08x", jnl_array[0]));
    }

    /* If the control record does not show normal completion, exit: */
    if (jnl_array[1] != MAP_EYE_CATCHER)
      return null;

    /* Determine if the journal entries contain a timestamp: */
    if (jnl_array[7] == 1)
    {
      common.set_debug(common.JOURNAL_ADD_TIMESTAMP);
      overrideConstants();
    }

    /* Compare length: */
    if (max_lba / key_blksize != key_blocks)
    {
      String msg = name + " Journal recovery failed. \nJournal contains xfersize " +
                   key_blksize + " and contains " + key_blocks + " blocks, " +
                   "for a total size of " + (long) key_blocks * key_blksize + " bytes." +
                   "\nThis does not match the size of " + lun + " which is " +
                   max_lba + " bytes";

      ErrorLog.ptod(msg);
      common.failure("Journal recovery failed");
    }

    /* We now have blocksize and length, so allocate map: */
    DV_map map = DV_map.allocateMap(jnl_dir_name, name, max_lba, key_blksize);
    storeMap(map);
    map.journal = this;

    /* The map size can be wrong if we did not know the proper size */
    /* during allocation:                                           */
    //if (current_map.map_entries != mapentries)
    //  current_map.reallocateByteMap(mapentries);
    //DV_map map = current_map;



    /* Start a thread for each one GB of journal: */
    ArrayList <Thread> threads = new ArrayList(8);
    long key_blocks_per_gb = 1024*1024*1024l / 480 * 480;
    for (long start_keyblk = 0; start_keyblk < key_blocks; start_keyblk += key_blocks_per_gb)
    {
      long  thread_blocks = Math.min(key_blocks_per_gb, (key_blocks - start_keyblk));
      common.ptod("Starting JournalThread for key block %,16d to %,16d",
                  start_keyblk, start_keyblk + thread_blocks);
      JournalThread rest = new JournalThread(this,
                                             map,
                                             handle,
                                             start_keyblk,
                                             thread_blocks,
                                             key_blksize,
                                             true);
      rest.start();
      threads.add(rest);
    }

    /* Wait for them: */
    JournalThread.waitForList(threads);


    /* Set offset to the start of the journal records: */
    jnl_offset = 512;
    jnl_offset += (key_blocks + 511) / 480 * 512;

    return map;
  }



  /**
   * Recover the Data Validation maps using journals.
   *
   * This is called on the slave.
   */
  public static void recoverSDJournalsIfNeeded(Vector sd_list)
  {
    if (!Validate.isJournalRecovery() || Validate.isJournalRecovered())
      return;

    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);
      if (!sd.journal_recovery_complete)
      {
        Jnl_entry journal = new Jnl_entry(sd.sd_name, sd.jnl_dir_name, "sd");
        DV_map    map     = journal.recoverOneMap(sd.jnl_dir_name, sd.sd_name, sd.end_lba, sd.lun);

        int xfersize = map.determineJnlRecoveryXfersize(sd.getMaxSdXfersize());
        sd.trackSdXfersizes(new double[] { xfersize});

        /* The WG_entry that is requesting this recovery does not have a valid */
        /* xfersize yet: store the xfersize you received from the journal:     */
        sd.wg_for_sd.setXfersizes(new double[] { xfersize});
        sd.wg_for_sd.mcontext.next_seq = -1;
        if (sd.canWeUseBlockZero())
          sd.wg_for_sd.mcontext.seek_low = 0;
        else
          sd.wg_for_sd.mcontext.seek_low = map.getKeyBlockSize();
      }
    }
  }


  /**
   * Recover the Data Validation maps using journals
   */
  public DV_map recoverOneMap(String jnl_dir_name, String sd_or_fsd_name, long max_lba, String lun)
  {
    ErrorLog.ptod("Starting journal recovery for " + sd_or_fsd_name);

    /* See if journal files are present (abort if not): */
    if (!isRawJournal(jnl_file_name))
    {
      File fptr = new File(jnl_file_name);
      if (!fptr.exists())
        common.failure("Journal recovery file does not exist: " + jnl_file_name);
      fptr = new File(map_file_name);
      if (!fptr.exists())
        common.failure("Journal map file does not exist: " + map_file_name);
    }


    /* Restore map from journal file: */
    plog("Restoring Data Validation map from: " + jnl_file_name);
    DV_map map = restoreMap(jnl_dir_name, jnl_handle, sd_or_fsd_name, max_lba, lun);

    /* A bad return code tells me that the map in the journal file was in */
    /* the middle of a map write that did not complete:                   */
    if (map == null)
    {
      /* Recover from mapfile: */
      plog("The last map dump to %s failed. Falling back to backup ", jnl_file_name);
      plog("Restoring Data Validation map from: " + map_file_name);
      map = restoreMap(jnl_dir_name, map_handle, sd_or_fsd_name, max_lba, lun);
      if (map == null)
        common.failure("Invalid journal map file ");
    }

    /* Apply journal changes to map: */
    applyJournal(jnl_dir_name);

    /* Now write the map back to the files: */
    //common.ptod("Writing recovered Data Validation map to " + jnl_filename(sd, ".jnl"));
    //jnl.jnl_dump_map(jnl.jnl_fhandle, sd.dv);
    //common.ptod("Writing recovered Data Validation map to " + jnl_filename(sd, ".map"));
    //jnl.jnl_dump_map(jnl.jnl_map_fhandle, sd.dv);

    ErrorLog.ptod("Completed journal recovery for %s. Starting data validation.", sd_or_fsd_name);

    return map;
  }

  public static boolean isRawJournal(String name)
  {
    if (name == null)
      return false;
    return name.startsWith("/dev/") || name.startsWith("\\\\");
  }

  /**
   * Apply journals to a just read Data Validation map.
   */
  private void applyJournal(String jnl_dir_name)
  {
    boolean debug   = false;
    long    before  = 0;
    long    after   = 0;
    long    lba;
    int     newkey;

    Elapsed elapsed = new Elapsed("applyJournal", 100*1000*1000);

    /* 'before_map' contains the BEFORE key value of keys that have been      */
    /* found in a 'before' record but are still waiting for an 'after' record: */
    /* 'current_map' is the map we're now restoring to. */
    before_map = new DV_map(jnl_dir_name,
                            current_map.map_name + ".before",
                            current_map.key_blocks,
                            current_map.getKeyBlockSize());

    /* In case this is for an FSD, pass the anchor: */
    before_map.recovery_anchor = this.recovery_anchor;

    ErrorLog.plog("Applying journal records from " + jnl_file_name);

    /**
     * The ultimate result of this loop is that 'before_map' will contain a
     * non-zero key for each write that we did not see an 'after' journal
     * record for, and that 'current_map' will contain the key of the writes
     * that we DID receive an 'after' journal record for.
     *
     * 'before_map' will contain DV_error for each block that we know of had
     * never been written before, this from 'current_map' having key=0.
     */

    while (true)
    {
      if (debug) common.ptod("jnl_offset: %,16d", jnl_offset);
      jnl_read_next_record(jnl_offset);
      jnl_offset += 512;

      elapsed.track(480);

      /* Pick up the amount of entries I have in these 512 bytes: */
      int blocks_in_record = jnl_array[1];
      if (debug) common.ptod("blocks_in_record: " + blocks_in_record);

      /* Scan through all those entries: */
      for (int i = 0; i < blocks_in_record; i++)
      {
        long long_value = make64(jnl_array[ JNL_SKIP_HDR + i * JNL_ENT_INTS + 0 ],
                                 jnl_array[ JNL_SKIP_HDR + i * JNL_ENT_INTS + 1 ]);

        newkey = getKey(long_value);
        lba    = getBlock(long_value) * current_map.getKeyBlockSize();

        if (debug) common.ptod("newkey: %02x", newkey);

        //debug = true;
        if (debug && newkey != 0)
          common.ptod("before: lba %08x key %02x", lba, newkey);
        if (debug && newkey == 0)
          common.ptod("after:  lba %08x key %02x", lba, newkey);
        //debug = false;


        // There is an opportunity here to tighten up the code:
        // compare NEW key with previous one. If 5 goes to 4 we have a problem!


        /* A nonzero key indicates this is a 'before' journal entry: */
        if (newkey != 0)
        {
          before++;

          /* The BEFORE map may not contain this block as pending: */
          if (before_map.dv_get_nolock(lba) != 0)
          {
            common.ptod("lba:                    %08x", lba);
            common.ptod("before_map.dv_get(lba): " + before_map.dv_get(lba));
            common.ptod("newkey:                 " + newkey);
            common.failure("Journal recovery: unmatched pending 'before' record found.");
          }

          /* If the old key was zero, block never written, save that status: */
          if (current_map.dv_get_nolock(lba) == 0)
          {
            if (debug) common.ptod("old key 0, before_map set to pending. lba: %08x", lba);
            before_map.dv_set_nolock(lba, DV_map.PENDING_KEY_0);

            /* The new key then must be '1': */
            if (newkey != 1)
              common.failure("Expecting DV key 1 for lba %08x", lba);
          }

          /* If the old key was 126, rolling over from 126 to 1: */
          else if (current_map.dv_get_nolock(lba) == 126)
          {
            if (debug) common.ptod("old key 0, before_map set to pending. lba: %08x", lba);
            before_map.dv_set_nolock(lba, DV_map.PENDING_KEY_ROLL);

            /* The new key then must be '1': */
            if (newkey != 1)
              common.failure("Expecting DV key 1 for lba %08x", lba);
          }

          /* With Dedup we roll over from 2 to 1 for duplicate blocks: */
          else if (current_map.dv_get_nolock(lba) == 2 && newkey == 1)
          {
            if (!Dedup.isDedup())
              common.failure("Out of sync data validation key found in journal");

            if (debug) common.ptod("old key 0, before_map set to pending. lba: %08x", lba);
            before_map.dv_set_nolock(lba, DV_map.PENDING_KEY_ROLL_DEDUP);
          }

          /* 'before_map' now will contain a 'pending' flag: */
          else
          {
            if (debug) common.ptod("old key 0x%02x lba: %08x", current_map.dv_get_nolock(lba), lba);
            before_map.dv_set_nolock(lba, DV_map.PENDING_WRITE);
          }

          /* 'current_map' now contains the 'after' key value. */
          current_map.dv_set_nolock(lba, newkey);
          if (debug) common.ptod("set current key 0x%02x lba: %08x", newkey, lba);
        }


        /* This is the 'after' entry, clear the saved 'before' flag: */
        else
        {
          after++;
          before_map.dv_set_nolock(lba, 0);
          if (debug) common.ptod("Reset before map lba: %08x", lba);
        }
      }

      /* If the record was not full, EOF: */
      if (blocks_in_record < JNL_ENTRIES)
        break;
    }

    elapsed.end(5);

    plog("Completed apply of journal records (before/after): " + before + "/" + after);
    if (after > before)
      common.failure("Unexpected before/after count. 'After' may not be greater than 'before'");


    /* After this, the before_map table contains all entries that were */
    /* in progress, and MAY have either a before or after key value.   */
    /* These blocks will be read and verified at the beginning of      */
    /* the journal_recovery seek=eof read                              */
    if (before + after > 0)
    {
      if (sd_or_fsd.equals("sd"))
      {
        /* Create an array with the pending blocks: */
        VerifyPending.findPendingBlocks(before_map, current_map, sd_or_fsd, jnl_name);
      }

      else
      {
        // there is something wrong here: there is no SD....
        /* Create an array with the pending blocks: */
        VerifyPending.findPendingBlocks(before_map, current_map, sd_or_fsd, jnl_name);

        /* Now check the contents of those blocks: */
        VerifyPending.checkFsdPendingBlocks(this.recovery_anchor, before_map);
      }
    }


    /* The journal recovery is complete. Delete the temporary mmap file */
    // must be done much later!!!!
    //common.where();
    //before_map.deleteByteMapFile();
  }



  /**
   * Read next sequential journal record from the journal file
   */
  private void jnl_read_next_record(long offset)
  {
    try
    {
      jnl_read(jnl_handle, offset, 512, jnl_native_buffer, jnl_array, JNL_EYE_CATCHER);
    }
    catch (Exception e)
    {
      common.ptod("Error while reading journal records.");
      common.ptod("This very likely means that the storage or file system where ");
      common.ptod("you wrote the journal was corrupted due to the writes of the ");
      common.ptod("journal records NOT being done synchronously");
      common.ptod(e);
      common.failure(e);
    }
  }



  public static void plog(String format, Object ... args)
  {
    ErrorLog.plog(String.format(format, args));
  }

  public static void main(String args[])
  {
    long    saved_journal_tod = 0;
    boolean found_jnl         = false;
    long    map_lba           = 0;
    long    tod_counts        = 0;

    SimpleDateFormat df = new SimpleDateFormat( "MM/dd/yyyy HH:mm:ss.SSS zzz" );

    for (int x = 0; x < args.length; x++)
    {
      long buffer = Native.allocBuffer(512);
      int[] array = new int[128];

      common.ptod("args[x]: " + args[x]);
      long    handle    = Native.openFile(args[x], 0);
      long    jnl_size  = new File(args[x]).length();
      long    xfersize  = -1;
      long    seek      = 0;
      int     block     = 0;
      int     records   = 0;
      int     after_eof = 0;
      boolean eof       = false;
      for (long i = 0; i < Long.MAX_VALUE ; i++)
      {
        if (seek >= jnl_size)
        {
          common.ptod("Trying to go beyond journal file size");
          break;
        }
        if (Native.readFile(handle, seek, 512, buffer) != 0)
        {
          common.ptod("read error");
          break;
        }
        Native.buffer_to_array(array, buffer, 512);


        /* This is the MAP: */
        if (array[0] == MAP_EYE_CATCHER)
        {
          /* Determine if the journale entries contain a timestamp: */
          if (array[7] == 1)
          {
            common.set_debug(common.JOURNAL_ADD_TIMESTAMP);
            overrideConstants();
          }


          /* The first map record, print info: */
          if (i == 0)
          {
            long tod = (long) array[4] << 32;
            tod += array[5];
            saved_journal_tod = array[5];
            common.ptod("dump_journal_tod: %016x %s", tod, new Date(tod));
            common.ptod("saved_journal_tod: %08x", saved_journal_tod);
          }

          /* Store the map's (key) xfersize: */
          if (xfersize < 0)
            xfersize = array[4];
          seek += 512;

          /* Scan through the map looking for certain lbas: */
          //   for (int j = MAP_SKIP_HDR; j < 128; j++)
          //   {
          //     int word = array[j];
          //     for (int z = 0; z < 4; z++)
          //     {
          //       int key  = word << (z*8) >> 24;
          //       if (map_lba == 0x1122aa0000l)
          //         common.ptod("map_lba: %012x %12d key: %2d word: %08x tod: %08x",
          //                     map_lba, map_lba, key, word, array[5]);
          //       map_lba += xfersize;
          //     }
          //   }

          continue;
        }

        /* This is the journal portion: */
        if (array[0] == JNL_EYE_CATCHER && !found_jnl)
        {
          found_jnl = true;
          common.ptod("Start of JNL records: %012x", seek);
        }



        /* Translate all journal entries in this record: */
        int entries = array[1];
        if (entries == 0)
          break;
        for (int j = 0; j < entries; j++)
        {
          int  key = array[ JNL_SKIP_HDR + (j*JNL_ENT_INTS) + 0 ] >>> 56;
          long lba = array[ JNL_SKIP_HDR + (j*JNL_ENT_INTS) + 1 ] * xfersize;

          if (JOURNAL_ADD_TIMESTAMP)
          {
            long tod = make64(array[ JNL_SKIP_HDR + (j*JNL_ENT_INTS) + 2 ],
                              array[ JNL_SKIP_HDR + (j*JNL_ENT_INTS) + 3 ]);
            String date = df.format(new Date(tod));

            if (key != 0)
              common.ptod("before: key: 0x%02x lba: %012x tod: %s", key, lba, date);
            else
              common.ptod("after:            lba: %012x tod: %s", lba, date);
          }
          records++;

          if (eof)
          {
            after_eof++;
          }
        }

        if (entries != JNL_ENTRIES)
        {
          if (records != 0)
            common.ptod("Found EOF after %6d journal records at seek: %08x ", records, seek);
          records = 0;
          eof = true;

          common.ptod("Found EOF after %6d journal records at seek: %08x ", records, seek);
        }

        seek += 512;
      }
      common.ptod("after_eof: " + after_eof);
      common.ptod("map_lba: " + map_lba);
      common.ptod("map_lba: " + map_lba * 4096);

      Native.closeFile(handle);
    }
  }
}





