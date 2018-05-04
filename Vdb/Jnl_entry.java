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

import java.io.File;
import java.util.Vector;

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
 *             24 bits Reserved for future use
 *              8 bits with Data Validation key of next write
 *                If key is zero it means that this journal entry describes
 *                the AFTER image of the record and the write was complete.
 *             32 bits with relative record# (lba / xfersize).
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
 */
public class Jnl_entry
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /* One entry per SD_entry or FileAnchor                                     */
  private SD_entry sd = null;
  private DV_map current_map;
  private long   jnl_handle;          /* File pointer to Journal file         */
  private long   map_handle;          /* File pointer to map file             */
  private long   jnl_offset;          /* Current journal file offset in bytes */
  private String map_file_name;       /* File name for map file (.map)        */
  private String jnl_file_name;       /* File name for jnl file (.jnl)        */

  private int    jnl_array[] = new int[ 1024 / 4 ];
  private int    jnl_index   = 0;     /* Ptr next free journal entry          */

  private long   jnl_native_buffer;  /* Buffer for JNI                        */

  private OpenFlags open_flags = new OpenFlags();     /* Flags for fast/slow read+write access */
  public  FileAnchor recovery_anchor = null;

  private long   dump_journal_tod;

  private static int JNL_IO_SIZE     = 512;
  private static int JNL_IO_SIZE_EOF = 1024;
  private static int JNL_ENTSIZE     = 8; /* word0: if 0: write complete               */
                                          /*        if 127, write failed DV_map.DV_ERROR */
                                          /*         else: before write; new key      */
                                          /* word1: lba / xfersize                */

  private static int MAP_HDR_SIZE    = 32; /* 32 bytes header on each map record  */
  private static int MAP_SKIP_HDR    = 8 ;

  private static int JNL_HDR_SIZE    = 16; /* 16 bytes header on each journal record  */
  private static int JNL_SKIP_HDR    = 4;

  private static int JNL_ENTRIES     = (JNL_IO_SIZE - JNL_HDR_SIZE) / JNL_ENTSIZE;
  /* Number of 8-byte entries in journal rec */

  private static int MAP_EYE_CATCHER = 0x48564442; /* eye catcher for map records */
  private static int JNL_EYE_CATCHER = 0x48454e4b; /* eye catcher for jnl record  */

  static int    RECOVERY_READ = -2;
  static String RECOVERY_RUN_NAME = "journal_recovery";



  /**
   * Create journals for an SD.
   */
  Jnl_entry(SD_entry sd_in)
  {
    sd = sd_in;
    common.ptod("Allocating a new Journal file for sd=" + sd.sd_name);
    if (sd.jnl_file_name != null)
    {
      /* If the journal file is on a raw device the MAp file stays in default: */
      if (isRawJournal(jnl_file_name))
      {
        map_file_name = sd.sd_name + ".map";
        jnl_file_name = sd.jnl_file_name;
      }

      else
      {
        map_file_name = sd.jnl_file_name + File.separator + sd.sd_name + ".map";
        jnl_file_name = sd.jnl_file_name + File.separator + sd.sd_name + ".jnl";
      }
    }

    else
    {
      map_file_name = sd.sd_name + ".map";
      jnl_file_name = sd.sd_name + ".jnl";
    }

    map_file_name = new File(map_file_name).getAbsolutePath();
    jnl_file_name = new File(jnl_file_name).getAbsolutePath();

    jnl_native_buffer = Native.allocBuffer((long) JNL_IO_SIZE_EOF);

    /* Delete the old files first to possibly shorten them: */
    if (!Validate.isJournalRecovery() || Validate.isJournalRecovered() )
    {
      new File(map_file_name).delete();
      new File(jnl_file_name).delete();
    }

    /* Open the journal and map files here (use 'fast' access): */
    openFiles(true);
  }


  /**
   * Create journals for an FSD.
   */
  Jnl_entry(String jnl_file, String fsd_name)
  {
    common.ptod("Allocating a new Journal file for fsd=" + fsd_name);
    if (jnl_file != null)
    {
      map_file_name = jnl_file + File.separator + fsd_name + ".map";
      jnl_file_name = jnl_file + File.separator + fsd_name + ".jnl";
    }

    else
    {
      map_file_name = fsd_name + ".map";
      jnl_file_name = fsd_name + ".jnl";
    }

    common.ptod("Opening Journal file: " + jnl_file_name);
    map_file_name = new File(map_file_name).getAbsolutePath();
    jnl_file_name = new File(jnl_file_name).getAbsolutePath();

    jnl_native_buffer = Native.allocBuffer((long) JNL_IO_SIZE_EOF);

    /* Delete the old files first to possibly shorten them: */
    if (!Validate.isJournalRecovery() || Validate.isJournalRecovered() )
    {
      new File(map_file_name).delete();
      new File(jnl_file_name).delete();
    }

    /* Open the journal and map files here (use 'fast' access): */
    openFiles(true);
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
    DV_map[] maps = DV_map.getAllMaps();

    for (int i = 0; i < maps.length; i++)
    {
      Jnl_entry jnl = maps[i].journal;
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
      default_flags = new OpenFlags(new String[] { "o_sync"});
    else if (common.onLinux())
      default_flags = new OpenFlags(new String[] { "o_sync"});
    else if (common.onWindows())
      default_flags = new OpenFlags(new String[] { "directio"});
    else
      common.failure("Synchronous i/o options needed for Journaling currently " +
                     "only known for Solaris, Linux, and Windows. Contact hv@sun.com");


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
  public static void dumpAllMaps()
  {
    if (!Validate.isJournaling())
      return;

    if (common.get_debug(common.DONT_DUMP_MAPS))
    {
      /* For file system formatting: */
      if (!SlaveWorker.work.format_run)
      {
        log("dumpAllMaps(): request ignored due to 'DONT_DUMP_MAPS'");
        return;
      }

      log("dumpAllMaps(): 'DONT_DUMP_MAPS' request ignored because this is a format run");
    }

    DV_map[] maps = DV_map.getAllMaps();

    for (int i = 0; i < maps.length; i++)
    {
      Jnl_entry jnl = maps[i].journal;
      if (jnl != null)
        jnl.dumpOneMap(maps[i]);
    }
  }


  public void dumpOneMap(DV_map map)
  {
    /* Remember when: */
    dump_journal_tod = System.currentTimeMillis();

    /* Possibly re-open the files with fast read+write access: */
    openFiles(true);

    log("Writing Data Validation map to " + jnl_file_name);
    jnl_dump_map(jnl_handle, map);

    log("Writing Data Validation map to " + map_file_name);
    jnl_dump_map(map_handle, map);

    /* Possibly re-open the files with slow read+write access: */
    openFiles(false);
  }


  /**
   * Read one physical record from Journal
   */
  private static void jnl_read(long handle, long seek, long length, long buffer) throws Exception
  {
    //common.ptod("jnl_read handle: " + handle + " seek: " + seek + " buffer: " + buffer);
    long rc = Native.readFile(handle, seek, length, buffer);
    if (rc != 0)
      throw new Exception("Journal read failed: "+ Errno.xlate_errno(rc));
  }


  /**
   * Write one physical record to journal
   */
  private static void jnl_write(long handle, long seek, long length, long buffer)
  {
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
      Native.array_to_buffer(jnl_array, jnl_native_buffer);
      jnl_write(jnl_handle, jnl_offset, 512, jnl_native_buffer);
    }
    else
    {
      /* Add EOF record: */
      jnl_array[128+0] = JNL_EYE_CATCHER;
      jnl_array[128+1] = 0;
      jnl_array[128+2] = (int) jnl_offset / 512;
      jnl_array[128+3] = (int) dump_journal_tod;

      Native.array_to_buffer(jnl_array, jnl_native_buffer);
      jnl_write(jnl_handle, jnl_offset, 1024, jnl_native_buffer);

      jnl_offset += 512;
      jnl_index   = 0;

      if (jnl_offset % (10*1024*1024) == 0)
        common.ptod("Journal file " + jnl_file_name + " is now " +
                    (jnl_offset / (1*1024*1024)) + "mb");
    }
  }


  /**
   * Write a journal entry at the start of a write
   */
  public synchronized void writeBeforeImage(Cmd_entry cmd)
  {
    if (Validate.isMapOnly())
      return;

    writeJournalEntry(cmd.dv_key, (int) (cmd.cmd_lba / cmd.cmd_xfersize), true);
  }


  /**
   * Write a journal entry after the write completes
   */
  public synchronized void writeAfterImage(Cmd_entry cmd)
  {
    if (Validate.isMapOnly())
      return;

    writeJournalEntry(0, (int) (cmd.cmd_lba / cmd.cmd_xfersize), true);
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
  public synchronized void writeJournalEntry(int key, int index, boolean last)
  {
    jnl_array [ JNL_SKIP_HDR + jnl_index*2 + 0 ] = key;
    jnl_array [ JNL_SKIP_HDR + jnl_index*2 + 1 ] = index;
    jnl_index++;

    if (last || jnl_index >= JNL_ENTRIES)
      writeJournalRecord();

    boolean debug = false;
    if (debug && key != 0)
      common.ptod("before: lba %08x key %02x", index * current_map.getBlockSize(), key);
    if (debug && key == 0)
      common.ptod("after:  lba %08x key %02x", index * current_map.getBlockSize(), key);

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
  public void jnl_dump_map(long handle, DV_map map)
  {
    /* Write control record saying "dump in progress": */
    jnl_array[0] = MAP_EYE_CATCHER;
    jnl_array[1] = -1;
    jnl_array[2] = map.map_entries;
    jnl_array[3] = map.getBlockSize();
    jnl_array[4] = (int) dump_journal_tod << 32;
    jnl_array[5] = (int) dump_journal_tod;
    jnl_array[6] = 0;
    jnl_array[7] = 0;

    Native.array_to_buffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, 0, 512, jnl_native_buffer);

    /* Start map at 512: */
    jnl_offset   = 512;
    jnl_array[1] = MAP_EYE_CATCHER;

    /* Mark all entries un-busy: */
    for (int i = 0; i < map.map_entries; i++)
      map.byte_map[i] &= 0x7f;

    /* Dump the map in chunks of x integers after byte to int translation: */
    int written = 0;
    for (int i = 0; i < map.map_entries;)
    {
      for (int j = MAP_SKIP_HDR; j < 128 && i < map.map_entries; j++)
      {
        jnl_array[ j ] = ( map.byte_map[i++] << 24 ) |
                         ( map.byte_map[i++] << 16 ) |
                         ( map.byte_map[i++] <<  8 ) |
                         ( map.byte_map[i++]       );
        written+=4;
      }

      /* Write: */
      Native.array_to_buffer(jnl_array, jnl_native_buffer);

      jnl_write(handle, jnl_offset, 512, jnl_native_buffer);
      jnl_offset += 512;
    }

    /* Write EOF record, meaning no journal records on file: */
    jnl_array[0] = JNL_EYE_CATCHER;
    jnl_array[1] = 0;                       // 0 means EOF
    jnl_array[2] = (int) jnl_offset / 512;
    jnl_array[3] = (int) dump_journal_tod;  // last 32 bits of tod.
    Native.array_to_buffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, jnl_offset, 512, jnl_native_buffer);

    /* Write control record again, now saying "dump completed": */
    jnl_array[0] = MAP_EYE_CATCHER;
    jnl_array[1] = MAP_EYE_CATCHER;
    jnl_array[2] = map.map_entries;
    jnl_array[3] = map.getBlockSize();
    jnl_array[4] = (int) dump_journal_tod << 32;
    jnl_array[5] = (int) dump_journal_tod;
    jnl_array[6] = 0;
    jnl_array[7] = 0;

    Native.array_to_buffer(jnl_array, jnl_native_buffer);
    jnl_write(handle, 0, 512, jnl_native_buffer);
  }


  /**
   * Restore Data Validation map from file
   * Layout: first sector: controlinfo
   *         rest: all data from map
   */
  public DV_map RestoreMap(long handle, String name, long max_lba, String lun)
  {
    /* Read controlinfo: */
    try
    {
      jnl_read(handle, 0, 512, jnl_native_buffer);
      Native.buffer_to_array(jnl_array, jnl_native_buffer, 512);
    }
    catch (Exception e)
    {
      common.ptod("Error while restoring the journal map. Journal likely is corrupted");
      common.ptod(e);
      common.failure(e);
    }

    /* Get map length and xfersize: */
    int mapentries  = jnl_array[2];
    int mapxfersize = jnl_array[3];

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

    /* Compare length: */
    if (max_lba / mapxfersize != mapentries)
    {
      String msg = name + " Journal recovery failed. \nJournal contains xfersize " +
                   mapxfersize + " and contains " + mapentries + " blocks, " +
                   "for a total size of " + (long) mapentries * mapxfersize + " bytes." +
                   "\nThis does not match the size of " + lun + " which is " +
                   max_lba + " bytes";

      SlaveJvm.sendMessageToConsole(msg);
      common.failure("Journal recovery failed");
    }

    /* We now have blocksize and length, so allocate map: */
    DV_map map = DV_map.allocateMap(name, max_lba, mapxfersize);
    storeMap(map);
    map.journal = this;

    /* The map size can be wrong if we did not know the proper size */
    /* during alloation:                                            */
    if (map.byte_map.length != mapentries)
    {
      map.byte_map = null;
      map.byte_map = new byte[(mapentries + 3) & ~3];
    }


    /* Start reading map at 512: */
    jnl_offset = 512;

    /* Read the map in chunks of x integers and do int to byte translation: */
    int restcnt = 0;
    for (int i = 0; i < mapentries;)
    {
      /* Read: */
      try
      {
        jnl_read(handle, jnl_offset, 512, jnl_native_buffer);
        Native.buffer_to_array(jnl_array, jnl_native_buffer, 512);
      }
      catch (Exception e)
      {
        common.ptod("Error while restoring the journal map. Journal likely is corrupted");
        common.ptod(e);
        common.failure(e);
      }

      for (int j = MAP_SKIP_HDR; j < 128 && i < mapentries; j++)
      {
        map.byte_map[ i++ ] = (byte) (jnl_array[ j ] >> 24);
        map.byte_map[ i++ ] = (byte) (jnl_array[ j ] >> 16);
        map.byte_map[ i++ ] = (byte) (jnl_array[ j ] >>  8);
        map.byte_map[ i++ ] = (byte) (jnl_array[ j ]);

        if (map.byte_map[ i - 4] != 0) restcnt++;
        if (map.byte_map[ i - 3] != 0) restcnt++;
        if (map.byte_map[ i - 2] != 0) restcnt++;
        if (map.byte_map[ i - 1] != 0) restcnt++;
      }

      jnl_offset += 512;
    }


    /* Warn users (and yourself since you keep forgetting): */
    int bad_blocks_found = 0;
    for (long i = 0; i < mapentries; i++)
    {
      int key = map.dv_get(i * map.getBlockSize());
      if (key == DV_map.DV_ERROR)
        bad_blocks_found++;
    }

    log("Journal restored for " + name + "; " + "SD contains " + restcnt + " modified blocks.");
    if (bad_blocks_found > 0)
      log(String.format("Blocks found in journal that are in error: %d. "+
                        "They will NOT be re-verified.", bad_blocks_found));

    map.setBlockSize(mapxfersize);

    return map;
  }



  /**
   * Recover the Data Validation maps using journals.
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
        Jnl_entry journal = new Jnl_entry(sd);
        DV_map map = journal.recoverOneMap(sd.sd_name, sd.end_lba, sd.lun);

        /* The WG_entry that is requesting this recovery does not have a valid */
        /* xfersize yet: store the xfersize you received from the journal: */
        sd.wg_for_sd.xfersize          = map.getBlockSize();
        sd.wg_for_sd.mcontext.seek_low = map.getBlockSize();
        sd.wg_for_sd.mcontext.next_seq = -1;
      }
    }
  }


  /**
   * Recover the Data Validation maps using journals
   */
  public DV_map recoverOneMap(String name, long max_lba, String lun)
  {
    SlaveJvm.sendMessageToConsole("Starting journal recovery for " + name);
    log("Starting journal recovery for " + name);

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
    log("Restoring Data Validation map from: " + jnl_file_name);
    DV_map map = RestoreMap(jnl_handle, name, max_lba, lun);

    /* A bad return code tells me that the map in the journal file was in */
    /* the middle of a map write that did not complete:                   */
    if (map == null)
    {
      /* Recover from mapfile: */
      log("Restoring Data Validation map from: " + map_file_name);
      map = RestoreMap(map_handle, name, max_lba, lun);
      if (map == null)
        common.failure("Invalid journal map file ");
    }

    /* Apply journal changes to map: */
    applyJournal();

    /* Now write the map back to the files: */
    //common.ptod("Writing recovered Data Validation map to " + jnl_filename(sd, ".jnl"));
    //jnl.jnl_dump_map(jnl.jnl_fhandle, sd.dv);
    //common.ptod("Writing recovered Data Validation map to " + jnl_filename(sd, ".map"));
    //jnl.jnl_dump_map(jnl.jnl_map_fhandle, sd.dv);

    SlaveJvm.sendMessageToConsole("Completed journal recovery for " + name);
    log("Completed journal recovery for " + name);

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
  private void applyJournal()
  {
    boolean debug = false;
    long before = 0;
    long after = 0;
    long lba;
    int  newkey;

    /* 'before_map' contains the BEFORE key value of keys that have been      */
    /* found in a 'before' record but are still waiting for an 'after' record: */
    /* 'current_map' is the map we're now restoring to. */
    DV_map before_map = new DV_map(current_map.map_name,
                                   current_map.map_entries,
                                   current_map.getBlockSize());
    /* In case this is for an FSD, pass the anchor: */
    before_map.recovery_anchor = this.recovery_anchor;

    common.ptod("Applying journal records from " + map_file_name);

    while (true)
    {
      jnl_read_next_record();

      /* Pick up the amount of entries I have in these 512 bytes: */
      int entries = jnl_array[1];

      /* Scan through all those entries: */
      for (int i = 0; i < entries; i++)
      {
        newkey =        jnl_array[ JNL_SKIP_HDR + i*2 + 0 ] & 0xff;
        lba    = (long) jnl_array[ JNL_SKIP_HDR + i*2 + 1 ] * current_map.getBlockSize();

        if (debug && newkey != 0)
          common.ptod("before: lba %08x key %02x", lba, newkey);
        if (debug && newkey == 0)
          common.ptod("after:  lba %08x key %02x", lba, newkey);

        /* A nonzero key indicates this is a 'before' journal entry: */
        if (newkey != 0)
        {
          before++;

          /* The BEFORE map may not contain this block as pending: */
          if (before_map.dv_get(lba) != 0)
          {
            common.ptod("lba: %08x", lba);
            common.ptod("before_map.dv_get(lba): " + before_map.dv_get(lba));
            common.ptod("newkey: " + newkey);
            common.failure("Journal recovery: unmatched pending 'before' record found.");
          }

          /* If the old key was zero, block never written, save that status: */
          if (current_map.dv_get(lba) == 0)
          {
            if (debug) common.ptod("old key 0, before_map set to 127. lba: %08x", lba);
            before_map.dv_set(lba, DV_map.DV_ERROR);

            /* The new key then must be '1': */
            if (newkey != 1)
              common.failure("Expecting DV key 1 for lba %08x", lba);
          }

          /* 'before_map' now will contain the 'before' key value: */
          else
          {
            if (debug) common.ptod("old key 0x%02x lba: %08x", current_map.dv_get(lba), lba);
            before_map.dv_set(lba, current_map.dv_get(lba));
          }

          /* 'current_map' now contains the 'after' key value. */
          current_map.dv_set(lba, newkey);
          if (debug) common.ptod("set current key 0x%02x lba: %08x", newkey, lba);
        }

        else
        {
          /* This is the 'after' entry, clear the saved 'before' key: */
          after++;
          before_map.dv_set(lba, 0);
          if (debug) common.ptod("Reset before map lba: %08x", lba);
        }
      }

      /* If the record was not full, EOF: */
      if (entries < JNL_ENTRIES)
        break;
    }

    /* After this, the before_map table contains all entries that were */
    /* in progress, and MAY have either a before or after key value.   */
    if (sd != null)
    {
      /* Create an array with the pending blocks: */
      VerifyPending.findPendingBlocks(before_map, current_map, sd);

      /* Now check the contents of those blocks: */
      VerifyPending.checkSdPendingBlocks(before_map, current_map, sd);
    }

    else
    {
      /* Create an array with the pending blocks: */
      VerifyPending.findPendingBlocks(before_map, current_map, sd);

      /* Now check the contents of those blocks: */
      VerifyPending.checkFsdPendingBlocks(this.recovery_anchor, before_map, current_map);
    }

    log("Completed apply of journal records (before/after): " + before + "/" + after);
    if (after > before)
      common.failure("Unexpected before/after count. 'After' may not be greater than 'before'");
  }



  /**
   * Read next sequential record from the journal file
   */
  private void jnl_read_next_record()
  {
    try
    {
      jnl_read(jnl_handle, jnl_offset, 512, jnl_native_buffer);
      Native.buffer_to_array(jnl_array, jnl_native_buffer, 512);
      jnl_offset += 512;
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


  private static void log(String txt)
  {
    String rd = "rd=" + SlaveWorker.work.work_rd_name;
    ErrorLog.sendMessageToLog(rd + ": " + txt);
  }

  private static void log(String format, Object ... args)
  {
    log(String.format(format, args));
  }

  public static void main(String args[])
  {
    for (int x = 0; x < args.length; x++)
    {
      long buffer = Native.allocBuffer(512);
      int[] array = new int[128];

      common.ptod("args[x]: " + args[x]);
      long handle = Native.openFile(args[x], 0);
      int xfersize = -1;
      long seek = 0;
      int block = 0;
      int records = 0;
      int after_eof = 0;
      boolean eof = false;
      for (int i = 0; i < 99999 ; i++)
      {
        long rc = Native.readFile(handle, seek, 512, buffer);
        Native.buffer_to_array(array, buffer, 512);
        if (rc != 0)
        {
          common.ptod("read error");
          break;
        }

        /* Ignore the map itself: */
        if (array[0] == 0x48564442)
        {
          if (xfersize < 0)
            xfersize = array[3];
          seek += 512;
          continue;
        }

        int entries = array[1] / 2;
        for (int j = 0; j < entries; j++)
        {
          records++;
          int key  = array[ 2 + (j*2) ];
          long lba = array[ 3 + (j*2) ] * xfersize;

          if (false)
          {
            if (key != 0)
              common.ptod("before: key: %2d %02x lba: %08x %2d", key, key, lba, j);
            else
              common.ptod("after:  key: %2d %02x lba: %08x %2d", key, key, lba, j);
          }

          if (eof)
          {
            after_eof++;
          }
        }

        if (entries != 63)
        {
          if (records != 0)
            common.ptod("Found EOF after %d journal records at seek: %08x ", records, seek);
          records = 0;
          eof = true;
        }
        seek += 512;
      }
      common.ptod("after_eof: " + after_eof);

      Native.closeFile(handle);
    }
  }
}





