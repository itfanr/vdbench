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

import java.util.*;
import java.io.Serializable;
import java.io.*;
import Utils.Format;
import Utils.Fput;


/**
 * This class contains information related to Storage Definitions (SDs)
 */
public class SD_entry implements Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public String  sd_name;           /* Name for this SD                       */
  private boolean active;           /* Is this SD currently active            */
  boolean sd_is_referenced = false; /* SD is referenced by a WD               */

  private String host_names[] = new String[] { "*"};

  transient Vector host_info      = new Vector(2, 0);

  public String lun;           /* Name of the LUN                             */
                               /* Can be modified in the slave to use a host  */
                               /* specific name from host_lun_list.           */

  int    filecount = 0;        /* How often to repeat the last SD             */
  int    filestart = 0;        /* With what to start counting                 */
  String wd_name;              /* Name of the WD currently using this SD      */
  public long   end_lba = 0;          /* Ending lba for this lun                     */
  int    threads = 8;          /* Thread count for this SD                    */
  String jnl_file_name = null;

  WG_entry wg_for_sd = null;

  String unique_dv_name = null;

  long   psize;                /* Confirmed physical size of this lun         */
  long   hitarea = 1024*1024;  /* portion of SD to get hits from              */

  long   offset = 0;
  long   align  = 0;

  boolean open_for_write = false;   /* Flag for all workloads, not just the current */

  int    threads_used;         /* Thread count for this SD                    */
  public transient FifoList wt_to_iot;    /* Fifo list that goes from Waiter to IO_task*/

  boolean   journal_recovery_complete = false;

  long      fhandle;
  long[]    win_handles = null; /* Handle because windows allows only one     */
                                /* outstanding to a file.  We should double   */
                                /* check if this is still the case!           */

  long      ios_for_this_sd = 0;
  long      bytes_written   = 0;  /* For last tape write operation. */
  boolean   fake_tape_drive = false;

  long      scsi_lun_reset = 0;
  long      scsi_bus_reset = 0;
  long      scsi_lun_tod   = 0;
  long      scsi_bus_tod   = 0;

  double   lowrange  = -1;    /* Starting and                                */
  double   highrange = -1;    /* Ending search range within SD               */

  public    OpenFlags    open_flags = new OpenFlags();

  double[]  possible_replay_numbers = new double[0];
  String[]  possible_replay_groups  = new String[0];

  String    disknames[]    = null;
  String    instance       = null;

  public transient   PrintWriter pw = null;     /* For SD level reports. */

  transient Vector  raw_device_list = null;  /* list of disks for this SD */

  public static String NEW_FILE_FORMAT_NAME = "File_format_or_append_for_sd=";

  boolean  format_inserted = false;

  private static Vector pending_host_parms = new Vector(8, 0);
  private static Vector pending_lun_parms  = new Vector(8, 0);

  transient HashMap hosts_using_sd;
  private static long max_xfersize = 0;    /* Maximum xfersize used in any SD */

  static SD_entry dflt = new SD_entry();

  private static boolean tape_testing = false;


  /**
   * This is a DEEP clone!
   * We must make sure that after the regular shallow clone we create a new copy
   * of every single instance that is an SD parameter.
   */
  public Object clone()
  {
    try
    {
      SD_entry sd = (SD_entry) super.clone();

      sd.host_info               = (Vector)   host_info.clone();

      sd.host_names              = (String[]) host_names.clone();
      sd.possible_replay_numbers = (double[]) possible_replay_numbers.clone();
      sd.possible_replay_groups  = (String[]) possible_replay_groups.clone();
      sd.open_flags              = (OpenFlags) open_flags.clone();

      return sd;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  public void setActive(boolean bool)
  {
    active = bool;
  }
  public boolean isActive()
  {
    return active;
  }


  /**
   * Open all SDs.
   */
  public static void sd_open_all_files(Vector sd_list)
  {
    int rewinds_pending = 0;
    boolean tape_testing = SlaveWorker.work.tape_testing;
    Work work = SlaveWorker.work;

    /* Solaris has one file handle, shared by all threads: */
    /* Open all non-windows files:                         */
    /* Open all windows files:                             */
    for (int i = 0; !common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      /* Already opened for an other WG_entry? */
      if (sd.fhandle != 0)
        continue;

      /* Open the file after getting the requested open flags: */
      OpenFlags flags = sd.open_flags;
      if (SlaveWorker.work.rd_open_flags != null)
        flags = SlaveWorker.work.rd_open_flags;

      common.ptod("Opening sd=" + sd.sd_name +
                  ",lun=" + sd.lun +
                  "; write: " + sd.open_for_write +
                  "; " + flags);

      if (common.onLinux() && sd.lun.startsWith("/dev/") && flags.getOpenFlags() == 0)
        common.failure("On Linux 'openflags=o_direct' is required for any lun starting with '/dev/': " + sd.lun);

      /* Determine how to open the file/lun. SOL_CLEAR_CACHE requires 'open for write' */
      int open_for = sd.open_for_write ? 1 : 0;
      if (flags.isOther(OpenFlags.SOL_CLEAR_CACHE))
        open_for = 1;

      sd.fhandle = Native.openFile(sd.lun, flags, open_for);
      if (sd.fhandle == -1)
        common.failure("Open for lun '" + sd.lun + "' failed");
      File_handles.addHandle(sd.fhandle, sd);

      /* Workaround for NFS still using EXISTING cached blocks  */
      /* inspite of directio() requested:                       */
      if (flags.isOther(OpenFlags.SOL_CLEAR_CACHE))
      {
        int rc = Native.eraseFileSystemCache(sd.fhandle, sd.end_lba);
        if (rc != 0)
          common.ptod("Native.eraseFileSystemCache() failed: " + rc);
      }

      // Experiment:
      // -1: cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET;
      // -2: cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET_ALL;
      if (common.get_debug(common.SCSI_RESET_AT_START))
      {
        long rc = Native.writeFile(sd.fhandle, 0, 0, -1);
        if (rc != 0)
          common.failure("Scsi lun reset failed: " + Errno.xlate_errno(rc));
      }
      if (common.get_debug(common.SCSI_RESET_ALL_START))
      {
        long rc = Native.writeFile(sd.fhandle, 0, 0, -2);
        if (rc != 0)
          common.failure("Scsi lun reset failed: " + Errno.xlate_errno(rc));
      }
    }


    /* For Windows I need one handle per thread: */
    /* Open all Windows files:                   */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      /* Already opened for an other WG_entry? */
      if (sd.win_handles != null)
        continue;

      /* For Windows I need one handle per thread: */
      sd.win_handles = new long[ work.getThreadsUsed(sd) ];
      for (int j = 0; j < sd.win_handles.length; j++)
      {
        /* Create a file handle: */
        sd.win_handles[j] = Native.openFile(sd.lun, sd.open_flags, sd.open_for_write ? 1 : 0);
        if (sd.win_handles[j] == -1)
          common.failure("Open for lun '" + sd.lun + "' failed");
        File_handles.addHandle(sd.win_handles[j], sd);

        /* If needed, start a tape rewind: */
        if (j == 0 && tape_testing && !sd.fake_tape_drive)
        {
          if (Native.windowsRewind(sd.win_handles[j], 0) < 0)
            common.failure("Tape rewind failed for " + sd.lun);
          rewinds_pending++;
        }
      }

      common.ptod("Opening sd=" + sd.sd_name +
                  ",lun=" + sd.lun +
                  "; write: " + sd.open_for_write + " " + sd.end_lba);
    }


    /* For windows tapes wait for the pending rewinds: */
    if (rewinds_pending > 0)
    {
      common.ptod("Rewinding all tapes.");
      for (int i = 0; i < work.wgs_for_slave.size(); i++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
        SD_entry sd = wg.sd_used;

        if (tape_testing)
        {
          if (Native.windowsRewind(sd.win_handles[0], 1) < 0)
            common.failure("Tape rewind failed for " + sd.lun);
        }
      }
      common.ptod("Tape rewind(s) complete");
    }
  }


  /**
   * Close all SDs. Rewind tapes if needed.
   */
  public static void sd_close_all_files(Vector sd_list)
  {
    int rewinds_pending = 0;
    Work work = SlaveWorker.work;

    /* Close all non-windows files: */
    for (int i = 0; !common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      /* Already closed for a different WG_entry? */
      if (sd.fhandle == 0)
        continue;

      //int open_flags = OpenFlags.translateFlags(sd.open_flag_list);
      //OpenFlags.clearCacheIfNeeded(sd.fhandle, sd.lun, open_flags, sd.end_lba);

      /* Which open flag to use for possible fsync? */
      OpenFlags flags = sd.open_flags;
      if (SlaveWorker.work.rd_open_flags != null)
        flags = SlaveWorker.work.rd_open_flags;

      long rc = Native.closeFile(sd.fhandle, flags);
      if (rc != 0)
        common.failure("File close failed: rc=" + rc + " " + sd.lun);
      File_handles.remove(sd.fhandle);
      sd.fhandle = 0;
    }

    /* Write tapemarks on all windows output tapes (no wait): */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      if (sd.isTapeDrive() && wg.readpct == 0 && !sd.fake_tape_drive)
      {
        common.ptod("Starting tapemark write for sd=" + sd.sd_name);
        if (Native.windows_tapemark(sd.win_handles[0], 1, 0) < 0)
          common.failure("Write tapemark failed for " + sd.lun);
      }
    }


    /* Write tapemarks on all windows output tapes (wait): */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      if (sd.isTapeDrive() && wg.readpct == 0 && !sd.fake_tape_drive)
      {
        if (Native.windows_tapemark(sd.win_handles[0], 1, 1) < 0)
          common.failure("Write tapemark failed for " + sd.lun);
        common.ptod("Ending tapemark write for sd=" + sd.sd_name);
      }
    }


    /* Start rewind for all tapes: */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      if (sd.isTapeDrive() && !sd.fake_tape_drive)
      {
        rewinds_pending++;
        if (Native.windowsRewind(sd.win_handles[0], 0) < 0)
          common.failure("Tape rewind failed for " + sd.lun);
      }
    }



    /* Wait for rewind for all tapes: */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      if (sd.isTapeDrive() && !sd.fake_tape_drive)
      {
        if (Native.windowsRewind(sd.win_handles[0], 1) < 0)
          common.failure("Tape rewind failed for " + sd.lun);
        common.ptod("Ending tape rewind for sd=" + sd.sd_name);
      }
    }


    /* Now close all windows files: */
    for (int i = 0; common.onWindows() && i < work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;

      /* Already closed for a different WG_entry? */
      if (sd.win_handles == null)
        continue;

      for (int j = 0; j < work.getThreadsUsed(sd); j++)
      {
        long rc = Native.closeFile(sd.win_handles[j]);
        if (rc != 0)
          common.failure("File close failed: rc=" + rc + " " + sd.lun);
        File_handles.remove(sd.win_handles[j]);
      }
      sd.win_handles = null;
    }
  }


  /**
   * Read Storage Definition input and interpret and store parameters.
   */
  static String readParms(Vector sd_list, String first)
  {
    String   str  = first;
    SD_entry sd   = null;
    Vdb_scan prm = null;

    try
    {
      while (true)
      {
        /* This null check exists to allow cache-only run from T3_only: */
        if (str != null)
        {
          prm = Vdb_scan.parms_split(str);

          if (prm.keyword.equals("wd") ||
              prm.keyword.equals("rd") ||
              prm.keyword.equals("fwd"))
          {
            /* Finsh the previous SD: */
            if (sd != null)
              sd.finishPendingParms();
            break;
          }

          if (prm.keyword.equals("sd"))
          {
            /* Finsh the previous SD: */
            if (sd != null)
              sd.finishPendingParms();

            if (prm.alphas[0].equals("default")  )
              sd = dflt;

            else
            {
              /* This is a new SD: */
              sd = (SD_entry) dflt.clone();
              sd.sd_name = prm.alphas[0];
              sd_list.addElement(sd);

              if (sd.sd_name.length() > 8 && Validate.isValidate())
                common.failure("With Data Validation an SD name may be no " +
                               "longer than 8 bytes ('" + sd.sd_name + "')");

              if (Validate.isValidate() && sd.sd_name.length() > 8)
                common.failure("For Data Validation an SD name may be only 8 " +
                               "characters or less: " + sd.sd_name);
            }
          }

          else if ("host".startsWith(prm.keyword) || prm.keyword.equals("hd"))
            pending_host_parms.add(prm.alphas);

          else if ("lun".startsWith(prm.keyword))
          {
            if (sd == dflt)
              common.failure("You may not specify the 'lun=' parameter for sd=default");
            sd.lun = prm.alphas[0];
            pending_lun_parms.add(sd.lun);
          }

          else if ("count".startsWith(prm.keyword))
          {
            if (prm.getNumCount() != 2)
              common.failure("'count=(start,count)' parameter requires two values");
            sd.filestart = (int) prm.numerics[0];
            if (prm.getNumCount() > 1)
            {
              sd.filecount = (int) prm.numerics[1];
              if (sd.filecount <= 0)
                common.failure("'count=(start,count)' parameter requires two "+
                               "values of which the second value must be greater than zero.");
            }
          }

          else if ("size".startsWith(prm.keyword))
            sd.end_lba   = (long) prm.numerics[0];

          else if ("threads".startsWith(prm.keyword))
            sd.threads = (int) prm.numerics[0];

          else if ("hitarea".startsWith(prm.keyword))
            sd.hitarea = (int) prm.numerics[0];

          else if ("offset".startsWith(prm.keyword))
          {
            sd.offset = (long) prm.numerics[0];
            if (sd.offset %512 != 0)
              common.failure("SD offset= parameter must be multiple of 512 bytes");
          }

          else if ("align".startsWith(prm.keyword))
          {
            sd.align = (long) prm.numerics[0];
            if (sd.align %512 != 0)
              common.failure("SD align= parameter must be multiple of 512 bytes");
          }


          else if ("kstat".startsWith(prm.keyword))
            sd.disknames = prm.alphas;

          else if ("instance".startsWith(prm.keyword))
            sd.instance = prm.alphas[0];

          else if ("replay".startsWith(prm.keyword))
          {
            RD_entry.dflt.setNoElapsed();
            //Vdbmain.setReplay(true);
            sd.possible_replay_numbers = prm.numerics;
            sd.possible_replay_groups  = prm.alphas;
          }

          else if ("journal".startsWith(prm.keyword))
            sd.jnl_file_name = prm.alphas[0];

          else if ("resetlun".startsWith(prm.keyword))
            sd.scsi_lun_reset = (long) prm.numerics[0];

          else if ("resetbus".startsWith(prm.keyword))
            sd.scsi_bus_reset = (long) prm.numerics[0];

          else if ("openflags".startsWith(prm.keyword))
            sd.open_flags = new OpenFlags(prm.alphas);

          else if ("tape".startsWith(prm.keyword))
            sd.fake_tape_drive = prm.alphas[0].toLowerCase().startsWith("y");

          else if ("range".startsWith(prm.keyword))
          {
            sd.lowrange = prm.numerics[0];
            if (prm.getNumCount() > 1)
              sd.highrange = prm.numerics[1];
            else
              common.failure("'range=' parameter must be specified with a "+
                             "beginning and ending range, e.g. 'range=(10,20)'");
          }

          else
            common.failure("Unknown keyword: " + prm.keyword);

        }

        str = Vdb_scan.parms_get();
        if (str == null)
          return null;
      }

      handleFileCount(Vdbmain.sd_list);
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }

    /* We now have read all SDs, adjust replay information: */
    adjustReplay(sd_list);

    /* Keep track of disk vs tape testing: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd2 = (SD_entry) Vdbmain.sd_list.elementAt(i);
      sd2.setTapeStatus();
    }

    return str;
  }


  /**
   * host= and lun= parameters are defined in pairs.
   * To eliminate the need to have them in a specific order we keep them
   * in a 'pending' area so that, once we have seen both lun= and host=
   * we can pick them up and store them as the new parameter values.
   *
   * Syntax:
   *
   * sd=sd1,lun=abc
   * or
   * sd=sd1,lun=abc,host=hosta
   * or
   * sd=sd1,host=(hosta,hostb),lun=abc,
   *        lun=def,host=hostc
   *
   *        lun/host must be in pairs.
   */
  private void finishPendingParms()
  {
    /* For 'default' we don't need this: */
    if (this == dflt)
      return;

    /* Must have at least a lun name: */
    if (pending_lun_parms.size() == 0 )
      common.failure("No lun name specified for sd=" + sd_name);

    /* Just a single lun, no host: use the default: */
    if (pending_lun_parms.size() == 1 && pending_host_parms.size() == 0)
      pending_host_parms.add(host_names);

    /* The pending counts must be equal: */
    if (pending_lun_parms.size() != pending_host_parms.size())
      common.failure("'host=' and 'lun=' parameters must be defined in pairs for sd=" + sd_name);

    /* Store the lun name on all hosts that are involved: */
    for (int i = 0; i < pending_lun_parms.size(); i++)
    {
      /* Get host names and translate possible wildcards to a list: */
      String[] hosts_from_parm = (String[]) pending_host_parms.elementAt(i);
      Vector all_host_names = Host.findSelectedHosts(hosts_from_parm);

      for (int j = 0; j < all_host_names.size(); j++)
      {
        Host host = Host.findHost((String) all_host_names.elementAt(j));

        /* If we have only a single SD, add this to the host: */
        if (filecount == 0)
          host.addLun(sd_name, (String) pending_lun_parms.elementAt(i));

        else
        {
          /* Add all proper sd names and lun names: */
          /* (The extra SDs will be created in handleFileCount()) */
          for (int k = 0; k < filecount; k++)
          {
            host.addLun(sd_name + (k + filestart),
                        createCountingFileName(((String) pending_lun_parms.elementAt(i)), filestart, k));
          }
        }
      }
    }


    /* Clear for the next SD: */
    pending_host_parms.removeAllElements();
    pending_lun_parms.removeAllElements();
  }



  /**
   * Look for tape testing.
   * I do not allow mixed tape and disk testing.
   */
  private static boolean checked_tape = false;
  private void setTapeStatus()
  {
    boolean yesorno = isTapeDrive();
    {
      if (checked_tape && tape_testing != yesorno)
        common.failure("Mixed disk and tape testing not allowed");
      SD_entry.tape_testing = yesorno;
      checked_tape = true;
    }
  }

  public boolean isTapeDrive()
  {
    String tmp = lun.toLowerCase();
    return(tmp.startsWith("\\\\.\\tape") ||
           tmp.startsWith("/dev/rmt")    ||
           tmp.startsWith("/dev/st")     ||
           tmp.startsWith("/dev/nst")    ||
           fake_tape_drive);
  }


  private static void adjustReplay(Vector sd_list)
  {
    /* We now have read all SDs, adjust replay information: */
    for (int x = 0; x < sd_list.size(); x++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(x);

      /* If any numeric parameters, create a new group name 'sd_name': */
      if (sd.possible_replay_numbers!= null && sd.possible_replay_numbers.length > 0)
      {
        ReplayGroup rg = new ReplayGroup(sd.sd_name);
        rg.addSD(sd);
        for (int i = 0; i < sd.possible_replay_numbers.length; i++)
          rg.addDevice((long) sd.possible_replay_numbers[i]);
      }

      else if (sd.possible_replay_groups.length > 0)
      {
        /* There is a group name, tell the group(s) about the SD: */
        for (int i = 0; i < sd.possible_replay_groups.length; i++)
          ReplayGroup.addSD(sd, sd.possible_replay_groups[i]);
      }
    }
  }


  /**
   * To facilitate playing with a (not too) large amount of different
   * files there is the count= parameter, which will take each SD and clone it
   * count=(nn,mm) times.
   * SD names and lun names will each be suffixed with mm++, e.g.
   * sd=sd,lun=file,count=(1,5) results in sd1-5 and file1-5
   */
  private static void handleFileCount(Vector sd_list)
  {
    boolean found = false;
    do
    {
      found = false;
      for (int i = 0; i < sd_list.size(); i++)
      {
        SD_entry sd = (SD_entry) sd_list.elementAt(i);
        for (int j = 0; j < sd.filecount; j++)
        {
          SD_entry sd2 = (SD_entry) sd.clone();
          sd2.sd_name += (j + sd.filestart);

          sd2.filecount = 0;
          sd_list.add(sd2);
          found = true;
          common.plog("'SD=" + sd.sd_name +
                      ",count=(start,count)' option added " + sd2.sd_name + " " +
                      createCountingFileName(sd.lun, sd.filestart, j));
        }

        if (sd.filecount > 0)
          sd_list.remove(sd);
      }
    } while (found);


    /* Look for duplicate names now: */
    for (int i = 0; i < sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) sd_list.elementAt(i);
      for (int j = i+1; j < sd_list.size(); j++)
      {
        SD_entry sd2 = (SD_entry) sd_list.elementAt(j);
        if (sd2.sd_name.equals(sd.sd_name))
          common.failure("Duplicate SD names not allowed: " + sd.sd_name);
      }
    }
  }


  private static String createCountingFileName(String lun, int start, int index)
  {
    if (lun.indexOf("*") == -1)
      return lun + (start + index);

    String ret = common.replace_string(lun, "*", "" + (start + index));
    return ret;
  }


  /**
   * Check and do scsi reset:
   */
  public synchronized void scsi_reset()
  {
    long tod = Native.get_simple_tod();

    /* Set initial timestamps: */
    if (scsi_lun_tod == 0)
    {
      scsi_lun_tod = (long) ownmath.uniform(0, scsi_lun_reset * 2 * 1000000);
      //common.ptod("lun offfset: " + (scsi_lun_tod));
      scsi_lun_tod += tod;
    }
    if (scsi_bus_tod == 0)
    {
      scsi_bus_tod = (long) ownmath.uniform(0, scsi_bus_reset * 2 * 1000000);
      scsi_bus_tod += tod;
    }


    /* Do lun reset if requested: */
    if ( scsi_lun_reset > 0 && tod > scsi_lun_tod)
    {
      common.ptod("Lun reset issued for lun=" + lun);
      long rc = Native.writeFile(fhandle, 0, 0, -1);
      if (rc != 0)
        common.failure("Scsi lun reset failed: " + Errno.xlate_errno(rc));
      scsi_lun_tod = (long) ownmath.uniform(0, scsi_lun_reset * 2 * 1000000);
      //common.ptod("lun offfset: " + (scsi_lun_tod));
      scsi_lun_tod += tod;
    }

    /* Do bus reset if requested: */
    if ( scsi_bus_reset > 0 && tod > scsi_bus_tod)
    {
      common.ptod("Bus reset issued for lun=" + lun);
      long rc = Native.writeFile(fhandle, 0, 0, -2);
      if (rc != 0)
        common.failure("Scsi bus reset failed: " + Errno.xlate_errno(rc));
      scsi_bus_tod = (long) ownmath.uniform(0, scsi_bus_reset * 2 * 1000000);
      scsi_bus_tod += tod;
    }
  }



  public static SD_entry findSD(String name)
  {
    for (int i = 0; i< Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (sd.sd_name.equals(name))
        return sd;
    }

    common.failure("findSD(): SD not found: " + name);
    return null;
  }


  public static SD_entry[] getActiveSds()
  {
    Vector sds = new Vector(8, 0);
    for (int i = 0; i< Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (sd.isActive())
        sds.add(sd);
    }


    return(SD_entry[]) sds.toArray(new SD_entry[0]);
  }

  public static long getMaxXfersize()
  {
    return max_xfersize;
  }
  public static void setMaxXfersize(long xf)
  {
    max_xfersize = xf;
  }

  public DV_map getDvMap()
  {
    return DV_map.findMap(sd_name);
  }
  public Jnl_entry getJournal()
  {
    DV_map map = DV_map.findMap(sd_name);
    if (map == null)
      common.failure("SD=" + sd_name + " does not have a Journal instance.");

    return map.journal;
  }

  public static String[] getSdNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      names.put(sd.sd_name, sd);
    }

    return (String[]) names.keySet().toArray(new String[0]);
  }

  public static boolean isTapeTesting()
  {
    return tape_testing;
  }

  /**
   * Check to see if a duplicate SD exists. Not allowed.
   */
  private static void checkForDuplicate(String new_sd)
  {
    SD_entry sd;
    int i;

    for (i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (sd.sd_name.compareTo(new_sd) == 0)
        common.failure("Duplicate SD names not allowed: " + new_sd);
    }
  }

  public static void clearHostMaps()
  {
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      sd.hosts_using_sd = new HashMap(32);
    }
  }

  public int howManySlavesForSD(Host host)
  {
    int slaves = 0;
    Host[] hosts = (Host[]) hosts_using_sd.keySet().toArray(new Host[0]);
    for (int i = 0; i < hosts.length; i++)
      slaves += hosts[i].getSlaves().size();

    return slaves;
  }
}


/**
 * Sort Sds by name, allowing correct order for sd1 and sd12.
 * Can use either a String as input, or SD_entry.
 */
class SdSort implements Comparator
{
  public int compare(Object o1, Object o2)
  {
    String e1;
    String e2;

    if (o1 instanceof SD_entry)
    {
      e1 = ((SD_entry) o1).sd_name;
      e2 = ((SD_entry) o2).sd_name;
    }
    else
    {
      e1 = (String) o1;
      e2 = (String) o2;
    }


    int    last_equal = -1;

    /* First compare byte for byte until we have a difference: */
    for (int i = 0; i < e1.length() && i < e2.length(); i++)
    {
      if (e1.charAt(i) == e2.charAt(i))
        last_equal = i;
    }

    /* No differences: */
    if (last_equal == -1)
      return e1.compareToIgnoreCase(e2);

    try
    {
      /* Get the remainder of both values: */
      String r1 = e1.substring(last_equal + 1);
      String r2 = e2.substring(last_equal + 1);

      //common.ptod("r1: " + r1 + " " + r2);

      int num1 = Integer.parseInt(r1);
      int num2 = Integer.parseInt(r2);

      return num1 - num2;
    }

    catch (Exception e)
    {
    }

    /* Anything down here: just do an alfa compare: */
    return e1.compareToIgnoreCase(e2);
  }

}


