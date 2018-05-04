package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;
import Utils.Fput;


/**
 * This class contains information related to Storage Definitions (SDs)
 */
public class SD_entry implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public boolean    concatenated_sd = false;
  public String     concat_wd_name  = null;

  /* This list MUST be in the order specified by the user in sd=(x,y,z)       */
  public ArrayList <SD_entry> sds_in_concatenation = null;

  public String  sd_name;           /* Name for this SD                       */
  public String  sd_name8;          /* 8 bytes for Data Validation.           */
  private boolean active;           /* Is this SD currently active            */
  boolean sd_is_referenced = false; /* SD is referenced by a WD               */
  public  int     relative_sd_num;  /* Order found by parser, starting with ONE */

  public  int     relative_fake_sd;

  private String host_names[] = new String[] { "*"};

  transient Vector host_info      = new Vector(2, 0);

  /**
   * LUN name: a better place to find the proper lun name is under
   * host.getLunNameForSd() because ultimately lun names CAN be different across
   * hosts.
   */
  public String lun;

  int    filecount = 0;        /* How often to repeat the last SD             */
  int    filestart = 0;        /* With what to start counting                 */
  public long   end_lba = 0;   /* Ending lba for this lun                     */

  public long   csd_start_lba = 0; /* Logical start lba within concatenated SD*/
                                   /* '-1' means not set yet.                 */
  public long   csd_end_lba   = 0; /* Logical end lba within concatenated SD  */

  int    threads = 8;          /* Thread count for this SD                    */
  String jnl_dir_name = null;

  public SdDedup     sdd                   = null;
  public Dedup       dedup                 = null;
  public DV_map      dv_map                = null;

  WG_entry wg_for_sd = null;

  long   psize;                /* Confirmed physical size of this lun         */
  long   hitarea = 1024*1024;  /* portion of SD to get hits from              */

  long   offset = 0;
  long   align  = 0;

  public  boolean open_for_write = false;   /* Flag for all workloads, not just the current */

  boolean pure_rand_seq;       /* A WG_entry flag: is this pur random or sequential? */

  public transient FifoList fifo_to_iot;    /* Fifo list that goes from Waiter to IO_task*/

  boolean   journal_recovery_complete = false;

  long      fhandle;

  long      scsi_lun_reset = 0;
  long      scsi_bus_reset = 0;
  long      scsi_lun_tod   = 0;
  long      scsi_bus_tod   = 0;

  double   lowrange  = -1;    /* Starting and                                */
  double   highrange = -1;    /* Ending search range within SD               */

  public    OpenFlags    open_flags = new OpenFlags();

  double[]  possible_replay_numbers = new double[0];
  String[]  possible_replay_groups  = new String[0];

  /* This duplicate number is used to tell ReplayGen.generate() what */
  /* the duplicate is he needs to look for. */
  long       duplicate_number = 0;

  String    disknames[]    = null;
  String    instance       = null;

  public long last_replay_lba_used = 0;

  private HashMap xfersizes_map = new HashMap(8);

  transient Vector  raw_device_list = null;  /* list of disks for this SD */

  boolean  format_inserted = false;

  long     sd_error_count = 0;

  private static Vector          pending_host_parms = new Vector(8, 0);
  private static Vector <String> pending_lun_parms  = new Vector(8, 0);

  private int     key_block_size       = 0; /* Greatest common divisor of DV xfer*/

  private int         max_xfersize = 0;         /* Maximum xfersize used for this SD */


  public transient Slave    slave_used_for_dv = null;

  private static int  all_max_xfersize = 0;     /* Maximum xfersize used for any  SD */

  public static int max_sd_name = 0;

  private static SD_entry dflt = new SD_entry();
  static { dflt.sd_name = "default";};

  public static String SD_FORMAT_NAME = "SD_format";

  public HashMap <Long, BadDataBlock> bad_data_map = null;

  public  Fput    rw_log = null;
  private static HashMap <String, Fput> open_rwlog_map = new HashMap(8);


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

      if (dedup != null)
        sd.dedup = (Dedup) dedup.clone();

      return sd;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  public static void clearAllActive()
  {
    for (SD_entry sd : Vdbmain.sd_list)
      sd.active = false;;
  }
  public void setActive()
  {
    active = true;
  }
  public boolean isActive()
  {
    return active;
  }


  /**
   * Open all SDs for all workloads.
   */
  public static void openAllSds()
  {
    for (WG_entry wg : SlaveWorker.work.wgs_for_slave)
    {
      if (Validate.sdConcatenation())
      {
        /* Make sure the CSD handle is never used: */
        wg.sd_used.fhandle = 0;

        ArrayList <SD_entry> sds = wg.sd_used.sds_in_concatenation;
        for (SD_entry real_sd : wg.sd_used.sds_in_concatenation)
          real_sd.openOneSd();
      }
      else
        wg.sd_used.openOneSd();
    }
  }


  private void openOneSd()
  {
    if (concatenated_sd)
      common.failure("Illegal open call for concatenated sd=%s", sd_name);

    /* Already opened for an other WG_entry? */
    if (fhandle != 0)
      return;

    /* Open the file after getting the requested open flags: */
    OpenFlags flags = open_flags;
    if (SlaveWorker.work.rd_open_flags != null)
      flags = SlaveWorker.work.rd_open_flags;


    common.ptod("Opening sd=%s,lun=%s; write: %b; flags: %s",
                sd_name, lun, open_for_write, flags.toString());

    if (common.onLinux() && lun.startsWith("/dev/") && flags.getOpenFlags() == 0)
      common.failure("On Linux 'openflags=o_direct' is required for any lun starting with '/dev/': " + lun);

    fhandle = Native.openFile(lun, flags, open_for_write ? 1 : 0);
    //common.ptod("opend: " + fhandle);
    if (fhandle == -1)
      common.failure("Open for lun '" + lun + "' failed");
    File_handles.addHandle(fhandle, this);



    /* Workaround for NFS still using EXISTING cached blocks  */
    /* inspite of directio() requested:                       */
    if (flags.isOther(OpenFlags.SOL_CLEAR_CACHE))
    {
      int rc = Native.eraseFileSystemCache(fhandle, end_lba);
      if (rc != 0)
      {
        common.ptod("Native.eraseFileSystemCache() failed. file: %s, rc=%d ", lun, rc);
        common.ptod("('openflags=clear_cache' is only available when file is opened for output.)");
      }
    }

    // Experiment:
    // -1: cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET;
    // -2: cmd.uscsi_flags = USCSI_WRITE | USCSI_RESET_ALL;
    if (common.get_debug(common.SCSI_RESET_AT_START))
    {
      long rc = Native.writeFile(fhandle, 0, 0, -1);
      if (rc != 0)
        common.failure("Scsi lun reset failed: " + Errno.xlate_errno(rc));
    }
    if (common.get_debug(common.SCSI_RESET_ALL_START))
    {
      long rc = Native.writeFile(fhandle, 0, 0, -2);
      if (rc != 0)
        common.failure("Scsi lun reset failed: " + Errno.xlate_errno(rc));
    }

    /* Need Dedup bit map for this sd? */
    if (Dedup.isDedup())
    {
      sdd.uniques_bitmap = DedupBitMap.findUniqueBitmap("sd=" + sd_name);
      if (sdd.uniques_bitmap == null)
      {
        sdd.uniques_bitmap = new DedupBitMap().createMapForUniques(dedup, end_lba, "sd=" + sd_name);
        DedupBitMap.addUniqueBitmap(sdd.uniques_bitmap, "sd=" + sd_name);
      }
    }

    if (common.get_debug(common.CREATE_READ_WRITE_LOG))
      setupReadWriteLog(this);
  }



  /**
   * Close all SDs.
   */
  public static void closeAllSds()
  {
    for (WG_entry wg : SlaveWorker.work.wgs_for_slave)
    {
      if (Validate.sdConcatenation())
      {
        for (SD_entry real_sd : wg.sd_used.sds_in_concatenation)
          real_sd.closeOneSd();
      }
      else
        wg.sd_used.closeOneSd();
    }
  }

  private void closeOneSd()
  {
    if (concatenated_sd)
      common.failure("Illegal close call for concatenated sd=%s", sd_name);

    /* Already closed for a different WG_entry? */
    if (fhandle == 0)
      return;

    /* Which open flag to use for possible fsync? */
    OpenFlags flags = open_flags;
    if (SlaveWorker.work.rd_open_flags != null)
      flags = SlaveWorker.work.rd_open_flags;

    //common.ptod("closed: " + fhandle);
    long rc = Native.closeFile(fhandle, flags);
    if (rc != 0)
      common.failure("File close failed: rc=" + rc + " " + lun);
    File_handles.remove(fhandle);
    fhandle = 0;
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
              sd_list.add(sd);
              sd.relative_sd_num = sd_list.size();
              max_sd_name = Math.max(max_sd_name, sd.sd_name.length());

              if (sd.sd_name.equalsIgnoreCase("concat"))
                common.failure("Creating an SD named 'concat' is not allowed");

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
          {
            sd.end_lba = (long) prm.numerics[0];
            if (prm.num_count > 1)
              sd.end_lba /= (int) prm.numerics[1];
          }

          else if ("threads".startsWith(prm.keyword))
          {
            sd.threads = (int) prm.numerics[0];
            if (prm.num_count > 1)
              sd.threads /= Math.max(1, (int) prm.numerics[1]);
          }

          else if ("hitarea".startsWith(prm.keyword))
          {
            ConcatSds.abortIf("'hitarea=' parameter may not be used.");
            sd.hitarea = (long) prm.numerics[0];
          }

          else if ("offset".startsWith(prm.keyword))
          {
            ConcatSds.abortIf("'offset=' parameter may not be used.");
            sd.offset = (long) prm.numerics[0];
            if (sd.offset %512 != 0)
              common.failure("SD offset= parameter must be multiple of 512 bytes");
            if (Validate.isValidate())
              common.failure("Data validation and offset= are mutually exclusive.");
          }

          else if ("align".startsWith(prm.keyword))
          {
            ConcatSds.abortIf("'align=' parameter may not be used.");
            sd.align = (long) prm.numerics[0];
            if (sd.align %512 != 0)
              common.failure("SD align= parameter must be multiple of 512 bytes");
            if (Validate.isValidate())
              common.failure("Data validation and align= are mutually exclusive.");
          }


          else if ("kstat".startsWith(prm.keyword))
            sd.disknames = prm.alphas;

          else if ("instance".startsWith(prm.keyword))
            sd.instance = prm.alphas[0];

          else if ("replay".startsWith(prm.keyword))
          {
            RD_entry.dflt.setNoElapsed();
            ReplayInfo.setReplay();
            sd.storeReplayParms(prm);
          }

          else if ("journal".startsWith(prm.keyword))
            sd.jnl_dir_name = prm.alphas[0];

          else if ("resetlun".startsWith(prm.keyword))
            sd.scsi_lun_reset = (long) prm.numerics[0];

          else if ("resetbus".startsWith(prm.keyword))
            sd.scsi_bus_reset = (long) prm.numerics[0];

          else if ("openflags".startsWith(prm.keyword))
            sd.open_flags = new OpenFlags(prm.alphas, prm.numerics);

          else if ("range".startsWith(prm.keyword))
          {
            sd.lowrange = prm.numerics[0];
            if (prm.getNumCount() > 1)
              sd.highrange = prm.numerics[1];
            else
              common.failure("'range=' parameter must be specified with a "+
                             "beginning and ending range, e.g. 'range=(10,20)'");
            ConcatSds.abortIf("'range=' parameter may not be used.");
          }

          else if (prm.keyword.startsWith("dedup"))
          {
            if (sd.dedup == null)
              sd.dedup = (Dedup) Dedup.dedup_default.clone();
            sd.dedup.parseDedupParms(prm, false);
          }

          else if ("streams".startsWith(prm.keyword))
            common.failure("The 'streams=' parameter no longer is a Storage Definition (SD) "+
                           "parameter, but has become a Workload Definition (WD) parameter. ");
          //{
          //  sd.threads_per_stream = (int) prm.numerics[0];
          //  if (prm.num_count > 1)
          //    sd.stream_size = (long) prm.numerics[1];
          //}

          else
            common.failure("Unknown keyword: " + prm.keyword);

        }

        str = Vdb_scan.parms_get();
        if (str == null)
          return null;
      }

      handleFileCount(Vdbmain.sd_list);

      Dedup.checkSdDedup();
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }


    /* For DV we need an 8-byte SD name: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      sd.sd_name8 = (sd.sd_name + "        ").substring(0,8);
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
    /* For 'default' we only need to pick up the host names: */
    if (this == dflt)
    {
      if (pending_host_parms.size() > 0)
        host_names = (String[]) pending_host_parms.firstElement();
      return;
    }

    /* Must have at least a lun name: */
    if (pending_lun_parms.size() == 0 )
      common.failure("No lun name specified for sd=" + sd_name);

    /* Just a single lun, no host: use the default: */
    if (pending_lun_parms.size() == 1 && pending_host_parms.size() == 0)
    {
      pending_host_parms.add(host_names);
    }

    /* The pending counts must be equal: */
    if (pending_lun_parms.size() != pending_host_parms.size())
      common.failure("'host=' and 'lun=' parameters must be defined in pairs for sd=" + sd_name);

    /* Store the lun name on all hosts that are involved: */
    for (int i = 0; i < pending_lun_parms.size(); i++)
    {
      /* Get host names and translate possible wildcards to a list: */
      String[] hosts_from_parm       = (String[]) pending_host_parms.elementAt(i);
      Vector <String> all_host_names = Host.findSelectedHosts(hosts_from_parm);

      for (String hostname : all_host_names)
      {
        Host host = Host.findHost(hostname);

        /* If we have only a single SD, add this to the host: */
        if (filecount == 0)
          host.addLun(sd_name, (String) pending_lun_parms.elementAt(i));

        else
        {
          /* Add all proper sd names and lun names: */
          /* (The extra SDs will be created in handleFileCount()) */
          for (int k = 0; k < filecount; k++)
          {
            String newname = createCountingName(sd_name, filestart, k);
            String newlun  = createCountingName(pending_lun_parms.get(i), filestart, k);
            host.addLun(newname, newlun);
          }
        }
      }
    }


    /* Clear for the next SD: */
    pending_host_parms.removeAllElements();
    pending_lun_parms.removeAllElements();
  }




  /**
  * Translate SD replay parameters to Replay Groups..
  * If only device numbers were specified create a new SD-level replay group.
  * If any group names were specified, add this SD to that group.
  */
  public static void adjustReplay(Vector <SD_entry> sd_list)
  {
    /* We now have read all SDs, adjust replay information: */
    for (SD_entry sd : sd_list)
    {
      /* If any device numbers, create a new group named 'sd_name': */
      if (sd.possible_replay_numbers != null && sd.possible_replay_numbers.length > 0)
      {
        ReplayGroup rg = new ReplayGroup(sd.sd_name);
        rg.addSD(sd);
        for (int i = 0; i < sd.possible_replay_numbers.length; i++)
        {
          rg.addDevice((long) sd.possible_replay_numbers[i]);

          /* With duplication we now know the duplicate# for this sd: */
          if (ReplayInfo.duplicationNeeded())
          {
            ReplayDevice rdev = ReplayDevice.findExistingDevice((long)sd.possible_replay_numbers[i]);
            sd.duplicate_number = rdev.duplicates_found;
          }

        }
      }

      /* There is a group name, tell the group(s) about the SD: */
      else if (sd.possible_replay_groups.length > 0)
      {
        /* Until I have time, duplication only when spcefying device numbers */
        /* using sd=xxx,replay=(num,num)                                     */
        if (ReplayInfo.duplicationNeeded())
          common.failure("Replay duplication not allowed when using Replay Groups");
        for (int i = 0; i < sd.possible_replay_groups.length; i++)
          ReplayGroup.addSDGroup(sd, sd.possible_replay_groups[i]);
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
  private static void handleFileCount(Vector <SD_entry> sd_list)
  {
    boolean found = false;
    do
    {
      found = false;
      for (int i = 0; i < sd_list.size(); i++)
      {
        SD_entry sd = sd_list.elementAt(i);
        for (int j = 0; j < sd.filecount; j++)
        {
          SD_entry sd2 = (SD_entry) sd.clone();
          found        = true;
          sd2.sd_name  = createCountingName(sd.sd_name, sd.filestart,  j);

          sd2.filecount = 0;
          sd_list.add(sd2);
          sd2.relative_sd_num = sd_list.size();
          max_sd_name = Math.max(max_sd_name, sd2.sd_name.length());
          sd2.lun     = createCountingName(sd.lun, sd.filestart, j);
          common.plog("Use of 'sd=%s,count=(%d,%d)' parameter added sd=%s,lun=%s",
                      sd.sd_name, sd.filestart, sd.filecount,
                      sd2.sd_name, sd2.lun);
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


  /**
   * Translate a given sd or lun name into a new value as a result from the
   * 'count=' parameter.
   */
  private static String createCountingName(String name, int start, int index)
  {
    String ret = null;
    try
    {
      /* If a '*' is included, just replace the '*' with the number: */
      if (name.contains("%"))
        ret = String.format(name, (start + index));

      /* If a '*' is included, just replace the '*' with the number: */
      else if (name.contains("*"))
        ret = common.replace_string(name, "*", "" + (start + index));

      /* otherwise, just add the number at the end: */
      else
        ret = name + (start + index);
    }
    catch (Exception e)
    {
      common.ptod("createCountingName: " + name);
      common.ptod("start:              " + start);
      common.ptod("index:              " + index);
      common.ptod("Exception using 'printf' mask resulting from the use of the 'count=' parameter");
      common.failure(e);
    }

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
      common.ptod("+Lun reset issued for lun=" + lun);
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
      common.ptod("+Bus reset issued for lun=" + lun);
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

  public int getMaxSdXfersize()
  {
    if (max_xfersize == 0)
      common.failure("getMaxSdXfersize(): max_xfersize still zero");
    return max_xfersize;
  }
  public static int getAllSdMaxXfersize()
  {
    return all_max_xfersize;
  }

  public DV_map getDvMap()
  {
    return DV_map.findExistingMap(sd_name);
  }
  public Jnl_entry getJournal()
  {
    if (dv_map == null)
      common.failure("SD=" + sd_name + " does not have a Journal instance.");

    return dv_map.journal;
  }

  public static String[] getSdNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      names.put(sd.sd_name, sd);
    }

    return(String[]) names.keySet().toArray(new String[0]);
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

  /**
   * Until 503 we refused to touch block zero to prevent messing up a vtoc.
   * With the arrival of Dedup and compression that can mess up the results when
   * using smaller tests with large xfersizes. We will no longer follow this rule
   * for anything that is not a raw device. For Unix anything starting with /dev
   * is considered a raw device, For Windows, anything starting with \\.
   */
  public boolean canWeUseBlockZero()
  {
    if (common.get_debug(common.ALLOW_BLOCK0_ACCESS))
      return true;

    if (common.get_debug(common.NO_BLOCK0_ACCESS))
      return false;

    /* For SD concatenation never allow block0 access.                    */
    /* This is merely a 'why bother', since why the heck would we want to */
    /* concatenate 'files', unless of course, just testing.               */
    if (Validate.sdConcatenation())
      return false;

    boolean access_block_zero = false;
    if (common.onWindows() && !lun.startsWith("\\\\"))
      access_block_zero = true;
    if (!common.onWindows() && !lun.startsWith("/dev"))
      access_block_zero = true;
    return access_block_zero;
  }


  /**
   * Xfersizes for Data Validation must be multiples of the shortest, so we keep
   * track of them.
   */
  public void trackSdXfersizes(double[] sizes)
  {
    boolean debug = false;
    if (sizes.length == 3)
    {
      int size     = (int) sizes[1];
      max_xfersize = Math.max(max_xfersize, size);
      trackAllSdXfersizes(size);
      Object ret = xfersizes_map.put(new Integer(size), null);
      if (debug)
      {
        if (ret == null)
        {
          common.ptod("trackXfersizes() %s Added xfersize %d", sd_name, size);
          //common.where(8);
        }
      }

      if ((int) sizes[2] != align)
        common.failure("The 'sd=%s,lun=%s,align=xxx' parameter must be set to the same alignment " +
                       "value used in 'xfersize=(min,max,align)' (%d)",
                       sd_name, lun, (int) sizes[2]);

      return;
    }

    /* Look at all new sizes: */
    for (int i = 0; i < sizes.length; i+=2)
    {
      int size     = (int) sizes[i];
      //if (size == 2048)
      //  common.where(8);
      max_xfersize = Math.max(max_xfersize, size);
      trackAllSdXfersizes(size);
      Object ret = xfersizes_map.put(new Integer(size), null);
      if (debug)
      {
        if (ret == null)
        {
          common.ptod("trackXfersizes() %s Added xfersize %d", sd_name, size);
          //common.where(8);
        }
      }
    }
  }

  public static void trackAllSdXfersizes(int xfer)
  {
    all_max_xfersize = Math.max(all_max_xfersize, xfer);
  }

  /**
   * Determine the lowest xfersize used to serve as the length for each
   * Data Validation key.
   *
   * At this time, this is done for the COMPLETE vdbench execution. It may be that
   * we can do it for each run, but I am not sure.
   */
  public static void calculateKeyBlockSizes()
  {
    for (SD_entry sd : SD_entry.getRealSds(Vdbmain.sd_list))
    {
      //common.ptod("Validate.sdConcatenation(): " + Validate.sdConcatenation());
      //common.ptod("sd.concatenated_sd: " + sd.concatenated_sd);

      //if (Validate.sdConcatenation())
      //{
      //  //if (sd.concatenated_sd && sd.sd_is_referenced)
      //  //  sd.calculateKeyBlockSize();
      //}
      //else if (sd.sd_is_referenced)
        sd.calculateKeyBlockSize();
    }
  }
  private void calculateKeyBlockSize()
  {
    String type = (Validate.isRealValidate()) ? "Data Validation" : "Dedup";

    /* With dedup the dedupunit size must be included in the xfersizes: */
    if (Dedup.isDedup())
      trackSdXfersizes(new double[] { dedup.getDedupUnit()});

    /* Sort the list of xfersizes: */
    Integer[] sizes = (Integer[]) xfersizes_map.keySet().toArray(new Integer[0]);
    Arrays.sort(sizes);

    /* For journal recovery we can run into this: */
    if (sizes.length == 0)
      common.failure("No 'xfersize=' parameters found for sd=" + sd_name +
                     ". Are you sure this SD is used?");


    /* For Dedup without DV we already have the proper size: */
    if (Validate.isValidateForDedup())
    {
      key_block_size = dedup.getDedupUnit();
      return;
    }


    /* All xfersizes must be a  multiple of the first: */
    if (Dedup.isDedup())
      key_block_size = dedup.getDedupUnit();
    else
      key_block_size = sizes[0].intValue();


    for (int i = 0; i < sizes.length; i++)
    {
      int next = sizes[i].intValue();
      if (next % key_block_size != 0)
      {
        common.ptod("During " + type + " all data transfer sizes used for ");
        common.ptod("an SD must be a multiple of the lowest xfersize.");
        common.ptod("(A format run may have added a transfer size of 128k).");
        if (Dedup.isDedup())
          common.ptod("(Dedup may have added dedupunit=%d)", dedup.getDedupUnit());

        for (int j = 0; j < sizes.length; j++)
        {
          next = sizes[j].intValue();
          common.ptod("Xfersize used in parameter file: " + next);
        }

        common.failure("Xfersize error");
      }
    }
  }


  public int getKeyBlockSize()
  {
    if (key_block_size == 0)
      common.failure("getKeyBlockSize(): key_block_size still zero for sd=%s", sd_name);
    return key_block_size;
  }


  /**
   * Split replay= parameters into numeric and alphanumerics.
   */
  private void storeReplayParms(Vdb_scan prm)
  {
    int numerics = 0;

    for (int i = 0; i < prm.alphas.length; i++)
    {
      if (common.isNumeric(prm.alphas[i]))
        numerics++;
    }

    possible_replay_numbers = new double[ numerics ];
    possible_replay_groups  = new String[ prm.alphas.length - numerics ];

    int num = 0;
    int alp = 0;
    for (int i = 0; i < prm.alphas.length; i++)
    {
      if (common.isNumeric(prm.alphas[i]))
        possible_replay_numbers[num++] = Double.parseDouble(prm.alphas[i]);
      else
        possible_replay_groups[alp++ ] = prm.alphas[i];
    }
  }

  public static void main(String[] args)
  {
    int count = 128;
    HashMap map = new HashMap(16);

    map.put("", null);
    map.put("", null);
    for (int i = 2; i < count; i++)
    {
      String sd = "sd" + (count -1 - i);
      map.put(sd, null);
    }

    String[] sds = (String[]) map.keySet().toArray(new String[0]);


    //for (int i = 0; i < sds.length; i++)
    //  common.ptod("sds1: " + sds[i]);

    Arrays.sort(sds, new SdSort());

    for (int i = 0; i < sds.length; i++)
      common.ptod("sds2: " + sds[i]);
  }


  public void setOpenForWrite()
  {
    open_for_write = true;

    /* Pass this on to real SDs if this is a concatenated SD: */
    if (sds_in_concatenation != null)
    {
      for (int i = 0; i < sds_in_concatenation.size(); i++)
        sds_in_concatenation.get(i).setOpenForWrite();
    }
  }
  public boolean isOpenForWrite()
  {
    return open_for_write;
  }


  /**
   * Read/write log for raw i/o functions.
   * This file is recreated at the start of a Vdbench execution.
   */
  public void setupReadWriteLog(SD_entry sd)
  {
    if (!Validate.isRealValidate())
      common.failure("Requesting read/write log while not using Data Validation");

    SimpleDateFormat df_log = new SimpleDateFormat( "MMddyy-HH:mm:ss.SSS zzz" );
    String temp  = System.getProperty("java.io.tmpdir");

    synchronized (open_rwlog_map)
    {
      /* If we already/still have it open, reuse: */
      if ((rw_log = open_rwlog_map.get(sd.sd_name)) != null)
      {
        rw_log.println("* This file was continued at " + df_log.format(new Date()));
        return;
      }

      common.where();
      String fname = new File(temp, sd.sd_name + ".log").getAbsolutePath();
      rw_log = new Fput(fname);
      open_rwlog_map.put(sd.sd_name, rw_log);

      //06172015-09:35:43.948 w     3      2654208     34111488
      rw_log.println("* ");
      rw_log.println("* Column description:");
      rw_log.println("* ");
      rw_log.println("* sd=%s,lun=%s", sd.sd_name, sd.lun);
      rw_log.println("* ");
      rw_log.println("* Following data is per 'key block size', the smallest xfersize during a test.");
      rw_log.println("* ");
      rw_log.println("* Column 1: Timestamp: HH:mm:ss.SSS");
      rw_log.println("* ");
      rw_log.println("* Column 2: Read or write");
      rw_log.println("* ");
      rw_log.println("* Column 3: Data Validation key just read or written");
      rw_log.println("* ");
      rw_log.println("* Column 4: Logical byte address of 'key block'");
      rw_log.println("* ");
      rw_log.println("* ONLY when the file ends with 'Log properly closed' can we be assured this file is complete");
      rw_log.println("* and nothing has been left behind in either java buffers or file system cache.");
      rw_log.println("* There may be multiple occurrences of 'Log properly closed'.");
      rw_log.println("* ");
      rw_log.println("* This file was created at " + df_log.format(new Date()));
      rw_log.println("* ");
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


  /**
   * Extract REAL SDs from the list provided.
   */
  public static SD_entry[] getRealSds(Vector <SD_entry> sd_list)
  {
    HashMap <SD_entry, SD_entry> sd_map = new HashMap(8);
    for (SD_entry sd : sd_list)
    {
      if (!sd.concatenated_sd)
        sd_map.put(sd, sd);
      else
      {
        for (SD_entry sdr : sd.sds_in_concatenation)
          sd_map.put(sdr, sdr);
      }
    }
    SD_entry[] sds = sd_map.keySet().toArray(new SD_entry[0]);
    Arrays.sort(sds, new SdSort());
    return sds;
  }
}


/**
 * Sort Sds by name, allowing correct order for sd1 and sd12.
 * Can use either a String as input, or SD_entry.
 */
class SdSort implements Comparator
{
  private static int bad_sds = 0;
  public int compare(Object o1, Object o2)
  {
    String sd1;
    String sd2;

    /* We can handle both SDs and Strings: */
    if (o1 instanceof SD_entry)
    {
      sd1 = ((SD_entry) o1).sd_name;
      sd2 = ((SD_entry) o2).sd_name;
    }
    else
    {
      sd1 = (String) o1;
      sd2 = (String) o2;
    }

    /* Get everything until we hit a numeric: */
    String char1 = getLetters(sd1);
    String char2 = getLetters(sd2);

    /* If these pieces don't match, just do alpha compare: */
    if (!char1.equalsIgnoreCase(char2))
      return sd1.compareToIgnoreCase(sd2);

    String r1 = null;
    String r2 = null;
    try
    {
      /* Get the remainder (numeric?) portion of both values: */
      r1 = sd1.substring(char1.length());
      r2 = sd2.substring(char2.length());

      /* If the results is not numberic, again do alpha compare: */
      if (!common.isNumeric(r1) || !common.isNumeric(r1))
        return sd1.compareToIgnoreCase(sd2);

      /* Just subtract these numbers and return: */
      int num1 = Integer.parseInt(r1);
      int num2 = Integer.parseInt(r2);
      return num1 - num2;
    }

    /* Any problem, report five of them: */
    catch (Exception e)
    {
      if (bad_sds++ < 5)
      {
        common.ptod("r1: char1: %s char2: %s sd1: %s sd2: %s r1: %s r2: %s",
                    char1, char2, sd1, sd2, r1, r2);
        common.ptod(e);
      }
    }

    /* Anything down here: just do an alpha compare: */
    return sd1.compareToIgnoreCase(sd2);
  }

  private String getLetters(String sd)
  {
    String char1 = "";
    for (int i = 0; i < sd.length(); i++)
    {
      char ch = sd.charAt(i);
      if (!Character.isLetter(ch))
        break;
      char1 += new Character(ch).toString();
    }

    return char1;
  }
}


