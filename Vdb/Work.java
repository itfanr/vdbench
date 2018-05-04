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
import java.io.*;
import Utils.Format;
import Oracle.OracleParms;


/**
 * This class contains information eneded for a slave to do its work
 */
public class Work extends VdbObject implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public transient RD_entry rd;   /* Used only on the master */
  public String   work_rd_name;
  public String   rd_mount;
  public String   slave_label;
  public int      slave_count;
  public int      slave_number;
  public String   host_label;
  public boolean  use_waiter;
  public boolean  tape_testing;
  public Vector   wgs_for_slave;
  public int      sequential_files;
  public int      distribution;
  public OpenFlags rd_open_flags;         /* Only for WD workloads */
  public String   pattern_dir;

  public Vector   instance_pointers;   /* Kstat pointers to collect data from */

  public Vector   fwgs_for_slave;
  public boolean  format_run;
  public FormatFlags format_flags;
  public double   fwd_rate;
  public long     maximum_xfersize;

  public boolean  replay;
  public String   replay_filename;
  public double   replay_adjust;
  public Vector   replay_device_list;

  public Validate validate_options;
  public boolean  journal;

  public boolean  force_fsd_cleanup = false;

  public long     dump_journals_every;

  public OracleParms oparms;
  public Debug_cmds rd_start_command;
  public Debug_cmds rd_end_command;

  public HashMap  threads_for_sd;

  public HashMap  bucket_types;



  /**
   * Since each WG_entry contains an instance of the SD to be used we can
   * extract that data and convert that into an SD list.
   * Since the slave can get multiple workloads (WG_entry) for an SD make sure
   * that you only pick up unique SDs.
   */
  public Vector convertWgListToSdList()
  {
    HashMap map = new HashMap(8);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.elementAt(i);
      SD_entry sd = wg.sd_used;
      map.put(sd.sd_name, sd);

      /* Also copy the proper lun/file name to the sd: */
      sd.lun       = wg.host_lun;

      /* Replay needs to have a WG_entry to be placed in Cmd_entry. */
      /* (This is a dummy WG_entry anyway).                         */
      /* This field is also used for DV, but I think we should find */
      /* an alternative:                                            */
      sd.wg_for_sd = wg;

      //common.ptod("sd1: " + sd.sd_name + " " + sd + " " + wg);
    }

    return new Vector(map.values());
  }


  /**
   * Prepare the Work() instances that need to be sent to each slave.
   *
   * Note: this code is called twice: once to help determine which reports must be
   * created, and once when the real work must be done.
   *
   */
  public static void prepareWorkForSlaves(RD_entry rd, boolean run)
  {
    if (Vdbmain.isWdWorkload())
    {
      HandleSkew.spreadWdSkew(rd);
      HandleSkew.spreadWgSkew(rd);
      HandleSkew.calcLeftoverWgSkew(rd);
      prepareWgWork(rd, run);
    }
    else
      prepareFwgWork(rd, run);
  }



  /**
   * For each slave, take the Workload generator entries (WG_entry) for an RD
   * and store them in the Slave instance as Work() instances.
   */
  private static void prepareWgWork(RD_entry rd, boolean run)
  {
    int count = 0;

    /* Set counter telling us how many slaves use each sd: */
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      sd.setActive(false);
      sd.format_inserted = false;
      HashMap slaves_used = new HashMap(32);

      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);
        if (wg.sd_used == sd)
          slaves_used.put(wg.slave, null);
      }
    }


    /* Calculate interarrival times for each WG: */
    WG_entry.wg_set_interarrival(rd.wgs_for_rd, rd);


    /* Clear 'work' in all slaves: */
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      slave.setCurrentWork(null);
      slave.wgs_for_slave = new Vector(4, 0);
      slave.reads = slave.writes = 0;
    }

    /* Give each slave the WG_entry instances they need: */
    int wgs_found = 0;
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);
        if (wg.slave == slave)
        {
          slave.wgs_for_slave.add(wg);
          wg.wd_used.total_io_done = 0;
          wgs_found++;

          wg.host_lun = unix2Windows(slave.getHost(), wg.host_lun);

          wg.sd_used.setActive(true);
          wg.slave.addSd(wg.sd_used, wg.host_lun);


          if (run)
            common.plog(Format.f("Sending to %-12s ",  wg.slave.getLabel()) +
                        "wd=" + wg.wd_name +
                        ",sd=" + wg.sd_used.sd_name + ",lun=" +
                        slave.getHost().getLunNameForSd(wg.sd_used.sd_name));

          if (rd.rd_name.indexOf(SD_entry.NEW_FILE_FORMAT_NAME) != -1)
            wg.sd_used.format_inserted = true;
        }
      }
    }

    if (rd.wgs_for_rd.size() != wgs_found)
      common.failure("Unmatched WG_entry count: " +
                     rd.wgs_for_rd.size() + "/" + wgs_found);


    /* Now translate all the WG_entry instances for a slave into Work(): */
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);

      /* Clear all flags, even for slaves that are not used: */
      slave.setSequentialDone(true);

      if (slave.wgs_for_slave.size() > 0)
      {
        Work work               = new Work();
        work.slave_label        = slave.getLabel();
        work.host_label         = slave.getHost().getLabel();
        work.rd                 = rd;
        work.work_rd_name       = rd.rd_name;
        work.rd_mount           = rd.rd_mount;
        work.use_waiter         = rd.use_waiter;
        work.pattern_dir        = DV_map.pattern_dir;
        work.threads_for_sd     = rd.getThreadMap();
        work.bucket_types       = BucketRanges.getBucketTypes();
        work.distribution       = rd.distribution;
        work.rd_open_flags      = rd.open_flags;
        work.wgs_for_slave      = slave.wgs_for_slave;
        work.validate_options   = Validate.getOptions();
        work.maximum_xfersize   = SD_entry.getMaxXfersize();
        Validate.setCompression(rd.compression_rate_to_use);
        Validate.setCompSeed(DV_map.compression_seed);
        work.tape_testing       = SD_entry.isTapeTesting();
        work.sequential_files   = WG_entry.sequentials_count(slave.wgs_for_slave);
        work.rd_start_command   = rd.start_cmd;
        work.rd_end_command     = rd.end_cmd;
        work.oparms             = OracleParms.getParms();

        if (Vdbmain.isReplay())
        {
          work.replay             = true;
          work.replay_filename    = RD_entry.replay_filename;
          work.replay_adjust      = ReplayRun.replay_adjust;
          work.replay_device_list = ReplayDevice.getDeviceList();
          //work.sequential_files   = ReplayDevice.countUsedDevices();
          slave.setSequentialDone(false);
        }

        else
          slave.setSequentialDone(work.sequential_files < 0);

        /* Create a list of Kstat instance names that a slave has to return data for: */
        if (slave.equals(slave.getHost().getFirstSlave()) &&
            slave.getHost().getHostInfo().isSolaris())
          work.instance_pointers = slave.getHost().getHostInfo().getInstancePointers();

        slave.setCurrentWork(work);
        AllWork.addWork(work);
        count++;
      }
    }


    if (count == 0)
      common.failure("prepareWgWork(): no work created for any slave");

  }



  /**
   * For each slave, take the Filesystem Workload generator entries
   * (FwgEntry) for an RD and store them in the Slave instance.
   */
  private static void prepareFwgWork(RD_entry rd, boolean run)
  {
    Vector slave_list = SlaveList.getSlaveList();

    /* We must have at least one anchor per slave: */
    //common.where();
    //if (slave_list.size() > FileAnchor.getAnchorList().size())
    //  common.failure("Each host/slave combination must use its own unique anchor. " +
    //                 "Number of slaves: " + slave_list.size() + "; " +
    //                 "Number of anchors: " + FileAnchor.getAnchorList().size());


    /* Create a list of round-robin per host and jvm: */
    int[] round_robin = new int[slave_list.size()];

    /* Clear all work: */
    for (int j = 0; j < slave_list.size(); j++)
    {
      Slave slave = (Slave) slave_list.elementAt(j);
      slave.setCurrentWork(null);
    }

    /* Go through all anchors: */
    Vector anchors = FileAnchor.getAnchorList();
    for (int i = 0; i < anchors.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchors.elementAt(i);


      for (int j = 99990; j < rd.fwgs_for_rd.size(); j++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(j);
        common.ptod("===fwg: " + fwg.getOperation());
      }

      /* Get all the FWG's using this anchor: */
      Vector fwgs_for_anchor = new Vector(8, 0);
      for (int j = 0; j < rd.fwgs_for_rd.size(); j++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(j);
        if (fwg.anchor == anchor)
          fwgs_for_anchor.add(fwg);

        /* Store the latest requested file sizes: */
        fwg.anchor.filesizes = fwg.filesizes;

        /* Make sure that the target anchor gets the same structure as the source: */
        if (fwg.target_anchor != null)
        {
          fwg.target_anchor.depth      = anchor.depth;
          fwg.target_anchor.width      = anchor.width;
          fwg.target_anchor.files      = anchor.files;
          fwg.target_anchor.total_size = anchor.total_size;
          fwg.target_anchor.filesizes  = anchor.filesizes;
        }
      }

      /* Is this anchor even used? */
      if (fwgs_for_anchor.size() == 0)
        continue;

      /* Get names of hosts using this anchor: */
      String host_names[] = getHostsUsingAnchor(fwgs_for_anchor);
      for (int h = 0; h < host_names.length; h++)
      {
        String host_name = host_names[h];

        /* Go find all the requested slaves for this host: */
        Vector slaves = new Vector(8, 0);
        for (int j = 0; j < slave_list.size(); j++)
        {
          Slave slave = (Slave) slave_list.elementAt(j);
          if (slave.getHost().getLabel().equalsIgnoreCase(host_name))
            slaves.add(slave);
        }

        /* Find the slave with the least amount of work: */
        Slave least = (Slave) slaves.firstElement();
        for (int j = 0; j < slaves.size(); j++)
        {
          Slave slave = (Slave) slaves.elementAt(j);
          if (slave.getCurrentFwgWorkSize() < least.getCurrentFwgWorkSize())
            least = slave;
        }

        /* That one gets the new work.                           */
        /* Create a new Work() instance or add them to existing: */
        SlaveList.AddFwgsToSlave(least, fwgs_for_anchor, rd, run);
      }
    }

    /* Change the fwd_rate from TOTAL to 'per slave': */
    SlaveList.adjustSkew();
  }


  private static String[] getHostsUsingAnchor(Vector fwgs_for_anchor)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < fwgs_for_anchor.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs_for_anchor.elementAt(i);
      map.put(fwg.host_name, fwg.host_name);
    }

    return(String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return a list of SDs used for this Work() instance for a slave or for a host.
   */
  public String[] getSdList()
  {
    return(String[]) getSdMap().keySet().toArray(new String[0]);
  }
  public HashMap getSdMap()
  {
    HashMap sds = new HashMap(32);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.elementAt(i);
      sds.put(wg.sd_used.sd_name, null);
    }

    return sds;
  }


  /**
   * Create a HashMap of all SDs used in a Work instance.
   */
  private HashMap getSdMap(Slave slave)
  {
    HashMap sds = new HashMap(32);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.elementAt(i);
      if (wg.slave.getLabel().equals(slave.getLabel()))
        sds.put(wg.sd_used.sd_name, null);
    }

    return sds;
  }

  /**
   * Return an array of sd names used in the current run by all slaves.
   */
  public static String[] getSdsForRun()
  {
    HashMap sds = new HashMap(32);

    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      Work work = slave.getCurrentWork();
      if (work == null)
        continue;
      sds.putAll(work.getSdMap(slave));
    }

    return(String[]) sds.keySet().toArray(new String[0]);
  }


  /**
   * Return an array of sd names used in the current run by a specific host
   */
  public static String[] getSdsForRun(Host host)
  {
    HashMap sds = new HashMap(32);

    for (int i = 0; i < host.getSlaves().size(); i++)
    {
      Slave slave = (Slave) host.getSlaves().elementAt(i);
      Work work = slave.getCurrentWork();
      if (work == null)
        continue;
      sds.putAll(work.getSdMap(slave));
    }

    return(String[]) sds.keySet().toArray(new String[0]);
  }


  /**
   * Translate a possible Unix lun name '/mnt/file' to 'f:/file'
   */
  public static String unix2Windows(Host host, String name)
  {
    String[] parms = MiscParms.unix2windows;

    /* Only when target is windows: */
    if (host != null && !host.onWindows())
      return name;

    /* Parameter used? */
    if (parms == null)
      return name;

    /* Name must START with the requested value: */
    if (!name.startsWith(parms[0]))
      return name;

    String newname = common.replace_string(name, parms[0], parms[1]);
    return newname;
  }
  /**
   * Get the amount of threads that we must use for an SD.
   */
  public int getThreadsUsed(SD_entry sd)
  {
    String key = sd.sd_name + "/" + SlaveJvm.getSlaveLabel();
    Integer count = (Integer) threads_for_sd.get(key);
    if (count == null)
    {
      String[] keys = (String[]) threads_for_sd.keySet().toArray(new String[0]);
      for (int i = 0; i < keys.length; i++)
        common.ptod("keys: " + keys[i]);
      common.failure("Thread count requested for unknown slave: " + key);
    }
    return count.intValue();
  }
}
