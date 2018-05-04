package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import Utils.Format;


/**
 * This class contains information eneded for a slave to do its work
 */
public class Work implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public transient RD_entry rd;   /* Used only on the master */
  public String   work_rd_name;
  public String   rd_mount;
  public String   slave_label;
  public int      slave_count;
  public int      slave_number;

  public String   host_label;
  private HashMap  <String, String> luns_on_host = null;  /* sd_name, lun */

  public boolean  use_waiter;
  public ArrayList  <WG_entry> wgs_for_slave;
  public int      sequential_files;
  public int      distribution;
  public OpenFlags rd_open_flags;         /* Only for WD workloads */

  public Vector   instance_pointers;   /* Kstat pointers to collect data from */

  public Vector   <FwgEntry> fwgs_for_slave;
  public boolean  format_run;
  public FormatFlags format_flags;
  public double   fwd_rate;
  public int      maximum_xfersize;

  public ReplayInfo replay_info;

  public Validate validate_options;
  public Patterns pattern_options;
  public boolean  journal;
  public boolean  only_eof_writes;

  public boolean  force_fsd_cleanup = false;

  public Debug_cmds rd_start_command;
  public Debug_cmds rd_end_command;

  public ArrayList <String[]> miscellaneous;

  public boolean   keep_controlfile = false;


  /* Map containing as key sd_name + '/' + slave label.                     */
  /* Value stored: an ArrayList of integers, one instance per thread.       */
  /* The integer will contain the stream number to be used for this thread. */
  /* See also RD_entry.threads_per_slave_map                                */
  /* (It looks as if the WHOLE map is sent to each slave, and not only the  */
  /* elements needed for the slave. I can live with that).                  */
  private HashMap  <String, ArrayList <StreamContext> > threads_for_slave_map;
  public int      threads_from_rd = 0;

  public HashMap  bucket_types;



  /**
   * Since each WG_entry contains an instance of the SD to be used we can
   * extract that data and convert that into an SD list.
   * Since the slave can get multiple workloads (WG_entry) for an SD make sure
   * that you only pick up unique SDs.
   */
  public Vector convertWgListToSdList()
  {
    HashMap <String, SD_entry> map = new HashMap(8);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.get(i);
      SD_entry sd = wg.sd_used;
      map.put(sd.sd_name, sd);

      /* Also copy the proper lun/file name to the sd: */
      sd.lun = getLunNameForSd(sd);
      if (!sd.concatenated_sd && sd.lun == null)
        common.failure("convertWgListToSdList: null lun");

      if (sd.concatenated_sd)
      {
        for (SD_entry sd2 : sd.sds_in_concatenation)
        {
          sd2.lun = getLunNameForSd(sd2);
          if (sd2.lun == null)
            common.ptod("convertWgListToSdList: null lun");
          //sd2.lun = unix2Windows(slave.getHost(), wg.host_lun);
        }
      }

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
   * Find the proper host specific lun name for an SD.
   * If non is found, use the one coded in the SD_entry itself.
   */
  public String getLunNameForSd(SD_entry sd)
  {
    String lun = luns_on_host.get(sd.sd_name);

    if (lun == null)
    {
      if (!sd.concatenated_sd && sd.lun == null)
        common.failure("getLunNameForSd: null sd.lun");
      return sd.lun;
    }
    else
      return lun;
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
      if (run)
      {
        rd.spreadStreamsAcrossSlaves(true);
        //reportStuff(rd);
      }
    }
    else
      prepareFwgWork(rd, run);
  }

  private static void reportStuff(RD_entry rd)
  {
    long total_size    = 0;
    long total_threads = 0;
    long key_blocks    = 0;
    long buffers       = 0;

    /* Collect all unique SDs used: */
    HashMap <SD_entry, Long> sd_map = new HashMap(8);
    for (Slave slave : SlaveList.getSlaveList())
    {
      Work work = slave.getCurrentWork();


      for (SD_entry sd : work.getSdsForRun())
      {
        long max_xfer = sd.getMaxSdXfersize();
        if (Validate.sdConcatenation() && work.threads_from_rd != 0)
          max_xfer = work.maximum_xfersize;
        sd_map.put(sd, max_xfer);
      }
    }

    /* Add up all the sizes: */
    for (SD_entry sd : sd_map.keySet())
    {
      long xfersize = sd_map.get(sd);
      total_size    += sd.end_lba;
      key_blocks    += (sd.end_lba / xfersize);
    }

    /* And now add up all the threads: */
    for (Slave slave : SlaveList.getSlaveList())
    {
      for (SD_entry sd : slave.getCurrentWork().getSdsForRun())
      {
        int threads = rd.getSdThreadsUsedForSlave(sd.sd_name, slave);
        total_threads += threads;
        buffers       += sd_map.get(sd) * threads * 2;
      }
    }

    common.ptod("total_size:    %,12d ", total_size);
    common.ptod("total_threads: %,12d ", total_threads);
    common.ptod("key_blocks:    %,12d ", key_blocks);
    common.ptod("buffers:       %,12d ", buffers);

    // these are JAVA
    long bytes_for_bits = key_blocks / 8;

    if (!Validate.isValidate())
      common.ptod("Estimated non-java memory needs for rd=%s: %,d bytes or %.3f mb",
                  rd.rd_name, buffers, buffers / 1048576.);
    else
      common.ptod("Estimated non-java memory needs for rd=%s: %,d bytes or %.3f mb",
                  rd.rd_name, (key_blocks + buffers), (key_blocks + buffers / 1048576.));
  }

  /**
   * For each slave, take the Workload generator entries (WG_entry) for an RD
   * and store them in the Slave instance as Work() instances.
   */
  private static void prepareWgWork(RD_entry rd, boolean run)
  {
    int     work_count   = 0;
    boolean have_streams = false;
    boolean have_concat  = false;
    HashMap used_sd_map = new HashMap(16);

    /* Set counter telling us how many slaves use each sd: */
    SD_entry.clearAllActive();
    for (SD_entry sd : Vdbmain.sd_list)
    {
      /* While we're here, initialize some stuff: */
      sd.format_inserted  = false;
      sd.pure_rand_seq    = false;
      HashMap slaves_used = new HashMap(32);

      for (WG_entry wg : Host.getAllWorkloads())
      {
        if (wg.sd_used == sd)
        {
          slaves_used.put(wg.getSlave(), null);

          /* Keep track of whether an SD is used for mixed random/seq: */
          if (wg.seekpct == 100 || wg.seekpct <= 0)
            sd.pure_rand_seq = true;

          if (sd.concatenated_sd)
            have_concat = true;

          if (rd.rd_stream_count != 0)
            have_streams = true;
        }
      }
    }

    if (have_streams && have_concat && rd.current_override.getThreads() != For_loop.NOVALUE)
    {
      if (!run)
      {
        common.ptod("Shared threads requested with SD concatenation SHARES all threads.");
        common.ptod("The 'streams=' parameter DEDICATES threads.");
        common.ptod("To resolve this contradiction threads will not be shared "+
                    "but will be dedicated to each stream.");
      }
    }


    /* Calculate interarrival times for each WG: */
    WG_entry.wg_set_interarrival(Host.getAllWorkloads(), rd);


    /* Clear 'work' in all slaves: */
    for (Slave slave : SlaveList.getSlaveList())
    {
      slave.setCurrentWork(null);
      //slave.clearWorkloads();
      slave.reads = slave.writes = 0;
    }

    /* Give each slave the WG_entry instances they need: */
    int wgs_found = 0;
    for (Slave slave : SlaveList.getSlaveList())
    {
      for (WG_entry wg : Host.getAllWorkloads())
      {
        if (wg.getSlave() == slave)
        {
          //slave.addWorkload(wg, rd);
          wg.wd_used.total_io_done = 0;
          wgs_found++;

          /* ConcatMarkers may have changed lun name, pick up proper names: */
          if (wg.sd_used.concatenated_sd)
          {
            for (SD_entry sd2 : wg.sd_used.sds_in_concatenation)
            {
              sd2.lun = wg.getSlave().getHost().getLunNameForSd(sd2);

              /* Also keep track of which SD names we used: */
              used_sd_map.put(sd2, null);
            }
          }
          else
          {
            wg.sd_used.setActive();
            used_sd_map.put(wg.sd_used, null);
          }
          //wg.getSlave().addName(wg.sd_used.sd_name);


          //if (run)
          //  common.plog(Format.f("Sending to %-12s ",  wg.slave.getLabel()) +
          //              "wd=" + wg.wd_name +
          //              ",sd=" + wg.sd_used.sd_name + ",lun=" +
          //              slave.getHost().getLunNameForSd(wg.sd_used.sd_name));

          if (rd.rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
            wg.sd_used.format_inserted = true;
        }
      }
    }


    if (Dedup.isDedup() && used_sd_map.size() != SD_entry.getRealSds(Vdbmain.sd_list).length)
      common.failure("rd=%s: All SDs must be used when dedup is active. %d/%d",
                     rd.rd_name, used_sd_map.size(), Vdbmain.sd_list.size());

    if (Host.getAllWorkloads().size() != wgs_found)
      common.failure("Unmatched WG_entry count: " +
                     Host.getAllWorkloads().size() + "/" + wgs_found);


    /* Now translate all the WG_entry instances for a slave into Work(): */
    boolean threads_reported = false;
    for (Slave slave : SlaveList.getSlaveList())
    {
      /* Clear all flags, even for slaves that are not used: */
      slave.setSequentialDone(true);

      if (slave.getWorkloads().size() > 0)
      {
        Work work               = new Work();
        work.slave_label        = slave.getLabel();
        work.host_label         = slave.getHost().getLabel();
        work.luns_on_host       = slave.getHost().getHostLunMap();
        work.rd                 = rd;
        work.work_rd_name       = rd.rd_name;
        work.rd_mount           = rd.rd_mount;
        work.use_waiter         = rd.use_waiter;

        work.threads_for_slave_map = rd.getThreadsPerSlaveMap();
        work.threads_from_rd       = 0;


        /* Concatenation without streams: equally divvy up the RD requested shared threads: */
        if (have_concat && !have_streams &&
            rd.current_override.getThreads() != For_loop.NOVALUE)
        {
          work.threads_from_rd = (int) rd.current_override.getThreads() /
                                 SlaveList.getSlaveList().size();
          if (!threads_reported && run)
          {
            threads_reported = true;
            common.ptod("SD Concatenation: rd=%s,threads=%d: each slave gets %d threads",
                        rd.rd_name, (int) rd.current_override.getThreads(),
                        work.threads_from_rd);
          }
        }

        work.bucket_types       = BucketRanges.getBucketTypes();
        work.distribution       = rd.distribution;
        work.rd_open_flags      = rd.open_flags;
        work.wgs_for_slave      = slave.getWorkloads();
        work.validate_options   = Validate.getOptions();
        work.pattern_options    = Patterns.getOptions();
        Validate.setCompressionRatio(rd.compression_ratio_to_use);
        work.maximum_xfersize   = SD_entry.getAllSdMaxXfersize();
        Validate.setCompSeed(DV_map.compression_seed);

        //if (slave.seq_files_on_slave == 0)
        work.sequential_files = slave.sequentialFilesOnSlave();
        //else
        //  work.sequential_files = slave.seq_files_on_slave;

        work.only_eof_writes    = rd.checkForSequentialWritesOnly();
        work.rd_start_command   = rd.start_cmd;
        work.rd_end_command     = rd.end_cmd;
        work.miscellaneous      = MiscParms.getMiscellaneous();

        work.replay_info = ReplayInfo.getInfo();
        if (ReplayInfo.isReplay())
          slave.setSequentialDone(false);
        else
          slave.setSequentialDone(work.sequential_files < 0);

        /* Create a list of Kstat instance names that a slave has to return data for: */
        if (slave.equals(slave.getHost().getFirstSlave()) &&
            slave.getHost().getHostInfo().isSolaris())
          work.instance_pointers = slave.getHost().getHostInfo().getInstancePointers();

        slave.setCurrentWork(work);
        work_count++;
      }
    }


    if (work_count == 0)
      common.failure("prepareWgWork(): no work created for any slave");


    if (have_streams && rd.iorate_req != RD_entry.MAX_RATE)
      common.failure("Use of the streams= parameter requires iorate=max");
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


    /* Go through all anchors looking for abuse: */
    Vector anchors = FileAnchor.getAnchorList();
    HashMap <String, String> anchor_name_fsd_map = new HashMap(32);
    for (FwgEntry fwg : rd.fwgs_for_rd)
    {
      String fsdname = anchor_name_fsd_map.get(fwg.anchor.getAnchorName());
      if (fsdname == null)
        anchor_name_fsd_map.put(fwg.anchor.getAnchorName(), fwg.fsd_name);
      else if (!fsdname.equals(fwg.fsd_name))
      {
        common.failure("Concurrent use of anchor=%s using multiple FSDs (%s/%s) in one RD is not allowed.",
                       fwg.anchor.getAnchorName(), fsdname, fwg.fsd_name);
      }
    }


    /* Go through all anchors: */
    anchors = FileAnchor.getAnchorList();
    HashMap used_anchor_map = new HashMap(32);
    for (int i = 0; i < anchors.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchors.elementAt(i);

      for (int j = 99990; j < rd.fwgs_for_rd.size(); j++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(j);
        common.ptod("===fwg: " + fwg.getName() + " " + fwg.fsd_name + " " + fwg.anchor.getAnchorName());
      }

      /* Get all the FWG's using this anchor: */
      Vector <FwgEntry> fwgs_for_anchor = new Vector(8, 0);
      for (int j = 0; j < rd.fwgs_for_rd.size(); j++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(j);
        used_anchor_map.put(fwg.anchor, null);

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
        Vector <Slave> slaves = new Vector(8, 0);
        for (int j = 0; j < slave_list.size(); j++)
        {
          Slave slave = (Slave) slave_list.elementAt(j);
          if (slave.getHost().getLabel().equalsIgnoreCase(host_name))
            slaves.add(slave);
        }

        /* Find the slave with the least amount of work: */
        Slave slave_to_use = (Slave) slaves.firstElement();
        for (int j = 0; j < slaves.size(); j++)
        {
          Slave slave = (Slave) slaves.elementAt(j);
          if (slave.getCurrentFwgWorkSize() < slave_to_use.getCurrentFwgWorkSize())
            slave_to_use = slave;
        }

        /* We now have the slave we think we want, but DV requires us */
        /* to keep the FSD on the same slave:                         */
        if (Validate.isRealValidate())
        {
          for (Slave slave : slaves)
          {
            for (String anchor_name : slave.getNamesUsedList())
            {
              if (anchor.getAnchorName().equals(anchor_name))
                slave_to_use = slave;
            }
          }
        }

        /* Remember which anchors go to this slave: */
        for (FwgEntry fwg : fwgs_for_anchor)
          slave_to_use.addName(fwg.anchor.getAnchorName());

        /* That one gets the new work.                           */
        /* Create a new Work() instance or add them to existing: */
        SlaveList.AddFwgsToSlave(slave_to_use, fwgs_for_anchor, rd, run);
      }
    }

    /* Change the fwd_rate from TOTAL to 'per slave': */
    SlaveList.adjustSkew();

    if (Dedup.isDedup() && used_anchor_map.size() != anchors.size())
      common.failure("All anchors must be used when dedup is active.");

    // FwgRun.calcSkew(fwgs_for_rd);
    if (Dedup.isDedup())
      Dedup.adjustFsdDedupValues(rd.fwgs_for_rd);
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
  public SD_entry[] getSdList()
  {
    return(SD_entry[]) getSdMap().values().toArray(new SD_entry[0]);
  }

  private HashMap <String, SD_entry> getSdMap()
  {
    HashMap <String, SD_entry> map = new HashMap(32);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.get(i);
      map.putAll(wg.getRealSdMap());
    }

    return map;
  }


  /**
   * Create a HashMap of all SDs used in a Work instance.
   */
  private HashMap <String, SD_entry> getSlaveSdMap(Slave slave)
  {
    HashMap <String, SD_entry> map = new HashMap(32);

    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.get(i);
      if (wg.getSlave().getLabel().equals(slave.getLabel()))
        map.putAll(wg.getRealSdMap());
    }

    return map;
  }

  /**
   * Return an array of sd names used in the current run by all slaves.
   */
  public static SD_entry[] getSdsForRun()
  {
    HashMap <String, SD_entry> sds = new HashMap(32);

    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      Work work = slave.getCurrentWork();
      if (work == null)
        continue;
      sds.putAll(work.getSlaveSdMap(slave));
    }

    SD_entry[] ret = (SD_entry[]) sds.values().toArray(new SD_entry[0]);
    //for (int i = 0; i < ret.length; i++)
    //  common.ptod("getSdsForRun: " + ret[i].sd_name);

    return ret;
  }


  /**
   * Return an array of sd names used in the current run by a specific host
   */
  public static SD_entry[] getHostSdsForRun(Host host)
  {
    HashMap sds = new HashMap(32);

    for (int i = 0; i < host.getSlaves().size(); i++)
    {
      Slave slave = (Slave) host.getSlaves().elementAt(i);
      Work work = slave.getCurrentWork();
      if (work == null)
        continue;

      sds.putAll(work.getSlaveSdMap(slave));
    }

    return(SD_entry[]) sds.values().toArray(new SD_entry[0]);
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
   * Get the amount of threads that we must use for an SD or WD.
   */
  public int getThreadsForSlave(String sd_name)
  {
    String key     = sd_name + "/" + SlaveJvm.getSlaveLabel();
    ArrayList list = threads_for_slave_map.get(key);
    if (list == null)
    {
      common.ptod("Requesting unknown thread count for '%s'", key);
      return 0;
    }

    return list.size();
  }


  /**
   * Get the stream context for an SD and it's relative IO_task() thread.
   * The stream context will be 'null' for those runs that do not use streams.
   */
  public StreamContext getStreamForSlave(String sd_name, int rel_thread)
  {
    String key = sd_name + "/" + SlaveJvm.getSlaveLabel();
    ArrayList <StreamContext> list = threads_for_slave_map.get(key);
    if (list == null)
      common.failure("Requesting unknown thread count for '%'", key);

    return list.get(rel_thread);
  }
}
