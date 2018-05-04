package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.*;




/**
 * This class communicates between master and slaves to get information
 * like:
 * - does lun/file exist
 * - lun/file size
 * - Kstat info
 * - etc
 */
public class InfoFromHost implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public  String  host_label;
  public  ArrayList  <LunInfoFromHost> luns_on_host = new ArrayList(8);
  private Vector  instance_pointers;

  private boolean windows_host = false;
  private boolean solaris_host = false;
  private boolean linux_host   = false;
  private boolean aix_host     = false;
  private boolean hp_host      = false;
  private boolean mac_host     = false;

  private Dedup      dedup_options      = null;
  private ReplayInfo replay_info     = null;

  private Kstat_cpu kstat_cpu = null;

  private boolean error_with_kstat   = false;

  private NfsV3 nfs3_delta = null;
  private NfsV4 nfs4_delta = null;

  private boolean fwd_workload;
  private boolean create_anchors;

  public  boolean validate = false;

  public  Mount   host_mount = null;

  private static  Semaphore  wait_for_host_info;

  /* HashMap per hosts that received work  */
  private static  HashMap <String, HashMap <String, LunInfoFromHost>> hosts_with_work;

  private static  Vector <LunInfoFromHost> all_returned_luns;  /* All Luns that were returned */
  private static  Vector  all_returned_hosts; /* Everything that was returned */
  private static  boolean any_kstat_errors     = false;
  private static  boolean configuration_loaded = false;

  private static HashMap sd_formats_done = new HashMap(8);

  public  static String NO_DISMOUNT_FILE = "no_dismount.txt";

  public InfoFromHost(String h)
  {
    host_label = h;
  }


  /**
   * Ask the first slave on each host for extra information about
   * luns/files.
   *
   * We'll wait for all the responses to come back.
   */
  public static void askHostsForStuff()
  {
    Status.printStatus("Query host configuration started", null);

    hosts_with_work = new HashMap(4);
    Vector list     = null;
    HashMap <String, LunInfoFromHost> luninfo_for_host = null;

    /* Do this before you send out the first message. Response can come too fast: */
    all_returned_luns  = new Vector(16, 0);
    all_returned_hosts = new Vector(16, 0);
    wait_for_host_info = new Semaphore(0);

    /* Create a list of host names and lun names (HashMap of HashMap): */
    RD_entry.next_rd = null;
    while (true)
    {
      RD_entry rd = RD_entry.getNextWorkload();
      if (rd == null)
        break;


      /* Store lun names info in a HashMap per host: */
      for (int i = 0; Vdbmain.isWdWorkload() && i < rd.wgs_for_rd.size(); i++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.get(i);

        /* Create a HashMap for this host if needed: */
        String host_label  = wg.getSlave().getHost().getLabel();
        if ((luninfo_for_host = hosts_with_work.get(host_label)) == null)
          hosts_with_work.put(host_label, new HashMap(4));
        luninfo_for_host = hosts_with_work.get(host_label);


        // 08/27/15:
        // Interesting: I asked for the lun name found in the SD_entry, and not
        // for the lun specifically for this host, which CAN be different!

        /* Create a LunInfoFromHost instance for the lun name for this host: */
        SD_entry[] real_sds = wg.getRealSds();
        for (SD_entry real_sd : real_sds)
        {
          //common.ptod("wg.getSlave().getHost(): " + wg.getSlave().getHost().getLabel());
          ////common.ptod("real_sd.lun: " + real_sd.lun);
          //common.ptod("real_sd.name: " + real_sd.sd_name);
          String lun_on_host = wg.getSlave().getHost().getLunNameForSd(real_sd);
          createLunInfoFromHost(luninfo_for_host,
                                wg.getSlave().getHost(),
                                lun_on_host,
                                real_sd.isOpenForWrite(),
                                real_sd.end_lba);
        }
      }

      /* Lun count on all sides must match: */
      if (Validate.sdConcatenation())
        ConcatMarkers.checkLunCounts(hosts_with_work);


      /* Store anchor names into a HashMap per host: */
      /* (WG and FWG are mutially exclusive so we never will do both) */
      for (int i = 0; Vdbmain.isFwdWorkload() && i < rd.fwgs_for_rd.size(); i++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(i);

        if (fwg.host_name.equals("*"))
          common.plog("fwg.host_name: " + fwg.host_name + " " +
                      fwg.fsd_name + " " + fwg.getName() + " " +
                      RD_entry.next_rd.rd_name);

        if ((luninfo_for_host = (HashMap) hosts_with_work.get(fwg.host_name)) == null)
          hosts_with_work.put(fwg.host_name, new HashMap(4));

        luninfo_for_host = (HashMap) hosts_with_work.get(fwg.host_name);
        LunInfoFromHost luninfo = new LunInfoFromHost();
        luninfo.lun = fwg.anchor.getAnchorName();
        luninfo.lun = Work.unix2Windows(Host.findHost(fwg.host_name), luninfo.lun);
        luninfo_for_host.put(luninfo.lun, luninfo);
      }
    }

    /* Send a message to the first slave on each host (sorting done for reporting purposes): */
    String[] host_names = hosts_with_work.keySet().toArray(new String[0]);
    Arrays.sort(host_names);
    for (int i = 0; i < host_names.length; i++)
    {
      String host_name       = host_names[i];
      InfoFromHost host_info = new InfoFromHost(host_name);
      host_info.validate     = Validate.isValidate();

      /* Pass mount information if present: */
      host_info.host_mount = Host.findHost(host_name).host_mount;

      /* Give all lun names to this host: */
      HashMap <String, LunInfoFromHost> infos = hosts_with_work.get(host_name);
      LunInfoFromHost[] luninfos = infos.values().toArray(new LunInfoFromHost[0]);
      for (int j = 0; j < luninfos.length; j++)
      {
        LunInfoFromHost luninfo = luninfos[j];
        host_info.luns_on_host.add(luninfo);

        if (Validate.sdConcatenation())
          common.plog("ConcatMarkers: Requesting LunInfoFromHost from host=%s,lun=%s",
                      host_info.host_label,luninfo.lun);

        /* Asking for replay information only once */
        //if (ReplayInfo.isReplay() && j == 0)
        if ( i == 0 && j == 0)
          host_info.replay_info = ReplayInfo.getInfo();
      }

      /* Store this entry. It will be replaced once it is returned from the host */
      //common.ptod("host_name: " + host_name);
      Host.findHost(host_name).setHostInfo(host_info);

      /* Slave must know what type of workload this is: */
      host_info.fwd_workload   = Vdbmain.isFwdWorkload();
      host_info.create_anchors = MiscParms.create_anchors;

      Slave slave = Host.findHost(host_name).getFirstSlave();

      SocketMessage sm = new SocketMessage(SocketMessage.GET_LUN_INFO_FROM_SLAVE);
      sm.setData(host_info);
      slave.getSocket().putMessage(sm);
    }


    /* Make sure all hosts are used. It would be cleaner to do it earlier in */
    /* the process, before we start any slaves, but this is easier:          */
    // Removed. If a user wants to define too many hosts it is his choice.
    // What if he wants to run just one RD of a larger parameter file?
    for (int i = 99990; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      if (host.getHostInfo() == null)
        common.failure("Host=" + host.getLabel() + " is defined but not used. Please remove.");
    }

    /* Wait for response */
    if (host_names.length > 0)
      waitForAllHosts();

    Status.printStatus("Query host configuration completed", null);
  }


  /**
   * Create new LunInfoFromHost() instance for a lun on a host, or combine an
   * other workload request with an already existing instance.
   */
  private static void createLunInfoFromHost(HashMap <String, LunInfoFromHost> host_map,
                                            Host    host,
                                            String  host_lun,
                                            boolean open_for_write,
                                            long    end_lba)
  {
    LunInfoFromHost luninfo = host_map.get(host_lun);
    if (luninfo == null)
    {
      luninfo               = new LunInfoFromHost();
      luninfo.lun           = Work.unix2Windows(host, host_lun);
      luninfo.original_lun  = host_lun;
      luninfo.host_name     = host.getLabel();
      luninfo.marker_needed = Validate.sdConcatenation();
      luninfo.end_lba       = end_lba;
      host_map.put(host_lun, luninfo);

      if (luninfo.lun == null)
        common.failure("'null' LunInfoFromHost.lun");
    }

    if (!luninfo.open_for_write && open_for_write)
      luninfo.open_for_write = true;

    //if (isv.sd_instance == null)
    //  isv.sd_instance = wg.sd_used.instance;
  }


  /**
   * All hosts must have returned proper status before we can start using it
   */
  private static void waitForAllHosts()
  {
    /* Wait for response */
    Signal signal = new Signal(30);

    try
    {
      while (!wait_for_host_info.attempt(1000))
      {
        if (signal.go())
        {
          String[] outstanding = (String[]) hosts_with_work.keySet().toArray(new String[0]);
          for (int i = 0; i < outstanding.length; i++)
            common.ptod("Waiting for configuration information from slave: " + outstanding[i]);
        }
      }
    }

    catch (InterruptedException e)
    {
    }
  }


  /**
   * Answer some simple questions about the host and about luns and files
   */
  public static void getInfoForMaster(InfoFromHost host_info)
  {
    long starttm = System.currentTimeMillis();

    Vector devices_this_host = new Vector(8, 0);

    host_info.windows_host = common.onWindows();
    host_info.solaris_host = common.onSolaris();
    host_info.linux_host   = common.onLinux();
    host_info.aix_host     = common.onAix();
    host_info.hp_host      = common.onHp();
    host_info.mac_host     = common.onMac();

    if (!host_info.windows_host &&
        !host_info.solaris_host &&
        !host_info.linux_host   &&
        !host_info.aix_host     &&
        !host_info.mac_host     &&
        !host_info.hp_host      )
    {
      common.ptod("Vdbench is supported on Windows, Solaris, Linux, AIX, OSX, and HP/UX");
      common.failure("Vdbench is running on an unknown platform: " + System.getProperty("os.name"));
    }

    /* Returning 'null' here means 'no cpu': */
    if (host_info.windows_host || host_info.solaris_host || host_info.linux_host)
    {
      if (CpuStats.getNativeCpuStats() != null)
        host_info.kstat_cpu = CpuStats.getDelta();
    }

    /* Access the file structure to honor auto-mount if needed: */
    checkAutoMount(host_info);

    /* Do some mount stuff if needed: */
    SlaveJvm.setMount(host_info.host_mount);
    if (host_info.host_mount != null)
      host_info.host_mount.mountIfNeeded();

    /* Setup device query from Kstat: */
    if (common.onSolaris() && !common.get_debug(common.NO_KSTAT))
      Native.openKstat();

    /* Get information for each requested lun: */
    for (int i = 0; i < host_info.luns_on_host.size(); i++)
    {
      LunInfoFromHost info = (LunInfoFromHost) host_info.luns_on_host.get(i);

      if (!common.onWindows() && info.lun.indexOf("\\") != -1)
      {
        common.failure("lun=" + info.lun + "; Lun requested on a non-windows " +
                       "system contains a backslash. Are you sure you wanted to " +
                       " send this to a non-windows system? Please correct");
      }

      /* For raw devices we must do it differently: */
      if (info.lun.toLowerCase().startsWith("\\\\.\\") || info.lun.startsWith("/dev/"))
        info.getRawInfo();

      else
      {
        /* Create anchor directory if needed: */
        host_info.maybeCreateDirectory(info.lun);

        /* And get file size etc: */
        info.getFileInfo();
      }

      if (info.marker_needed)
        ConcatMarkers.readMarker(info);

      /* Pick up Kstat information: */
      // Bug from Richard and Maureen: we should give up completely
      // if one Kstat is not found.
      if (common.onSolaris() && !common.get_debug(common.NO_KSTAT))
      {
        /* Pick up nfsstat data to tell master which versions we have: */
        NfsStats.getAllNfsDeltasFromKstat();
        host_info.setNfsData(NfsStats.getNfs3(), NfsStats.getNfs4());

        /* The instance name has been coded in the SD: */
        if (info.sd_instance != null)
        {
          Devxlate devx      = new Devxlate(info.lun);
          devx.instance      = info.sd_instance;
          devx.kstat_pointer = Native.getKstatPointer(devx.instance);
          devices_this_host.add(devx);

          continue;
        }

        /* Get all needed device information from Solaris: */
        if (!configuration_loaded)
        {
          Devxlate.getDeviceLookupData();
          configuration_loaded = true;
        }

        /* Get a list of Kstat instances for this file/lun: */
        Vector devxlate_list;
        if (host_info.fwd_workload)
          devxlate_list = Devxlate.get_device_info(info.lun + "/tmp");
        else
          devxlate_list = Devxlate.get_device_info(Kstat_data.translateSoftLink(info.lun));

        /* The instance list may contain error messages: */
        for (int j = 0; j < devxlate_list.size(); j++)
        {
          Object obj = devxlate_list.elementAt(j);
          if (obj instanceof String)
            info.kstat_error_messages.add(obj);
          else if (obj == null)
            info.kstat_error_messages.add("null entry found in devxlate_list");

          else
          {
            Devxlate devx = (Devxlate) obj;
            devx.kstat_pointer = Native.getKstatPointer(devx.instance);
            devices_this_host.add(devx);
          }
        }
      }
    }

    /* Gather replay data if needed: */
    ReplayInfo.setInfo(host_info.replay_info);
    if (ReplayInfo.getInfo() != null && ReplayInfo.isReplay())
    {
      ReplaySplit.readAndSplitTraceFile();

      /* Send the replay info back with the info that the master needs: */
      host_info.replay_info = ReplayInfo.getInfo();
    }

    /* Notify master if there were any Kstat errors: */
    host_info.error_with_kstat = SlaveJvm.getKstatError();

    /* Translate Devxlate entries to a Vector of InstancePointer(): */
    host_info.instance_pointers = InstancePointer.createInstancePointers(devices_this_host);

    /* Send the response to the master: */
    SlaveJvm.sendMessageToMaster(SocketMessage.GET_LUN_INFO_FROM_SLAVE, host_info);

    double elapsed = (System.currentTimeMillis() - starttm) / 1000.0;
    common.ptod("Configuration interpretation took %.2f seconds", elapsed);
  }


  /**
   * We received an answer from the slave. Store it for now, but keep
   * track of which slaves have returned data.
   */
  public void receiveInfoFromHost()
  {
    synchronized (all_returned_luns)
    {
      /* Find the host in the list to mark him 'complete': */
      if (hosts_with_work.get(host_label) == null)
        common.failure("Receiving info from unknown host: " + host_label);
      hosts_with_work.remove(host_label);

      /* Save the response: */
      all_returned_luns.addAll(luns_on_host);
      all_returned_hosts.add(this);

      /* Any Kstat problems? */
      if (error_with_kstat)
      {
        any_kstat_errors = true;
        common.ptod("Host=" + host_label + ": Unable to find proper Kstat information for one or more luns or files");
        common.ptod("Please look at the logfile of host=" + host_label + " for more information");
      }

      /* Solaris gets a copy of /var/adm/messages: */
      if (solaris_host || linux_host)
        Host.findHost(host_label).createAdmMessagesFile();

      /* This is for replay: */
      possibleReplayInfo();


      /* If we have an answer from all the hosts: */
      if (hosts_with_work.isEmpty())
      {
        /* Look at Kstat data returned from the hosts: */
        checkDevxlateEntries();

        checkForKstatErrors();

        /* Store it with each SD: */
        matchDataWithSds();

        /* Make sure data matches across hosts: */
        storeAndCompare();

        /* Make sure that across hosts things also match: */
        if (Validate.sdConcatenation())
          ConcatMarkers.verifyMarkerResults(all_returned_hosts);

        /* Take the info received and put that in concatenated SDs if needed: */
        ConcatSds.calculateSize();

        /* Since this is running async from main(), notify main: */
        wait_for_host_info.release();

        /* Give heartbeat monitor a decent time again: */
        //HeartBeat.setLate();

      }
    }
  }

  /**
   * Look for Kstat errors and turn off the '-k' execution option if there
   * are ANY failures.
   */
  private static void checkForKstatErrors()
  {
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      InfoFromHost hostinfo = (InfoFromHost) host.getHostInfo();

      for (int j = 0; hostinfo != null && j < hostinfo.luns_on_host.size(); j++)
      {
        LunInfoFromHost info = (LunInfoFromHost) hostinfo.luns_on_host.get(j);
        if (info.kstat_error_messages.size() != 0)
        {
          if (Vdbmain.kstat_console)
          {
            Vdbmain.kstat_console = false;
            common.ptod("");
            common.ptod("There were probems collecting Kstat information on at " +
                        "least one host (host=" + info.host_name + ").");
            common.ptod("Execution option '-k' has been deactivated");
            common.ptod("");
          }
        }
      }
    }
  }


  /**
   * Only when all hosts are either Solaris or Windows can we report cpu usage
   */
  public static void checkCpuReporting()
  {
    int windows = 0;
    int linux   = 0;
    int solaris = 0;
    int aix     = 0;
    int hp      = 0;
    int mac     = 0;
    boolean missing = false;

    for (int i = 0; i < all_returned_hosts.size(); i++)
    {
      InfoFromHost host_info = (InfoFromHost) all_returned_hosts.elementAt(i);
      if (host_info.windows_host) windows ++;
      if (host_info.solaris_host) solaris ++;
      if (host_info.linux_host)   linux   ++;
      if (host_info.aix_host)     aix     ++;
      if (host_info.hp_host)      hp      ++;
      if (host_info.mac_host)     mac     ++;

      if (host_info.windows_host && host_info.kstat_cpu == null)
        missing = true;
      if (host_info.solaris_host && host_info.kstat_cpu == null)
        missing = true;
      if (host_info.linux_host   && host_info.kstat_cpu == null)
        missing = true;
      if (host_info.mac_host     && host_info.kstat_cpu == null)
        missing = true;
    }

    int total = windows + linux + solaris + aix + hp;

    //if (total != all_returned_luns.size())
    //  common.failure("Unmatched host count: " + all_returned_luns.size() + "/" + total);

    /* Set the status for cpu reporting: */
    if (solaris + windows + linux == total && !missing)
      CpuStats.setCpuReporting();

    else
      common.ptod("Not every host is reporting Cpu statistics. Cpu reporting disabled");
  }



  private static void checkDevxlateEntries()
  {
    for (int i = 0; i < all_returned_hosts.size(); i++)
    {
      InfoFromHost host_info = (InfoFromHost) all_returned_hosts.elementAt(i);
      Host.findHost(host_info.host_label).setHostInfo(host_info);

      //
      //for (int j = 0; j < host_info.devxlate_entries.size(); j++)
      //{
      //  Devxlate devx = (Devxlate) host_info.devxlate_entries.elementAt(j);
      //  common.ptod("devx: " + devx);

      //if (devx.instance_long.startsWith("nfs"))
      //  NfsStats.setNfsReportsNeeded(true);
      //}

    }
  }


  /**
   * We have the info from all the slaves. Match this with the SD list.
   */
  private static void matchDataWithSds()
  {
    boolean errors = false;

    /* The host/lun combination must be found in the data we received from the slaves: */
    for (int j = 0; j < Host.getDefinedHosts().size(); j++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(j);
      for (int k = 0; k < all_returned_luns.size(); k++)
      {
        LunInfoFromHost info = (LunInfoFromHost) all_returned_luns.elementAt(k);

        /* Only look at the proper host: */
        if (!host.getLabel().equals(info.host_name))
          continue ;

        //common.ptod("info.host_name:  " + info.host_name);
        //common.ptod("k: " + k);

        /* Find all the SDs used for this lun name:     */
        /* (same lun name can be used in different SDs) */
        String[] sds_for_host = host.getSdNamesForLun(info.original_lun);
        for (int x = 0; x < sds_for_host.length; x++)
        {
          SD_entry sd = SD_entry.findSD(sds_for_host[x]);

          //common.ptod("matchDataWithSds sd: " + sd);
          //common.ptod("matchDataWithSds sd.sd_name:      " + sd.sd_name);
          //common.ptod("matchDataWithSds host.getLabel(): " + host.getLabel());

          /* We now have the proper info: */
          sd.host_info.add(info);

          //common.ptod("info.lun_exists: " + info.lun_exists);
          //common.ptod("info.lun_size:   " + info.lun_size);
          //common.ptod("sd.sd_name:      " + sd.sd_name);
          if (info.lun_exists)
          {
            if (sd.isOpenForWrite() && !info.write_allowed)
            {
              errors = true;
              common.ptod("sd=" + sd.sd_name + ",host=" + info.host_name +
                          ",lun=" + info.lun + " does not have write access.");
            }

            if (!sd.isOpenForWrite() && !info.read_allowed)
            {
              errors = true;
              common.ptod("sd=" + sd.sd_name + ",host=" + info.host_name +
                          ",lun=" + info.lun + " does not have read access.");
            }
          }

          else if (info.lun.toLowerCase().startsWith("\\\\.\\") || info.lun.startsWith("/dev/"))
          {
            errors = true;
            common.ptod("Raw device 'sd=%s,lun=%s' does not exist, or no permissions.",
                        sd.sd_name, info.lun);
          }
        }
      }
    }

    if (errors)
      common.failure("Please check above failures");

    /* The file system anchor must be there: */
    if (Vdbmain.isFwdWorkload())
    {
      for (int j = 0; j < Host.getDefinedHosts().size(); j++)
      {
        Host host = (Host) Host.getDefinedHosts().elementAt(j);
        for (int k = 0; k < all_returned_luns.size(); k++)
        {
          LunInfoFromHost info = (LunInfoFromHost) all_returned_luns.elementAt(k);

          if (!info.lun_exists)
            common.failure("File system anchor does not exist on host=" +
                           host.getLabel() + "; anchor=" + info.lun +
                           " \n\tMaybe add 'create_anchors=yes' at the beginning of "+
                           "your parameter file?");
        }
      }
    }
  }


  /**
   * Pick up information obtained by the slave from the replay file.
   * Information is requested only once from the first InfoFromHost
   * instance.
   *
   * Only pick up those fields of existing entries that are relevant.
   * A new ReplayDevice can be copied completely.
   * This was needed to prevent the inbound de-serialization from
   * bringing along with them new copies of ReplayExtent, ReplayGroup, and
   * SD_entry instances.
   *
   */
  private void possibleReplayInfo()
  {
    ReplayInfo info = ReplayInfo.getInfo();

    /* There is ReplayInfo only for the first slave on the first host: */
    if (replay_info == null)
      return;

    Vector devices_from_slave = replay_info.getDeviceList();

    //ReplayDevice.printDevices("before");

    /* Look at all devices found on the slave. This includes devices   */
    /* that will be marked 'reporting only' for those devices that     */
    /* have been found on the slaves but have not ben requested in the */
    /* parameter file.                                                 */
    loop:
    for (int i = 0; i < devices_from_slave.size(); i++)
    {
      ReplayDevice newd = (ReplayDevice) devices_from_slave.elementAt(i);
      //common.ptod("newd: " + host_label + " device=" +  newd.getDevString() + " " +
      //            newd.getRecordCount() + " " + newd.isReportingOnly());

      /* Look for the matching device number on the master's list:  */
      /* If it is not there it is a new 'for reporting only' device */
      /* that was found on the slave and will be added.             */
      ReplayDevice oldd = ReplayDevice.findDeviceAndCreate(newd.getDeviceNumber());


      /* The slave does not pass any data about duplicates, so ignore: */
      if (oldd.getDuplicateNumber() != 0)
        continue;

      newd.copyTo(oldd);


      /* We also need to copy the data to the duplicates: */
      if (replay_info.duplicationNeeded())
      {
        for (long dup = 1; dup <= oldd.duplicates_found; dup++)
        {
          long dup_number = ReplayDevice.addDupToDevnum(oldd.getDeviceNumber(), dup);
          ReplayDevice dupdev = ReplayDevice.findExistingDevice(dup_number);
          newd.copyTo(dupdev);
        }
      }
    }

    //ReplayDevice.printDevices("after");

  }


  /**
   * Store the information after making sure it matches across hosts.
   */
  private static void storeAndCompare()
  {
    // debugging
    for (int i = 9999990; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.get(i);
      common.ptod("storeAndCompare sd: " + sd.sd_name);

      for (int j = 1; j < sd.host_info.size(); j++)
      {
        LunInfoFromHost info2 = (LunInfoFromHost) sd.host_info.elementAt(j);
        common.ptod("storeAndCompare info2: " + info2.host_name);
      }
    }


    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.get(i);

      /* We stay away from unused SDs or from concatenated sds: */
      //common.ptod("sd.sd_is_referenced: " + sd.sd_is_referenced);
      //common.ptod("sd.concatenated_sd: " + sd.concatenated_sd);
      if (!sd.sd_is_referenced || sd.concatenated_sd)
        continue;

      /* Compare values from all hosts. They must match: */
      //common.ptod("sd.host_info: " + sd.host_info);
      //common.ptod("sd.host_info: " + sd.host_info.size());
      //common.ptod("sd.host_info: " + sd.sd_name);
      LunInfoFromHost info1 = (LunInfoFromHost) sd.host_info.get(0);
      for (int j = 1; j < sd.host_info.size(); j++)
      {
        LunInfoFromHost info2 = (LunInfoFromHost) sd.host_info.get(j);
        if (info1.lun_exists != info2.lun_exists ||
            info1.lun_size   != info2.lun_size)
        {
          info1.mismatch(sd);
          info2.mismatch(sd);
          common.failure("Lun status must be equal across hosts. ");
        }
      }

      /* Save proper size: */
      sd.psize = info1.lun_size;
      if (sd.end_lba == 0)
        sd.end_lba = sd.psize;

      if (sd.end_lba == 0)
      {
        common.ptod("");
        common.ptod("Undefined size for sd=" + sd.sd_name + ",lun=" + info1.lun);
        common.ptod("Either the lun or file does not exist, or you do not have permission " +
                    "to open the lun or file.");
        common.failure("No lun or file size available.");
      }

      if (sd.psize != 0 && sd.psize < sd.end_lba)
      {

        common.ptod("");
        common.ptod("Warning: size requested for sd=" + sd.sd_name +
                    ",lun=" + sd.lun + ": " + sd.end_lba + "; size available: " +
                    sd.psize + ". Insufficient size.");


        if (Validate.sdConcatenation())
        {
          common.ptod("Insufficient size.");
          common.failure("If this is a file: auto file creation or file expansion "+
                         "not supported when requesting 'concatenatesds=yes'");
        }

        if (!MiscParms.do_not_format_sds)
        {
          common.ptod("If this is a file system file vdbench will try to expand it. ");
          common.ptod("");
        }
      }

      common.plog("sd=%s,lun=%s lun size: %,d bytes; %,.4f GB (1024**3); %,.4f GB (1000**3)",
                  sd.sd_name, sd.lun, sd.psize,
                  (float) sd.psize / (1024.*1024.*1024.),
                  (float) sd.psize / (1000.*1000.*1000.));

      if (false)
      {
        common.ptod("");
        common.ptod("info1.lun:        " + info1.lun);
        common.ptod("info1.lun_exists: " + info1.lun_exists);
        common.ptod("sd.end_lba:       " + sd.end_lba);
        common.ptod("sd.psize:         " + sd.psize);
      }
    }

    //common.ptod("lun_size: " + info1.lun_size);
    //common.ptod("sd.end_lba: " + sd.end_lba);
  }

  public boolean isSolaris()
  {
    return solaris_host;
  }

  private static void runMountCommand(String mount_command)
  {
    if (common.onWindows())
    {
      common.plog("Running on Windows. No 'mount' command available.");
      return;
    }

    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(mount_command);
    ocmd.execute();

    boolean rc = ocmd.getRC();
    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();

    common.plog("mount command output for " + mount_command + ":");
    for (int i = 0; i < stdout.length; i++)
      common.plog("stdout: " + stdout[i]);
    for (int i = 0; i < stderr.length; i++)
      common.plog("stderr: " + stderr[i]);

    if (!rc)
    {
      SlaveJvm.sendMessageToConsole(mount_command + " failed. Maybe mount was not needed?.");
      SlaveJvm.sendMessageToConsole("See host's stdout.html file for messages.");
    }
  }

  public Vector getInstancePointers()
  {
    return instance_pointers;
  }

  public static boolean anyKstatErrors()
  {
    return any_kstat_errors;
  }

  public ArrayList getLuns()
  {
    return luns_on_host;
  }

  public void setNfsData(NfsV3 nfs3, NfsV4 nfs4)
  {
    nfs3_delta = nfs3;
    nfs4_delta = nfs4;
  }

  public NfsV3 getNfs3()
  {
    return nfs3_delta;
  }
  public NfsV4 getNfs4()
  {
    return nfs4_delta;
  }

  /**
   * If the directory does not exist, create it, but create the parents only
   * when the create_anchors parameter is there.
   */
  private void maybeCreateDirectory(String lun)
  {

    if (fwd_workload)
    {
      File fptr = new File(lun);
      if (!fptr.exists())
      {
        /* Create anchors only or all of its parents too? */
        if (create_anchors)
        {
          if (!fptr.mkdirs())
          {
            maybeSomeHost(lun);
            return;
          }
        }
        else
        {
          if (!fptr.mkdir())
          {
            maybeSomeHost(lun);
            return;
          }
        }
        SlaveJvm.sendMessageToConsole("Created anchor directory: " + lun);
      }
    }
  }


  /**
   * Anchor creation failed, but maybe some other host just did it already.
   */
  private void maybeSomeHost(String lun)
  {
    Signal signal = new Signal(10);

    while (!new File(lun).exists())
    {
      common.sleep_some_usecs(500);
      if (signal.go())
        common.failure("Unable to create anchor directory: " + lun);
    }
  }

  /**
   * Just do a quick file check with as objective to catch any auto-mount
   * that has to be done before we gather Kstat information.
   * Without this, Kstat structure may not exist yet for this file.
   *
   * For file system workloads create and keep open a small temporary file
   * to work around auto-dismount, making sure that this file system does not
   * get dismounted with Vdbench then failing later on.
   *
   * It of course would have been cleaner to rebuild the Kstat stuff, but why
   * waste hours and hours when a simple fix like this will do?
   */
  private static void checkAutoMount(InfoFromHost host_info)
  {
    /* Do we have an auto mount on windows? Does it matter? */
    //if (common.onWindows())
    //  return;

    for (int i = 0; i < host_info.luns_on_host.size(); i++)
    {
      LunInfoFromHost info = (LunInfoFromHost) host_info.luns_on_host.get(i);
      File fptr = new File(info.lun);
      fptr.isFile();

      if (host_info.fwd_workload && Fget.dir_exists(info.lun))
      {
        /* If the file already exists, OK. */
        if (Fget.file_exists(info.lun, NO_DISMOUNT_FILE))
          continue;

        Fput fp = new Fput(info.lun, NO_DISMOUNT_FILE);
        fp.println("This file was created to keep anchor busy and prevent auto-dismount");
        fp.flush();
        fp.chmod(info.lun + File.separator + NO_DISMOUNT_FILE);

        /* On Windows this won't work if the file is not closed? */
        // haven't checked Unix yet. Doesn't matter though

        // No longer deleteOnExit since I want to be able to reuse it
        //new File(fp.getName()).deleteOnExit();
      }
    }
  }
}




