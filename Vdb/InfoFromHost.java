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
import java.util.HashMap;
import java.util.Vector;

import Utils.Format;
import Utils.NfsV3;
import Utils.NfsV4;
import Utils.OS_cmd;
import Utils.Semaphore;




/**
 * This class communicates between master and slaves to get information
 * like:
 * - does lun/file exist
 * - lun/file size
 * - Kstat info
 * - etc
 */
public class InfoFromHost extends VdbObject implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String  host_label;
  private Vector  luns_on_host = new Vector(8, 0);
  private Vector  instance_pointers;

  private boolean windows_host = false;
  private boolean solaris_host = false;
  private boolean linux_host   = false;
  private boolean aix_host     = false;
  private boolean hp_host      = false;
  private boolean mac_host     = false;

  private String  replay_filename    = null;
  private Vector  replay_device_list = null;

  private Kstat_cpu kstat_cpu = null;

  private static  boolean configuration_loaded = false;

  private boolean error_with_kstat   = false;

  private NfsV3 nfs3_delta = null;
  private NfsV4 nfs4_delta = null;

  private boolean fwd_workload;
  private boolean create_anchors;

  public  Mount   host_mount = null;

  private static  Semaphore  wait_for_host_info;
  private static  HashMap hosts_with_work;    /* HashMap per hosts that received work  */
  private static  Vector  all_returned_luns;  /* All Luns that were returned */
  private static  Vector  all_returned_hosts; /* Everything that was returned */
  private static  boolean any_kstat_errors = false;

  private static HashMap sd_formats_done = new HashMap(8);

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
    hosts_with_work = new HashMap(4);
    Vector list     = null;

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
      for (int i = 0; rd.wgs_for_rd != null && i < rd.wgs_for_rd.size(); i++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(i);

        HashMap luns_for_host  = null;
        String host_label = wg.slave.getHost().getLabel();
        if ((luns_for_host = (HashMap) hosts_with_work.get(host_label)) == null)
          hosts_with_work.put(host_label, new HashMap(4));

        luns_for_host = (HashMap) hosts_with_work.get(host_label);
        createLunInfoFromHost(luns_for_host, host_label, wg);
      }

      /* Store anchor names into a HashMap per host: */
      /* (WG and FWG are mutially exclusive so we never will do both) */
      for (int i = 0; rd.fwgs_for_rd != null && i < rd.fwgs_for_rd.size(); i++)
      {
        FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(i);

        HashMap luns_for_host = null;

        if (fwg.host_name.equals("*"))
          common.plog("fwg.host_name: " + fwg.host_name + " " +
                      fwg.fsd_name + " " + fwg.getName() + " " +
                      RD_entry.next_rd.rd_name);

        if ((luns_for_host = (HashMap) hosts_with_work.get(fwg.host_name)) == null)
          hosts_with_work.put(fwg.host_name, new HashMap(4));

        luns_for_host = (HashMap) hosts_with_work.get(fwg.host_name);
        LunInfoFromHost isv = new LunInfoFromHost();
        isv.lun = fwg.anchor.getAnchorName();
        isv.lun = Work.unix2Windows(Host.findHost(fwg.host_name), isv.lun);
        luns_for_host.put(isv.lun, isv);
      }
    }

    /* Send a message to the first slave on each host: */
    String[] host_names = (String[]) hosts_with_work.keySet().toArray(new String[0]);
    for (int i = 0; i < host_names.length; i++)
    {
      InfoFromHost host_info = new InfoFromHost(host_names[i]);

      /* Pass mount information if present: */
      host_info.host_mount = Host.findHost(host_names[i]).host_mount;

      /* Give all lun names to this host: */
      HashMap infos = (HashMap) hosts_with_work.get(host_names[i]);
      LunInfoFromHost[] luninfos = (LunInfoFromHost[]) infos.values().toArray(new LunInfoFromHost[0]);
      for (int j = 0; j < luninfos.length; j++)
      {
        LunInfoFromHost isv = luninfos[j];
        host_info.luns_on_host.add(isv);

        /* Asking for replay information only once */
        if (Vdbmain.isReplay() && j == 0)
        {
          host_info.replay_device_list = ReplayDevice.getDeviceList();
          host_info.replay_filename    = RD_entry.replay_filename;
        }
      }

      /* Store this entry. It will be replaced once it is returned from the host */
      //common.ptod("host_names[i]: " + host_names[i]);
      Host.findHost(host_names[i]).setHostInfo(host_info);

      /* Slave must know what type of workload this is: */
      host_info.fwd_workload   = Vdbmain.isFwdWorkload();
      host_info.create_anchors = MiscParms.create_anchors;

      Slave slave = Host.findHost(host_names[i]).getFirstSlave();

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
  }


  /**
   * Create new LunInfoFromHost() instance for a lun on a host, or combine an
   * other workload request with an already existing instance.
   */
  private static void createLunInfoFromHost(HashMap host_map, String host_name, WG_entry wg)
  {
    LunInfoFromHost isv = (LunInfoFromHost) host_map.get(wg.host_lun);
    if (isv == null)
    {
      isv           = new LunInfoFromHost();
      isv.lun       = Work.unix2Windows(wg.slave.getHost(), wg.host_lun);
      isv.original_lun = wg.host_lun;
      isv.host_name = host_name;
      host_map.put(wg.host_lun, isv);
    }

    if (!isv.open_for_write && wg.sd_used.open_for_write)
      isv.open_for_write = true;

    if (isv.sd_instance == null)
      isv.sd_instance = wg.sd_used.instance;
  }


  /**
   * All hosts must have returned proper status before we can start using it
   */
  private static void waitForAllHosts()
  {
    /* Wait for response */
    long signaltod = 0;

    try
    {
      while (!wait_for_host_info.attempt(1000))
      {
        if ( (signaltod = common.signal_caller(signaltod, 10 * 1000)) == 0)
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
      CpuStats.getNativeCpuStats();
      host_info.kstat_cpu = CpuStats.getDelta();
    }

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
      LunInfoFromHost info = (LunInfoFromHost) host_info.luns_on_host.elementAt(i);

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

        /* Regular file system file is much easier: */
        File fptr   = new File(info.lun);
        File parent = fptr.getParentFile();
        if (parent == null || !parent.exists())
          info.parent_exists = info.lun_exists = false;

        else
        {
          info.parent_exists = true;
          info.lun_exists    = fptr.exists();
        }

        if (info.lun_exists && fptr.isFile())
        {
          info.read_allowed  = fptr.canRead();
          info.write_allowed = fptr.canWrite();
          info.lun_size      = fptr.length();
          long handle        = Native.openFile(info.lun);
          if (handle == -1)
            info.error_opening = true;
          else
            Native.closeFile(handle);
        }
      }

      /* Pick up Kstat information: */
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
          devxlate_list = Devxlate.get_device_info(info.lun);

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
    if (host_info.replay_device_list != null)
    {
      ReplayDevice.setDeviceList(host_info.replay_device_list);
      ReplayRun.readTraceFile(host_info.replay_filename);
      host_info.replay_device_list = ReplayDevice.getDeviceList();
    }


    /* Notify master if there were any Kstat errors: */
    host_info.error_with_kstat = SlaveJvm.getKstatError();

    /* Translate Devxlate entries to a Vector of InstancePointer(): */
    host_info.instance_pointers = InstancePointer.createInstancePointers(devices_this_host);

    /* Send the response to the master: */
    SlaveJvm.sendMessageToMaster(SocketMessage.GET_LUN_INFO_FROM_SLAVE, host_info);
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
        common.failure("Receiving info from unkown host: " + host_label);
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
      if (solaris_host)
        Host.findHost(host_label).createAdmMessagesFile();

      /* If we have an answer from all the hosts: */
      if (hosts_with_work.isEmpty())
      {
        /* Look at Kstat data returned from the hosts: */
        checkDevxlateEntries();

        checkForKstatErrors();

        /* Store it with each SD: */
        matchDataWithSds();

        /* This is for replay: */
        possibleReplayInfo();

        /* Make sure data matches across hosts: */
        storeAndCompare();

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
        LunInfoFromHost info = (LunInfoFromHost) hostinfo.luns_on_host.elementAt(j);
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

          //common.ptod("sd: " + sd);
          //common.ptod("sd.sd_name:      " + sd.sd_name);
          //common.ptod("host.getLabel(): " + host.getLabel());

          /* We now have the proper info: */
          sd.host_info.add(info);

          if (info.lun_exists)
          {
            if (sd.open_for_write && !info.write_allowed)
              common.failure("sd=" + sd.sd_name + ",host=" + info.host_name +
                             ",lun=" + info.lun + " does not have write access.");
            if (!sd.open_for_write && !info.read_allowed)
              common.failure("sd=" + sd.sd_name + ",host=" + info.host_name +
                             ",lun=" + info.lun + " does not have read access.");
          }
          else if (info.lun.toLowerCase().startsWith("\\\\.\\") || info.lun.startsWith("/dev/"))
            common.failure("Raw device '%s' does not exist", info.lun);
        }
      }
    }

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
   * bringing along with then new copies of ReplayExtent, ReplayGroup, and
   * SD_entry instances.
   *
   */
  private void possibleReplayInfo()
  {
    if (replay_device_list != null)
    {
      Vector old_list = ReplayDevice.getDeviceList();
      Vector new_list = replay_device_list;
      loop:
      for (int i = 0; i < new_list.size(); i++)
      {
        ReplayDevice newd = (ReplayDevice) new_list.elementAt(i);

        for (int j = 0; j < old_list.size(); j++)
        {
          ReplayDevice oldd = (ReplayDevice) old_list.elementAt(j);
          if (oldd.device_number == newd.device_number)
          {
            oldd.records        = newd.records;
            oldd.max_lba        = newd.max_lba;
            oldd.min_lba        = newd.min_lba;
            oldd.max_xfersize   = newd.max_xfersize;
            oldd.last_tod       = newd.last_tod;
            oldd.reporting_only = newd.reporting_only;
            continue loop;
          }
        }

        old_list.add(newd);
      }
    }
  }


  /**
   * Store the information after making sure it matches across hosts.
   */
  private static void storeAndCompare()
  {
    // debugging
    for (int i = 99990; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      common.ptod("sd: " + sd.sd_name);

      for (int j = 1; j < sd.host_info.size(); j++)
      {
        LunInfoFromHost info2 = (LunInfoFromHost) sd.host_info.elementAt(j);
        common.ptod("info2: " + info2.host_name);
      }
    }


    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (!sd.sd_is_referenced)
        continue;

      /* Compare values from all hosts. They must match: */
      LunInfoFromHost info1 = (LunInfoFromHost) sd.host_info.firstElement();
      for (int j = 1; j < sd.host_info.size(); j++)
      {
        LunInfoFromHost info2 = (LunInfoFromHost) sd.host_info.elementAt(j);
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
        common.ptod("If this is a file system file vdbench will try to expand it. ");
        common.ptod("");
      }

      common.plog("sd=" + sd.sd_name +
                  ",lun=" + sd.lun + " lun size: " + sd.psize +
                  Format.f(" bytes; %.4f GB (1024**3);", (float) sd.psize / (1024.*1024.*1024.)) +
                  Format.f(" %.4f GB (1000**3)", (float) sd.psize / (1000.*1000.*1000.)));

      if (false)
      {
        common.ptod("");
        common.ptod("info1.lun:        " + info1.lun);
        common.ptod("info1.lun_exists: " + info1.lun_exists);
        common.ptod("sd.end_lba:       " + sd.end_lba);
        common.ptod("sd.psize:         " + sd.psize);
      }

      /*
      if (!info1.lun_exists && sd.end_lba > 0 &&
          sd.psize == 0 &&
          !common.get_debug(common.NO_FILE_FORMAT) &&
          !sd.isTapeTesting())
      {
        Object done = sd_formats_done.get(info1.lun);
        if (done == null)
        {
          sd.lun = info1.lun;
          sd.check_new_file_init();
          sd_formats_done.put(info1.lun, info1.lun);
        }
      }
      */
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

  public Vector getLuns()
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
        if (create_anchors)
        {
          if (!fptr.mkdirs())
            common.failure("Unable to create anchor directory: " + lun);
        }
        else
        {
          if (!fptr.mkdir())
            common.failure("Unable to create anchor directory: " + lun);
        }
        SlaveJvm.sendMessageToConsole("Created anchor directory: " + lun);
      }
    }
  }
}




