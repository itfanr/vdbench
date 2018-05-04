package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.net.*;
import java.io.*;
import Utils.ClassPath;
import Utils.Format;
import Utils.NfsV3;
import Utils.NfsV4;


/**
 * This class contains information for each host defined by the user,
 * or the default 'localhost'.
 */
public class Host implements Cloneable, Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public  String  host_ip       = "localhost";  /* network address */
  public  String  host_label    = "localhost";  /* host label, dflt network address */
  public  String  host_vdbench  = ClassPath.classPath();
  public  int     relative_hostno;

  public  int     client_count = 0;

  public  Mount   host_mount = null;

  public  String       host_shell        = "rsh";
  public  String       host_user         = null;
  private int          host_jvms         = 1;
  private int          last_created_jvm  = 0;
  public  boolean      jvms_in_parm      = false;

  private Vector <Slave> slaves            = null;

  private InfoFromHost host_info         = null;

  private HashMap <String, String> luns_used = new HashMap(16); /* key sd_name, for lun name */

  private Report       summary_report    = null;
  private Report       kstat_report      = null;
  private HashMap      host_report_map   = null;

  private PrintWriter  adm_msg_file;

  private reporting[] nfs3_rep  = null;
  private reporting[] nfs4_rep  = null;

  public static int jvmcount_override = 0;

  private String os_name;
  private String os_arch;

  /* This list is used per RD, so must be reallocated/cleared each time: */
  //private ArrayList <WG_entry> wg_list   = null;

  private static Vector <Host> defined_hosts = new Vector(16, 0);

  public static int max_host_name = 0;

  /**
   * This is a DEEP clone!
   * We must make sure that after the regular shallow clone we create a new copy
   * of every single instance that is a HOST parameter.
   */
  public Object clone()
  {
    try
    {
      Host host      = (Host) super.clone();
      host.luns_used = (HashMap) luns_used.clone();
      return host;

    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public static void addHost(Host h)
  {
    h.relative_hostno = defined_hosts.size();
    defined_hosts.add(h);
    max_host_name = Math.max(max_host_name, h.getLabel().length());

    if (jvmcount_override != 0)
    {
      h.host_jvms = jvmcount_override;
      h.jvms_in_parm = true;
    }
  }
  public void addSlave(Slave sl)
  {
    if (slaves == null)
      slaves = new Vector(8, 0);
    slaves.add(sl);
  }

  public static Vector <Host> getDefinedHosts()
  {
    return defined_hosts;
  }

  public static String[] getHostNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);
      names.put(host.getLabel(), host);
    }

    return(String[]) names.keySet().toArray(new String[0]);
  }

  public boolean jvmsInParmfile()
  {
    return jvms_in_parm;
  }
  public int getJvmCount()
  {
    return host_jvms;
  }
  public void setJvmCount(int jvms)
  {
    host_jvms = jvms;
  }

  public void setHostInfo(InfoFromHost info)
  {
    host_info = info;
  }
  public InfoFromHost getHostInfo()
  {
    return host_info;
  }
  public InstancePointer getInstancePointer(int index)
  {
    return(InstancePointer) host_info.getInstancePointers().elementAt(index);
  }


  public void addReport(String name, Report report)
  {
    if (host_report_map == null)
      host_report_map = new HashMap(8);
    host_report_map.put(name, report);
  }
  public Report getReport(String name)
  {
    Report report = (Report) host_report_map.get(name);
    if (report == null)
    {
      String[] reps = (String[]) host_report_map.keySet().toArray(new String[0]);
      for (int i = 0; i < reps.length; i++)
        common.ptod("Host reps: " + reps[i]);
      common.failure("Requesting an unknown report file: " + name);
    }

    return report;
  }

  public String[] getReportedKstats()
  {
    return Report.filterSds(host_report_map, false);
  }


  public void setSummaryReport(Report report)
  {
    summary_report = report;
  }
  public void setKstatReport(Report report)
  {
    kstat_report = report;
  }
  public Report getKstatReport()
  {
    return kstat_report;
  }
  public Report getSummaryReport()
  {
    return summary_report;
  }

  public String getIP()
  {
    return host_ip;
  }
  public String getLabel()
  {
    return host_label;
  }
  public String getVdbench()
  {
    return host_vdbench;
  }
  public String getShell()
  {
    return host_shell;
  }
  public String getUser()
  {
    if (host_user == null)
      common.failure("'user=' subparameter not specified for host " + host_label);
    return host_user;
  }
  public Slave getFirstSlave()
  {
    return(Slave) slaves.firstElement();
  }
  public Vector <Slave> getSlaves()
  {
    return slaves;
  }

  public static void dumpAll(String txt)
  {
    for (int j = 0; j < defined_hosts.size(); j++)
    {
      Host host = (Host) defined_hosts.elementAt(j);

      common.ptod("host.slaves.size(): " + host.slaves.size());
      for (int i = 0; i < host.slaves.size(); i++)
        common.ptod(i + " " + txt + " host: " + host.getLabel() + " " +
                    ((Slave) host.slaves.elementAt(i)).getLabel());
    }
  }

  public static boolean anyMountCommands()
  {
    for (int j = 0; j < defined_hosts.size(); j++)
    {
      Host host = (Host) defined_hosts.elementAt(j);
      if (host.host_mount != null)
        return true;
    }
    return false;
  }


  /**
   * This check is necessary because there is a layer of code missing while
   * spreading out the workloads across slaves.
   * For raw i/o I spread work across hosts, then slaves on those hosts.
   * For file system I spread the work across slaves, resulting in, surprise,
   * slaves not getting their portion of the work.
   *
   * So it really is not only a 'skew' problem, 'skew' calculations because of
   * this are also in error.
   * It took almost 10 years for this problem to show up, and I just can't
   * justify the work needed to rectify this.
   * So this will stay as-is until I can justify the effort, or get bored and
   * just fix it :-)
   */
  public static void noMultiJvmForFwdSkew()
  {
    for (Host host : defined_hosts)
    {
      if (host.host_jvms > 1)
      {
        BoxPrint box = new BoxPrint();
        box.add("Use of FWD skew= currently not supported when running multi-JVM");
        box.add("This can be resolved by specifying more Host Definitions (HD)");
        box.add("For instance:");
        box.add("");
        box.add("hd=host1,system=localhost");
        box.add("hd=host2,system=localhost");
        box.add("");
        box.add("or");
        box.add("");
        box.add("hd=host1,clients=2");
        box.add("");
        box.add("or");
        box.add("");
        box.add("hd=default,clients=2");
        box.add("");
        box.add("");
        box.add("This is a workaround for a bug in a rarely used Vdbench option.");
        box.print();
        common.failure("No multi-JVM supported for FWD skew");
      }
    }
  }

  /**
   * Create a list of slaves using the host names and the requested JVM counts.
   * This can be called twice; the second time after the default number of JVMs
   * has been overridden because too many iops per JVM
   */
  public static void createSlaves()
  {
    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);

      for (int j = host.last_created_jvm; j < host.host_jvms; j++)
      {
        Slave slave = new Slave(host, j);
        host.last_created_jvm++;
      }
    }
  }



  /**
   * Check for duplicate host names. Not allowed.
   */
  public static void checkDuplicates()
  {
    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);

      for (int j = i+1; j < defined_hosts.size(); j++)
      {
        Host host2 = (Host) defined_hosts.elementAt(j);
        if (host.host_label.equalsIgnoreCase(host2.host_label))
          common.failure("Using duplicate host names: " + host.host_label);

        //if (host.host_ip.equalsIgnoreCase(host2.host_ip))
        //  common.failure("Using duplicate host IP addresses : " + host.host_ip);
      }
    }
  }


  /**
   * Find a certain host name.
   */
  public static Host findHost(String name)
  {
    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);
      if (host.host_label.equals(name))
        return host;
    }

    common.failure("Unable to locate host %s (No wildcards allowed)", name);

    return null;
  }


  /**
   * Validate existence of a host name
   */
  public static boolean doesHostExist(String host)
  {
    long start = System.currentTimeMillis();
    if (host.startsWith("localhost"))
      return true;

    try
    {
      InetAddress.getByName(host);
      long end = System.currentTimeMillis();
      if ((end - start) > 5000)
        common.ptod("Obtaining network information about host=" + host +
                    " took more than 5 seconds: " + (end - start) + " milliseconds.");
      return true;
    }
    catch (UnknownHostException e)
    {
      return false;
    }
    catch (SecurityException e)
    {
      return false;
    }
  }

  /**
   * Create an output file that is to contain the last 'nn' lines of file
   * /var/adm/messages and /var/adm/message.0
   */
  public void createAdmMessagesFile()
  {
    String fname = host_label + ".var_adm_msgs";
    adm_msg_file = Report.createHmtlFile(fname);
    getSummaryReport().printHtmlLink("Link to /var/adm/messages", fname, "messages");

    adm_msg_file.println("This file will contain the last 'nn' lines of files ");
    adm_msg_file.println("/var/adm/messages and /var/adm/messages.0 on the target host. ");
    adm_msg_file.println(" ");
    adm_msg_file.flush();

  }

  public void writeAdmMessagesFile(String line)
  {
    adm_msg_file.println(line);
    adm_msg_file.flush();
  }
  public static void closeAdmMessagesFiles()
  {

    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);

      if (host.adm_msg_file != null)
        host.adm_msg_file.close();
      host.adm_msg_file = null;
    }
  }


  /**
   * Return a list of existing host names using a (possible) wildcard search.
   */
  public static Vector findSelectedHosts(String[] search_list)
  {
    Vector hosts_found = new Vector(8, 0);

    for (int i = 0; i < search_list.length; i++)
    {
      String search = search_list[i];

      /* Scan all hosts, looking for a match with the requested name: */
      for (int j = 0; j < defined_hosts.size(); j++)
      {
        Host host = (Host) defined_hosts.elementAt(j);

        //common.ptod("host.getLabel(): " + host.getLabel());
        //common.ptod("sd.host_names[i]: " + sd.host_names[i]);
        if (common.simple_wildcard(search, host.getLabel()) ||
            common.simple_wildcard(search, host.getIP()))
        {
          hosts_found.add(host.getLabel());
          //common.ptod("findSelectedHosts: " + hosts_found.lastElement());
        }
      }

      if (hosts_found.size() == 0)
        common.failure("Could not find host=" + search);
    }

    return hosts_found;
  }


  /**
   * Store sd names and lun names so that we can later on know who to call what
   * on which system.
   */
  public void addLun(String sdname, String lun)
  {
    //common.ptod("Adding SD to host: " + this.getLabel() + " " + sdname + " " + lun);
    if (luns_used.put(sdname, lun) != null)
      common.failure("An SD can only be defined once: sd=" + sdname);
  }

  public boolean anyWork()
  {
    for (int j = 0; j < slaves.size(); j++)
    {
      Slave slave = (Slave) slaves.elementAt(j);
      Work work = slave.getCurrentWork();
      if (work != null)
        return true;
    }

    return false;
  }


  /**
   * Find the proper host specific lun name for an SD.
   * If none is found, use the one coded in the SD_entry itself.
   */
  public String getLunNameForSd(SD_entry sd)
  {
    String lun = luns_used.get(sd.sd_name);

    if (lun == null)
    {
      if (!sd.concatenated_sd && sd.lun == null)
        common.failure("getLunNameForSd: null sd.lun for sd=%s", sd.sd_name);
      return sd.lun;
    }
    else
      return lun;
  }

  public boolean doesHostHaveSd(SD_entry sd)
  {
    String lun = luns_used.get(sd.sd_name);
    return (lun != null);
  }

  public void replaceLunForSd(String sdname, String new_lun)
  {
    if (luns_used.put(sdname, new_lun) == null)
      common.failure("replaceLunForSd: unknown lun for sd=%s,host=%s,lun=%s",
                     sdname, getLabel(), new_lun);
  }

  public int getLunCount()
  {
    return luns_used.size();
  }

  /**
   * HashMap of SD name with lun name for this host
   */
  public HashMap <String, String> getHostLunMap()
  {
    return luns_used;
  }

  /**
   * Get list of sd names used for a lun on this host.
   *
   * This code depends on keySet() and values() returning the Map entries in the
   * same order!!
   */
  public String[] getSdNamesForLun(String lun)
  {
    String[] sds  = (String[]) luns_used.keySet().toArray(new String[0]);
    String[] luns = (String[]) luns_used.values().toArray(new String[0]);
    Vector sds_used = new Vector(4, 0);

    for (int i = 0; i < luns.length; i++)
    {
      if (luns[i].equals(lun))
        sds_used.add(sds[i]);
    }

    return(String[]) sds_used.toArray(new String[0]);
  }


  /**
   * Determine how many hosts are using this specific SD
   */
  public static int countHostsForSD(String sdname)
  {
    int count = 0;

    for (int j = 0; j < defined_hosts.size(); j++)
    {
      Host host = (Host) defined_hosts.elementAt(j);
      if (host.luns_used.get(sdname) != null)
        count++;
    }

    return count;
  }

  /**
   * Check to see if any host= parameter specified jvms=count.
   */
  public static boolean anyJvmOverrides()
  {
    for (int j = 0; j < defined_hosts.size(); j++)
    {
      Host host = (Host) defined_hosts.elementAt(j);
      if (host.jvmsInParmfile())
        return true;
    }
    return false;
  }


  public void setOS(String name, String arch)
  {
    os_name = name;
    os_arch = arch;
  }
  public boolean onWindows()
  {
    return os_name.toLowerCase().startsWith("windows");
  }



  /**
   * Create NFS reports for this host.
   */
  public void createNfsReports()
  {
    InfoFromHost info = getHostInfo();
    int links         = 0;

    NfsV3 nfs3_delta = info.getNfs3();
    NfsV4 nfs4_delta = info.getNfs4();


    if (nfs3_delta != null || nfs4_delta != null)
      kstat_report.println("");

    /* Create Report for nfs: */
    if (info.getNfs3() != null)
    {
      Report nfs3_report = new Report(getLabel() + ".nfsstat3",
                                      "Host nfs statistics report for host=" + getLabel());
      String link = (links++ == 0) ? "Host NFS statistics report" : null;
      kstat_report.printHtmlLink(link, nfs3_report.getFileName(), "nfsv3");
      addReport("nfsstat3", nfs3_report);

      nfs3_rep = NfsStats.nfsLayout(nfs3_delta, new NfsV3());
      nfs3_report.println(NfsStats.warning);
    }

    if (info.getNfs4() != null)
    {
      Report nfs4_report = new Report(getLabel() + ".nfsstat4",
                                      "Host nfs statistics report for host=" + getLabel());
      String link = (links++ == 0) ? "Host NFS statistics report" : null;
      kstat_report.printHtmlLink(link, nfs4_report.getFileName(), "nfsv4");
      addReport("nfsstat4", nfs4_report);

      nfs4_rep = NfsStats.nfsLayout(nfs4_delta, new NfsV4());
      nfs4_report.println(NfsStats.warning);
    }
  }


  public void PrintNfsstatInterval(String title)
  {
    InfoFromHost info = getHostInfo();

    NfsV3 nfs3_delta = info.getNfs3();
    NfsV4 nfs4_delta = info.getNfs4();

    if (nfs3_delta != null)
      nfs3_delta = (NfsV3) getReport("nfsstat3").getData().getIntervalNfsStats(new NfsV3());
    if (nfs4_delta != null)
      nfs4_delta = (NfsV4) getReport("nfsstat4").getData().getIntervalNfsStats(new NfsV4());

    /* Print headers if needed: */
    if (Reporter.needHeaders())
    {
      if (nfs3_delta != null)
        reporting.report_header(getReport("nfsstat3").getWriter(), nfs3_rep);
      if (nfs4_delta != null)
        reporting.report_header(getReport("nfsstat4").getWriter(), nfs4_rep);
    }

    if (nfs3_delta != null)
      NfsStats.NfsPrint(getReport("nfsstat3"), nfs3_delta, nfs3_rep, title, new NfsV3());
    if (nfs4_delta != null)
      NfsStats.NfsPrint(getReport("nfsstat4"), nfs4_delta, nfs4_rep, title, new NfsV4());
  }


  public void PrintNfsstatTotals(String title)
  {
    InfoFromHost info = getHostInfo();

    NfsV3 nfs3_totals = info.getNfs3();
    NfsV4 nfs4_totals = info.getNfs4();

    if (nfs3_totals != null)
      nfs3_totals = (NfsV3) getReport("nfsstat3").getData().getTotalNfsStats(new NfsV3());
    if (nfs4_totals != null)
      nfs4_totals = (NfsV4) getReport("nfsstat4").getData().getTotalNfsStats(new NfsV4());

    /* Print headers if needed: */
    if (Reporter.needHeaders())
    {
      if (nfs3_totals != null)
        reporting.report_header(getReport("nfsstat3").getWriter(), nfs3_rep);
      if (nfs4_totals != null)
        reporting.report_header(getReport("nfsstat4").getWriter(), nfs4_rep);
    }

    if (nfs3_totals != null)
      NfsStats.NfsPrint(getReport("nfsstat3"), nfs3_totals, nfs3_rep, title, new NfsV3());
    if (nfs4_totals != null)
      NfsStats.NfsPrint(getReport("nfsstat4"), nfs4_totals, nfs4_rep, title, new NfsV4());


    if (nfs3_totals != null)
      NfsStats.NfsPrintVertical(getReport("nfsstat3"), nfs3_totals);
    if (nfs4_totals != null)
      NfsStats.NfsPrintVertical(getReport("nfsstat4"), nfs4_totals);

  }



  /**
   * Get workloads for all hosts.
   * See also RD_entry.getAllWorkLoads()
   */
  public static ArrayList <WG_entry> getAllWorkloads()
  {
    ArrayList <WG_entry> list = new ArrayList(64);
    for (Host host : getDefinedHosts())
      list.addAll(host.getWorkloads());
    return list;
  }


  /**
   * Get workloads for just this host.
   */
  public ArrayList <WG_entry> getWorkloads()
  {
    ArrayList <WG_entry> list = new ArrayList(64);
    for (Slave slave : getSlaves())
      list.addAll(slave.getWorkloads());
    return list;
  }

  public SD_entry[] getSds()
  {
    HashMap <String, SD_entry> sdmap = new HashMap(8);
    for (WG_entry wg : getWorkloads())
      sdmap.put(wg.sd_used.sd_name, wg.sd_used);

    String[] names = sdmap.keySet().toArray(new String[0]);
    Arrays.sort(names);

    SD_entry[] sds = new SD_entry[ names.length ];
    for (int i = 0; i < names.length; i++)
      sds[ i ] = sdmap.get(names[i]);

    return sds;
  }

  public String[] getSdNames()
  {
    HashMap <String, SD_entry> sdmap = new HashMap(8);
    for (WG_entry wg : getWorkloads())
      sdmap.put(wg.sd_used.sd_name, wg.sd_used);

    String[] names = sdmap.keySet().toArray(new String[0]);
    Arrays.sort(names);

    return names;
  }




  public ArrayList <WG_entry> getWgsForSd(SD_entry sd)
  {
    ArrayList <WG_entry> wgs = new ArrayList(16);
    for (WG_entry wg : getWorkloads())
    {
      if (wg.sd_used == sd)
        wgs.add(wg);
    }

    return wgs;
  }

  public int getSdCount()
  {
    HashMap <String, String> sd_name_map = new HashMap(16);
    for (WG_entry wg : getWorkloads())
      sd_name_map.put(wg.sd_used.sd_name, wg.sd_used.sd_name);
    return sd_name_map.size();
  }

  /**
   * Remove a workload from one of this hosts slaves.
   * Be careful: we must manipulate the original Vector of workloads from the
   * slave in order to remove it from that slave. A newly generated Vector would
   * not remove it from the original.
   */
  public int removeWorkload(WG_entry remove)
  {
    int removes = 0;
    for (Slave slave : getSlaves())
    {
      removes += slave.removeWorkload(remove);
    }

    return removes;
  }


  /**
   * All random workloads are given to each slave. The remainder, seq, DV,
   * Replay, etc will be given to the slave that has the least amount of work.
   *
   * Note: there is no need to worry about the order in which workloads are
   * given to the slaves. All non-sequential stuff will be given to ALL slaves
   * anyway,  so there is no need to first handle all non-seq and then seq
   * stuff.
   */
  public Slave getLeastBusySlave()
  {
    if (slaves.size() == 0)
      common.failure("getLowBusySlave: no slaves available for host=%s", host_label);

    Slave lowest = slaves.get(0);
    for (Slave slave : slaves)
    {
      if (slave.getWorkloads().size() < lowest.getWorkloads().size())
        lowest = slave;
    }

    return lowest;
  }

  /**
   * Return the slave that has the least amount of threads doing work for a
   * specific SD. This is done to make sure that all the complex workloads are
   * not accidentally overloading a specific slave.
   */
  public Slave getLeastBusyThreads(SD_entry sd)
  {
    if (slaves.size() == 0)
      common.failure("getLowBusyThreads: no slaves available for host=%s", host_label);

    /* Find the first slave that uses this SD: */
    Slave lowest = null;
    first_loop:
    for (Slave slave : slaves)
    {
      for (WG_entry wg : slave.getWorkloads())
      {
        if (wg.sd_used == sd)
        {
          lowest = slave;
          break first_loop;
        }
      }
    }

    if (lowest == null)
      common.failure("getLeastBusyThreads: unable to find available slave for sd=%s", sd.sd_name);


    /* Find the slave that uses this SD and has the lowest thread count: */
    for (Slave slave : slaves)
    {
      for (WG_entry wg : slave.getWorkloads())
      {
        if (wg.sd_used == sd)
        {
          if (slave.getWorkloads().size() > 0 &&
              slave.threads_given_to_slave < lowest.threads_given_to_slave)
            lowest = slave;
        }
      }
    }

    return lowest;
  }


  /**
   * Report all ThreadMonitor totals for each slave and each host.
   * Note that because of the possibility of different hosts having different
   * processor counts we can not report an 'all' total if there is indeed a
   * difference.
   */
  public static void reportMonTotals()
  {
    ThreadMonList full_totals  = new ThreadMonList();
    full_totals.processors     = 0;
    long    last_processors    = 0;
    boolean processor_mismatch = false;


    /* Look at each host: */
    for (Host host : defined_hosts)
    {
      ThreadMonList host_totals = new ThreadMonList();

      /* Pick up all slaves for this host: */
      for (Slave slave : host.slaves)
      {
        /* report this slave's info: */
        ThreadMonList slave_totals = slave.reportThreadMonSlaveTotals();

        /* Now pick up the slave totals and add them together for this host */
        /* and also for the overall totals:                                 */
        for (ThreadMonData td : slave_totals.list)
        {
          ThreadMonData total = host_totals.map.get(td.label);
          if (total == null)
            host_totals.map.put(td.label, td);
          else
            total.accum(td);

          host_totals.elapsed    = slave_totals.elapsed;
          host_totals.processors = slave_totals.processors;

          if (last_processors != 0 && slave_totals.processors != last_processors)
            processor_mismatch = true;
          else
            last_processors = slave_totals.processors;
        }
      }

      host_totals.list = new ArrayList(host_totals.map.values());
      ThreadMonitor.reportTotals(host.getLabel(),host_totals );

      full_totals.elapsed     = host_totals.elapsed;
      full_totals.processors += host_totals.processors;

      for (ThreadMonData td : host_totals.list)
      {
        ThreadMonData total = full_totals.map.get(td.label);
        if (total == null)
          full_totals.map.put(td.label, td);
        else
          total.accum(td);

      }
    }


    if (processor_mismatch)
      common.ptod("Reporting ThreadMonitor run totals not possible because of processor count mismatch.");
    else
    {
      full_totals.list = new ArrayList(full_totals.map.values());
      ThreadMonitor.reportTotals("Total", full_totals);
    }
  }


  /**
   * Sort by Host.
   * Though it feels correct to compare by host name, we actually want the hosts
   * to be sorted by the relative host as defined in the parameter file.
   *
   * E.g. host1 and host2 sort nicely alphabetically, but
   * system1 and abc don't.
   */
  public int compareTo(Object obj)
  {
    Host o1 = (Host) this;
    Host o2 = (Host) obj;
    return o1.relative_hostno - o2.relative_hostno;
  }
}


