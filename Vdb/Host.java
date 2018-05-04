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
public class Host implements Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public  String  host_ip       = "localhost";  /* network address */
  public  String  host_label    = "localhost";  /* host label, dflt network address */
  public  String  host_vdbench  = ClassPath.classPath();

  public  int     client_count = 0;

  public  Mount   host_mount = null;

  public  String       host_shell        = "rsh";
  public  String       host_user         = null;
  private int          host_jvms         = 1;
  private int          last_created_jvm  = 0;
  public  boolean      jvms_in_parm      = false;

  private Vector       slaves            = null;

  private InfoFromHost host_info         = null;

  private HashMap      luns_used         = new HashMap(16);

  private Report       summary_report    = null;
  private Report       kstat_report      = null;
  private HashMap      host_report_map   = null;

  private PrintWriter  adm_msg_file;

  private reporting[] nfs3_rep  = null;
  private reporting[] nfs4_rep  = null;

  public static int jvmcount_override = 0;

  private String os_name;
  private String os_arch;


  private static Vector defined_hosts = new Vector(16, 0);


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
    defined_hosts.add(h);

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

  public static Vector getDefinedHosts()
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
  public Vector getSlaves()
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
   * Create a list of slaves using the host names and the requested JVM counts.
   * This can be called twice; the second time after the default number of JVMs
   * has been overridden because too many iops per JVM
   */
  public static void createSlaves()
  {

    for (int i = 0; i < defined_hosts.size(); i++)
    {
      Host host = (Host) defined_hosts.elementAt(i);

      if (Vdbmain.isReplay() && host.host_jvms > 1)
        common.failure("Only one Slave allowed for Replay.");

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
    //common.plog("Adding SD to host: " + this.getLabel() + " " + sdname + " " + lun);
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
   * Find the proper host specific lun name for an SD
   */
  public String getLunNameForSd(String sdname)
  {
    String lun = (String) luns_used.get(sdname);

    if (lun != null)
      return lun;

    //printSdLunList();

    //common.ptod("Unable to find lun name for sd=" + sdname + ",host=" + getLabel());

    return null;
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
}


