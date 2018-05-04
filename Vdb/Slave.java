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
import java.util.*;
import java.net.*;

import Utils.Format;
import Utils.Fput;

/**
 * This class contains master level information and code for a specific slave
 */
public class Slave
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private int    slave_jvmno; /* Sequence number of the JVM on this host      */
  private String slave_name;  /* Combination of host and vdbench start time   */
                              /* This uniquely connects a slave with a master */
                              /* and therefore prevents picking up stale JVMs */

  private String slave_label; /* short name for referencing purposes          */
  private int    slave_number;

  private Host   host;        /* Host information for this slave */

  private SlaveSocket  slave_socket = null;   /* Client socket for this slave */

  private SlaveStarter slave_starter;

  private HashMap      sds_used     = new HashMap(16);

  private boolean seqwork_done             = false;
  private boolean connected                = false;
  private boolean work_done                = false;
  private boolean ready_for_more           = false;
  private boolean shut_down                = false;
  private boolean aborted                  = false;
  private boolean ready_to_go              = false;
  private boolean structure_pending        = false;

  private boolean slave_shutdown_requested = false;
  private boolean slave_has_terminated     = false;

  private Report  console_log = null;   /* stdout and stderr goes here */

  public  Vector  wgs_for_slave = null;

  private Work    current_work = null;

  private HashMap sd_report_map = new HashMap(8);
  private Report  summary_report = null;

  public long     reads  = 0;
  public long     writes = 0;

  public String abort_msg = null;

  private static boolean any_slave_aborted = false;


  /**
   * Create a Slave instance.
   * Note: there is a dependency in SlaveJvm with the slave_label. The very
   * first slave_label on any host must end with '-0'.
   */
  public Slave(Host host_in, int jvmno)
  {
    host         = host_in;
    slave_jvmno  = jvmno;
    slave_label  = host.getLabel() + "-" + jvmno;

    slave_number = SlaveList.getSlaveCount();
    slave_name   = host.getIP()    + "-" + (slave_number + 10) + "-" + Vdbmain.getRunTime();

    SlaveList.addSlave(this);

    host.addSlave(this);
  }



  public void createSummaryFile()
  {
    Report report = new Report(getLabel(), "Slave summary report for slave=" + getLabel());
    setSummaryReport(report);

    createConsoleLog();


    String txt = "";
    txt += Format.f("%-32s", "Link to slave summary report" + ":");
    txt += " <A HREF=\"" + report.getFileName()      + ".html\">" + getLabel() + " summary</A>";
    txt += "  ";
    txt += " <A HREF=\"" + console_log.getFileName() + ".html\">" + getLabel() + " stdout</A>";

    host.getSummaryReport().println(txt);
  }

  /**
   * Create a console_log file that contains all the stdout and stderr output
   * received from the slave.
   */
  public void createConsoleLog()
  {
    String console_name = getLabel() + ".stdout";
    console_log = new Report(console_name, "stdout/stderr for slave=" + getLabel());
    String txt = "Console log";
    getSummaryReport().printHtmlLink(txt, console_log.getFileName(), "Slave stdout/stderr");
    //getSummaryReport().println("");
  }



  /**
   * Check to see if the host provided is the current host.
   * This allows us to bypass ssh/rsh.
   */
  public boolean isLocalHost()
  {
    /* 127.0.0.1 is considered local: */
    if (host.getIP().equals("localhost"))
      return true;

    try
    {
      InetAddress local = InetAddress.getLocalHost();
      String current_ip = local.getHostAddress();
      String remote_ip  = InetAddress.getByName(host.getIP()).getHostAddress();

      return(current_ip.equals(remote_ip));
    }
    catch (UnknownHostException e)
    {
      common.ptod("UnknownHostException: " + host);
      return false;
    }
  }

  public String getLabel()
  {
    return slave_label;
  }

  public String getName()
  {
    return slave_name;
  }

  public String getSlaveIP()
  {
    return host.getIP();
  }

  public SlaveSocket getSocket()
  {
    return slave_socket;
  }

  public Host getHost()
  {
    return host;
  }

  public void setReadyToGo(boolean bool)
  {
    ready_to_go = bool;
  }
  public boolean isReadyToGo()
  {
    return ready_to_go;
  }

  public void setSlaveSocket(SlaveSocket ss)
  {
    slave_socket = ss;
  }

  public void addReport(String name, Report report)
  {
    //common.ptod("added slave report name: " + name + " for slave=" + getLabel());
    sd_report_map.put(name, report);
  }
  public Report getReport(String name)
  {
    Report report = (Report) sd_report_map.get(name);
    if (report == null)
    {
      String[] reps = (String[]) sd_report_map.keySet().toArray(new String[0]);
      for (int i = 0; i < reps.length; i++)
        common.ptod("Host reps: " + reps[i]);
      common.failure("Requesting an unknown report file: " + name + " for slave=" + getLabel());
    }

    return report;
  }


  public Report getConsoleLog()
  {
    return console_log;
  }

  public void set_may_terminate()
  {
    slave_shutdown_requested = true;
  }
  public boolean may_terminate()
  {
    return slave_shutdown_requested;
  }

  public void setTerminated()
  {
    slave_has_terminated = true;
  }
  public boolean hasTerminated()
  {
    return slave_has_terminated;
  }

  public void setCurrentWork(Work work)
  {
    current_work = work;
  }
  public Work getCurrentWork()
  {
    return current_work;
  }
  public int getCurrentFwgWorkSize()
  {
    if (current_work == null)
      return 0;
    return current_work.fwgs_for_slave.size();
  }

  public void setSummaryReport(Report report)
  {
    summary_report = report;
  }
  public Report getSummaryReport()
  {
    return summary_report;
  }
  public boolean isSdReported(String sdname)
  {
    return sd_report_map.get(sdname) != null;
  }


  public int getSlaveNumber()
  {
    return slave_number;
  }

  public void setSlaveStarter(SlaveStarter sst)
  {
    slave_starter = sst;
  }
  public SlaveStarter getSlaveStarter()
  {
    return slave_starter;
  }

  public void setConnected()
  {
    connected = true;
    common.plog("Slave " + this.slave_label + " connected");
  }
  public boolean isConnected()
  {
    return connected;
  }


  public boolean isWorkDone()
  {
    return work_done;
  }
  public void setWorkDone(boolean bool)
  {
    work_done = bool;
  }
  public boolean isReadyForMore()
  {
    return ready_for_more;
  }
  public void setReadyForMore(boolean bool)
  {
    ready_for_more = bool;
  }

  public boolean isShutdown()
  {
    return shut_down;
  }
  public void setShutdown(boolean bool)
  {
    shut_down = bool;
  }

  public void setSequentialDone(boolean bool)
  {
    seqwork_done = bool;
  }
  public boolean isSequentialDone()
  {
    return seqwork_done;
  }


  public void setAborted(String txt)
  {
    aborted           = true;
    any_slave_aborted = true;
    abort_msg         = txt;
  }
  public boolean isAborted()
  {
    return aborted;
  }
  public static boolean anyAborted()
  {
    return any_slave_aborted;
  }


  public boolean isUsingSd(SD_entry sd)
  {
    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) wgs_for_slave.elementAt(i);
      if (wg.sd_used == sd)
        return true;
    }
    return false;
  }

  public void addSd(SD_entry sd, String lun)
  {
    sds_used.put(sd, lun);
  }
  public HashMap getSdMap()
  {
    return sds_used;
  }
  public SD_entry[] getSdList()
  {
    return(SD_entry[]) sds_used.keySet().toArray(new SD_entry[0]);
  }


  public String toString()
  {
    return "Slave instance: " + getLabel();
  }

  public void setStructurePending(boolean bool)
  {
    structure_pending = bool;
  }
  public boolean getStructurePending()
  {
    return structure_pending;
  }
}


