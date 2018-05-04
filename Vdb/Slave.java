package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.net.*;
import java.util.*;

import Utils.Format;
import Utils.Fput;

/**
 * This class contains master level information and code for a specific slave
 */
public class Slave
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private int    slave_jvmno; /* Sequence number of the JVM on this host      */
  private String slave_name;  /* Combination of host and vdbench start time   */
                              /* This uniquely connects a slave with a master */
                              /* and therefore prevents picking up stale JVMs */

  private String slave_label; /* short name for referencing purposes          */
  private int    slave_number;

  private Host   host;        /* Host information for this slave */

  private SlaveSocket  slave_socket = null;   /* Client socket for this slave */

  private SlaveStarter slave_starter;

  private HashMap <String, String> names_used = new HashMap(16);

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

  /* This list has info ONLY for the RD currently being handled. */
  private ArrayList  <WG_entry> wgs_for_slave = new ArrayList(16);

  private Work    current_work = null;

  private HashMap sd_report_map = new HashMap(8);
  private Report  summary_report = null;

  public long     reads  = 0;
  public long     writes = 0;

  public String abort_msg = null;

  public int threads_given_to_slave = 0;

  private ThreadMonList threadmon_totals = new ThreadMonList();

  private static boolean any_slave_aborted = false;
  public  static int  max_slave_name = 0;

  /**
   * Create a Slave instance.
   *
   * Note: there is a dependency in SlaveJvm with the slave_label. The very
   * first slave_label on any host must end with '-0'.
   * (See SlaveJvm.scan_args())
   */
  public Slave(Host host_in, int jvmno)
  {
    host         = host_in;
    slave_jvmno  = jvmno;
    slave_label  = host.getLabel() + "-" + jvmno;

    /* See note above about slave name dependency!!! */
    max_slave_name = Math.max(max_slave_name, slave_label.length());

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
      common.ptod("UnknownHostException: hd=%s,system=%s", host.getLabel(), host.getIP());
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
    //common.ptod("setCurrentWork: %s %s", getLabel(), work);
    //common.where(4);
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

  public void setConnected(String pid)
  {
    connected = true;
    common.plog("Slave %s (pid %6s) connected to master %s", this.slave_label, pid,
                common.getProcessIdString());
    Status.printStatus(String.format("Slave %s (pid %6s) connected to master %s", slave_label, pid,
                  common.getProcessIdString()), null);
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
      WG_entry wg = (WG_entry) wgs_for_slave.get(i);
      if (wg.sd_used == sd)
        return true;
    }
    return false;
  }

  /**
   * Maintain a list of which SD or FSD anchor names are used on this slave.
   * This info will be used with Data Validation to make sure that an SD or FSD
   * always stays on the same slave.
   */
  public void addName(String name)
  {
    names_used.put(name, name);
  }

  public String[] getNamesUsedList()
  {
    return (String[]) names_used.keySet().toArray(new String[0]);
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


  public void clearWorkloads()
  {
    wgs_for_slave.clear();
    names_used.clear();
  }

  public void addWorkload(WG_entry wg, RD_entry rd)
  {
    //common.where(8);
    wgs_for_slave.add(wg);
    addName(wg.sd_used.sd_name);
    //common.ptod("addWorkload() to rd=%-10s,slave=%s %s (%d)",
    //            rd.rd_name, getLabel(), wg.report(rd), wgs_for_slave.size());
  }

  public ArrayList <WG_entry> getWorkloads()
  {
    //common.where(8);
    //common.ptod("wgs_for_slave: " + wgs_for_slave.size());
    return wgs_for_slave;
  }


  /**
   * Remove a workload from the slave.
   * Note that we can not just compare the instance. That could be a clone, so
   * we therefore need to compare the wd_name and sd.
   */
  public int removeWorkload(WG_entry remove)
  {
    //common.ptod("remove:      %s %s", remove.wd_name, remove.sd_used.sd_name);

    int removes = 0;
    for (int i = 0; i < wgs_for_slave.size(); i++)
    {
      WG_entry wg = wgs_for_slave.get(i);
      //common.ptod("removeWorkload slave=%s wg.wd_name.: %s %s %s",
      //            getLabel(), wg.wd_name, wg.sd_used.sd_name, remove.sd_used.sd_name );
      if (wg.wd_name.equals(remove.wd_name) && wg.sd_used == remove.sd_used)
      {
        wgs_for_slave.set(i, null);
        removes++;
        RD_entry.printWgInfo("Removed wd=%s from slave=%s", wg.wd_name, getLabel());
      }
    }
    while (wgs_for_slave.remove(null));

    //for (WG_entry wg : wgs_for_slave)
    //  common.ptod("removeWorkload slave=%s wd left: %s ", getLabel(), wg.wd_name);

    return removes;
  }


  /**
   * Count the SDs that request sequential processing.
   */
  public int sequentialFilesOnSlave()
  {
    int files = 0;

    for (WG_entry wg : wgs_for_slave)
    {
      wg.seq_eof = false;
      if (wg.seekpct < 0)
        files ++;
    }

    if (files == 0)
      files = -1;

    return files;
  }


  /**
   * Store ThreadMonitor information about important threads on a Slave level.
   */
  public void accumMonData(ThreadMonList deltas)
  {
    /* As always, we'll ignore warmup: */
    if (!Reporter.isWarmupDone())
      return;

    for (ThreadMonData td : deltas.list)
    {
      ThreadMonData total = threadmon_totals.map.get(td.label);
      if (total == null)
        threadmon_totals.map.put(td.label, td);
      else
        total.accum(td);
    }

    /* Increment total elapsed time: */
    threadmon_totals.elapsed += deltas.elapsed;
  }

  public ThreadMonList reportThreadMonSlaveTotals()
  {
    /* First convert the current HashMap to an ArrayList: */
    threadmon_totals.list = new ArrayList(threadmon_totals.map.values());

    /* Now report that list: */
    ThreadMonitor.reportTotals(getLabel(),threadmon_totals );

    /* Clear them for the next interval: */
    threadmon_totals.map.clear();
    threadmon_totals.list.clear();

    return threadmon_totals;
  }
}


