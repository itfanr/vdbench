package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

/**
 * This class helps figure out what can run where, or where it has been sent.
 *
 * This class was created after several problems getting things like Data
 * Validation SDs to the proper place.
 * There were just too many places where this info was maintained.
 *
 * Of course, getting it now all working may just be wishful thinking?
 *
 * File 'WORKflow.txt' should give some info as to how it is on 7/16/2015
 *
 */
public class WhereWhatWork
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


  /* SDs using Data Validation MUST stay on the same slave throughout    */
  /* the complete Vdbench execution.                                     */
  /* If they would move around they would need to create a new mmap file */
  /* each time, losing the previous mmap contents.                       */
  /* Worse yet, if they move BACK to a slave they'll use a STALE mmap    */
  /* therefore reporting false corruptions. Ouch!                        */
  private static HashMap <String, Slave> dv_sdmap  = new HashMap(8);

  /* 100% sequential workloads may move around between Run Definitions,         */
  /* but WITHIN a Run Definition they may run on only ONE slave.                */
  /* without this check we could have two slaves reading blocks 1,1,2,2,3,3 etc */
  private static HashMap <String, Slave> seq_wdmap = new HashMap(8);



  /**
   * Verify that if a host has work, at least slave0 gets some.
   * Slave0 is the slave that returns CPU and Kstat usage.
   */
  public static void checkWorkForSlave0(RD_entry rd)
  {

    for (Host host : Host.getDefinedHosts())
    {
      int has_work = 0;
      for (Slave slave : host.getSlaves())
      {
        if (slave.getCurrentWork() != null)
          has_work++;
      }

      if (has_work > 0)
      {
        if (host.getSlaves().get(0).getCurrentWork() == null)
        {
          BoxPrint box = new BoxPrint();
          box.add("If ANY work is selected for a host the first slave " +
                  "on that host MUST have work.");
          box.add("rd=%s", rd.rd_name);
          box.add("This is a Vdbench bug, please report it.");
          box.add("");
          box.add("Workaround: use only a single slave, see 'jvms=' in the doc.");
          box.print();
          common.failure("If ANY work is selected for a host the first slave " +
                         "on that host MUST have work.");
        }
      }
    }
  }




  /**
   * Clear appropriate maps.
   * This clear is necessary because this code will be called TWICE, once to
   * determine things like number of jvms needed or which reports to create, and
   * then once again to create the final workload information for all slaves.
   */
  public static void clearWdMap()
  {
    seq_wdmap.clear();
  }
  public static void clearDvMap()
  {
    dv_sdmap.clear();
  }


  public static void addDvSd(WG_entry wg, Slave slave)
  {
    dv_sdmap.put(wg.sd_used.sd_name, slave);
    RD_entry.printWgInfo("addDvSd wd=%s,sd=%s for slave %s",
                         wg.wd_name,
                         wg.sd_used.sd_name, slave.getLabel());
  }

  public static Slave getDvSlave(String sdname)
  {
    return dv_sdmap.get(sdname);
  }



  //
  // still need to make sure we STAY on the same slave!!!!!
  //

  /**
   * Data Validation MUST keep an SD on the same slave.
   * Otherwise they will:
   * - step on each other's toes,
   * - in a different RD, lose the mmap file, therefore losing history of the
   *   previous RD.
   *
   *
   * BTW: dedup also has some requirements like this.... TBD.
   */
  public static void paranoiaDvCheck()
  {
    if (!Validate.isRealValidate())
      return;
    if (!Vdbmain.isWdWorkload())
      return;

    for (WG_entry wg1 : RD_entry.getAllWorkloads())
    {
      for (WG_entry wg2 : RD_entry.getAllWorkloads())
      {
        if (wg1 != wg2 && wg1.getSlave() != wg2.getSlave())
        {
          if (wg1.sd_used.sd_name.equals(wg2.sd_used.sd_name))
          {
            BoxPrint box = new BoxPrint();
            box.add("Sending Data Validation for sd=%s to multiple slaves.",
                    wg1.sd_used.sd_name);
            box.add("");
            box.add("This will lead to false data corruptions being reported.");
            box.add("");
            box.add("This is a Vdbench bug, please report it.");
            box.add("");
            box.add("Workaround: use only a single slave, see 'jvms=' in the doc,");
            box.add("or, if multiple hosts are used, cut down to a single host with single JVM.");
            box.print();
            common.failure("Sending Data Validation sd=%s to multiple slaves.",
                           wg1.sd_used.sd_name);
          }
        }
      }
    }
  }


  /**
   * For Data Validation we need to make sure that an SD, once sent to a slave,
   * will go back to the same slave.
   */
  public static void rememberWhereDvWent()
  {
    if (!Validate.isRealValidate())
      return;
    if (!Vdbmain.isWdWorkload())
      return;

    for (WG_entry wg : RD_entry.getAllWorkloads())
      wg.sd_used.slave_used_for_dv = wg.getSlave();
  }


  /**
   * Debugging info for raw.
   */
  public static void printWorkForSlaves(String title, RD_entry rd)
  {
    if (!Vdbmain.isWdWorkload())
      return;

    if (!common.get_debug(common.DETAIL_SLV_REPORT))
      return;

    RD_entry.printWgInfo2("");
    RD_entry.printWgInfo2(title);
    RD_entry.printWgInfo2("SlaveList.printWorkForSlaves() for rd=%s (%s)",
                          rd.rd_name, (RD_entry.next_rd.use_waiter ? "w" : "nw"));

    /* Print work for each WD: */
    RD_entry.printWgInfo2("Sort order: WD");
    for (WG_entry wg : WG_entry.sortWorkloads(RD_entry.getAllWorkloads(), "wd"))
      RD_entry.printWgInfo2(wg.report(rd));

    /* Print work for all slaves: */
    RD_entry.printWgInfo2("Sort order: slave");
    for (WG_entry wg : WG_entry.sortWorkloads(RD_entry.getAllWorkloads(), "slave"))
      RD_entry.printWgInfo2(wg.report(rd));


    /* Print work for each SD: */
    RD_entry.printWgInfo2("Sort order: SD");
    for (WG_entry wg : WG_entry.sortWorkloads(RD_entry.getAllWorkloads(), "sd"))
      RD_entry.printWgInfo2(wg.report(rd));



    /* Print total thread + WD count for all slaves: */
    RD_entry.printWgInfo2("");
    for (Host host : Host.getDefinedHosts())
    {
      int    host_threads = 0;
      for (Slave slave : host.getSlaves())
      {
        HashMap <String, String> wd_map = new HashMap(8);
        int slave_threads = 0;
        if (slave.getCurrentWork() != null)
        {
          double slave_skew    = 0;
          for (WG_entry wg : slave.getCurrentWork().wgs_for_slave)
          {
            wd_map.put(wg.wd_name, wg.wd_name);
            slave_threads += rd.getSdThreadsUsedForSlave(wg.sd_used.sd_name, slave);
            slave_skew    += wg.skew;
          }

          RD_entry.printWgInfo2("slave=%s received work for %4d threads and %2d WDs, and %6.2f%% skew.",
                                slave.getLabel(), slave_threads, wd_map.size(), slave_skew);
        }
      }
    }


    /* Print total thread count for all hosts: */
    RD_entry.printWgInfo2("");
    int total_threads = 0;
    int cum_threads   = 0;
    for (Host host : Host.getDefinedHosts())
    {
      int    host_threads = 0;
      double host_skew    = 0;
      for (Slave slave : host.getSlaves())
      {
        if (slave.getCurrentWork() != null)
        {
          for (WG_entry wg : slave.getCurrentWork().wgs_for_slave)
          {
            host_threads += rd.getSdThreadsUsedForSlave(wg.sd_used.sd_name, slave);
            host_skew    += wg.skew;
          }
        }
      }

      cum_threads += host_threads;
      RD_entry.printWgInfo2("host=%s received work for %4d threads and %6.2f%% skew.",
                            host.getLabel(), host_threads, host_skew);
      total_threads += host_threads;
    }

    RD_entry.printWgInfo2("Total amount of work received for %4d threads. Small differences "+
                          "with the requested thread count may be caused by integer truncation.", cum_threads);
    RD_entry.printWgInfo2("");
  }


  /**
   * Some workloads may be run on only ONE slave
   * - 100% sequential workloads (not using streams)
   * - Swat Replay
   * - Data Validation
   *
   * However, streams MAY run on multiple slaves; the code makes sure that these
   * slaves use different StreamContext() instances.
   */
  public static boolean mustRunOnSingleSlave(WD_entry wd)
  {
    if (wd.stream_count != 0)
      return false;

    boolean rc;

    if (Validate.isRealValidate()  ||
        wd.seekpct <= 0            ||
        ReplayInfo.isReplay())
      rc = true;
    else
      rc = false;

    if (common.get_debug(common.PLOG_WG_STUFF))
      common.ptod("mustRunOnSingleSlave: wd=%s %5b rv=%b seek=%d", wd.wd_name, rc, Validate.isRealValidate(), (int) wd.seekpct);

    return rc;
  }
}





