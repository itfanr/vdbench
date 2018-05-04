package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

/**
 * This class stores all Work() instances and with that can keep track of which
 * SD is used where.
 *
 * We could maintain individual lists, but it seems easier to just always
 * recreate whatever we need from one central Work list when needed.
 * The information is not asked for often enough to warrant worrying about
 * performance.
 *
 * During report creation AllWork has information about ALL runs, during a real
 * run however it knows only about what is CURRENTLY running by calling
 * clearWork() at appropriate times.
 */
public class AllWork
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private static Vector <Work> work_list = new Vector(8, 0);



  public static void addWork(Work work)
  {
    common.where();
    work_list.add(work);
  }
  public static void clearWork()
  {
    common.where();
    work_list.removeAllElements();
  }


  /**
   * Return SDs used anywhere
   */
  public static String[] obsolete_getRealUsedSdNames()
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(j);
        String[] names = wg.getRealSdNames();
        for (int k = 0; k < names.length; k++)
          map.put(names[k], null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a host
   */
  public static String[] obsolete_getRealHostSdList(Host host)
  {
    HashMap <String, Object> map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (Host.findHost(work.host_label) != host)
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(j);
        String[] names = wg.getRealSdNames();
        for (int k = 0; k < names.length; k++)
          map.put(names[k], null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a slave
   */
  public static String[] obsolete_getSlaveSdList(Slave slave)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (work.slave_label.equals(slave.getLabel()))
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used for this RD_entry
   */
  public static String[] obsolete_getRdSdList(RD_entry rd)
  {
    HashMap map = new HashMap(16);

    for (Work work : work_list)
    {
      if (work.rd != rd)
        continue;

      for (WG_entry wg : work.wgs_for_slave)
      {
        for (String name : wg.getRealSdNames())
          map.put(name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a host for this RD_entry
   */
  public static String[] obsolete_getHostRdSdList(Host host, RD_entry rd)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (work.rd != rd)
        continue;
      if (Host.findHost(work.slave_label) != host)
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a slave for this RD_entry
   */
  public static String[] obsolete_getSlaveRdSdList(Slave slave, RD_entry rd)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (work.rd != rd)
        continue;
      if (work.slave_label.equals(slave.getLabel()))
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }
}
