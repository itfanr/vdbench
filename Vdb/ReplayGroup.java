package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import Utils.Format;
import Utils.printf;
import java.io.*;


class ReplayGroup implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private String  group_name;
  private Vector  device_list       = new Vector(8,0);
  private double  total_group_bytes = 0;
  private boolean reporting_only    = false;
  private long    bytes_needed;

  transient private Vector  sds_in_this_group = new Vector(8,0);

  private static ReplayGroup reporting_group = new ReplayGroup();


  static double GB = 1024. * 1024. * 1024.;

  /**
   * Group for reporting only
   */
  public ReplayGroup()
  {
    group_name = "reporting_only";
    reporting_only = true;
  }
  public ReplayGroup(String name)
  {
    Vector group_list = ReplayInfo.getGroupList();
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);
      if (group.group_name.equals(name))
        common.failure("Adding duplicate ReplayGroup: " + name);
    }

    group_name = name;
    group_list.addElement(this);
  }

  public String getName()
  {
    return group_name;
  }
  public boolean isReportingOnly()
  {
    return reporting_only;
  }
  public static ReplayGroup getReportingOnlyGroup()
  {
    return reporting_group;
  }

  public ReplayGroup findGroup(String name)
  {
    Vector group_list = ReplayInfo.getGroupList();
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);
      if (group.group_name.equals(name))
        return group;
    }
    return null;
  }


  public Vector getDeviceList()
  {
    return device_list;
  }


  public Vector getSdList()
  {
    return sds_in_this_group;
  }


  /**
   * Add a new device number to this group
   */
  public ReplayDevice addDevice(long number)
  {
    ReplayDevice ndev = new ReplayDevice(this, number);
    device_list.addElement(ndev);
    return ndev;
  }


  /**
   * Add an SD to this group
   */
  public void addSD(SD_entry sd)
  {
    sds_in_this_group.addElement(sd);
  }


  /**
   * Add an SD to a named group
   */
  public static void addSDGroup(SD_entry sd, String grp)
  {
    Vector group_list = ReplayInfo.getGroupList();
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);
      if (group.group_name.equals(grp))
      {
        group.sds_in_this_group.addElement(sd);
        return;
      }
    }

    common.failure("addSD: no Replay Group found: " + grp);
  }


  /**
   * Calculate the combined length of SDs within a group
   */
  public static void calculateGroupSDSizes(Vector sd_list)
  {
    /* Check all groups: */
    Vector group_list = ReplayInfo.getGroupList();
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);

      if (group.sds_in_this_group.size() == 0)
      {
        common.ptod("No SDs found for replay group: " + group.group_name);
        common.ptod("Did you possibly use both replay groups and replay devices in the SD?");
        common.failure("No SDs found for replay group: " + group.group_name);
      }

      /* Check all SDs in this group: */
      for (int j = 0; j < group.sds_in_this_group.size(); j++)
      {
        SD_entry sd = (SD_entry) group.sds_in_this_group.elementAt(j);
        group.total_group_bytes += sd.end_lba;
      }

      //common.ptod("group.total_group_bytes: " + group.group_name +
      //            Format.f(" %.3fg", group.total_group_bytes / GB));
    }
  }


  /**
   * Calculate the combined length of devices within a group
   */
  public static void calculateGroupSizes()
  {
    boolean bad_one = false;

    /* Check all groups: */
    Vector group_list = ReplayInfo.getGroupList();
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);

      if (group.sds_in_this_group.size() == 0)
        common.failure("No SDs found for replay group: " + group.group_name);

      /* Check all devices in this group: */
      group.bytes_needed = 0;
      for (int j = 0; j < group.device_list.size(); j++)
      {
        ReplayDevice rdev = (ReplayDevice) group.device_list.elementAt(j);
        group.bytes_needed += rdev.getMaxLba();
      }

      if (group.bytes_needed > group.total_group_bytes)
      {
        printf pf = new printf("Replay Group %s needs a total of %7.3fg; only %7.3fg available");
        pf.add(group.group_name);
        pf.add(group.bytes_needed / GB);
        pf.add(group.total_group_bytes / GB);
        common.ptod(pf.print());
        bad_one = true;
      }
    }

    if (bad_one && !common.get_debug(common.IGNORE_MISSING_REPLAY))
      common.failure("Not every replay group has enough lun space available");
  }
}
