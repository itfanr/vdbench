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
import Utils.Format;
import Utils.printf;
import java.io.*;


class ReplayGroup extends VdbObject implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  public String  group_name;
  public Vector  device_list       = new Vector(8,0);
  transient public Vector  sds_in_this_group = new Vector(8,0);
  public double  total_group_bytes = 0;
  public boolean reporting_only    = false;
  public long    bytes_needed;

  /* List of all existing groups: */
  private static Vector group_list           = new Vector(16, 0);

  public static ReplayGroup reporting_group = new ReplayGroup();


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
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);
      if (group.group_name.equals(name))
        common.failure("Adding duplicate ReplayGroup: " + name);
    }

    group_name = name;
    group_list.addElement(this);
  }

  public static Vector getGroupList()
  {
    return group_list;
  }
  public static void setGroupList(Vector list)
  {
    group_list = list;
  }


  public Vector getDeviceList()
  {
    return device_list;
  }


  public Vector getSDList()
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
  public static void addSD(SD_entry sd, String grp)
  {
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
    for (int i = 0; i < group_list.size(); i++)
    {
      ReplayGroup group = (ReplayGroup) group_list.elementAt(i);

      if (group.sds_in_this_group.size() == 0)
        common.failure("No SDs found for replay group: " + group.group_name);

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
        group.bytes_needed += rdev.max_lba;
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

    if (bad_one)
      common.failure("Not every replay group has enough lun space available");
  }
}
