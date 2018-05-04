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

/**
 * The class stores all Work() instances and with that can keep track of which
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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Vector work_list = new Vector(8, 0);



  public static void addWork(Work work)
  {
    work_list.add(work);
  }
  public static void clearWork()
  {
    work_list.removeAllElements();
  }


  /**
   * Return SDs used anywhere
   */
  public static String[] getSdList()
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a host
   */
  public static String[] getSdList(Host host)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (Host.findHost(work.host_label) != host)
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a slave
   */
  public static String[] getSdList(Slave slave)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (work.slave_label.equals(slave.getLabel()))
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used for this RD_entry
   */
  public static String[] getSdList(RD_entry rd)
  {
    HashMap map = new HashMap(16);
    for (int i = 0; i < work_list.size(); i++)
    {
      Work work = (Work) work_list.elementAt(i);
      if (work.rd != rd)
        continue;

      for (int j = 0; j < work.wgs_for_slave.size(); j++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a host for this RD_entry
   */
  public static String[] getSdList(Host host, RD_entry rd)
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
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }

  /**
   * Return SDs used in a slave for this RD_entry
   */
  public static String[] getSdList(Slave slave, RD_entry rd)
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
        WG_entry wg = (WG_entry) work.wgs_for_slave.elementAt(j);
        map.put(wg.sd_used.sd_name, null);
      }
    }

    return (String[]) map.keySet().toArray(new String[0]);
  }
}
