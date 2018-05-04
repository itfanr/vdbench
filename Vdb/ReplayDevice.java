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
 * This class contains code and information related to each device number
 * that must be replayed.
 */
class ReplayDevice implements java.io.Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public  long device_number;
  int     records;                  /* Number of TNF records found in input  */
  long    max_lba = 0;              /* Maximum lba address (bytes)           */
                                    /* Includes xfersize.                    */
  long    min_lba = Long.MAX_VALUE; /* Minimum lba address (bytes)           */
  int     max_xfersize        = 0;
  long    last_tod            = 0;  /* last tod found for this device        */
  boolean reporting_only      = false;

  public  Vector extent_list  = new Vector (4, 0);

  public ReplayGroup group;      /* Which group owns this device? */

  static Vector  ios_table = null;

  private static Vector all_device_list = new Vector(16, 0);



  public ReplayDevice(ReplayGroup grp, long number)
  {
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) all_device_list.elementAt(i);
      if (rdev.device_number == number)
        common.failure("Adding duplicate ReplayDevice: " + number);
    }

    group          = grp;
    device_number  = number;
    reporting_only = grp.reporting_only;

    /* Insert the device number numerically: */
    int i = 0;
    for (i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) all_device_list.elementAt(i);
      //common.ptod(i + " rdev.device_number: " + rdev.device_number);
      if (rdev.device_number > number)
        break;
    }

    all_device_list.insertElementAt(this, i);
  }


  public void addExtent(ReplayExtent extent)
  {
    extent_list.addElement(extent);
  }

  public static Vector getDeviceList()
  {
    return all_device_list;
  }
  public static void setDeviceList(Vector list)
  {
    all_device_list = list;
  }

  public static int countUsedDevices()
  {
    int count = 0;
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) all_device_list.elementAt(i);
      if (!rdev.reporting_only)
        count++;
    }

    return count;
  }

  /**
   * Find a device number.
   * If not found, create a new device in the 'reporting only' group.
   */
  private static HashMap lookup_map = new HashMap(1024);
  public static ReplayDevice findDevice(long number)
  {
    /* We create here a private hashmap to speed up things. */
    ReplayDevice rdev = (ReplayDevice) lookup_map.get(new Long(number));
    if (rdev != null)
      return rdev;

    for (int i = 0; i < all_device_list.size(); i++)
    {
      rdev = (ReplayDevice) all_device_list.elementAt(i);
      if (rdev.device_number == number)
      {
        lookup_map.put(new Long(number), rdev);
        return rdev;
      }
    }

    /* Create a new device, just for reporting: */
    rdev = ReplayGroup.reporting_group.addDevice(number);
    lookup_map.put(new Long(number), rdev);

    return rdev;
  }

  public static void clearMap()
  {
    lookup_map = new HashMap(1024);
  }


  /**
   * Figure out which SD should be used for this replay lba
   */
  public SD_entry findExtentForLba(Cmd_entry cmd, long replay_lba)
  {
    for (int i = 0; i < extent_list.size(); i++)
    {
      ReplayExtent re = (ReplayExtent) extent_list.elementAt(i);
      if (re.findLbaInExtent(cmd, replay_lba))
        return cmd.sd_ptr;
    }

    common.failure("Extent not found for lba: " + replay_lba);
    return null;
  }

  public static long getTotalIoCount()
  {
    long total = 0;
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rpd = (ReplayDevice) all_device_list.elementAt(i);
      if (!rpd.reporting_only)
        total += rpd.records;
    }

    return total;
  }

  public static long getLastTod()
  {
    long tod = 0;
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rpd = (ReplayDevice) all_device_list.elementAt(i);
      if (!rpd.reporting_only)
        tod = Math.max(tod, rpd.last_tod);
    }

    return tod;
  }

  public static long getMaxXfersize()
  {
    int xfersize = 0;
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rpd = (ReplayDevice) all_device_list.elementAt(i);
      if (!rpd.reporting_only)
        xfersize = Math.max(xfersize, rpd.max_xfersize);
    }

    return xfersize;
  }
}
