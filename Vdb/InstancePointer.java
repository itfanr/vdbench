package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.HashMap;
import java.util.Vector;

import Utils.Semaphore;

/**
 * This class handles communication of Kstat instance names and Kstat pointers
 * for master-slave and reporting.
 */
public class InstancePointer implements Serializable, Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private String lun;
  private String instance;
  private String special;
  private long   native_pointer;



  public InstancePointer(String lun,
                         String instance,
                         String special,
                         long   pointer)
  {
    this.lun            = lun;
    this.instance       = instance;
    this.special        = special;
    this.native_pointer = pointer;

    if (pointer == 0)
      common.failure("No Kstat instance pointer found for device " + lun);
  }


  /**
   * Create a list of unique kstat instance pointers for Kstat reporting to be
   * sent from the slave to the master.
   */
  public static Vector createInstancePointers(Vector devxlate_entries)
  {
    HashMap pointers = new HashMap(16);
    for (int i = 0; i < devxlate_entries.size(); i++)
    {
      Devxlate devx = (Devxlate) devxlate_entries.elementAt(i);
      InstancePointer ip = new InstancePointer(devx.fullname,
                                               devx.instance,
                                               devx.special,
                                               devx.kstat_pointer);
      pointers.put(devx.instance, ip);

      //if (pointers.put(devx.instance, ip) == null)
      //  common.ptod("====getInstancePointers: " + devx.fullname + " " + devx.kstat_pointer);
    }

    return new Vector(pointers.values());
  }

  public long getNativePointer()
  {
    return native_pointer;
  }

  public String getLun()
  {
    return lun;
  }

  public String getID()
  {
    return getKstatPrefix() + instance;
  }
  public static String getKstatPrefix()
  {
    return "ks.";
  }
  public String getInstance()
  {
    return instance;
  }


  /**
   * Get delta kstat statistics.
   * This method keeps track of the previously returned data and uses that to
   * then return delta values.
   *
   * If an error is found it possibly is caused by an NFS instance being
   * remounted.
   *
   */
  private static HashMap last_data = new HashMap(16);
  public Kstat_data getDeltaKstatData(long tod)
  {
    Kstat_data kd_new = new Kstat_data();
    Kstat_data kd_dlt = new Kstat_data();
    kd_new.tod = tod;

    if (Native.getKstatData(kd_new, native_pointer) < 0)
      common.failure("Error reading Kstat data");

    /* Get the previous data, and if found, calculate delta: */
    Long pointer      = new Long(native_pointer);
    Kstat_data kd_old = (Kstat_data) last_data.get(pointer);
    if (kd_old != null)
    {
      kd_dlt.nread    = kd_new.nread    - kd_old.nread    ;
      kd_dlt.nwritten = kd_new.nwritten - kd_old.nwritten ;
      kd_dlt.reads    = kd_new.reads    - kd_old.reads    ;
      kd_dlt.writes   = kd_new.writes   - kd_old.writes   ;
      kd_dlt.wlentime = kd_new.wlentime - kd_old.wlentime ;
      kd_dlt.rtime    = kd_new.rtime    - kd_old.rtime    ;
      kd_dlt.rlentime = kd_new.rlentime - kd_old.rlentime ;
      kd_dlt.elapsed  = kd_new.tod      - kd_old.tod      ;
      kd_dlt.totalio  = kd_new.totalio  - kd_old.totalio  ;
      //common.ptod("kd_dlt.reads: " + kd_dlt.reads + " " + kd_dlt.writes);
    }

    last_data.put(pointer, kd_new);

    return kd_dlt;
  }

  public int compareTo(Object obj)
  {
    InstancePointer ip = (InstancePointer) obj;
    return this.lun.compareTo(ip.lun);
  }


  public static void rebuildNativePointers(Vector pointers)
  {
    /* Throw away all old device stuff and start over */
    Native.closeKstat();
    Native.openKstat();
    Devxlate.clearMnttab();

    for (int i = 0; i < pointers.size(); i++)
    {
      InstancePointer ip = (InstancePointer) pointers.elementAt(i);

      /* Remove this device from the device translation list: */
      //common.ptod("replacing ip+lun: " + " " + ip.instance + " " + ip.lun);
      if (ip.instance.startsWith("nfs"))
        Devxlate.removeFromInstanceList(ip.lun);

      /* Go get the instance name for this device: */
      Vector devxlate_list = Devxlate.get_device_info(ip.lun);

      /* There will be only one response: */
      if (devxlate_list.size() != 1)
        common.failure("Unexpected device translation return: " + devxlate_list.size());

      Object obj = devxlate_list.firstElement();

      /* if there is a message, give up: */
      if (obj instanceof String || obj == null)
      {
        common.ptod("obj: " + obj);
        common.failure("Error reading Kstat data for " + ip.lun + " " + ip.instance + " " + ip.special);
      }
      else
      {
        Devxlate devx = (Devxlate) obj;
        ip.native_pointer = Native.getKstatPointer(devx.instance);
        if (!ip.instance.equals(devx.instance))
          common.ptod("Switching Kstat instance from " + ip.instance + " to " + devx.instance);
        //common.ptod("instance:       " + ip.instance);
        //common.ptod("native_pointer: " + ip.native_pointer);
        //common.ptod("ks:             " + Native.ks_global);
        ip.instance = devx.instance;
      }
    }
  }


  public String toString()
  {
    return String.format("InstancePointer: %s %s %s", lun, instance, special);
  }
}


