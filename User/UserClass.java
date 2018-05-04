package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.HashMap;

import Vdb.Cmd_entry;
import Vdb.SlaveJvm;
import Vdb.SlaveWorker;
import Vdb.common;

public class UserClass implements UserInterface
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private WorkloadInfo class_workinfo;

  private static Object shared_lock   = new Object();
  private static Object shared_object = null;

  private static HashMap <String, String> one_time_message_map = new HashMap(16);


  public UserClass()
  {
    /* On a Vdbench slave, do a one time creation of an Object that is */
    /* shared between all UserClass instances:                         */
    /* (This code is here as an example. It should never be activated) */
    // obsolete, but kept around
    if (isThisSlave())
    {
      //synchronized (getSharedLock())
      //{
      //  if (getSharedObject() == null)
      //    setSharedObject(new Integer(9876));
      //}
    }

    /* Clear the map of message labels sent out. It's static, but that's OK */
    one_time_message_map = new HashMap(16);
  }

  /**
   * Parse the input parameters specified in the parameter file.
   *
   * This method is called twice: once in the Vdbench Master, solely for parameter
   * validation, and then again on the Vdbench Slave immediately after the
   * instance of this class has been created.
   *
   * Any data stored in this current instance will be lost when called from the
   * master, however, when called from a slave this data will be available for ont
   * current Run Definitoon (RD).
   */
  public boolean parser(String[] parms)
  {
    return true;
  }

  /**
   * Preparation for the generation of a workload.
   * Called once per WD/SD pair per run.
   */
  public UserDeviceInfo initialize(WorkloadInfo wi)
  {
    UserDeviceInfo di = UserDeviceInfo.findDeviceInfo(wi.getSd().sd_name);
    if (di == null)
      di = new UserDeviceInfo(wi.getSd());

    return di;
  }

  /**
   * Generate workload. Do not return until the last i/o has been scheduled, or
   * until SlaveJvm.isWorkloadDone().
   */
  public boolean generate()
  {
    common.failure("Method %s.generate() does not exist.", this.getClass().getName());
    return true;
  }

  /**
   * Called before and after an i/o is sent to JNI code.
   *
   * I should replace Cmd_entry with something more 'hidden'!
   */
  public boolean preIO(UserCmd ucmd)
  {
    common.failure("Method %s.preIO() does not exist.", this.getClass().getName());
    return true;
  }

  public boolean postIO(UserCmd ucmd)
  {
    common.failure("Method %s.postIO() does not exist.", this.getClass().getName());
    return true;
  }

  public void passData(String data)
  {
    common.failure("Method %s.passData() does not exist.", this.getClass().getName());
  }

  public WorkloadInfo getWorkloadInfo()
  {
    return class_workinfo;
  }
  public void setWorkloadInfo(WorkloadInfo wi)
  {
    class_workinfo = wi;
  }

  /**
   * Let Vdbench know if the generate method needs to be called.
   */
  public boolean isGenerateNeeded()
  {
    return true;
  }

  public boolean isWorkloadDone()
  {
    return SlaveJvm.isWorkloadDone();
  }
  public void setWorkloadDone()
  {
    SlaveJvm.setWorkloadDone(true);
  }

  public final boolean isThisSlave()
  {
    return SlaveJvm.isThisSlave();
  }

  /**
   * Lock to be used when users want to manipulate the user defined shared_object.
   */
  public final Object getSharedLock()
  {
    return shared_lock;
  }

  public final void setSharedObject(Object s)
  {
    shared_object = s;
  }
  public final Object getSharedObject()
  {
    return shared_object;
  }

  public Object[] getDeviceList()
  {
    return UserDeviceInfo.getDeviceList();
  }

  public boolean xxgetIntervalDataForMaster()
  {
    return true;
  }

  public static String getSlaveLabel()
  {
    return SlaveJvm.getSlaveLabel();
  }

  /**
   * Send a message only once to the console.
   * This is needed to prevent all independently running UserClass instances from
   * flooding the console.
   * And of course, this is PER slave, not global.
   */
  public static void sendOneMessageToConsole(String label, String format, Object ... args)
  {
    synchronized(one_time_message_map)
    {
      if (one_time_message_map.get(label) != null)
        common.plog("Duplicate message suppressed: " + format, args);
      else
      {
        one_time_message_map.put(label, label);
        sendMessageToConsole(format, args);
      }
    }
  }

  public static void sendMessageToConsole(String format, Object ... args)
  {
    SlaveJvm.sendMessageToConsole(String.format(format, args));
  }

  /**
   * A message that is important enough to go to summary.html also is important
   * enough to go to the console, so on the master it ends up in both p
   */
  public static void sendOneMessageToSummary(String label, String format, Object ... args)
  {
    synchronized(one_time_message_map)
    {
      if (one_time_message_map.get(label) != null)
        common.plog("Duplicate message suppressed: " + format, args);
      else
      {
        one_time_message_map.put(label, label);
        SlaveJvm.sendMessageToSummary(String.format(format, args));
      }
    }
  }


  public static void sendMessageToSummary(String format, Object ... args)
  {
    SlaveJvm.sendMessageToSummary(String.format(format, args));
  }

  public static int getSlaveCount()
  {
    return SlaveWorker.work.slave_count;
  }

  /*
  public boolean endOfInterval(Object parm)
  {
    common.failure("May not be called at this time");
    return true;
  }
  */
}
