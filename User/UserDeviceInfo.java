package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.HashMap;

import Vdb.SD_entry;
import Vdb.SlaveWorker;
import Vdb.common;


/**
 * One Object like this is created for each different SD used.
 * It is created by WorkloadInfo.
 * The user can use class inheritance to do with this whatever he pleases.
 */
public class UserDeviceInfo
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String  sd_name;
  private String  sd_lun;
  private long    sd_size;
  private long    next_lba;
  private long    high_lba;
  private boolean use_block_zero;


  private static HashMap <String, UserDeviceInfo> device_map = new HashMap(32);


  public UserDeviceInfo()
  {
  }

  public UserDeviceInfo(SD_entry sd)
  {
    sd_name  = sd.sd_name;
    sd_lun   = sd.lun;
    sd_size  = sd.end_lba;
    if (use_block_zero = sd.canWeUseBlockZero())
      next_lba = 0;
    else
      next_lba = 4096;

    if (device_map.put(sd_name, this) != null)
      common.failure("DeviceInfo: duplicate created for sd=%s", sd_name);
  }

  public boolean  canWeUseBlockZero()
  {
    return use_block_zero;
  }
  public static UserDeviceInfo findDeviceInfo(String sd)
  {
    return device_map.get(sd);
  }

  public static void clearDeviceList()
  {
    device_map = new HashMap(32);
  }
  public String getLun()
  {
    return sd_lun;
  }
  public String getSdName()
  {
    return sd_name;
  }
  public long obsolete_getHighLba()
  {
    return high_lba;
  }
  public long getSdSize()
  {
    return sd_size;
  }
  public static int getSdCount()
  {
    return SlaveWorker.sd_list.size();
  }
  public static int getRelativeSdNumber(String sdname)
  {
    for (int i = 0; i < SlaveWorker.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) SlaveWorker.sd_list.elementAt(i);
      if (sd.sd_name.equals(sdname))
        return i;
    }

    common.failure("getRelativeSdNumber(): unknown sd name: " + sdname); 
    return 0;
  }
  public static Object[] getDeviceList()
  {
    Object[] obj = device_map.values().toArray();
    //for (int i = 0; i < obj.length; i++)
    //  common.ptod("obj: " + obj[i]);
    return obj;
  }

  public synchronized long getNextSeqLba(long xfersize)
  {
    if (next_lba + xfersize > sd_size)
      next_lba = (use_block_zero) ? 0 : xfersize;

    long temp = next_lba;
    next_lba += xfersize;

    return temp;
  }


  /**
   * Keep track of the highest lba used.
   * Code runs synchronized to allow multiple callers to update this value.
   */
  public synchronized void obsolete_setHighLba(long lba, int xfersize)
  {
    if (lba == 0 && !use_block_zero)
      common.failure("WorkloadInfo.setHighLba(): trying to access block zero. " +
                     "sd=%s,lun=%s", sd_name, sd_lun);

    high_lba = Math.max(high_lba, lba + xfersize);

    if (high_lba > sd_size)
      common.failure("WorkloadInfo.setHighLba(): trying to get beyond the end of " +
                     "the lun or file. sd=%s,lun=%s,size=%d",
                     sd_name, sd_lun, sd_size);
  }
}
