package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.*;

import Utils.Bin;
import Utils.Flat_record;


/**
 * This class contains code and information related to each device number
 * that must be replayed.
 */
public class ReplayDevice implements java.io.Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private long device_number;       /* First 16 bits for duplicate#, rest for devnum */
  private long devno;
  private long duplicate;

  private long records;             /* Number of TNF records found in input  */
  private long max_lba = 0;         /* Maximum lba address (bytes)           */
                                    /* Includes xfersize.                    */
  private long min_lba = Long.MAX_VALUE; /* Minimum lba address (bytes)      */
  private int  max_xfersize        = 0;
  private long first_tod           = Long.MAX_VALUE;
  private long last_tod            = 0;
  private boolean reporting_only   = false;

  private long xfersize_counts        = 0;
  private long xfersize_sum_of_deltas = 0;
  private long lba_counts             = 0;
  private long lba_sum_of_deltas      = 0;
  private boolean duplicate_device    = false;

  public  int duplicates_found = 0;

  private int dedup_unit = 0;

  private  ArrayList <ReplayExtent> extent_list  = new ArrayList (32);

  private  ReplayGroup group;      /* Which group owns this device? */

  private  transient Bin split_bin_out = null;



  /**
  * Create a new instance.
  * Instances are stored in a HashMap for faster retrieval.
  */
  public ReplayDevice(ReplayGroup grp, long number)
  {
    ReplayInfo info = ReplayInfo.getInfo();
    erase();
    HashMap <Long, ReplayDevice> device_map = ReplayInfo.getDeviceMap();
    ReplayDevice rdev = device_map.get(new Long(number));


    group          = grp;
    device_number  = number;
    devno          = number;
    reporting_only = grp.isReportingOnly();

    if (rdev != null)
    {
      if (!info.duplicationNeeded())
      {
        common.ptod("Adding duplicate ReplayDevice: " + number);
        common.ptod("Were you planning to use Replay duplication using 'duplication=yes'?");
        common.failure("Adding duplicate ReplayDevice: " + number);
      }

      /* This is duplicate 2 and higher: */
      duplicate      = ++rdev.duplicates_found;
      device_number  = ReplayDevice.addDupToDevnum(device_number, duplicate);
    }

    else if (info.duplicationNeeded())
    {
      /* There is no duplicate yet. This means this is the first time we saw this device. */
      /* This means that the instance we are creating here will be the first duplicate, */
      /* while we also create the non-duplicate: */
      try
      {
        ReplayDevice cloned     = (ReplayDevice) this.clone();
        cloned.duplicates_found = 1;
        cloned.duplicate        = 0;
        if (device_map.put(cloned.device_number, cloned) != null)
          common.failure("Trying to add duplicate ReplayDevice: " + cloned.getDevString());

        common.plog("ReplayDevice added1 " + cloned.getDevString());

        /* The current instance now becomes the duplicate: */
        this.duplicate     = cloned.duplicates_found;
        this.device_number = ReplayDevice.addDupToDevnum(device_number, duplicate);
      }
      catch (Exception e)
      {
        common.failure(e);
      }

    }

    if (device_map.put(device_number, this) != null)
      common.failure("Trying to add duplicate ReplayDevice: " + this.getDevString());

    common.plog("ReplayDevice added2 " + getDevString());
    //common.where(8);
  }

  public boolean isReportingOnly()
  {
    return reporting_only;
  }


  public void copyTo(ReplayDevice target)
  {
    target.records                = records;
    target.max_lba                = max_lba;
    target.min_lba                = min_lba;
    target.max_xfersize           = max_xfersize;
    target.first_tod              = first_tod;
    target.last_tod               = last_tod;
    target.reporting_only         = reporting_only;
    target.xfersize_counts        = xfersize_counts;
    target.xfersize_sum_of_deltas = xfersize_sum_of_deltas;
    target.lba_counts             = lba_counts;
    target.lba_sum_of_deltas      = lba_sum_of_deltas;
    //target.duplicate              = duplicate;
  }

  public void erase()
  {
    records                = 0;
    max_lba                = 0;
    min_lba                = Long.MAX_VALUE;
    max_xfersize           = 0;
    first_tod              = Long.MAX_VALUE;
    last_tod               = 0;
    reporting_only         = false;
    xfersize_counts        = 0;
    xfersize_sum_of_deltas = 0;
    lba_counts             = 0;
    lba_sum_of_deltas      = 0;
    //duplicate       = 0;
  }

  public static long addDupToDevnum(long dev, long dup)
  {
    long newnum = dev | (dup << 48);
    return newnum;
  }

  public long getDeviceNumber()
  {
    return device_number;
  }
  public void countRecords()
  {
    records++;
  }
  public void setRecordCount(long c)
  {
    records = c;
  }
  public long getRecordCount()
  {
    return records;
  }
  public void setMaxLba(long l)
  {
    max_lba = Math.max(max_lba, l);
  }
  public long getMaxLba()
  {
    return max_lba;
  }
  public void setMinLba(long m)
  {
    min_lba = Math.min(min_lba, m);
  }
  public long getMinLba()
  {
    return min_lba;
  }

  public void addExtent(ReplayExtent extent)
  {
    extent_list.add(extent);
  }

  public static int countUsedDevices()
  {
    int count = 0;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = all_device_list.elementAt(i);
      if (!rdev.reporting_only)
        count++;
    }

    return count;
  }

  public void setDuplicate()
  {
    duplicate_device = true;
  }
  public boolean isDuplicate()
  {
    return duplicate_device;
  }

  /**
   * Find a device number, add it if it does not exist.
   */
  public static ReplayDevice findDeviceAndCreate(long number)
  {
    HashMap <Long, ReplayDevice> device_map = ReplayInfo.getDeviceMap();
    ReplayDevice rdev = device_map.get(new Long(number));
    if (rdev != null)
    {
      //common.ptod("Not created ReplayDevice entry for device " + number);
      return rdev;
    }

    /* Create a new device, just for reporting: */
    rdev = ReplayGroup.getReportingOnlyGroup().addDevice(number);

    //common.plog("Created ReplayDevice entry for device " + number);
    //common.where(8);
    return rdev;
  }

  public static ReplayDevice findExistingDevice(long number)
  {
    HashMap <Long, ReplayDevice> device_map = ReplayInfo.getDeviceMap();
    ReplayDevice rdev = device_map.get(new Long(number));
    //if (rdev == null)
    //  common.failure("findExistingDevice(number): Unable to find device %d", number);
    return rdev;
  }


  /**
   * Figure out which SD should be used for this replay lba
   */
  public SD_entry findExtentForLba(Cmd_entry cmd, long replay_lba)
  {
    for (int i = 0; i < extent_list.size(); i++)
    {
      ReplayExtent re = (ReplayExtent) extent_list.get(i);
      if (re.findLbaInExtent(cmd, replay_lba))
        return cmd.sd_ptr;
    }

    return null;
  }

  public SD_entry findExtentForLbaFlat(SD_entry sd_used, Flat_record flat)
  {
    for (ReplayExtent re : extent_list)
    {
      SD_entry sd = re.findLbaInExtentFlat(sd_used, flat);
      if (sd != null)
        return sd;
    }

    return null;
  }
  /*

  public SD_entry findExtentForLba(Cmd_entry cmd, long replay_lba)
  {
    for (int i = 0; i < extent_list.size(); i++)
    {
      ReplayExtent re = (ReplayExtent) extent_list.get(i);
      if (re.findLbaInExtent(cmd, replay_lba))
        return cmd.sd_ptr;
    }

    return null;
  }
  */

  public static long getTotalIoCount()
  {
    long total = 0;
    Vector <ReplayDevice> device_list = ReplayInfo.getInfo().getDeviceList();
    if (ReplayInfo.duplicationNeeded())
      device_list = ReplayInfo.getDupDevs();

    for (int i = 0; i < device_list.size(); i++)
    {
      ReplayDevice rpd = device_list.elementAt(i);
      if (!rpd.reporting_only)
      {
        total += rpd.records;
        //common.ptod("rpd.records: " + rpd.records);
      }
    }

    return total;
  }

  /**
   * Copy the current Flat_record to a device specific Bin file.
   */
  public void writeSplitRecord(Flat_record flat)
  {
    if (split_bin_out == null)
    {
      split_bin_out = new Bin(getSplitFileName());
      split_bin_out.output();
      common.ptod("Created Replay split file: " + split_bin_out.getFileName());
    }

    /* See note at ReplaySplit.readAndSplitTraceFile() for */
    /* possible reasons for this write to fail!            */
    flat.export(split_bin_out);
  }

  /**
   * Close all the bin files used for replay splitting.
   * 'reporting only' does not have a file.
   */
  public static void closeSplitFiles()
  {
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      if (all_device_list.elementAt(i).split_bin_out != null)
        all_device_list.elementAt(i).split_bin_out.close();
    }
  }

  /**
   * Create file name for split file.
   * Decided to have them automatically gzipped. This adds cpu time, but it cuts
   * the amount of i/o done by a factor of almost five. This being a storage
   * performance test, removing extra i/o will be beneficial.
   * If we change our mind, fine.
   */
  public String getSplitFileName()
  {
    String txt;
    if (ReplayInfo.compress())
      txt = String.format("%s%s%s%05d.bin.gz",
                          ReplayInfo.getSplitDirectory(),
                          File.separator,
                          getSplitFileNamePrefix(),
                          devno);
    else
      txt = String.format("%s%s%s%05d.bin",
                          ReplayInfo.getSplitDirectory(),
                          File.separator,
                          getSplitFileNamePrefix(),
                          devno);

    return txt;
  }
  public static String getSplitFileNamePrefix()
  {
    return "replay_dev_";
  }
  public void setLastTod(long l)
  {
    last_tod = l;
  }
  public long getLastTod()
  {
    return last_tod;
  }
  public void setFirstTod(long l)
  {
    first_tod = l;
  }
  public long getFirstTod()
  {
    return first_tod;
  }
  public static long getAllDevicesLastTod()
  {
    long tod = 0;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rpd = all_device_list.elementAt(i);
      if (!rpd.reporting_only)
        tod = Math.max(tod, rpd.last_tod);
    }

    return tod;
  }
  public static long getAllDevicesFirstTod()
  {
    long tod = Long.MAX_VALUE;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rpd = all_device_list.elementAt(i);
      if (!rpd.reporting_only)
        tod = Math.min(tod, rpd.first_tod);
    }

    return tod;
  }

  public static int getAllDevicesMaxXfersize()
  {
    int xfersize = 0;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (ReplayDevice rdev : all_device_list)
    {
      if (!rdev.reporting_only)
        xfersize = Math.max(xfersize, rdev.max_xfersize);
    }

    if (xfersize == 0)
      common.failure("Unknown maximum xfersize");
    return xfersize;
  }
  public void setMaxXfersize(int x)
  {
    max_xfersize = Math.max(max_xfersize, x);
  }
  public int getMaxXfersize()
  {
    return max_xfersize;
  }


  /**
   * For Dedup adjust xfersize to be a multiple of dedupunit.
   * BTW: we only do this for writes. Reads we just leave alone.
   *
   * Count the deltas of the adjustments.
   */
  public void adjustDedupXfersizeFlat(Flat_record flat)
  {
    if (!Dedup.isDedup() || flat.flag == 1)
      return;

    int xfersize = flat.xfersize;

    if (xfersize <= dedup_unit)
      xfersize = dedup_unit;
    else
    {
      int units = xfersize / dedup_unit;
      int diff  = xfersize - units * dedup_unit;
      if (diff < (dedup_unit >> 1))
        xfersize = units * dedup_unit;
      else
        xfersize = (units+1) * dedup_unit;
      //common.ptod("units: " + units+ " " + diff);
    }

    /* No adjustment needed? */
    if (xfersize - flat.xfersize == 0)
      return;

    //common.ptod("adjustDedupXfersize() old: %6d new: %6d delta: %6d",
    //            rent.xfersize, xfersize, xfersize - rent.xfersize);

    xfersize_counts++;
    xfersize_sum_of_deltas += (xfersize - flat.xfersize);
    flat.xfersize = xfersize;
  }

  /**
   * For Dedup adjust xfersize to be a multiple of dedupunit.
   * BTW: we only do this for writes. Reads we just leave alone.
   *
   * Count the deltas of the adjustments.
   */
  public void adjustDedupLbaFlat(Flat_record flat)
  {
    if (!Dedup.isDedup() || flat.flag == 1)
      return;

    long lba   = flat.lba;
    long delta = lba % dedup_unit;

    if (delta < (dedup_unit >> 1))
      delta = delta * -1;
    else
      delta = dedup_unit - delta;

    /* No adjustment needed? */
    if (delta == 0)
      return;

    //common.ptod("adjustDedupLba() old: %8d new: %8d delta: %8d",
    //            rent.lba, rent.lba + delta, delta);

    lba_counts++;
    lba_sum_of_deltas += delta;
    flat.lba += delta;
  }



  /**
   * Report statistics for the devices that were found
   */
  public static void reportNumbers()
  {
    long xfersize_counts        = 0;
    long xfersize_sum_of_deltas = 0;
    long lba_counts             = 0;
    long lba_sum_of_deltas      = 0;
    long total_bytes_needed     = 0;

    boolean missing = false;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    if (ReplayInfo.duplicationNeeded())
      all_device_list = ReplayInfo.getDupDevs();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = all_device_list.elementAt(i);
      if (!rdev.isReportingOnly())
        total_bytes_needed += rdev.getMaxLba();

      if (!rdev.isReportingOnly() && rdev.getRecordCount() == 0)
      {
        missing = true;
        common.ptod("Replay was requested for device number %s, but no replay " +
                    "records were found for this device. ", rdev.getDevString());
        continue;
      }

      String tmp =
      String.format("Replay dev: %16s ios: %,10d lba: %7s - %7s Max xf: %7s adjx: %6d adjl: %6d rg=%s",
                    rdev.getDevString(),
                    rdev.getRecordCount(),
                    (rdev.getMinLba() == Long.MAX_VALUE) ? 0  : FileAnchor.whatSize1(rdev.getMinLba()),
                    FileAnchor.whatSize1(rdev.getMaxLba()),
                    FileAnchor.whatSize1(rdev.getMaxXfersize()),
                    (rdev.xfersize_counts > 0) ? rdev.xfersize_sum_of_deltas / rdev.xfersize_counts : 0,
                    (rdev.lba_counts      > 0) ? rdev.lba_sum_of_deltas      / rdev.lba_counts : 0,
                    rdev.group.getName());

      /* Some times we have too many 'reporting' devices, clogs up the screen: */
      if (rdev.reporting_only)
        common.plog(tmp);
      else
        common.ptod(tmp);

      xfersize_counts        += rdev.xfersize_counts;
      xfersize_sum_of_deltas += rdev.xfersize_sum_of_deltas;
      lba_counts             += rdev.lba_counts;
      lba_sum_of_deltas      += rdev.lba_sum_of_deltas;

      /* Need to know the maximum xfersize: */
      SD_entry.trackAllSdXfersizes(rdev.getMaxXfersize());
    }

    long total_bytes_available = 0;
    long total_bytes_size      = 0;
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      if (sd.isActive())
      {
        total_bytes_available += sd.end_lba;
        total_bytes_size      += sd.psize;
      }
    }

    common.ptod("Total bytes needed:    %s", FileAnchor.whatSize(total_bytes_needed));
    common.ptod("Total bytes available: %s (%s)", FileAnchor.whatSize(total_bytes_available),
                FileAnchor.whatSize(total_bytes_size));

    if (Dedup.isDedup())
    {
      common.ptod("");
      common.ptod("Dedup requires that all data transfer sizes and lbas for write operations");
      common.ptod("are a multiple of dedupunit=%d.", 12345);
      common.ptod("Average xfersize adjustment: %d bytes.",
                  (xfersize_counts > 0 ) ? xfersize_sum_of_deltas / xfersize_counts : 0);
      common.ptod("Average lba adjustment:      %d bytes.",
                  (lba_counts > 0) ? lba_sum_of_deltas    / lba_counts : 0);
      common.ptod("");
    }

    if (missing && !common.get_debug(common.IGNORE_MISSING_REPLAY) &&
        !ReplayInfo.duplicationNeeded())
      common.failure("One or more requested devices found without replay records");

  }


  /**
   * Create a list of device numbers that are targeting a specific SD.
   */
  public static long[] getDeviceNumbersForSd(String sdname)
  {
    //common.ptod("sdname: " + sdname);
    Vector <ReplayDevice> sds         = new Vector(16);
    Vector <ReplayDevice> device_list = ReplayInfo.getInfo().getDeviceList();
    if (ReplayInfo.duplicationNeeded())
      device_list = ReplayInfo.getDupDevs();

    //ReplayDevice.printDevices("slave");

    for (int i = 0; i < device_list.size(); i++)
    {
      ReplayDevice rdev = device_list.elementAt(i);
     //common.ptod("getDeviceNumbersForSd: " + rdev);
      for (int j = 0; j < rdev.extent_list.size(); j++)
      {
        if ( rdev.extent_list.get(j).getSdName().equals(sdname))
          sds.add(rdev);
      }
    }

    long[] numbers = new long[ sds.size() ];
    for (int i = 0; i < sds.size(); i++)
    {
      ReplayDevice rdev = sds.elementAt(i);
      numbers[i] = rdev.getDeviceNumber();
    }

    return numbers;
  }

  public long getDuplicateNumber()
  {
    return duplicate;
  }


  public String getDevString()
  {
    // paranoia check:
    if (device_number != ReplayDevice.addDupToDevnum(devno, duplicate))
    {
      //common.ptod(String.format("%3d.%03d %016x", devno, duplicate, device_number));
      common.failure("Illegal device number: %016x %08x %08x", device_number, devno, duplicate);
    }

    if (ReplayInfo.duplicationNeeded())
      return String.format("%8d.%03d", devno, duplicate);
    else
      return String.format("%8d", devno);
  }

  public static void printDevices(String label)
  {
    common.ptod("");
    ReplayInfo info = ReplayInfo.getInfo();
    Vector <ReplayDevice> devices = info.getDeviceList();
    for (ReplayDevice rdev : devices)
    {
      common.ptod("printDevices %s %s lba: %12d records: %6d",
                  label, rdev.getDevString(), rdev.getMaxLba(), rdev.getRecordCount());
    }
  }

  public String toString()
  {
    return getDevString();
  }

  public static void main(String[] args)
  {
  }
}
