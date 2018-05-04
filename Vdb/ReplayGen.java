package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;

import User.UserClass;
import User.UserCmd;
import User.UserDeviceInfo;
import User.WorkloadInfo;

import Utils.Bin;
import Utils.ClassPath;
import Utils.Fget;
import Utils.Flat_record;


/**
 * This class handles the UserClass interface for Replay.
 */
public class ReplayGen extends UserClass
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private SD_entry       sd_used;
  private UserDeviceInfo dev_info;
  private WorkloadInfo   work_info;

  private Bin[]         split_bin_files;
  private Flat_record[] split_records;
  private int           lowest_index;
  private long          begin_offset;

  private long          last_start_time   = 0;
  private int           repeat_count      = 0;
  private long          repeat_tod_adjust = 0;




  public UserDeviceInfo initialize(WorkloadInfo wi)
  {
    work_info = wi;
    dev_info  = super.initialize(wi);
    sd_used   = wi.getSd();
    //common.ptod("ReplayGen, initializing for sd=%s dup# %d: ",
    //             sd_used.sd_name, sd_used.duplicate_number);

    /* Open the input replay files: */
    openSplitInputFiles();

    /* Subtract start time of first io (which allows us to ignore a period */
    /* of silence in the trace where the devices don't do anything)        */
    /* This caused some confusion when comparing a second by second replay */
    /* with the original tnfe detailed output. The replay was .3 seconds   */
    /* off, causing mismatch in second by second detail.                   */
    /* Now making sure we truncate the first tod to one second:            */
    begin_offset = (ReplayDevice.getAllDevicesFirstTod() / 1000000) * 1000000;

    return dev_info;
  }


  /**
   * Generate replay i/o for this SD.
   * We read the split replay files for those device numbers that target the
   * current SD.
   *
   * We can have a case where the block straddles two SDs.  The first piece of
   * the block is processed if it belongs to the current SD, though the xfersize
   * is adjusted to what fits.  The second piece is ignored.  This is such a
   * rare occurence that it was not worth the effort to support it.
   *
   */
  public boolean generate()
  {
    long last_device     = Long.MAX_VALUE;
    ReplayDevice rdev    = null;
    double replay_adjust = ReplayInfo.getAdjustValue();
    work_info.setFirstStartDelta(0);
    long ios_scheduled = 0;

    boolean debug = false; //sd_used.sd_name.equals("sd2");
    long count = 0;

    while (!isWorkloadDone())
    {
      if (debug && count++ > 20)
        System.exit(777);;
      //common.ptod("lowest_index: " + sd_used.sd_name + " " + lowest_index );
      Flat_record flat = split_records[lowest_index];
      flat.start -= begin_offset;

      //this should find the DUPLICATE rdevs!!!!
      if (ReplayInfo.duplicationNeeded())
        flat.device = ReplayDevice.addDupToDevnum(flat.device, (sd_used.duplicate_number));

      /* Find the proper ReplayDevice, bypassing the hash search if possible */
      if (last_device != flat.device)
      {
        //common.ptod("flat.device: " + flat.device);
        rdev = ReplayDevice.findExistingDevice(flat.device);
        last_device = flat.device;
      }
      //common.ptod("flat.device: %08x", flat.device);
      //


      /* Translate the lba into the proper SD and lba combination. */
      /* If the SD does not match it must be a straddling block. */
      if (debug) common.where();
      if (rdev.findExtentForLbaFlat(sd_used, flat) != sd_used)
      {
        if (debug) common.where();
        //common.ptod("mismatch sd_used: " + sd_used.sd_name);
        if (!getNextReplayRecord())
          break;
        continue;
      }

      if (debug) common.where();


      /* Don't touch first block: */
      /* When we have a replay device whose only reference is to block 0 */
      /* we never signal eof from WG_entry.subtract_io(). To avoid lots  */
      /* of new code let's just change the i/o to block 1                */
      if (flat.lba == 0)
        flat.lba = flat.xfersize = 4096;

      /* Interarrival time adjusts when iorate= is requested: */
      flat.start *= replay_adjust;

      /* Adjust start time for possible repeat loops: */
      flat.start += repeat_tod_adjust;

      /* Calculate the delta between the new and the previous io: */
      long delta = flat.start - last_start_time;

      /* Schedule the i/o for execution: */
      if (!work_info.scheduleIO(delta, flat.lba, flat.xfersize, flat.flag == 1))
        break;
      ios_scheduled++;
      last_start_time = flat.start;

      /* Keep track of how many ios are outstanding, so that we */
      /* later on after subtract_io() can trigger 'end of run': */
      //work_info.setNotEOF();

      if (!getNextReplayRecord())
        break;
    }

    /* Tell this workload generator that no more i/o will be scheduled: */
    work_info.setEOF(ios_scheduled);

    closeSplitInputFiles();

    //common.ptod("ios_scheduled for %-8s: %8d", sd_used.sd_name, ios_scheduled);
    if (ios_scheduled == 0)
      return false;

    return true;
  }

  public boolean preIO(UserCmd ucmd)
  {
    return true;
  }
  public boolean postIO(UserCmd ucmd)
  {
    return true;
  }

  /**
   * Figure out which device numbers are being replayed by this SD, and then
   * open a Bin file for each.
   * Then determine which relative start time of the ios in those 'n' files is
   * the lowest.
   */
  private void openSplitInputFiles()
  {
    long[] numbers   = ReplayDevice.getDeviceNumbersForSd(sd_used.sd_name);
    split_bin_files  = new Bin[numbers.length];
    split_records    = new Flat_record[numbers.length];

    for (int i = 0; i < numbers.length; i++)
    {
      ReplayDevice rdev = ReplayDevice.findDeviceAndCreate(numbers[i]);

      common.ptod("rdev.getSplitFileName(): " + rdev.getSplitFileName());
      split_bin_files[i] = new Bin(rdev.getSplitFileName());
      split_bin_files[i].input();
      split_bin_files[i].read_record();

      split_records[i] = new Flat_record();
      emportBinRecord(i);
    }

    /* Figure out who the first lowest is: */
    lowest_index = 0;
    for (int i = 1; i < numbers.length; i++)
    {
      if (split_records[i].start < split_records[lowest_index].start)
        lowest_index = i;
    }
  }


  // This should be a --real-- close
  private void closeSplitInputFiles()
  {
    long[] numbers  = ReplayDevice.getDeviceNumbersForSd(sd_used.sd_name);

    for (int i = 0; i < split_bin_files.length; i++)
    {
      split_bin_files[i].close();
    }
  }


  /**
   * Get the next replay record in start time sequence.
   *
   * Theoretical problem:  if multiple files have lots of equal start times, the
   * lowest file always goes first.  If this ends up happening we may have to
   * include a roundrobin method here.
   */
  private boolean getNextReplayRecord()
  {
    /* Read the next record from the file whose record we just scheduled an i/o for: */
    if (!split_bin_files[lowest_index].read_record())
      split_records[lowest_index].start = Long.MAX_VALUE;
    else
      emportBinRecord(lowest_index);

    /* Look for the record with the lowest timestamp: */
    lowest_index = 0;
    for (int i = 1; i < split_records.length; i++)
    {
      if (split_records[i].start < split_records[lowest_index].start)
        lowest_index = i;
    }

    /* Normal operation: return 'true' if we're not at EOF: */
    if (split_records[lowest_index].start != Long.MAX_VALUE)
      return true;

    /* If all files are EOF, check to see if we need to 'rewind' to */
    /* allow us to repeat this whole thing over again:              */
    repeat_count++;
    //common.ptod("getRepeatCount(): " + info.getRepeatCount() + " " +
    //            repeat_count);
    if (ReplayInfo.getRepeatCount() == repeat_count)
      return false;

    /* 'Rewind': */
    closeSplitInputFiles();
    openSplitInputFiles();

    //this below should use the last start time of the COMPLETE replay, not just
    //  from this one split file!!!!!

    /* Remember the start time when we rolled over. Round it upwards to one second */
    /* to make the second-by-second statistics repeatable:                         */
    repeat_tod_adjust = last_start_time;
    repeat_tod_adjust = (repeat_tod_adjust / 1000000 * 1000000) + 1000000;

    return true;
  }


  /**
   * 'Generalized' Bin_record.emport to facilitate a 'stagger' adjustment to the
   * device start time.
   */
  private void emportBinRecord(int index)
  {
    split_records[index].emport(split_bin_files[index]);
    long stagger = (sd_used.duplicate_number - 1) * ReplayInfo.getStagger();
    //common.ptod("stagger: " + stagger);
    split_records[index].start += stagger;
  }
}


