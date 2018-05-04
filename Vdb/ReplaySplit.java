package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.Vector;

import Utils.*;


/**
 * Read and interpret contents of replay file.
 * Information needed:
 * - device numbers
 * - maximum xfersize
 * - maximum lba
 * - adjustments for Dedup to lba and xfersiz.
 *
 * File will also be split into one file per device number. These files then
 * will later on be read by the Replay thread that it is only servicing those
 * SDs that will be replaying the device numbers.
 */
public class ReplaySplit
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Read the trace file and split.
   * The split will only be done when any of the Replay parameters used have
   * changed and/or changes have been made to the replay file or the split
   * files.
   *
   * Note:
   *
   * This method is called by the first slave on a host.
   * If we are running multi-host this will be run multiple times.
   * If the target split directory is on local storage, wonderful.
   * If the split directory is the same across all hosts, then in theory the
   * slaves CAN step on each other, like one slave that is running behind
   * deleting a file that the other slave is writing to. That then could cause
   * the other slave to have a failed write.
   * In that case they'll just have to use local storage.
   *
   */
  public static void readAndSplitTraceFile()
  {
    long   records_read      = 0;
    String fname             = ReplayInfo.getReplayFile();
    String split_dir         = ReplayInfo.getSplitDirectory();
    long   low_start_filter  = ReplayInfo.getLowFilter();
    long   high_start_filter = ReplayInfo.getHighFilter();
    long   lba_fold_size     = ReplayInfo.getFoldSize();

    boolean rebuild = doWeNeedToReadReplayFile();
    common.ptod("doWeNeedToReadReplayFile: " + rebuild);
    if (!rebuild)
    {
      common.ptod("Reading replay file bypassed: " + fname);
      return;
    }


    /* Cleanup stuff: */
    deleteFiles();
    resetCounters();

    /* Read all binary records: */
    common.ptod("+");
    common.ptod("+Reading replay file " + fname);
    common.ptod("+");
    Bin bin = new Bin(fname);
    bin.input();

    Flat_record flat = new Flat_record();
    Signal signal    = new Signal(5);  // must be smaller than SHORTER_HEARTBEAT

    read_record:
    while (bin.read_record())
    {
      /* A large replay file can take quite a while to finish. Keep the     */
      /* heartbeat alive once every 5 seconds:                              */
      /* It is no perfect solution, because the heartbeat messages coming   */
      /* from the master are NOT picked up by this slave because it's busy  */
      /* running this ReplaySplit. There therefore will be a queue of these */
      /* messages waiting for the slave. If there is a max queue depth and  */
      /* it is reached this split is running too slow anyway.               */
      if (++records_read % 100000 == 0)
      {
        if (signal.go())
        {
          SlaveJvm.sendMessageToMaster(SocketMessage.HEARTBEAT_MESSAGE);
          SlaveJvm.getMasterSocket().setlastHeartBeat();
          SlaveJvm.sendMessageToConsole("Splitting replay file. %,d records processed.", records_read);
        }
      }

      flat.emport(bin);

      /* Ignore records beyond the requested start and end time: */
      if (flat.start < low_start_filter)
        continue;
      if (flat.start > high_start_filter)
        break;

      /* Allow for folding of lba: */
      if (lba_fold_size != Long.MAX_VALUE)
        flat.lba %= lba_fold_size;

      /* Some lba0 reads of 36 bytes have shown up. They are likely disguised */
      /* diagnostics reads. Xfersizes must be multiple of 512 bytes, so I am  */
      /* making sure that is the case.                                        */
      flat.xfersize = (flat.xfersize + 511) & ~0x1ff;


      /* Look for device: */
      ReplayDevice rdev = ReplayDevice.findDeviceAndCreate(flat.device);
      rdev.countRecords();

      /* Dedup requires everything to be on dedupunit boundaries: */
      /* Obsolete as of 50405 since I am allowing any xfersize
      //rdev.adjustDedupXfersizeFlat(flat);
      //rdev.adjustDedupLbaFlat(flat);

      /* calculate stuff: */
      rdev.setMinLba(flat.lba);
      rdev.setMaxLba(flat.lba + flat.xfersize);
      rdev.setMaxXfersize(flat.xfersize);

      if (rdev.isReportingOnly())
        continue read_record;

      /* Need to remember the detailed time range for each device: */
      if (rdev.getFirstTod() == Long.MAX_VALUE)
        rdev.setFirstTod(flat.start);
      rdev.setLastTod(flat.start);

      /* Copy the (possibly modified) record to the device specific file: */
      rdev.writeSplitRecord(flat);
      //if (flat.device > 1)
      //  common.ptod("flat: %08x", flat.device);
    }


    bin.close();
    ReplayDevice.closeSplitFiles();
    saveReplayFileInformation();
    common.ptod("Reading replay file completed: " + fname);
  }


  /**
   * Delete any existing split files in the target directory.
   */
  private static void deleteFiles()
  {
    String dir = ReplayInfo.getSplitDirectory();
    File[] files = new File(dir).listFiles();
    for (int i = 0; files != null && i < files.length; i++)
    {
      if (files[i].getName().startsWith(ReplayDevice.getSplitFileNamePrefix()))
        files[i].delete();
    }
  }

  /** While looking to see if we have to split the files again we may have left
   *  some things that must be cleared if we end up having the split anyway.
  */
  private static void resetCounters()
  {

    Vector device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < device_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) device_list.elementAt(i);
      //common.ptod("rdev: " + rdev.getDevString() + " " + rdev.isReportingOnly());
      rdev.erase();
    }
  }


  /**
   * To avoid the overhead incurred with the constant reading of the replay
   * file and writing the split files we need to remember everything that
   * relates to the contents of both the Replay file and the split files.
   * Any change requires us to do it over again.
   */
  private static void saveReplayFileInformation()
  {
    Fput fp = new Fput(ReplayInfo.getSplitDirectory(), "replay_file_status.txt");

    long fsize = new File(ReplayInfo.getReplayFile()).length();
    fp.println("replay_file %s %d",    ReplayInfo.getReplayFile(), fsize);
    fp.println("low_start_filter %d",  ReplayInfo.getLowFilter());
    fp.println("high_start_filter %d", ReplayInfo.getHighFilter());
    fp.println("lba_fold_size_mb %d",  ReplayInfo.getFoldSize());
    fp.println("compress %b",          ReplayInfo.compress());

    Vector device_list = ReplayInfo.getInfo().getDeviceList();
    if (ReplayInfo.duplicationNeeded())
      device_list = ReplayInfo.getNodupDevs();

    for (int i = 0; i < device_list.size(); i++)
    {
      ReplayDevice rpd = (ReplayDevice) device_list.elementAt(i);

      /* Only write devices with work. This eliminates us including those */
      /* devices that were requested but not found from the file.         */
      if (rpd.getRecordCount() > 0)
      {
        fp.println("replay_device %12d rep %d fsize%12d minlba %12d maxlba %12d maxxfer %7d " +
                   "first %12d last %12d records %12d dup# %4d",
                   rpd.getDeviceNumber(),
                   (rpd.isReportingOnly()) ? 1 : 0,
                   new File(rpd.getSplitFileName()).length(),
                   rpd.getMinLba(), rpd.getMaxLba(), rpd.getMaxXfersize(),
                   rpd.getFirstTod(),
                   rpd.getLastTod(),
                   rpd.getRecordCount(),
                   rpd.getDuplicateNumber());
      }
    }

    fp.close();
  }


  /**
   * Check to see if we need to split the files again.
   */
  private static boolean doWeNeedToReadReplayFile()
  {

    if (common.get_debug(common.FORCE_REPLAY_SPLIT))
    {
      common.plog("doWeNeedToReadReplayFile(): Forced by debug parameter");
      return true;
    }

    String dir = ReplayInfo.getSplitDirectory();
    if (!new File(dir, "replay_file_status.txt").exists())
    {
      common.plog("doWeNeedToReadReplayFile(): no replay_file_status.txt file found");
      return true;
    }

    String[] lines = Fget.readFileToArray(dir, "replay_file_status.txt");
    for (int i = 0; i < lines.length; i++)
    {
      String   line = lines[i];
      //common.plog("line: " + line);
      String[] split = line.split(" +");
      if (line.startsWith("replay_file"))
      {
        if (!split[1].equals(ReplayInfo.getReplayFile()))
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched replay file name.");
          common.plog("Expected: " + ReplayInfo.getReplayFile());
          common.plog("Received: " + split[1]);
          return true;
        }

        long size = new File(ReplayInfo.getReplayFile()).length();
        if (size != Long.parseLong(split[2]))
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched replay file size.");
          common.plog("Expected: " + size);
          common.plog("Received: " + split[2]);
          return true;
        }
        continue;
      }

      else if (line.startsWith("lba_fold_size_mb"))
      {
        if (Long.parseLong(split[1]) != ReplayInfo.getFoldSize())
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched fold size.");
          common.plog("Expected: " + ReplayInfo.getFoldSize());
          common.plog("Received: " + split[1]);
          return true;
        }
        continue;
      }

      else if (line.startsWith("low_start_filter"))
      {
        if (Long.parseLong(split[1]) != ReplayInfo.getLowFilter())
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched low filter.");
          common.plog("Expected: " + ReplayInfo.getLowFilter());
          common.plog("Received: " + split[1]);
          return true;
        }
        continue;
      }

      else if (line.startsWith("high_start_filter"))
      {
        if (Long.parseLong(split[1]) != ReplayInfo.getHighFilter())
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched high filter.");
          common.plog("Expected: " + ReplayInfo.getHighFilter());
          common.plog("Received: " + split[1]);
          return true;
        }
        continue;
      }

      /* The main reason for including 'compress' is the fact that there */
      /* is a bug in what is split when we switch compress options.      */
      /* It was just easier to not allow switching from yes to no or vv. */
      else if (line.startsWith("compress"))
      {
        if (split[1].equalsIgnoreCase("true")  && !ReplayInfo.compress() ||
            split[1].equalsIgnoreCase("false") && ReplayInfo.compress())
        {
          common.plog("doWeNeedToReadReplayFile(): mismatched in compress= parameter.");
          return true;
        }
        continue;
      }

      else if (!line.startsWith("replay_device"))
      {
        common.plog("doWeNeedToReadReplayFile(): Unknown input: " + line);
        return true;
      }

      /* We now have a 'replay_device' line: */
      long devnum            = Long.parseLong(split[1]);
      boolean reporting_only = (findLabel(split, "rep") == 1) ? true : false;
      ReplayDevice rdev      = ReplayDevice.findExistingDevice(devnum);

      if (!ReplayInfo.duplicationNeeded() && !reporting_only && rdev == null)
      {
        common.plog("doWeNeedToReadReplayFile(): device %d switched to 'reporting only'", devnum);
        return true;
      }

      //devices added here from reading the split file must be removed before we split again!//

      /* Now create the ReplayDevice entry if needed: */
      rdev = ReplayDevice.findDeviceAndCreate(devnum);
      String splitname = rdev.getSplitFileName();

      /* The replay split file must exist (except for reporting only): */
      if (!reporting_only && !new File(splitname).exists())
      {
        common.plog("doWeNeedToReadReplayFile(): Replay split file does not exist: " + splitname);
        return true;
      }

      /* Check the split file size: */
      if (!reporting_only)
      {
        long size_in_status = findLabel(split, "fsize");
        long size_of_file   = new File(splitname).length();
        if (size_in_status != size_of_file)
        {
          common.plog("doWeNeedToReadReplayFile(): Replay split file size for %s does not match: %d/%d",
                      splitname, size_in_status, size_of_file);

          return true;
        }
      }

      /* Now pick up the saved values: */
      long minlba  = findLabel(split, "minlba");
      long maxlba  = findLabel(split, "maxlba");
      long maxxfer = findLabel(split, "maxxfer");
      long first   = findLabel(split, "first");
      long last    = findLabel(split, "last");
      long records = findLabel(split, "records");
      long fsize   = findLabel(split, "fsize");

      rdev.setMinLba(minlba);
      rdev.setMaxLba(maxlba);
      rdev.setMaxXfersize((int) maxxfer);
      rdev.setFirstTod(first);
      rdev.setLastTod(last);
      rdev.setRecordCount(records);

      //common.ptod("rdev: " + rdev);
      //common.ptod("minlba : " + minlba );
      //common.ptod("maxlba : " + maxlba );
      //common.ptod("maxxfer: " + maxxfer);
      //common.ptod("first  : " + first  );
      //common.ptod("last   : " + last   );
      //common.ptod("records: " + records);
    }

    /* Now that we're this far, we need to make sure that for all the   */
    /* non-reporting only devices we indeed have a split file. We can't */
    /* handle it if someone AFTER the split adds a device:              */
    Vector <ReplayDevice> devs = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < devs.size(); i++)
    {
      ReplayDevice rdev = devs.elementAt(i);
      if (rdev.isReportingOnly())
        continue;

      if (!new File(rdev.getSplitFileName()).exists())
      {
        common.plog("doWeNeedToReadReplayFile(): Replay split file for device %d does not exist: %s",
                    rdev.getDeviceNumber(), rdev.getSplitFileName());
        return true;
      }
    }


    return false;
  }

  /**
   * For simplicity I decided not to make most values positional.
   * This method does a label search; the next field then contains the value.
   *
   * On error, I could return 'null' and have the caller mark the replay file as
   * having to be re-read, but then I would never find any errors that show up.
   * I therefore abort.
   */
  private static long findLabel(String[] split, String label)
  {
    int found = -1;
    for (int i = 0; i < split.length; i++)
    {
      if (split[i].equals(label))
        found = i;
    }

    if (found < 0)
      common.failure("failure scanning file 'replay_file_status.txt'");
    if (found == split.length)
      common.failure("failure scanning file 'replay_file_status.txt'");

    return Long.parseLong(split[found+1]);
  }
}

