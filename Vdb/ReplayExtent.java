package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.Vector;

import Utils.Flat_record;
import Utils.printf;



/**
 * This class describes the lba ranges of a Replay device to be
 * replayed on which lba range of an SD.
 */
class ReplayExtent implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private String rsd_name;
  private ReplayDevice repd;     /* Which Replay device is this for?       */

  private long low_replay_lba;   /* The starting lba for the replay device */
  private long high_replay_lba;  /* The ending lba for the replay device   */

  private long low_sd_lba;       /* The starting SD lba for this extent    */
  private long high_sd_lba;      /* The ending SD lba for this extent      */

  transient private SD_entry     rsd;      /* SD for this extent           */

  private static double MB = 1024. * 1024.;
  private static double GB = 1024. * 1024. * 1024.;


  public ReplayExtent()
  {
  }


  public String getSdName()
  {
    return rsd_name;
  }

  /**
   * See if requested lba fits inside of the extent return values needed.
   *
   * If true, set the lba within the SD, and adjust the xfersize if the
   * currently requested block will not fit in this extent.
   */
  public boolean findLbaInExtent(Cmd_entry cmd, long replay_lba)
  {
    if (replay_lba >= low_replay_lba && replay_lba < high_replay_lba)
    {
      /* Lookup the sd pointer only once: */
      if (rsd == null)
        rsd = SlaveWorker.findSd(rsd_name);

      cmd.cmd_lba = replay_lba - low_replay_lba + low_sd_lba;
      cmd.sd_ptr  = rsd;

      /* See if this whole block fits on the sd, if not, adjust: */
      long bytes_after_lba = rsd.end_lba - cmd.cmd_lba;
      if (bytes_after_lba < cmd.cmd_xfersize)
        cmd.cmd_xfersize = bytes_after_lba;


      if (cmd.cmd_lba + cmd.cmd_xfersize > rsd.end_lba)
      {
        common.ptod("replay_lba: " + replay_lba);
        common.ptod("cmd.cmd_lba: " + cmd.cmd_lba);
        printRoom("bad", rsd.sd_name);
        common.failure("Seek too high");
      }

      return true;
    }

    return false;
  }


  public SD_entry findLbaInExtentFlat(SD_entry sd_used, Flat_record flat)
  {
    boolean debug = false; //sd_used.sd_name.equals("sd2");
    if (debug)
    {
      common.ptod("flat.lba:        " + flat.lba);
      common.ptod("low_replay_lba:  " + low_replay_lba);
      common.ptod("high_replay_lba: " + high_replay_lba);
      common.ptod("rsd_name:        " + rsd_name);
    }
    if (flat.lba >= low_replay_lba && flat.lba < high_replay_lba)
    {
      /* Lookup the sd pointer only once: */
      if (rsd == null)
        rsd = SlaveWorker.findSd(rsd_name);

      flat.lba = flat.lba - low_replay_lba + low_sd_lba;

      /* See if this whole block fits on the sd, if not, adjust: */
      long bytes_after_lba = rsd.end_lba - flat.lba;
      if (bytes_after_lba < flat.xfersize)
        flat.xfersize = (int) bytes_after_lba;

      if (flat.lba + flat.xfersize > rsd.end_lba)
      {
        common.ptod("replay_lba: " + flat.lba);
        printRoom("bad", rsd.sd_name);
        common.failure("Seek too high");
      }

      return rsd;
    }

    return null;
  }


  /**
   * Take all devices in a group and figure out which replay lba's go
   * on which SD.
   */
  public static void createExtents()
  {
    Vector groups = ReplayInfo.getGroupList();

    /* All groups: */
    for (int i = 0; i < groups.size(); i++)
    {
      int  last_used_sd          = 0;
      long bytes_used_on_last_sd = 0;
      ReplayGroup group = (ReplayGroup) groups.elementAt(i);

      //for (int j = 0; j < group.getDeviceList().size(); j++)
      //{
      //  ReplayDevice rdev = (ReplayDevice) group.getDeviceList().elementAt(j);
      //  common.ptod("createExtents: " + group.getName() + " " + rdev.getDevString() + " " + rdev.getRecordCount());
      //}

      /* All devices in that group: */
      rdev_loop:
      for (int j = 0; j < group.getDeviceList().size(); j++)
      {
        ReplayDevice rdev = (ReplayDevice) group.getDeviceList().elementAt(j);
        long bytes_needed_for_rdev = rdev.getMaxLba();
        long rdev_low_lba          = 0;
        long rdev_low_sd_lba       = 0;


        if (rdev.getRecordCount() == 0)
          common.failure("Replay requested for device %s. No Replay records found",
                         rdev.getDevString(), rdev.getRecordCount());

        /* All (remaining) SDs: */
        for (; last_used_sd < group.getSdList().size(); last_used_sd++)
        {
          SD_entry sd = (SD_entry) group.getSdList().elementAt(last_used_sd);
          long bytes_used_on_this_sd = 0;

          /* Create a new extent as long as there is room on this SD: */
          ReplayExtent re   = new ReplayExtent();
          re.rsd_name       = sd.sd_name;
          re.repd           = rdev;
          re.low_sd_lba     = bytes_used_on_last_sd;
          re.low_replay_lba = rdev_low_lba;
          rdev.addExtent(re);
          //common.ptod("added extent for sd=%s", re.rsd_name);

          /* If everything fits on this SD: */
          if (bytes_needed_for_rdev <= sd.end_lba - bytes_used_on_last_sd)
          {
            re.high_replay_lba      = rdev.getMaxLba();// + rdev.getMaxXfersize();
            re.high_sd_lba          = re.low_sd_lba + bytes_needed_for_rdev;
            bytes_used_on_this_sd  += re.high_sd_lba - re.low_sd_lba;
            bytes_used_on_last_sd   = re.high_sd_lba;
            sd.last_replay_lba_used = bytes_used_on_last_sd;

            /* We have more space on this SD than we need; get next replay device: */
            re.printRoom("room left", group.getName());
            continue rdev_loop;
          }
          else
          {
            long bytes_on_this_sd   = sd.end_lba - re.low_sd_lba;
            bytes_needed_for_rdev  -= bytes_on_this_sd;
            rdev_low_lba           += bytes_on_this_sd;
            re.high_replay_lba      = re.low_replay_lba + bytes_on_this_sd;
            re.high_sd_lba          = re.low_sd_lba + bytes_on_this_sd;
            bytes_used_on_this_sd  += re.high_sd_lba - re.low_sd_lba;
            bytes_used_on_last_sd   = 0;

            sd.last_replay_lba_used = bytes_used_on_this_sd;

            /* We need an other SD to satisfy this group's request: */
            re.printRoom("need more", group.getName());
            continue;
          }
        }
      }
    }

    /* Code can not handle it when it gets more SDs than needed: */
    String too_many = "";
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      //common.ptod("sd.last_replay_lba_used: %s %12d ", sd.sd_name, sd.last_replay_lba_used);
      if (sd.isActive() && sd.last_replay_lba_used == 0)
        too_many += sd.sd_name + " ";
    }

    if (!ReplayInfo.duplicationNeeded() && too_many.length() > 0)
      common.failure("ReplayExtent.createExtents(): no replay i/o targeted for "+
                     "one or more SDs (%s) Please remove.", too_many.trim());

    //for (int x = 0; x < ReplayDevice.getDeviceList().size(); x++)
    //{
    //  ReplayDevice rpd = (ReplayDevice) ReplayDevice.getDeviceList().elementAt(x);
    //  common.ptod("yyrpd: " + rpd.extent_list.size());
    //}
  }

  public void printRoom(String label, String group)
  {
    common.plog("%s sd: group: %-8s %-8s %16s replay lba: %11.3fm - %11.3fm sd lba: %11.3fm - %11.3fm",
                label,
                group,
                ( (rsd_name != null) ? rsd_name : "n/a"),
                repd.getDevString(),
                (double) low_replay_lba  / MB,
                (double) high_replay_lba / MB,
                (double) low_sd_lba      / MB,
                (double) high_sd_lba     / MB);
  }
}
