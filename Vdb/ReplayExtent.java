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

import java.io.Serializable;
import java.util.Vector;

import Utils.printf;



/**
 * This class describes the lba ranges of a Replay device to be
 * replayed on which lba range of an SD.
 */
class ReplayExtent implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  transient public SD_entry     rsd;      /* SD for this extent           */
  public String rsd_name;
  public ReplayDevice repd;     /* Which Replay device is this for?       */

  public long low_replay_lba;   /* The starting lba for the replay device */
  public long high_replay_lba;  /* The ending lba for the replay device   */

  public long low_sd_lba;       /* The starting SD lba for this extent    */
  public long high_sd_lba;      /* The ending SD lba for this extent      */


  static double GB = 1024. * 1024. * 1024.;


  public ReplayExtent()
  {
  }


  /**
   * See if requested lba fits inside of the extent return values needed.
   *
   * If true, set the lba within the SD, and adjust the xfersize if the
   * currently requested block will not fit in this extent.
   */
  public boolean findLbaInExtent(Cmd_entry cmd, long replay_lba)
  {
    //common.ptod("replay_lba: " + replay_lba);
    //common.ptod("low_replay_lba: " + low_replay_lba);
    //common.ptod("high_replay_lba: " + high_replay_lba);
    //common.where();
    if (replay_lba >= low_replay_lba && replay_lba < high_replay_lba)
    {
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
        print("bad", rsd.sd_name);
        common.failure("Seek too high");
      }

      return true;
    }

    return false;
  }


  /**
   * Take all devices in a group and figure out which replay lba's go
   * on which SD.
   */
  public static void createExtents()
  {
    Vector groups = ReplayGroup.getGroupList();

    /* All groups: */
    for (int i = 0; i < groups.size(); i++)
    {
      int  last_used_sd          = 0;
      long bytes_used_on_last_sd = 0;
      ReplayGroup group = (ReplayGroup) groups.elementAt(i);

      /* All devices in that group: */
      rdev_loop:
      for (int j = 0; j < group.getDeviceList().size(); j++)
      {
        ReplayDevice rdev = (ReplayDevice) group.getDeviceList().elementAt(j);
        long bytes_needed_for_rdev = rdev.max_lba;
        //common.ptod("rdev.max_lba: " + rdev.max_lba);
        long rdev_low_lba          = 0;
        long rdev_low_sd_lba       = 0;

        /* All (remaining) SDs: */
        for (; last_used_sd < group.getSDList().size(); last_used_sd++)
        {
          SD_entry sd = (SD_entry) group.getSDList().elementAt(last_used_sd);
          long bytes_used_on_this_sd = 0;

          /* Create a new extent as long as there is room on this SD: */
          ReplayExtent re   = new ReplayExtent();
          re.rsd_name       = sd.sd_name;
          re.repd           = rdev;
          re.low_sd_lba     = bytes_used_on_last_sd;
          re.low_replay_lba = rdev_low_lba;
          rdev.addExtent(re);

          /* If everything fits on this SD: */
          //common.ptod("bytes_needed_for_rdev: " + bytes_needed_for_rdev);
          //common.ptod("bytes_used_on_last_sd: " + bytes_used_on_last_sd);
          if (bytes_needed_for_rdev <= sd.end_lba - bytes_used_on_last_sd)
          {
            re.high_replay_lba      = rdev.max_lba + rdev.max_xfersize;
            re.high_sd_lba          = re.low_sd_lba + bytes_needed_for_rdev;
            bytes_used_on_this_sd  += re.high_sd_lba - re.low_sd_lba;
            bytes_used_on_last_sd   = re.high_sd_lba;

            /* We have more space on this SD than we need; get next replay device: */
            re.print("room left", group.group_name);
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

            /* We need an other SD to satisfy this group's request: */
            re.print("need more", group.group_name);
            continue;
          }
        }
      }
    }

    //for (int x = 0; x < ReplayDevice.getDeviceList().size(); x++)
    //{
    //  ReplayDevice rpd = (ReplayDevice) ReplayDevice.getDeviceList().elementAt(x);
    //  common.ptod("yyrpd: " + rpd.extent_list.size());
    //}
  }

  public void print(String label, String group)
  {
    printf pf = new printf("sd: %-8s %-8s %6d replay lba: %11.6fg %11.6fg sd lba: %11.6fg %11.6fg");
    pf.add(group);
    pf.add( (rsd_name != null) ? rsd_name : "n/a");
    pf.add(repd.device_number);

    pf.add((double) low_replay_lba / GB);
    pf.add((double) high_replay_lba / GB);
    pf.add((double) low_sd_lba / GB);
    pf.add((double) high_sd_lba / GB);

    common.plog(label + " " + pf.print());
  }
}
