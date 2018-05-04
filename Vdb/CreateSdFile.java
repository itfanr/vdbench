package Vdb;
import java.util.HashMap;
import java.util.Vector;

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


/**
 * This class handles the creation or expansion of SD disk files.
 */
public class CreateSdFile
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  /**
   * Insert an extra format run if needed.
   * This is done for files that do not exist, or that are not long enough.
   * This latter option allows for a restart if for some reason the format failed
   * for a large file.
   */
  public static boolean insertFormatsIfNeeded()
  {
    /* List of each SD that needs a file created with a WG_entry that uses it: */
    HashMap sd_map = new HashMap(32);

    /* Scan through all RDs: */
    for (int i = 0; i < Vdbmain.rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) Vdbmain.rd_list.elementAt(i);

      /* Look for those WG_entry instances that use a non-existing file: */
      for (int j = 0; j < rd.wgs_for_rd.size(); j++)
      {
        WG_entry wg = (WG_entry) rd.wgs_for_rd.elementAt(j);
        SD_entry sd = wg.sd_used;

        if (sd.sd_is_referenced)
        {
          if (sd.lun.startsWith("/dev/"))
            continue;
          if (sd.lun.startsWith("\\\\"))
            continue;
          if (sd.isTapeTesting())
            continue;

          for (int k = 0; k < sd.host_info.size(); k++)
          {
            LunInfoFromHost info = (LunInfoFromHost) sd.host_info.elementAt(k);
            if (!info.lun_exists && sd.end_lba == 0)
              continue;

            if (info.lun_exists && sd.psize >= sd.end_lba)
              continue;

            /* We now have a file that either does not exist or that */
            /* is too small. Add it to the list if not there yet:    */
            if (sd_map.put(sd.sd_name, wg) == null)
            {
              common.ptod("lun=" + sd.lun + " does not exist or is too small. host=" +
                          wg.slave.getHost().getLabel());
              common.ptod("Vdbench will attempt to expand a disk file if the requested " +
                          "file size is a multiple of 1mb");
            }

          }
        }
      }
    }

    /* We now have a list of WG_entry instances that use this file.       */
    /* Clone them and give them all to a new RD:                          */
    /* The reason why I am chosing to do it this way is that at this      */
    /* point it has already been decided which file belongs on which host */
    /* and I did not want to fiddle with this again.                      */
    Vector wgs = new Vector(sd_map.values());
    if (wgs.size() == 0)
      return false;

    /* Determine minimum file size so that we can provide a decent xfersize: */
    long min_size = Long.MAX_VALUE;
    for (int i = 0; i < wgs.size(); i++)
      min_size = Math.min(min_size, ((WG_entry) wgs.elementAt(i)).sd_used.end_lba);

    /* xfersize is set to 64k to prevent any DV run that also does formatting */
    /* to use max_xfersize=1m for its native buffer allocation:               */
    int xfersize = 64*1024;
    if (min_size < 64*1024)
      xfersize = 512;

    /* However, it may be overridden: */
    if (MiscParms.formatxfersize > 0)
      xfersize = MiscParms.formatxfersize;

    /* Add an extra run at the beginning: */
    RD_entry rd   = new RD_entry();
    rd.rd_name    = SD_entry.NEW_FILE_FORMAT_NAME;
    rd.iorate_req = Vdbmain.IOS_PER_JVM; // Keep within one JVM
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.wd_names   = new String [ wgs.size() ];
    Vdbmain.rd_list.insertElementAt(rd, 0);

    /* Create overrides for those things I can't do directly in WD or RD */
    double[] threads = new double[] { 2 };
    double[] rate    = new double[] { Vdbmain.IOS_PER_JVM };
    new For_loop("forthreads",  threads, rd.for_list);
    new For_loop("foriorate",   rate,    rd.for_list);
    Vector next_do_list = new Vector(1, 0);
    For_loop.for_get(0, rd, next_do_list);
    rd.current_override = (For_loop) next_do_list.firstElement();


    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg  = (WG_entry) wgs.elementAt(i);

      /* Create a new WD entry: */
      WD_entry wd      = new WD_entry();
      wd.wd_name       = SD_entry.NEW_FILE_FORMAT_NAME + wg.sd_used.sd_name;
      wd.sd_names      = new String [] { wg.sd_used.sd_name};
      wd.wd_sd_name    = wg.sd_used.sd_name;
      wd.skew_original = 0;
      wd.seekpct       = -1;
      wd.readpct       = 0;
      wd.host_names    = new String[] { wg.slave.getHost().getLabel()};
      wd.xfersize      = xfersize;

      rd.wd_names[i]   = wd.wd_name;

      Vdbmain.wd_list.insertElementAt(wd, 0);
    }

    rd.getWdsForRd();

    if (wgs.size() > 0)
      return true;
    else
      return false;
  }
}



