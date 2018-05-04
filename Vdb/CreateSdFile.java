package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;




/**
 * This class handles the creation or expansion of SD disk files.
 */
public class CreateSdFile
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Insert an extra format run if needed.
   * This is done for files that do not exist, or that are not long enough.
   * This latter option allows for a restart if for some reason the format failed
   * for a large file.
   */
  public static boolean insertFormatsIfNeeded()
  {
    int msg_count = 0;

    /* List of each SD that needs a file created with a WG_entry that uses it: */
    HashMap <String, WG_entry> sds_to_format = new HashMap(32);

    /* Scan through all RDs: */
    for (int i = 0; i < Vdbmain.rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) Vdbmain.rd_list.elementAt(i);

      /* Look for those WG_entry instances that use a non-existing file: */
      for (WG_entry wg : rd.wgs_for_rd)
      {
        SD_entry sd = wg.sd_used;

        /* Create a loop here, for either one SD or a concateneated SD  */
        /* so that we can look also at concatenated SDs                 */
        /* We WON'T however do file creations; we just check and abort. */
        ArrayList <SD_entry> sds = new ArrayList(1);
        if (sd.concatenated_sd)
          sds = sd.sds_in_concatenation;
        else
          sds.add(sd);

        for (int c = 0; c < sds.size(); c++)
        {
          sd = sds.get(c);

          // why the heck this 'if'? If it is in a WG, it is referenced!!!!
          if (sd.sd_is_referenced)
          {
            if (!MiscParms.format_sds && sd.lun.startsWith("/dev/"))
              continue;
            if (!MiscParms.format_sds && sd.lun.startsWith("\\\\"))
              continue;

            for (int k = 0; k < sd.host_info.size(); k++)
            {
              LunInfoFromHost info = (LunInfoFromHost) sd.host_info.elementAt(k);

              /* If the lun does not exist and we have no size there's nothing we can do: */
              if (!info.lun_exists && sd.end_lba == 0)
                continue;

              /* If this is a soft link to a raw disk, leave things alone: */
              if (info.soft_link != null && info.soft_link.startsWith("/dev/"))
                continue;

              /* If the lun exists, and the size is large enough, don't bother, */
              /* unless we're specifically told to format it:                   */
              if (!MiscParms.format_sds && info.lun_exists && sd.psize >= sd.end_lba)
                continue;

              /* We now have a file that either does not exist or that */
              /* is too small. Add it to the list if not there yet:    */
              if (sds_to_format.put(sd.sd_name, wg) == null)
              {
                if (!MiscParms.format_sds)
                {
                  if (msg_count++ == 0)
                    common.ptod("Vdbench will attempt to expand a disk file if the requested " +
                                "file size is a multiple of 1mb");
                  common.ptod("lun=" + sd.lun + " does not exist or is too small. host=" +
                              wg.getSlave().getHost().getLabel());

                  if (Validate.sdConcatenation())
                    common.failure("File creation or expansion not supported for SD concatenation");
                }
                else
                  common.ptod("'formatsds=yes' is causing %s to be (re)formatted.", sd.lun);
              }
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
    Vector <WG_entry> wgs = new Vector(sds_to_format.values());
    if (wgs.size() == 0)
      return false;

    /* Determine minimum file size so that we can provide a decent xfersize: */
    long min_size = Long.MAX_VALUE;
    for (int i = 0; i < wgs.size(); i++)
      min_size = Math.min(min_size, ((WG_entry) wgs.elementAt(i)).sd_used.end_lba);

    /* xfersize is set to 128k to prevent any DV run that also does formatting */
    /* to use max_xfersize=1m for its native buffer allocation:                */
    int xfersize = 128*1024;
    if (min_size < 128*1024)
    {
      common.ptod("The creation of a very small file causes xfersize=512 to be set");
      xfersize = 512;
    }

    /* However, it may be overridden: */
    if (MiscParms.formatxfersize > 0)
    {
      xfersize = MiscParms.formatxfersize;

      /* There is a bug in IO_task when determining the buffer size to use. */
      /* An inserted format gets confuses with 'this is replay'.            */
      /* Not worth the effort!                                              */
      if (ReplayInfo.isReplay())
        common.failure("Use of the formatxfersize= parameter not allowed during replay");
    }

    /* Add an extra run at the beginning: */
    RD_entry rd   = new RD_entry();
    rd.rd_name    = SD_entry.SD_FORMAT_NAME;
    rd.end_cmd    = rd.dflt.end_cmd;
    rd.start_cmd  = rd.dflt.start_cmd;
    rd.iorate_req = RD_entry.MAX_RATE;
    rd.setNoElapsed();
    rd.setInterval(1);
    rd.wd_names   = new String [ wgs.size() ];
    Vdbmain.rd_list.insertElementAt(rd, 0);

    /* Create overrides for those things I can't do directly in WD or RD */
    double[] threads = new double[] { 2};
    double[] rate    = new double[] { RD_entry.MAX_RATE /* Vdbmain.IOS_PER_JVM */};
    new For_loop("forthreads",  threads, rd.for_list);
    new For_loop("foriorate",   rate,    rd.for_list);
    Vector next_do_list = new Vector(1, 0);
    For_loop.for_get(0, rd, next_do_list);
    rd.current_override = (For_loop) next_do_list.firstElement();

    /* Create a WD_entry for each file that must be created/expanded: */
    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg      = (WG_entry) wgs.elementAt(i);

      WD_entry wd      = new WD_entry();
      wd.wd_name       = SD_entry.SD_FORMAT_NAME + "_" + wg.sd_used.sd_name;
      wd.sd_names      = new String [] { wg.sd_used.sd_name };
      wd.wd_sd_name    = wg.sd_used.sd_name;
      wd.skew_original = 0;
      wd.seekpct       = -1;
      wd.readpct       = 0;
      //wd.host_names    = new String[] { wg.getSlave().getHost().getLabel()};
      wd.host_names    = new String[] { "*" };
      wd.xf_table      = new double[] { xfersize};
      wg.sd_used.trackSdXfersizes(wd.xf_table);

      rd.wd_names[i]   = wd.wd_name;

      WD_entry.max_wd_name = Math.max(WD_entry.max_wd_name, wd.wd_name.length());

      Vdbmain.wd_list.insertElementAt(wd, 0);
    }

    rd.getWdsForRd();

    if (wgs.size() > 0)
      return true;
    else
      return false;
  }
}



