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

import java.util.Vector;
import java.util.Random;
import java.io.Serializable;
import Utils.Format;


/**
 * This contains context information for determining low and high seek address
 * for cache hits or misses.
 */
class WG_context implements Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  long      next_seq = -1;      /* Next sequential seek address               */
  long      last_lba;           /* To assist with next sequential io          */
  long      seek_low;           /* Low seek address for wg                    */
  long      seek_high;          /* High seek address for wg                   */
  long      seek_width;         /* Seek width (high - low)                    */
  long      max_xfersize;       /* maximum xfersize for this WG               */
  boolean   seq_eof = false;    /* ???                                        */

  long      xfersize;           /* fixed xfersize                             */
  long      rhpct;              /* readhit%                                   */
  long      whpct;              /* writehit%                                  */
  long      rdpct;              /* read%                                      */
  long      seekpct;            /* seek%                                      */
  long      sd_num;             /* Number of SD for error reporting           */


  public Object clone()
  {
    try
    {
      return super.clone();
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }



  /**
   * Send the necessary information to JNI so that it can run the workload
   */
  public static void setup_jni_context(Vector wg_list)
  {
    RD_entry rd = RD_entry.next_rd;

    //check_jni_context(wg_list);

    Native.alloc_jni_shared_memory(false, common.get_shared_lib());

    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.elementAt(i);
      SD_entry sd = (SD_entry) wg.sd_used;
      String sdname = sd.sd_name;
      if (sdname.length() < 8)
        sdname = Format.f("%-8s", sdname);

      // threads_used is no longer used in JNI. (was for vdblite).
      //if (sd.getThreadsUsed() == 0)
      //  common.failure("setup_jni_context(): thread count is zero");

      if (wg.hcontext == null)
        common.failure("hcontext is missing for " + wg.wg_name);
      if (wg.mcontext == null)
        common.failure("mcontext is missing for " + wg.wg_name);

      /*
      common.ptod("wg.xfersize            : " + wg.xfersize           );
      common.ptod("wg.seekpct             : " + wg.seekpct            );
      common.ptod("wg.readpct             : " + wg.readpct            );
      common.ptod("wg.rhpct               : " + wg.rhpct              );
      common.ptod("wg.whpct               : " + wg.whpct              );
      common.ptod("wg.hcontext.seek_low   : " + wg.hcontext.seek_low  );
      common.ptod("wg.hcontext.seek_high  : " + wg.hcontext.seek_high );
      common.ptod("wg.hcontext.seek_width : " + wg.hcontext.seek_width);
      common.ptod("wg.mcontext.seek_low   : " + wg.mcontext.seek_low  );
      common.ptod("wg.mcontext.seek_high  : " + wg.mcontext.seek_high );
      common.ptod("wg.mcontext.seek_width : " + wg.mcontext.seek_width);
      common.ptod("sd.fhandle             : " + sd.fhandle            );
      common.ptod("sd.threads_used        : " + sd.threads_used      );
      common.plog("sd.lun                 : " + sd.lun               );
      */

      //common.plog(" ++ setup_jni_context: " + rd.jni_controlled_run + "/" + sd.lun + " " + wg.xfersize);

      /* For a tape read operation the high seek address must be switched from */
      /* whatever is set in the context (context is determined before the first */
      /* run) to the byte count set during the last write: */
      long tape_bytes = wg.mcontext.seek_high;
      if (sd.isTapeDrive() && wg.readpct > 0)
      {
        if (sd.bytes_written == 0)
        {
          SlaveJvm.sendMessageToConsole("Tape read can be only done after a tape write.");
          common.failure("Tape read can be only done after a tape write.");
        }

        tape_bytes = sd.bytes_written;
        SlaveJvm.sendMessageToConsole("Tape read request to sd=" + sd.sd_name +
                                      " limited to the amount of "+
                                      "bytes written in the previous write run: " +
                                      Format.f("%7.3f MB", ((double) sd.bytes_written / 1048576)));
      }

      Native.setup_jni_context(wg.xfersize,
                               wg.seekpct,
                               wg.readpct,
                               wg.rhpct,
                               wg.whpct,
                               wg.hcontext.seek_low,
                               wg.hcontext.seek_high,
                               wg.hcontext.seek_width,
                               wg.mcontext.seek_low,
                               tape_bytes,
                               wg.mcontext.seek_width,
                               sd.fhandle,
                               777777, i,
                               sd.lun,
                               (common.get_debug(common.DV_UNIQUE_SD) ? sd.unique_dv_name : sdname));
    }
  }

}

