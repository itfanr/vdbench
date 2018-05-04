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

import java.io.*;
import java.util.*;
import java.text.*;
import Utils.Fput;

/**
 * This class handles all data manipulation related to data transferred from
 * vdbench to TNFE (now Swat).
 *
 * It makes most sense to also convert this format from ascii to the new Bin
 * file format, but at this time that is just too much work. 3/14/06
 */
class Tnfe_data
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Fput fp =  null;

  static DateFormat printdf = new SimpleDateFormat("MMddyyyy-HH:mm:ss.000" );


  /**
   * Create Swat file and store fixed configuration data
   */
  private static void openSwatFile(Date ts)
  {
    fp = new Fput(Vdbmain.output_dir, "swat_mon.txt");

    fp.println(":date " + printdf.format(ts));
    fp.println(":fixed_data");
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      fp.println(i + " " + i + " 0 0 " + "sd_" + sd.sd_name);
    }
  }


  /**
   * Write header to start a new interval
   */
  public static void writeIntervalHeader(Date begin_ts, Date end_ts)
  {
    /* If file not open yet, do so: */
    if (fp == null)
      openSwatFile(begin_ts);

    /* Write performance data for this interval: */
    fp.println(":");
    fp.println(":vdbench_data");
    fp.println(":date " + printdf.format(begin_ts) + " " + printdf.format(end_ts));
  }


  /**
   * Write data for one SD
   */
  public static void writeOneSd(String sdname, SdStats stats)
  {
    /* Find relative position in the SD list: */
    int index = Vdbmain.sd_list.indexOf(SD_entry.findSD(sdname));

    fp.println(index +                         //  0  index
               " " + "0" +                     //  1  this_second
               " " + stats.reads +             //  2  kio.reads
               " " + stats.writes +            //  3  kio.writes
               " " + stats.resptime / 1000 +   //  4  kio.rlentime
               " " + "0" +                     //  5  kio.rtime
               " " + stats.rbytes / 512 +      //  6  kio.nread
               " " + stats.wbytes / 512 +      //  7  kio.nwritten
               " " + stats.respmax / 1000 +    //  8  kio.resp_max
               " " + "0" +                     //  9  kio.resp_max_r
               " " + "0" +                     // 10  kio.resp_max_w
               " " + "0" +                     // 11  kio.q_max
               " " + "0" +                     // 12  kio.q_max_r
               " " + "0");                     // 13  kio.q_max_w
  }


  public static void close()
  {
    if (fp != null)
    {
      fp.close();
      fp = null;
    }
  }
}

