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
import Utils.ClassPath;

/**
 * Miscellaneous parameters.
 * Thse all need to be first in the parameter files.
 */
public class MiscParms
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public static boolean  maintain_run_totals = false;
  public static String[] unix2windows = null;
  public static int      formatxfersize = 0;
  public static boolean  create_anchors = false;

  /**
   * Read Host information.
   */
  static String readParms()
  {

    String str;
    Vdb_scan prm;
    SD_entry sd = null;
    int seqno = 0;

    str = Vdb_scan.parms_get();
    if (str == null)
      common.failure("Early EOF on input parameters");

    try
    {
      while (true)
      {
        prm = Vdb_scan.parms_split(str);

        if (prm.keyword.equals("rg")  ||
            prm.keyword.equals("sd")  ||
            prm.keyword.equals("fsd") ||
            prm.keyword.equals("wd")  ||
            prm.keyword.equals("fwd") ||
            prm.keyword.equals("rd")  ||
            prm.keyword.equals("hd")  ||
            prm.keyword.equals("host"))
          break;


        if ("compression".startsWith(prm.keyword))
          DV_map.compression_rate = prm.numerics[0];

        else if ("compressionseed".startsWith(prm.keyword))
        {
          if (prm.getNumCount() > 0)
            DV_map.compression_seed = (long) prm.numerics[0];
          else
          {
            if (prm.alphas[0].equals("tod"))
              DV_map.compression_seed = System.currentTimeMillis();
            else
              common.failure("Invalid 'compressionseed=' parameter: " + prm.alphas[0]);
          }
        }

        else if ("dedup".startsWith(prm.keyword))
          Validate.parseDedupParms(prm);

        else if ("port".startsWith(prm.keyword))
          SlaveSocket.setMasterPort((int) prm.numerics[0]);

        else if ("patterns".startsWith(prm.keyword))
          DV_map.pattern_dir = prm.alphas[0];

        else if ("data_errors".startsWith(prm.keyword))
          Validate.parseDataErrors(prm);

        else if ("swat".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() == 1)
            new SwatCharts(prm.alphas[0], ClassPath.classPath("swatcharts.txt"));
          else
            new SwatCharts(prm.alphas[0], prm.alphas[1]);
        }

        else if ("validate".startsWith(prm.keyword))
          Validate.parseValidateOptions(prm);

        else if ("journal".startsWith(prm.keyword))
          Validate.parseJournalOptions(prm);

        else if ("force_error_after".startsWith(prm.keyword))
        {
          //if (!Validate.isValidate())
          //  common.failure("'force_error_after' requires Data Validation to be active");
          if (prm.getNumCount() == 1)
            Validate.setForceError((int) prm.numerics[0], 1);
          else
            Validate.setForceError((int) prm.numerics[0], (int) prm.numerics[1]);
        }

        else if (prm.keyword.equals("start_cmd"))
        {
          Debug_cmds.starting_command.command = prm.alphas[0];
          if (prm.getAlphaCount() > 1)
            Debug_cmds.starting_command.setTarget(prm.alphas[1]);

          // Problems with parser, so for now just ignore the 'seconds' parameter

          /* This allows for a second parameter, e.g. 'wait 30' */
          //if (prm.getAlphaCount() > 1)
          //{
          //  String parm1 = prm.alphas[1];
          //  Debug_cmds.start_cmd_sleep = Integer.parseInt(parm1.substring(parm1.lastIndexOf(" ") + 1));
          //}
        }

        else if (prm.keyword.equals("end_cmd"))
        {
          Debug_cmds.ending_command.command = prm.alphas[0];
          if (prm.getAlphaCount() > 1)
            Debug_cmds.ending_command.setTarget(prm.alphas[1]);
        }

        else if (prm.keyword.equals("heartbeat_error"))
          HeartBeat.heartbeat_error = new Debug_cmds(prm.alphas[0]);

        else if (prm.keyword.equals("debug"))
          common.set_debug((int) prm.numerics[0]);

        else if (prm.keyword.equals("parm="))   // already used in vdbench main
          continue;

        else if ("javaparms".startsWith(prm.keyword))
        {
          common.ptod("'javaparms=' parameter is ignored.");
          common.ptod("To change java heap size, change ./vdbench or ./vdbench.bat script");
        }

        else if ("unix2windows".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() != 2)
            common.failure("'unix2windows=' requires two subparameters, e.g. 'unix2windows=(/mnt,c:\\)");
          unix2windows = prm.alphas;
        }

        else if ("formatxfersize".startsWith(prm.keyword))
          formatxfersize = (int) prm.numerics[0];

        else if ("create_anchors".startsWith(prm.keyword))
          create_anchors = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("report".startsWith(prm.keyword))
          Report.parseParameters(prm.alphas);

        else if ("report_run_totals".startsWith(prm.keyword))
          maintain_run_totals = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("histogram".startsWith(prm.keyword))
        {
          if (prm.alpha_count != 2)
            common.failure("'histogram=' must have just two subparameters (Use double quotes around the numbers!).");

          new BucketRanges(prm.alphas[0], prm.alphas[1]);
        }

        else
          common.failure("Unknown keyword: " + prm.keyword);

        str = Vdb_scan.parms_get();
      }
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }

    return str;
  }
}


