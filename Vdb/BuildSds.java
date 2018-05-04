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

import java.util.*;
import java.io.*;
import Utils.*;

/**
 * This was an experiment. Keep code for now. */
public class BuildSds
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Vector paths;
  private static String partition = "6";
  private static int sd_num = 1;
  private static Vector luns = new Vector(16, 0);

  public static void main(String[] args)
  {
    if (args.length < 2)
    {
      common.ptod("");
      common.ptod("Usage: ./vdbench sds -px [-sx] path .....");
      common.ptod("");
      common.ptod("'-px' partition number, e.g. -p6, default 6");
      common.ptod("'-sx' starting sd number, default 1");
      common.ptod("");
      common.ptod("Example: ./vdbench sds -p6 -s1 c34 c35");
      common.failure("Invalid parameters. Fix and try again");
    }


    Getopt g = new Getopt(args, "s:p:", 99);
    if (!g.isOK())
      common.failure("Parameter scan error");

    g.print("BuilsSds");
    common.ptod("");

    if (g.check('p'))
      partition = g.get_string();

    if (g.check('s'))
      sd_num = Integer.parseInt(g.get_string());

    paths = g.get_positionals();
    if (paths.size() == 0)
      common.failure("No paths requested");


    OS_cmd ocmd = new OS_cmd();

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             processLine(line, type);

                             return true;
                           }
                         });


    ocmd.addText("format << EOF");
    ocmd.execute();

    if (luns.size() == 0)
      common.failure("No valid devices found.");

    System.out.println("\n\n");
    for (int i = 0; i < luns.size(); i++)
    {
      String lun = (String) luns.elementAt(i);
      System.out.println("sd=sd" + (sd_num++) + ",lun=/dev/rdsk/" + lun + "s" + partition);
    }
    System.out.println("\n\n");
  }

  private static void processLine(String line, String type)
  {
    if (type.equals("stdout") && line.indexOf("<") != -1)
    {
      common.ptod(type + ": " + line);
      StringTokenizer st = new StringTokenizer(line);
      String disk = st.nextToken();
      String lun  = st.nextToken();

      for (int i = 0; i < paths.size(); i++)
      {
        String path = (String) paths.elementAt(i);
        if (lun.startsWith(path))
        {
          luns.add(lun);
          break;
        }
      }
    }
  }
}
