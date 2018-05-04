package VdbComp;

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
import Utils.common;
import Utils.Fget;
import Utils.Fput;

/**
 * This class handles parameters saved across sessions.
 *
 */
public class StoredParms
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  static int last_width  = 800;
  static int last_height = 400;
  static int last_x      = 20;
  static int last_y      = 20;

  public static void loadParms()
  {
    double[] limits  = null;

    String ini = Utils.ClassPath.classPath("wlcomp.ini");
    if (!Fget.file_exists(ini))
      return;

    Fget fg = new Fget(ini);
    int idx = 0;
    String line = null;
    while ((line = fg.get()) != null)
    {
      if (line.startsWith("old"))
        WlComp.old_dir = line.substring(4);

      else if (line.startsWith("new"))
        WlComp.new_dir = line.substring(4);

      else if (line.startsWith("delta"))
      {
        if (idx == 0)
          limits = new double[Delta.getDeltas().length];
        limits[idx++] = Double.parseDouble(line.substring(6));
      }

      else if (line.startsWith("width"))
        last_width = Integer.parseInt(line.substring(6));

      else if (line.startsWith("height"))
        last_height = Integer.parseInt(line.substring(7));

      else if (line.startsWith("x"))
        last_x = Integer.parseInt(line.substring(2));

      else if (line.startsWith("y"))
        last_y = Integer.parseInt(line.substring(2));
    }

    fg.close();

    /* If there were any deltas in the input, copy them: */
    if (limits != null)
      Delta.setDeltas(limits);
  }



  public static void storeParms()
  {
    String ini = Utils.ClassPath.classPath("wlcomp.ini");

    Fput fp = new Fput(ini);

    fp.println("old "    + WlComp.old_dir);
    fp.println("new "    + WlComp.new_dir);
    fp.println("width "  + (int) WlComp.wlcomp.getSize().getWidth());
    fp.println("height " + (int) WlComp.wlcomp.getSize().getHeight());
    fp.println("x "      + (int) WlComp.wlcomp.getLocation().getX());
    fp.println("y "      + (int) WlComp.wlcomp.getLocation().getY());

    Delta[] deltas = Delta.getDeltas();
    for (int i = 0; i < deltas.length; i++)
      fp.println("delta " + deltas[i].limit);

    fp.close();
  }
}

