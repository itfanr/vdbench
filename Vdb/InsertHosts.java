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
 * This class handles code around an idea that I had to assist users with
 * complex multi host runs.
 *
 * Some times it is necessary to repeat SDs, FSD, WDs or FWDs over and over
 * again for each separate host. That's a waste of time. The computer can do
 * that.
 * After initially writing the GenParms() program as an external attempt to
 * do this I decided to move the logic of that code to THIS class.
 *
 * Every occurrence of '$host' or '#host' in a name or a lun will cause that
 * parameter to be automatically repeated once for each parameter for each host,
 * replacing $host with the host name and #host with the relative host number.
 *
 */
public class InsertHosts
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static boolean any_changes_made = false;

  private static String replace(String stuff, String hostname, int index)
  {
    String out;
    out = common.replace(stuff, "$host", hostname);
    out = common.replace(out,   "#host", "" + index);

    /* !host to work around having $host in a wrapper script and */
    /* the script picking it up! */
    out = common.replace(out, "!host", hostname);

    if (!out.equals(stuff))
    {
      any_changes_made = true;
      return out;
    }

    return null;
  }


  public static String[] repeatParameters(String[] lines)
  {
    int index;

    /* The new array first starts off as a Vector: */
    Vector newlines = new Vector(lines.length * 2, 0);
    Vector hosts = Host.getDefinedHosts();


    /* Scan and copy until we find an SD: */
    for (index = 0;  index < lines.length; index++)
    {
      String line = lines[index];

      /* A null line indicates 'last line' (the array is over allocated): */
      if (line == null)
        break;

      /* Copy until we find the start of a parameter: */
      if (!isStart(line) || line.startsWith("rd="))
      {
        newlines.add(line);
        continue;
      }

      /* We have a parameter, gather every line belonging to this: */
      Vector parm_lines = getParameterLines(lines, index);
      index += parm_lines.size() - 1;

      /* Now repeat these lines once for each host: */
      loop:
      for (int h = 0; h < hosts.size(); h++)
      {
        Host host = (Host) hosts.elementAt(h);

        /* 'filerserver' is NOT repeated: */
        if (host.getLabel().equals("fileserver"))
          continue ;

        for (int i = 0; i < parm_lines.size(); i++)
        {
          line = replace((String) parm_lines.elementAt(i), host.getLabel(), h);

          /* null means line did not change. Just add it and break: */
          if (line == null)
          {
            newlines.add((String) parm_lines.elementAt(i));
            break loop;
          }

          newlines.add(line);
        }
      }
    }

    /* No changes were made, so leave things alone: */
    if (!any_changes_made)
      return null;

    /* Now replace the original array coming from Vdb_scan: */
    String[] array = (String[]) newlines.toArray(new String[0]);

    return array;
  }




  private static boolean isStart(String line)
  {
    return(line.startsWith("sd"  ) ||
           line.startsWith("fsd" ) ||
           line.startsWith("wd"  ) ||
           line.startsWith("fwd" ) ||
           line.startsWith("rd"  ) );
  }

  private static Vector getParameterLines(String[] lines, int index)
  {
    /* Begin with adding the current line: */
    Vector parm_lines = new Vector(16, 0);
    parm_lines.add(lines[index]);

    /* Now add the rest: */
    for (index++;index < lines.length; index++)
    {
      String line = lines[index];
      if (line == null || isStart(line))
        break;
      parm_lines.add(line);
    }

    return parm_lines;
  }

}

