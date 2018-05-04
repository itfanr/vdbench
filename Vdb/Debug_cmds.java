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

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;

import Utils.CommandOutput;
import Utils.OS_cmd;


/**
 * This class handles the prmfile start_cmd= and end_cmd= parameters
 */
class Debug_cmds implements Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  public  String command = null;
  private String target  = "log";

  private PrintWriter command_output = null;

  public static Debug_cmds starting_command = new Debug_cmds(null);
  public static Debug_cmds ending_command   = new Debug_cmds(null);

  private static Vector command_filenames = new Vector(8, 0);

  public Debug_cmds(String cmd)
  {
    command = cmd;
  }


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


  public static String[] getTargets()
  {
    return(String[]) command_filenames.toArray(new String[0]);
  }

  public void setTarget(String tgt)
  {
    target = tgt.trim().toLowerCase();

    if (!target.startsWith("cons") &&
        !target.startsWith("sum") &&
        !target.startsWith("log"))
      command_filenames.add(target);
  }

  /**
   * Run the requested command.
   *
   * When the command issued does not write stderr/stdout, for instance it pipes
   * its output to a file, the process started here will not react properly to the
   * OS_cmd.killCommand() method.
   *
   * Maybe we should redirect it to a new file in here instead of writing it to
   * either of the cons/sum/log files?
   * Maybe introduce an extra 'else' branch assuming that this is a file name that
   * can be opened at the first newLine()????
   * This file name then in turn can be linked to from summary.html.
   *
   * Done. 10/14/08
   */
  public boolean run_command()
  {
    if (command == null)
      return false;

    String use_command = command;
    /* Get the first word of the command. If it is in the shared library, call that: */
    String[] split = use_command.split(" +");
    if (new File(common.get_shared_lib() + File.separator + split[0]).exists())
    {
      use_command = common.get_shared_lib() + File.separator + split[0];
      for (int i = 1; i < split.length; i++)
        use_command += " " + split[i];
    }

    /* Replace occurrences of $output with the output directory: */
    String cmd = common.replace_string(use_command, "$output", Vdbmain.output_dir);

    /* Replace one occurence of $anchor with all file system anchors: */
    //cmd = addAnchors(cmd);

    common.ptod("Start/end command: executing '" + cmd + "'");
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             if (target.startsWith("cons"))
                               common.ptod("Debug_cmds(); " + type + " " + line);

                             else if (target.startsWith("sum"))
                               common.psum("Debug_cmds(); " + type + " " + line);

                             else if (target.startsWith("log"))
                               common.plog("Debug_cmds(); " + type + " " + line);

                             else
                             {
                               if (command_output == null)
                                 command_output =  Report.createHmtlFile(target);
                               command_output.println(type + " " + line);
                             }

                             return true;
                           }
                         });

    ocmd.execute();

    return true;
  }

  // NOT TESTED
  private static String addAnchors(String cmd)
  {
    Vector fwgs = SlaveWorker.work.fwgs_for_slave;
    if (fwgs == null || fwgs.size() == 0)
      return cmd;

    if (cmd.indexOf("$mount") == -1)
      return cmd;

    HashMap map = new HashMap(16);
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      String mount = fwg.anchor.getAnchorName();
      mount = mount.substring(0, mount.indexOf(File.separator));
      common.ptod("mount: " + mount);
      map.put(mount, mount);
    }

    String txt = "";
    String[] names = (String[]) map.values().toArray(new String[0]);
    for (int i = 0; i < names.length; i++)
      txt += names[i] + " ";

    String ret = common.replace(cmd, "$mount", txt.trim());
    common.ptod("ret: " + ret);
    return ret;
  }
}
