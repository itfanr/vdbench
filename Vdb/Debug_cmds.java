package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
 * This class handles the parmfile start_cmd= and end_cmd= parameters
 */
class Debug_cmds implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Vector commands = new Vector(1);
  private String target   = "log";
  private boolean master  = false;

  public static Debug_cmds starting_command = new Debug_cmds();
  public static Debug_cmds ending_command   = new Debug_cmds();


  public Debug_cmds storeCommands(String[] parms)
  {
    for (int i = 0; i < parms.length; i++)
    {
      String parm = parms[i];
      if (parm.startsWith("cons") || parm.startsWith("sum") || parm.startsWith("log"))
        target = parm;
      else if (parm.equalsIgnoreCase("master"))
        master = true;
      else if (parm.equalsIgnoreCase("slave"))
        master = false;
      else
      {
        parm = common.replace_string(parm, "$output", Vdbmain.output_dir);
        commands.add(parm);
      }
    }

    return this;
  }


  public Object clone()
  {
    try
    {
      Debug_cmds dc = (Debug_cmds) super.clone();
      dc.commands   = (Vector) commands.clone();
      return dc;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
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
    /* We execute on slave, unless specifically requested to run on master: */
    if (SlaveJvm.isThisSlave() && master)
      return true;

    for (int i = 0; i < commands.size(); i++)
    {
      String use_command = (String) commands.elementAt(i);

      /* Get the first word of the command. If it is in the shared library, call that: */
      String[] split = use_command.split(" +");
      if (new File(common.get_shared_lib() + File.separator + split[0]).exists())
      {
        use_command = common.get_shared_lib() + File.separator + split[0];
        for (int j = 1; j < split.length; j++)
          use_command += " " + split[j];
      }

      common.ptod("Start/end command: executing '" + use_command + "'");
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText(use_command);

      ocmd.setOutputMethod(new CommandOutput()
                           {
                             public boolean newLine(String line, String type, boolean more)
                             {
                               /* Note that the ':' here causes output to go to master's console: */
                               /* (not longer sure what that means.... */
                               if (type.equals("stdout"))
                               {
                                 if (target.startsWith("cons"))
                                 {
                                   if (SlaveJvm.isThisSlave())
                                     SlaveJvm.sendMessageToConsole("Cmd: %s", line);
                                   else
                                     common.ptod("Cmd: %s", line);
                                 }

                                 else if (target.startsWith("sum"))
                                   common.psum("Cmd: %s ", line);

                                 else if (target.startsWith("log"))
                                   common.plog("Cmd: %s", line);

                                 else
                                   common.failure("Invalid target: " + target);
                               }
                               else
                               {
                                 if (target.startsWith("cons"))
                                 {
                                   if (SlaveJvm.isThisSlave())
                                     SlaveJvm.sendMessageToConsole("Cmd: stderr %s", line);
                                   else
                                     common.ptod("Cmd: stderr %s", line);
                                 }

                                 else if (target.startsWith("sum"))
                                   common.psum("Cmd: stderr %s", line);

                                 else if (target.startsWith("log"))
                                   common.plog("Cmd: stderr %s", line);

                                 else
                                   common.failure("Invalid target: " + target);
                               }

                               return true;
                             }
                           });

      ocmd.execute(true);
    }

    return true;
  }

  public boolean masterOnly()
  {
    return master;
  }
}
