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
import java.util.Vector;

import Utils.Fget;
import Utils.OS_cmd;

/**
 * This class handles monitoring of /var/adm/messages, and reports new lines
 * added at the end of the file, or from a newly switched /var/adm/messages.
 * After a switch, first read messages.0 and report the last new lines there.
 */

class Adm_msgs extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String logname;
  private long   last_changed_time;
  private int    next_line_number;

  private static boolean valid_file = false;
  public static Thread adm_thread = null;

  private static String  var_msgs =  "/var/adm/messages";
  private static String  var_msgs0 = "/var/adm/messages.0";
  private static int     sleep_time = 5000;

  //private static String  var_msgs =  "/tmp/messages";
  //private static String  var_msgs0 = "/tmp/messages.0";
  //private static int     sleep_time = 1000;



  public Adm_msgs()
  {
    setName("Adm_msgs");
  }

  /**
   * Create a new instance, and set last time referenced and last line count
   */
  public Adm_msgs(String file)
  {
    /* Store filename: */
    logname = file;

    /* Get the last file change timestamp: */
    last_changed_time = new File(logname).lastModified();

    /* First see if the file really exists: */
    if (!new File(file).exists())
    {
      common.ptod("File '" + file + "' does not exist. Monitoring bypassed");
      return;
    }

    /* If we can't read, OK: */
    if (new File(logname).canRead())
    {
      /* Get the next line number from file: */
      Fget fg = new Fget(logname);
      while (fg.get() != null)
        next_line_number++;
      fg.close();
      valid_file = true;
    }
  }

  public static void terminate()
  {
    if (adm_thread != null)
      adm_thread.interrupt();
  }


  public void run()
  {
    adm_thread = Thread.currentThread();

    try
    {
      //common.ptod("Starting Adm_msgs");

      Adm_msgs log  = new Adm_msgs(var_msgs);
      Adm_msgs log0 = new Adm_msgs(var_msgs0);


      while (true)
      {
        common.sleep_some(sleep_time);
        if (Thread.interrupted())
          break;


        /* If messages.0 has changed, go from there: */
        if (log0.valid_file && log0.has_file_changed())
        {
          /* Store the old messages last line# in that of messages.0 */
          log0.next_line_number = log.next_line_number;

          /* Report the last lines from messages0: */
          log0.report_new_lines(log.next_line_number);

          /* Report all lines from messages: */
          log.report_new_lines(0);
        }

        /* If messages has changed, report them: */
        else if (log.valid_file && log.has_file_changed())
          log.report_new_lines(log.next_line_number);
      }
    }
    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  /**
   * Report the new lines starting with 'starting_line'
   */
  private void report_new_lines(int starting_line)
  {
    String line        = null;
    boolean title_done = false;
    Vector messages    = new Vector(16, 0);


    /* Go through all lines: */
    Fget fg = new Fget(logname);
    int linecount = 0;
    while ((line = fg.get()) != null)
    {
      if (linecount++ < starting_line)
        continue;

      if (!title_done)
      {
        messages.add("New messages found on /var/adm/messages. Do they belong to you?");
        title_done = true;
      }
      messages.add(this.logname + ": " + line);
    }
    fg.close();

    /* When messages is switched and no lines are found, we don't want to */
    /* print the extra blank line:                                        */
    if (title_done)
      messages.add("");

    next_line_number = linecount;

    /* Now send this stuff to the master or only to the local log: */
    if (!common.get_debug(common.NO_ADM_ON_CONSOLE))
      SlaveJvm.sendMessageToConsole(messages);
    else
    {
      for (int i = 0; i < messages.size(); i++)
        common.ptod(messages.elementAt(i));
    }
  }


  /**
   * Check the last changed time for a file.
   */
  private boolean has_file_changed()
  {
    /* Get the last file change timestamp: */
    long time = new File(this.logname).lastModified();

    /* I had one case of the file disappearing! */
    if (time == 0)
      return false;

    if (time != last_changed_time)
    {
      last_changed_time = time;
      return true;
    }

    return false;
  }


  /**
   * Send the last 500 lines of /var/adm/messages to the master.
   * This will be done in common.failure() but also after sucessful completion of
   * vdbench.
   */
  public static void copy_varadmmsgs()
  {
    /* Only do this when the run was far enough along to get work started: */
    if (!SlaveWorker.isAdmRunning())
      return;

    String cmd = "/usr/bin/cat ";
    Vector lines = new Vector(500, 0);

    /* If none exist, don't waste your time: */
    if (!common.onSolaris() &&
        !new File(var_msgs).exists() &&
        !new File(var_msgs0).exists())
        return;

    /* Figure out which of the files exists: */
    if (Fget.file_exists(var_msgs0))
      cmd += var_msgs0;

    if (Fget.file_exists(var_msgs))
      cmd += " " + var_msgs;

    cmd += " | tail -500";

    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);
    ocmd.execute(false);
    String[] stdout = ocmd.getStdout();


    lines.add("Last 500 lines of " + var_msgs0 + " and " + var_msgs);
    lines.add("command used: " + cmd);
    lines.add("");

    for (int i = 0; i < stdout.length; i++)
      lines.add(stdout[i]);

    /* Now send this stuff to the master: */
    SlaveJvm.sendMessageToMaster(SocketMessage.ADM_MESSAGES, lines);
  }


  public static void main(String args[])
  {
  }
}


