package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.ArrayList;
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String logname;
  private long   last_changed_time;
  private int    next_line_number;

  private static int    message_count = 0;

  private static boolean valid_file = false;
  public static Thread adm_thread = null;

  private static String  var_msgs;
  private static String  var_msgs0;
  private static int     sleep_time = 5000;

  static
  {
    if (common.onSolaris())
    {
      var_msgs =  "/var/adm/messages";
      var_msgs0 = "/var/adm/messages.0";
    }
    else if (common.onLinux())
    {
      var_msgs =  "/var/log/messages";
      var_msgs0 = "/var/log/messages.1";
    }
  };



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
      common.interruptThread(adm_thread);
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

        /* If we have more messages than allowed, shut down: */
        if (message_count > getMaxCount())
        {
          Vector <String> msgs = new Vector(8);
          msgs.add("*");
          msgs.add("*");
          msgs.add(String.format("* Maximum of %d %s messages reported. Shutting down scan.",
                                 getMaxCount(), var_msgs));
          msgs.add("*");
          msgs.add("* This maximum count can be overriden using 'messagescan=12345'");
          msgs.add("*");
          SlaveJvm.sendMessageToConsole(msgs);
          return;
        }

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
      message_count++;
    }
    fg.close();

    /* When messages is switched and no lines are found, we don't want to */
    /* print the extra blank line:                                        */
    if (title_done)
      messages.add("");

    next_line_number = linecount;

    /* Now send this stuff to the master or only to the local log: */
    // messagescan=nodisplay causes the message to also not go to messages.html!!!!
    if (Adm_msgs.displayMessages())
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
   *
   * Note: this ONLY runs on the slave, so when the master shuts down during a
   * failure the slaves will NOT call this.
   * This is a hole that needs to be fixed.
   */
  public static void copy_varadmmsgs()
  {
    /* Only do this when the run was far enough along to get work started: */
    if (!SlaveWorker.isAdmRunning())
      return;

    /* Wrong systems? */
    if (!common.onSolaris() && ! common.onLinux())
      return;

    /* If none exist, don't waste your time: */
    if (!new File(var_msgs).exists() &&
        !new File(var_msgs0).exists())
        return;

    /* Figure out which of the files exists: */
    String cmd   = "cat ";
    Vector lines = new Vector(500, 0);
    if (Fget.file_exists(var_msgs0))
      cmd += var_msgs0;

    if (Fget.file_exists(var_msgs))
      cmd += " " + var_msgs;

    cmd += " | tail -500";

    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);
    ocmd.execute(false);
    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();


    lines.add("Last 500 lines of " + var_msgs0 + " and " + var_msgs);
    lines.add("command used: " + cmd);
    lines.add("");

    for (int i = 0; i < stderr.length; i++)
      lines.add("stderr: " + stderr[i]);
    for (int i = 0; i < stdout.length; i++)
      lines.add(stdout[i]);

    /* Now send this stuff to the master: */
    SlaveJvm.sendMessageToMaster(SocketMessage.ADM_MESSAGES, lines);
  }


  /**
   * Figure out what the message scan options are.
   * For compatibility we'll always have -d25 and -d26.
   *
   * messagescan=nodisplay
   * messagescan=no
   * messagescan=nnnn
   */
  private static boolean already_checked = false;
  private static boolean display         = true;
  private static boolean scan            = true;
  private static int     max_messages    = 1000;
  public  static boolean displayMessages()
  {
    if (!already_checked)
      already_checked = parseMessageOptions();
    return display;
  }
  public  static boolean scanMessages()
  {
    if (!already_checked)
      already_checked = parseMessageOptions();
    return scan;
  }
  public static int getMaxCount()
  {
    //common.ptod("max_messages: " + max_messages);
    return max_messages;
  }
  public static boolean parseMessageOptions()
  {
    for (String[] array : MiscParms.getMiscellaneous())
    {
      if (!array[0].equals("messagescan"))
        continue;

      for (int i = 1; i < array.length; i++)
      {
        String parm    = array[i];
        String[] split = parm.trim().split("=");
        for (String spl : split)
        {
          if (spl.equals("nodisplay"))
            display = false;
          if (spl.equals("no"))
            scan = false;
          if (common.isNumeric(spl))
            max_messages = Integer.parseInt(spl);
        }
      }
    }

    //common.ptod("display:      " + display);
    //common.ptod("scan:         " + scan);
    //common.ptod("max_messages: " + max_messages);

    return true;
  }
}


