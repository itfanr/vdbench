package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.CommandOutput;
import Utils.OS_cmd;


/**
 * This class handles writing to error_log.html.
 *
 * Messages generated from the master go directly to the file,
 * while messages from the slave are sent to the master using sockets.
 * The master then will write them to file.
 */
public class ErrorLog
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static PrintWriter pw                = null;
  private static int         errors            = 0;
  private static long        tod_last_dv_error = 0;

  private static String      NOCONSOLE = "noconsole";

  private static String header1 = "Error log. If there are no error messages beyond this line " +
                                  "then there were no Data Validation or I/O errors.";
  private static String header2 = "(Memory map allocation messages for Data Validation and Dedup are just FYI.)";

  public static void create()
  {
    pw = Report.createHmtlFile("errorlog.html");

    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss MMM dd yyyy zzz"  );
    pw.printf("Vdbench error log, created %s \n", df.format(new Date()));
    pw.println(header1);
    //if (Validate.isRealValidate())
    //  pw.println(header2);
    pw.println();
  }

  public static long getLastErrorTod()
  {
    return tod_last_dv_error;
  }

  /**
   * Messages on slave to be sent to master
   */
  public static void sendMessagesToMaster(Vector messages)
  {
    SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, messages);
  }


  public static void ptodSlave(Slave slave, Vector <String> messages)
  {
    synchronized (pw)
    {
      for (String msg : messages)
        ptod(slave.getLabel() + ": " + msg);
    }
  }


  /**
   * Message methods.
   * If on slave, send to master,
   * If on master, send to proper target
   *
   * ptod(): send to both console and errorlog
   * plog(): send only to errorlog
   */
  public static void ptod(String format, Object ... args)
  {
    if (pw == null)
    {
      /* If there are no arguments that don't use String.format(), */
      /* Because there could be REAL non-mask % sings there: */
      String txt;
      if (args.length == 0)
        txt = common.tod() + " " + format;
      else
        txt = common.tod() + " " + String.format(format, args);
      SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, txt);
      common.ptod(txt);
    }
    else
    {
      /* If there are no arguments that don't use String.format(), */
      /* Because there could be REAL non-mask % sings there: */
      String txt;
      if (args.length == 0)
        txt = common.tod() + " " + format;
      else
        txt = common.tod() + " " + String.format(format, args);
      pw.println(txt);
      common.stdout.println(txt);
    }
  }
  public static void ptodVector(Vector <String> messages)
  {
    for (String msg : messages)
    {
      if (pw == null)
        SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_MESSAGE, msg);
      else
        pw.println(msg);
    }
  }

  public static void plog(String format, Object ... args)
  {
    String txt = String.format(format, args);
    if (pw == null)
    {
      SlaveJvm.sendMessageToMaster(SocketMessage.ERROR_LOG_MESSAGE, txt);
      common.ptod(txt);
    }
    else
    {
      pw.println(common.tod() + " " + txt);
      //common.plog(txt, common.stdout);
    }
  }

  /**
   * Allow for a Vector of messages to be created which then later on will be
   * sent to the master.
   * This prevents intermixing of messages from different slaves once received
   * on the master.
   * Most errorlog messages are created single threaded, so not too much worry
   * about intermixing threads on a slave.
   *
   * A BIG problem though is that these messages arrive later, with an out of
   * sync timestamp. That confuses the heck out of me!
   * Now adding timestamp HERE.
   */
  private static Vector <String> messages = new Vector(16);
  public static void add(String format, Object ... args)
  {
    synchronized (messages)
    {
      messages.add(common.tod() + " " + String.format(format, args));
    }
  }
  public static void flush()
  {
    synchronized (messages)
    {
      if (messages.size() > 0)
      {
        ptodVector(messages);
        common.ptod(messages);
      }
      messages.clear();
    }
  }
  public static int size()
  {
    return messages.size();
  }


  /**
   * Count i/o or DV errors.
   */
  public static void countErrorsOnSlave(String lun, long lba, int xfersize)
  {
    if (Validate.getErrorCommand() == null)
    {
      SlaveJvm.sendMessageToMaster(SocketMessage.COUNT_ERRORS);
      return;
    }

    startErrorCommand(lun, lba, xfersize);
  }

  public static void countErrorsOnMaster()
  {
    /* Terminate upon request: */
    tod_last_dv_error = System.currentTimeMillis();

    errors++;
    if (errors >= Validate.getMaxErrorCount())
    {
      if (common.get_debug(common.NO_ERROR_ABORT))
      {
        common.ptod("'data_error=%d (%d) requested", Validate.getMaxErrorCount(), errors);
        common.ptod("'NO_ERROR_ABORT' debug option requested. Shutting down run.");
        common.ptod("(You may get this message multiple times because of pending errors).");
        Vdbmain.setWorkloadDone(true);
      }
      else
      {
        /* Give stdout from the slave some time to arrive on the master: */
        /* This however also gives extra time to NEWER corruptions to show up */
        //common.sleep_some(500);
        // journal check removed, Binia, 05/23/16
        //if (!Validate.isJournalRecoveryActive())
          common.failure("'data_errors=%d' requested. Abort rd=%s after last error.",
                         Validate.getMaxErrorCount(), RD_entry.next_rd.rd_name);
      }
    }
  }

  public static void clearCount()
  {
    errors = 0;
  }
  public static int getErrorCount()
  {
    return errors;
  }


  /**
   * Start the optional data_errors="xyz $output $lun $lba $size" command.
   */
  private static synchronized void startErrorCommand(String lun, long lba, int xfersize)
  {
    String cmd = Validate.getErrorCommand();
    if (cmd.startsWith("stop"))
      common.failure("'data_error=stop' requested. Abort after first error");

    /* Start error script, substituting a few fields: */
    cmd = common.replace(cmd, "$output", Validate.getOutput());
    cmd = common.replace(cmd, "$lun",    lun);
    cmd = common.replace(cmd, "$lba",    "" + lba);
    cmd = common.replace(cmd, "$size",   "" + xfersize);
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);
    ocmd.execute();

    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();
    for (int i = 0; i < stdout.length; i++)
      common.ptod("stdout: " + stdout[i]);
    for (int i = 0; i < stderr.length; i++)
      common.ptod("stderr: " + stderr[i]);

    /* Give master a little time to receive the message before we abort: */
    common.sleep_some(1000);

    common.failure("'data_error=" + cmd + "' requested. Abort after first error");
  }
}




