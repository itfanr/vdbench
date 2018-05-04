package Utils;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.Vector;

import javax.swing.*;

import Vdb.Signal;


/**
  * This class handles all requests to run OS level commands.
  */
public class OS_cmd
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private   String         command_line   = "";

  private   Vector         stdout         = new Vector(256, 0);
  protected CommandOutput  output_method  = null;

  private   Vector         stderr         = new Vector(256, 0);
  private   PrintWriter    stdout_pw      = null;
  private   PrintWriter    stderr_pw      = null;
  private   boolean        ignore         = false;
  protected Process        lproc          = null;
  private   boolean        wait           = false;
  private   boolean        rc             = false;
  protected boolean        save_and_print = false;
  private   boolean        log_command    = true;
  private   boolean        wait_later     = false;
  protected boolean        wait_for_jvm   = false;

  private   Get_cmd_stream error_stream;
  private   Get_cmd_stream output_stream;

  private static Vector need_to_kill = new Vector(8, 0);



  private void killAtEnd()
  {
    need_to_kill.add(this);
  }
  public void removeKillAtEnd()
  {
    if (!need_to_kill.remove(this))
      common.plog("removeKillAtEnd(): Unable to find " + getCmd());
  }
  public static void killAll()
  {
    for (int i = 0; i < need_to_kill.size(); i++)
    {
      OS_cmd ocmd = (OS_cmd) need_to_kill.elementAt(i);
      ocmd.killCommand();
    }
  }

  /**
   * Kill currently outstanding command.
   * have not tested this on Windows.
   */
  public boolean killing = false;
  public void killCommand()
  {
    common.plog("killCommand: " + getCmd());
    killing = true;

    removeKillAtEnd();

    try
    {
      if (lproc != null)
        lproc.destroy();
      if (error_stream != null)
        error_stream.interrupt();
      if (output_stream != null)
        output_stream.interrupt();
    }
    catch (Exception e)
    {
      common.ptod("Exception during killCommand: " + getCmd() + "; continuing");
      common.ptod(e);
    }
  }



  /**
   * Add a string to the current command line.
   */
  public void addText(long cmd)
  {
    addText("" + cmd);
  }
  public void addText(String cmd)
  {
    command_line += cmd + " ";
    //common.ptod("addText: " + command_line);
  }


  /**
   * Add a file name to the current command line.
   * If file name has an embedded blank, add quotes.
   */
  public void addQuot(String file)
  {
    String Q = "\"";
    if (!common.onWindows())
    {
      command_line += file + " ";
      return;
    }

    file = file.trim();

    /* If the string ends with a '\' and we then add a double quote, */
    /* the wonderful world of Windows then thinks that the '\' is an */
    /* escape character, not recognizing the closing quote.          */
    /* Remove any terminating back-slash:                            */
    if (file.endsWith("\\"))
      file = file.substring(0, file.length() - 1);

    if (file.indexOf(" ") == -1)
      command_line += file + " ";
    else
      command_line += Q + file + Q + " ";
  }


  /**
   * To go around embedded blank problems we surround a file name that
   * is passed as a parameter with "{" "}" instead of quotes.
   * Getopt() then gathers it all back together again.
   */
  public void addFile(String file)
  {
    file = file.trim();
    if (file.indexOf(" ") == -1)
      command_line += file + " ";
    else
      command_line += "{" + file + "}" + " ";
    //common.ptod("addFile: " + command_line);
  }


  /**
   * Set ignore flag
   */
  public void setIgnore(boolean bool)
  {
    ignore = bool;
  }

  public String[] getStdout()
  {
    return(String[]) stdout.toArray(new String[0]);
  }
  public String[] getStderr()
  {
    return(String[]) stderr.toArray(new String[0]);
  }
  public void printStdout()
  {
    for (int i = 0; i < stdout.size(); i++)
      common.ptod("stdout: " + stdout.elementAt(i));
  }
  public void printStderr()
  {
    for (int i = 0; i < stderr.size(); i++)
      common.ptod("stderr: " + stderr.elementAt(i));
  }

  public void setCmd(String cmd)
  {
    command_line = cmd;
  }
  public String getCmd()
  {
    return command_line;
  }
  public boolean getRC()
  {
    return rc;
  }



  /**
   * This method executes a simple command.
   */
  public static OS_cmd execute(String cmd)
  {
    return execute(cmd, true);
  }
  public static OS_cmd execute(String cmd, boolean need_log)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.log_command = need_log;
    ocmd.addText(cmd);
    ocmd.execute();

    return ocmd;
  }
  /**
   * This method executes a simple command.
   */
  public static OS_cmd executeCmd(String cmd)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);
    ocmd.execute();
    return ocmd;
  }


  /**
  * Issue OS command and return output lines in a Vector.
   */
  public boolean execute(boolean bool)
  {
    log_command = bool;
    return execute();
  }
  public boolean execute()
  {
    /* Keep track so that if needed we can kill this at termination: */
    killAtEnd();

    /* Trim the command. Because of the fact that we always add a blank each */
    /* time that we add text or a file, the command will end on a blank.     */
    /* Somewhere down here in this class we check to see if the command ends */
    /* with an '&', causing an asynchronous execution. because of the        */
    /* extra blank this '&' check failed.                                    */
    command_line = command_line.trim();

    /* Create array to be passed to Runtime: */
    String[] cmd_array = null;
    if (common.onWindows())
      cmd_array = new String[] { "cmd", "/q /c", command_line};

    else if (common.onSolaris())
      cmd_array = new String[] { "/bin/bash", "-c", command_line};

    else if (common.onAix())
      cmd_array = command_line.split(" +");

    else
      cmd_array = new String[] { "/bin/bash", "-c", command_line};

    /* chmod commands just clutter up the output: */
    if (log_command && command_line.indexOf("chmod") == -1)
    {
      Vdb.common.ptod("execute(): " + command_line);
    }


    lproc = null;
    try
    {
      /* Windows uses a temporary .bat file: */
      if (common.onWindows())
        cmd_array[2] = writeTempFile();

      lproc = Runtime.getRuntime().exec(cmd_array);


      /* Setup output threads: */
      if (stderr_pw != null)
        error_stream  = new Get_cmd_stream(this, lproc.getErrorStream(), "stderr", stderr_pw);
      else
        error_stream  = new Get_cmd_stream(this, lproc.getErrorStream(), "stderr", stderr);

      if (stdout_pw != null)
        output_stream = new Get_cmd_stream(this, lproc.getInputStream(), "stdout", stdout_pw);
      else
        output_stream = new Get_cmd_stream(this, lproc.getInputStream(), "stdout", stdout);

      error_stream.start();
      output_stream.start();

      //error_stream.waitForActive();
      //output_stream.waitForActive();

      /* If we don't need to wait, exit right here: */
      //common.ptod("rc1: " + rc + " " + this + " " + getCmd());
      if (wait_later || command_line.trim().endsWith("&"))
      {
        return true;
      }

      //double start_ms = System.currentTimeMillis();
      lproc.waitFor();
      rc = (lproc.exitValue() == 0);

      /* Wait for command to finish, unless it is specifically background: */
      Signal signal = new Signal(1000);
      while (output_stream.isAlive() && !command_line.trim().endsWith("&"))
      {
        common.sleep_some_no_int(2);
        if (output_stream.isAlive() && signal.go())
          common.plog("Waiting for command1 '" + command_line + "' completion");
      }

      //double end_ms = System.currentTimeMillis();
      //Vdb.common.ptod("OS_cmd elapsed time: %8.3f seconds. %s",
      //                (end_ms - start_ms) / 1000., command_line);

      /* Delete temp file once the command is done:                                  */
      /* (Don't want to wait for Java termination which in some cases can take days) */
      if (common.onWindows())
        new File(cmd_array[2]).delete();
    }

    catch (InterruptedException e)
    {
      common.ptod("InterruptedException, continuing: " + e);
      stderr.add("InterruptedException, continuing: " + Vdb.common.get_stacktrace(e));

      rc = false;
      removeKillAtEnd();
      return false;
    }

    catch (Exception e)
    {
      for (int i = 0; stderr != null && i < stderr.size(); i++)
      {
        String str = (String) stderr.elementAt(i);
        common.ptod("stderr: " + str);
      }
      if (ignore)
      {
        rc = false;
        removeKillAtEnd();
        common.ptod("Exception ignored.");
        return false;
      }

      common.ptod("Error occurred in os_command(3) " + command_line);
      e.printStackTrace();

      if (command_line.indexOf("chmod") != -1)
        common.ptod("chmod command failed; error ignored.");
      else
      {
        common.ptod("InterruptedException, continuing: " + e);
        stderr.add("InterruptedException, continuing: " + Vdb.common.get_stacktrace(e));

        rc = false;
        removeKillAtEnd();
        common.ptod("Exception ignored.");
        return false;
      }
    }

    if (!command_line.trim().endsWith("&"))
    {
      removeKillAtEnd();
    }

    return rc;
  }


  /**
   * Windows systems have a problem with rsh.
   * The Process() does not terminate, neither do the stdout and stderr
   * streams get EOF.
   * Therefore, for this situation only in the stdout and stderr stream
   * reader will I wait for a specific DONE message from a slave.
   * This message then will cause me to forcibly close the stream readers
   * and also terminate the Process().
   *
   * There is a theoretical chance that, since the DONE message comes from
   * stdout, possible outstanding data on stderr will be lost.
   */
  protected boolean isWindowsRsh()
  {
    if (!common.onWindows())
      return false;

    if (command_line.startsWith("rsh"))
      return true;

    return false;
  }


  public void setStdout()
  {
    stdout = new Vector(256, 0);
  }
  public void setOutputMethod(CommandOutput cout)
  {
    output_method = cout;
  }
  public void setStdout(PrintWriter pw)
  {
    stdout_pw = pw;
  }
  public void setStderr()
  {
    stderr = new Vector(256, 0);
  }
  public void setStderr(PrintWriter pw)
  {
    stderr_pw = pw;
  }

  public void setWaitForJvm()
  {
    wait_for_jvm = true;
  }
  public void setNoWait()
  {
    wait_later = true;
  }
  public boolean waitFor()
  {
    long start = System.currentTimeMillis();
    if (!wait_later)
      common.failure("OS_cmd.waitFor(): Waiting for command that is already done. Oops");

    /* Wait for output stream to finish: */
    while (output_stream.isAlive())
    {
      common.sleep_some_no_int(50);
      if (output_stream.isAlive() &&
          (System.currentTimeMillis() - start) > 5000)
      {
        common.ptod("OS_cmd.waitFor(); waiting for command: " + command_line);
        start = System.currentTimeMillis();
      }
    }

    try
    {
      lproc.waitFor();
    }

    catch (InterruptedException e)
    {
    }

    rc = (lproc.exitValue() == 0);

    return rc;
  }

  /**
   * Both save stdout/stderr in a Vector, AND, print it on stdout.
   *
   * Default is to either save it. OR, print it. Not both.
   */
  public void setSaveAndPrint()
  {
    save_and_print = true;;
  }


  /**
   * Cancel a run by creating a dummy file in the output directory.
   * The 'batch' program will periodically check for this file.
   */
  public static boolean cancel_run(String cancel_dir, String run)
  {
    /*
    common.where();
    if (command_active != null)
    {
      common.where();
      synchronized (command_active)
      {
        if (command_active.intValue() == 0)
        {
          common.where();
          Message.infoMsg("os_command(): No command active");
          return false;
        }
      }
    }
    */


    File fptr = new File(cancel_dir, "cancel_run");
    if (!fptr.delete())
    {
      Message.infoMsg(run + " cancel requested failed. " +
                      "Do you have write access to the trace directory?\n" +
                      "Or is the trace not far along enough yet to recognize this request?");
      return false;
    }
    else
    {
      Message.infoMsg(run + " cancel requested by user.\n" +
                      "Run will terminate within 10 seconds.");
      return true;
    }
  }


  public static String getWindowsSystemDrive()
  {
    String drive = "C:\\";
    OS_cmd ocmd = OS_cmd.execute("echo %systemdrive%");
    if (ocmd.getRC())
      drive = ocmd.getStdout()[0] + "\\";

    return drive;
  }


  /**
   * Write the command to a temporary file to be used as a staging area
   * for command execution under windows.
   * There are just too many problems on Windows with long file names and
   * especially with space embedded file names that creating a temporary
   * script file just made it easier and avoided the problem where
   * getRuntime.exec() did multiple passes of parsing a quoted file name,
   * and then passed it on downstream to wherever (unknown) without the
   * quotes.
   *
   */
  private String writeTempFile()
  {
    File tmp_file = null;

    try
    {
      try
      {
        tmp_file = File.createTempFile("vdb", ".bat");
      }
      catch (IOException e)
      {
        common.ptod("Unable to create temporary 'bat' file for windows. ");
        common.failure(e);
      }

      tmp_file.deleteOnExit();
      FileOutputStream os = new FileOutputStream(tmp_file);
      PrintWriter pw      = new PrintWriter(new BufferedOutputStream(os));
      pw.print(command_line);
      pw.close();
    }
    catch (FileNotFoundException e)
    {
      common.failure(e);
    }

    return tmp_file.getAbsolutePath();
  }


  public static String getShutdownMessage()
  {
    return "JVM is shutting down";
  }



  public static void main(String[] args)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("nicstat 1 10");
    ocmd.setNoWait();

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             common.ptod("type: " + type + " " + line);
                             return true;
                           }
                         });


    ocmd.execute();
    common.sleep_some_no_int(3000);
    ocmd.killCommand();
  }
}


/**
 * Asyncronous reading of command output streams.
 * Resolves the issue that if we don't pick up the output fast enough the buffer
 * will be full and everything will hang.
 *
 * As original base I used some sample code found on the web. There are so many
 * different versions out there that it is virtually impossible to determine
 * which version I originally picked up. Since there are so many versions out
 * there without any legal gobbledegook (and only one with), that the safest
 * thing is to use the JavaWorld article at
 * http://www.javaworld.com/javaworld/jw-12-2000/jw-1229-traps.html
 * as base reference.
 */
class Get_cmd_stream extends Thread
{
  private OS_cmd      current_command;
  private InputStream istream;
  private String      type;
  private Vector      output_vector = null;
  private PrintWriter pw = null;

  protected BufferedReader    br   = null;


  Get_cmd_stream(OS_cmd cmd, InputStream istream, String type, Object output)
  {
    this.current_command = cmd;
    this.istream = istream;
    this.type    = type;
    this.setName("Get_cmd_stream " + type + " " + cmd.getCmd());

    if (output instanceof Vector)
      this.output_vector = (Vector) output;

    else if (output instanceof PrintWriter)
      this.pw = (PrintWriter) output;

    else
      common.failure("Unexpected output type: " + output);
  }


  /**
   * Intercept output from os_command.
   */
  public void run()
  {
    String line = null;

    synchronized(this)
    {

      try
      {
        br = new BufferedReader(new InputStreamReader(istream));

        while (true)
        {
          if ((line = br.readLine()) == null)
            break;

          if (current_command.killing)
          {
            br.close();
            break;
          }
          //common.ptod("line: " + line);
          //common.ptod("output_vector: " + output_vector);
          //common.ptod("current_command.save_and_print: " + current_command.save_and_print);

          /* Always save output in a Vector: */
          if (output_vector != null && current_command.output_method == null)
            output_vector.addElement(line);

          if (current_command.save_and_print)
            System.out.println(line);

          /* This code to forcibly shutdown a running exec() command */
          /* that otherwise could be hanging trying to read stderr/stdout */
          if (current_command.wait_for_jvm && line.equals(OS_cmd.getShutdownMessage()))
          {
            if (current_command.isWindowsRsh())
              current_command.lproc.destroy();
            break;
          }

          /* Directly call user? */
          if (current_command.output_method != null)
            current_command.output_method.newLine(line, type, br.ready());

          /* Send directly to printwriter: */
          else if (pw != null)
            pw.println(line);
        }
      }

      catch (IOException e)
      {
        if (!current_command.killing)
        {
          common.ptod("IOException for: " + current_command.getCmd());
          common.failure(e);
        }
      }

      catch (Exception e)
      {
        common.ptod("Unexpected exception:");
        common.failure(e);
      }

      this.notifyAll();

      //if (current_command.output_method != null && type.equals("stdout"))
      //  current_command.output_method.newLine(null, type, true);
    }
  }
}


