package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.*;

import Utils.*;

/**
 * This program handles the execution of the jstack Java utility that can
 * print an execution stack from an other active process.
 */
public class Jstack
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String pid;
  String program;

  static Vector programs = new Vector(64, 0);
  static String jstack = null;
  private static Fput fp = null;

  private static boolean skip_streams = false;


  public Jstack(String p, String prog)
  {
    pid     = p;
    program = prog;
  }


  /**
   * Run the 'ps -ef' command to find Java programs running.
   *
   * Solaris: use th 'ps' command
   *
   * Windows: - use the 'ps' command (from MKS tools?)
   *          - use the 'tasklist' command from the Microsoft 'Support Tools'
   */
  private static void getPrograms()
  {
    if (common.onSolaris())
      runSolaris();

    else if (common.onLinux())  //????
      runSolaris();

    else if (common.onWindows())
      runWindows();

    else
      common.failure("Vdbench jstack: Current platform not supported (yet?)");
  }

  private static void runSolaris()
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("ps -ef");
    ocmd.setStdout();
    ocmd.execute();

    if (!ocmd.getRC())
      common.failure("Error executing 'ps -ef'");

    String[] lines = ocmd.getStdout();

    for (int i = 0; i < lines.length; i++)
    {
      String line = lines[i].toLowerCase();
      if (line.indexOf("java") == -1)
        continue;
      if (line.indexOf("jstack") != -1)
        continue;


      StringTokenizer st = new StringTokenizer(line);
      if (st.countTokens() < 8)
        continue;

      st.nextToken();
      String pidx = st.nextToken();
      st.nextToken();
      st.nextToken();
      st.nextToken();
      st.nextToken();
      st.nextToken();
      String start = st.nextToken();

      String program = line.substring(line.indexOf(start));
      print("program: pid=" + pidx + " " + program);

      common.ptod("pidx: " + pidx);
      common.ptod("common.getProcessId(): " + common.getProcessId());
      System.exit(777);
      if (Integer.parseInt(pidx) == common.getProcessId())
      {
        common.ptod("Skipping myself");
        continue;
      }

      Jstack js = new Jstack(pidx, program);
      programs.add(js);
    }
  }

  private static void runWindows()
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("xps -ef");         // force use of tasklist!
    ocmd.setStdout();
    ocmd.execute();

    if (ocmd.getRC())
    {
      String[] lines = ocmd.getStdout();
      for (int i = 0; i < lines.length; i++)
      {
        String line = lines[i];
        //common.ptod("line: " + line);
        if (line.indexOf("java") == -1)
          continue;
        if (line.indexOf("jstack") != -1)
          continue;
        if (line.indexOf("javaw") != -1)
          continue;

        String[] split = line.trim().split(" +");
        String program;

        if (split.length == 8)
          program = split[7];
        else if (split.length == 9)
          program = split[8];
        else
          continue;

        String pidx = split[1];

        print("program: " + program);


        common.ptod("pidx: " + pidx);
        common.ptod("common.getProcessId(): " + common.getProcessId());
        System.exit(777);
        if (Integer.parseInt(pidx) == common.getProcessId())
        {
          common.ptod("Skipping myself");
          continue;
        }

        Jstack js = new Jstack(pidx, program);
        programs.add(js);
      }
    }

    else
    {
      ocmd = new OS_cmd();
      ocmd.addText("tasklist");  /* For windows XP and (maybe?) up */
      ocmd.setStdout();
      ocmd.execute();

      if (ocmd.getRC())
      {
        String[] lines = ocmd.getStdout();
        for (int i = 0; i < lines.length; i++)
        {
          String line = lines[i].trim().toLowerCase();
          //common.ptod("line: " + line);
          if (line.indexOf("java.exe") != -1)
          {
            String[] split = line.split(" +");
            Jstack js = new Jstack(split[1], split[0]);

            String pidx = split[1];
            if (Integer.parseInt(pidx) == common.getProcessId())
            {
              common.ptod("Skipping myself");
              continue;
            }

            programs.add(js);
            print("program: " + line);
          }
        }
      }

      else
        print("Both 'ps -ef' and 'tasklist.exe' failed");
    }
  }


  private static void doJstack()
  {
    for (int i = 0; i < programs.size(); i++)
    {
      Jstack js = (Jstack) programs.elementAt(i);
      print("");

      OS_cmd ocmd = new OS_cmd();
      ocmd.addQuot(jstack);
      ocmd.addText(js.pid);
      ocmd.setStdout();
      ocmd.setStderr();
      ocmd.execute();

      lines = ocmd.getStdout();
      boolean vdbmain   = false;
      boolean slavejvm  = false;
      boolean stream    = false;
      for (int j = 0; j < lines.length; j++)
      {
        //common.ptod("lines[j]: " + lines[j]);
        if (lines[j].indexOf("Vdb.Vdbmain")    != -1)  vdbmain  = true;
        if (lines[j].indexOf("Vdb.SlaveJvm")   != -1)  slavejvm = true;
        if (lines[j].indexOf("Get_cmd_stream") != -1)  stream   = true;
      }

      if (stream && skip_streams)
      {
        print("Skipping Get_cmd_stream");
        continue;
      }

      if (vdbmain)
        print("Jstatus for VdbMain " + js.pid + " " + js.program);
      else if (slavejvm)
        print("Jstatus for SlaveJvm " + js.pid + " " + js.program);
      else
        print("Jstatus for 'other' " + js.pid + " " + js.program);


      String[] lines = ocmd.getStderr();
      for (int j = 0; j < lines.length; j++)
      {
        String line = lines[j];
        print("stderr: " + line);
      }

      lines = ocmd.getStdout();
      for (int j = 0; j < lines.length; j++)
      {
        String line = lines[j];
        print("stdout: " + line);
      }
    }

    /* Just make sure I report that we're done in case the output is incomplete: */
    print("Jstack completed successfully");
  }

  private static void print(String txt)
  {
    common.ptod(txt);
    fp.println(txt);
  }

  static int count = 1;
  static String[] lines;
  static int index = 0;
  private static Vector getBlock()
  {
    Vector block = new Vector(32, 0);

    /* A block starts with a blank line: */
    for (index++; index < lines.length; index++)
    {
      /* Get everything until the next blank line: */
      String line = lines[index];
      String[] split = line.trim().split(" +");
      if (split.length <= 2)
        break;
      //print("index: " + index + " " + line);
      block.add(line.substring(8));
      //print("add block: " + count + " " + line);
    }

    count++;
    index--;
    return block;
  }

  // Not sure anymore what this is for :-)
  // Likely to remove garbage?
  private static void parseExistingJstackOutput(String fname)
  {
    int gc_skips = 0;
    lines       = Fget.readFileToArray(fname);
    String line = null;
    fp          = new Fput("jstack2.txt");

    top:
    for (index = 0; index < lines.length; index++)
    {
      line = lines[index];

      if (line.indexOf("Jstack for") != -1)
      {
        fp.println(line);
        continue;
      }
      if (line.indexOf("stdout:") == -1)
        continue;


      /* A blank line starts a block: */
      String[] split = line.trim().split(" +");
      //if (split.length != 2)
      //  continue;

      /* Get a block of info: */
      Vector <String> block = getBlock();
      if (block.size() == 0)
        continue;
      line = (String) block.firstElement();

      /* Get rid of stuff we don't want: */
      if (line.indexOf("Attach Listener") != -1)
        continue;

      if (line.contains("GC task thread#"))
      {
        gc_skips++;
        continue;
      }

      //if (line.indexOf("IO_task") != -1)
      //{
      //  String line3 = (String) block.elementAt(2);
      //  if (line3.indexOf("multi_io") != -1)
      //    continue;
      //}

      String[] skips =
      {
        "Low Memory",
        "Signal Dispatcher",
        "Low Memory",
        "Signal Dispatcher",
        "Compiler",
        "Finalizer",
        "Reference Handler",
        "VM Thread",
        "VM Periodic Task Thread",
        "JNI global references",
        "jstack",
        "process reaper",
        "JNI global references"
      };

      for (int i = 0; i < skips.length; i++)
      {
        if (line.indexOf(skips[i]) != -1)
          continue top;
      }
      //if (line.indexOf("WG_task") != -1)
      //  continue;

      fp.print(String.format("block %4d: ", count));
      for (int i = 0; i < block.size(); i++)
        fp.print(block.elementAt(i).trim() + " ");
      fp.println("");
    }

    fp.println("Skipped %d 'GC task threads'", gc_skips);

    fp.close();
  }


  public static void main(String[] args)
  {
    Getopt getopt = new Getopt(args, "j:d:s", 99);
    getopt.print("Jstack");
    if (!getopt.isOK())
    {
      common.failure("Parameter scan error");
    }

    /* parse an output file, and print just one line per 'block'. */
    if (args.length == 3 && args[1].equals("-"))
    {
      parseExistingJstackOutput(args[2]);
      return;
    }

    fp = new Fput("jstack.txt");

    /* Some times they add 'jre' at the end: */
    String home = System.getProperty("java.home");
    print("home: " + home);
    if (home.endsWith("/jre") || home.endsWith("\\jre"))
      home = home.substring(0, home.length() - 4);
    print("home: " + home);

    if (common.onWindows() && new File(home + "\\bin\\jstack.exe").exists())
      jstack = home + "\\bin\\jstack.exe";

    else if (new File(home + "/bin/jstack").exists())
      jstack = home + "/bin/jstack";

    else
    {
      common.where();
      if (common.onWindows())
        jstack = "C:\\Program Files\\Java\\jdk1.6.0_10\\bin\\jstack.exe";
      common.where();
      if (common.onSolaris())
        jstack = "/usr/java/bin/jstack";
    }

    print("jstack: " + jstack);
    if (args.length == 2)
      jstack = args[1];
    print("jstack: " + jstack);

    getPrograms();

    doJstack();

    fp.println("Jstack ended at %s", new Date());

    fp.close();

    /* Experiment: */
    parseExistingJstackOutput("jstack.txt");
  }
}

