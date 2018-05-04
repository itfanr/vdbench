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
import java.util.*;

import Utils.*;

/**
 * This program handles the execution of the jstack Java utility that can
 * print an execution stack from an other active process.
 */
public class Jstack
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String pid;
  String program;

  static Vector programs = new Vector(64, 0);
  static String jstack = null;
  private static Fput fp = null;


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
      common.failure("Current platform not supported (yet?)");
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
      print("program: " + program);

      Jstack js = new Jstack(pidx, program);
      programs.add(js);
    }
  }

  private static void runWindows()
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("ps -ef");
    ocmd.setStdout();
    ocmd.execute();

    if (ocmd.getRC())
    {
      String[] lines = ocmd.getStdout();
      for (int i = 0; i < lines.length; i++)
      {
        String line = lines[i];
        if (line.indexOf("java") == -1)
          continue;
        if (line.indexOf("jstack") != -1)
          continue;
        if (line.indexOf("javaw") != -1)
          continue;


        StringTokenizer st = new StringTokenizer(line);
        if (st.countTokens() < 9)
          continue;

        st.nextToken();
        String pidx = st.nextToken();
        st.nextToken();
        st.nextToken();
        st.nextToken();
        st.nextToken();
        st.nextToken();
        st.nextToken();
        String start = st.nextToken();

        String program = line.substring(line.indexOf(start));
        print("program: " + program);

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
          if (line.indexOf("java.exe") != -1)
          {
            String[] split = line.split(" +");
            Jstack js = new Jstack(split[1], split[0]);
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
      boolean nwmonitor = false;
      for (int j = 0; j < lines.length; j++)
      {
        if (lines[j].indexOf("Vdb.Vdbmain")  != -1) vdbmain   = true;
        if (lines[j].indexOf("Vdb.SlaveJvm") != -1) slavejvm  = true;
        if (lines[j].indexOf("NwMonitor")    != -1) nwmonitor = true;
      }

      if (nwmonitor)
      {
        print("Skipping NwMonitor");
        continue;
      }
      else if (vdbmain)
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
      block.add(line.substring(21));
      //print("add block: " + count + " " + line);
    }

    count++;
    index--;
    return block;
  }

  private static void parse(String fname)
  {
    lines       = Fget.readFileToArray(fname);
    String line = null;

    top:
    for (index = 0; index < lines.length; index++)
    {
      line = lines[index];

      if (line.indexOf("Jstack for") != -1)
      {
        System.out.println(line);
        continue;
      }
      if (line.indexOf("stdout:") == -1)
        continue;


      /* A blank line starts a block: */
      String[] split = line.trim().split(" +");
      //if (split.length != 2)
      //  continue;

      /* Get a block of info: */
      Vector block = getBlock();
      if (block.size() == 0)
        continue;
      line = (String) block.firstElement();

      /* Get rid of stuff we don't want: */
      if (line.indexOf("Attach Listener") != -1)
        continue;

      if (line.indexOf("IO_task") != -1)
      {
        String line3 = (String) block.elementAt(2);
        if (line3.indexOf("multi_io") != -1)
          continue;
      }

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
      if (line.indexOf("WG_task") != -1)
        continue;

      for (int i = 0; i < block.size(); i++)
        System.out.println("block: " + count + " " + block.elementAt(i));
      System.out.println();
    }
  }


  public static void main(String[] args)
  {
    if (args.length == 3 && args[1].equals("-"))
    {
      parse(args[2]);
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

    fp.close();
  }
}

