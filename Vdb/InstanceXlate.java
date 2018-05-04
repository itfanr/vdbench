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

public class InstanceXlate
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static String   IO_STAT         = "/usr/bin/iostat -xpX";
  private static String[] left_lines      = null;
  private static String[] right_lines     = null;

  private static Date     last_ls_command = new Date(0);
  private static String[] ls_lines        = null;


  public static void main(String[] args)
  {
    Vector lines = simulateLibdev();

    for (int i = 0; i < lines.size(); i++)
    {
      String line = (String) lines.elementAt(i);

      if (args.length == 0)
        System.out.println(line);

      else if (line.indexOf(args[0]) != -1)
        System.out.println(line);
    }

    printIostat();
  }



  public static void main2(String[] args)
  {
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s0"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s1"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s2"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s3"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s4"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s5"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s6"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s7"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s8"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0s9"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0sa"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0sb"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0sc"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0sd"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0se"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0sf"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pg"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0ph"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pi"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pj"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pk"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pl"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pm"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pn"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0po"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pp"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pq"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0pr"));
    common.ptod("slice: " + translateSliceToNumber("c0t0d0ps"));
  }


  /**
   * Get a Vector with configuration data.
   * Each line in the vector is:
   * - lun /dev/rdsk/cxxx
   * - instance sd1,a
   * - major device number
   * - minor device number.
   *
   * This is obtained from merging two iostat -xd outputs, one with and one
   * without the 'n' parameter, together with output from the 'ls /dev/rdsk'
   * command.
   *
   * The device numbers are no longer needed, but the device NAMEs are.
   */
  public static Vector simulateLibdev()
  {
    long start = System.currentTimeMillis();
    int attempts = 0;
    Vector output_lines = new Vector(256, 0);;


    // /home/henkv/swat/swat -c -n -v300   /home/henkv/swat/monitor_data/local

    /* Not every system supports the 'X' parameter. Try it: */
    OS_cmd tmp = OS_cmd.execute(IO_STAT);
    if (!tmp.getRC())
      IO_STAT = "/usr/bin/iostat -xp";

    //
    // don't do this more than 'x' times per hour!!!!
    // it can be too expensive!
    //
    //

    // Changed from -rlL on 4/15/09. Don't need the 'L', and removing
    // eliminates 'ls' from hanging on bad devices.
    // Didn't need major/minor, but just left them zero.
    String LS_DEV  = "/usr/bin/ls -rl /dev/rdsk";

    /* In theory it is possible for there to be a slight difference  */
    /* in the length of the output of either command, cause by a new */
    /* device being added in the middle of things.                   */
    /* That should be resolved in fice tries:                        */
    do
    {
      /* Make sure we start with an empty array: */
      left_lines = new String[0];
      right_lines = new String[0];

      if (attempts++ > 5)
        common.failure("failed to get a stable configuration listing");

      /* Get all the LEFT data for instance names: */
      OS_cmd left = OS_cmd.execute(IO_STAT);
      if (!left.getRC())
        continue;

      left_lines = left.getStdout();

      /* Get all the RIGHT data for device names names: */
      OS_cmd right = OS_cmd.execute(IO_STAT + "n");
      if (!right.getRC())
        continue;

      right_lines = right.getStdout();

    } while (left_lines.length != right_lines.length);


    /* Match left and right (ignore column headers): */
    /* But don't do it more often than once every 'n' minutes, because */
    /* the ls command in a large shop can get expensive:               */
    if (new Date().getTime() > last_ls_command.getTime() + 900000)
    {
      OS_cmd lsdev = OS_cmd.execute(LS_DEV);

      /* There was one instance where ls returned non-zero because of garbage. */
      /* Vdbench aborted. devfsadm -C fixed it. Removed rc check:              */
      if (!lsdev.getRC())
      {
        common.ptod("Non-zero returncode from " + LS_DEV + "; continuing");
        ls_lines = lsdev.getStderr();
        for (int i = 0; i < ls_lines.length; i++)
          common.ptod("stderr: " + ls_lines[i]);
      }

      //  common.failure("Unable to execute command: " + LS_DEV);
      ls_lines = lsdev.getStdout();
      last_ls_command = new Date();
    }

    if (common.get_debug(common.FAKE_LIBDEV))
    {
      left_lines  = Fget.readFileToArray("iostat_left");
      right_lines = Fget.readFileToArray("iostat_right");
      ls_lines    = Fget.readFileToArray("iostat_ls");
    }


    /* Match left and right (ignore column headers): */
    for (int i = 2; i < right_lines.length; i++)
    {
      String linel    = left_lines[i];
      String liner    = right_lines[i];
      String instance = linel.substring(0, linel.indexOf(" "));
      String name     = liner.substring(liner.lastIndexOf(" ") + 1);

      /* These are the only ones that I recognize right now: */
      if (!(instance.startsWith("sd")  ||
            instance.startsWith("ssd") ||
            instance.startsWith("ramdisk") ||
            instance.startsWith("dad") ||
            instance.startsWith("st")  ||
            instance.startsWith("vdc") ||
            instance.startsWith("xvf") ||
            instance.startsWith("cmdk") ||   // For XVM?
            instance.startsWith("nfs")))
        continue;

      if (name.indexOf(".fp") != -1)
        name = name.substring(0, name.indexOf(".fp"));

      /* Look in ls /dev output for a line ENDING with the disk name (includes partition) */
      boolean found = false;
      String ls = null;
      for (int j = 0; j < ls_lines.length; j++)
      {
        ls = ls_lines[j];
        String[] split = ls.split(" +");
        if (split.length < 9)
          continue;
        if (split[8].endsWith(name))
        {
          found = true;
          long devnum = 0; // xlateLS(ls);
          output_lines.addElement(Format.f("/dev/rdsk/%-50s", name) +
                                  Format.f(" %-12s ", instance) + " " +
                                  (devnum >> 32) + " " + (int) devnum +
                                  " ends");
          break;
        }
      }


      /* Else look for a line CONTAINING the disk name (excludes partition) */
      if (!found)
      {
        for (int j = 0; j < ls_lines.length; j++)
        {
          ls = ls_lines[j];
          if (ls.indexOf(name) != -1)
          {
            //common.ptod("ls: " + ls.substring(ls.lastIndexOf(" ")));
            String[] split = ls.split(" +");
            //int slice = translateSliceToNumber(ls.substring(ls.lastIndexOf(" ")));
            int slice = translateSliceToNumber(split[8]);
            if (slice == -1)
              continue;


            found = true;
            long devnum = 0; //xlateLS(ls);
            output_lines.addElement(Format.f("/dev/rdsk/%-50s", name) +
                                    Format.f(" %-12s ", instance) + " " +
                                    (devnum >> 32) + " " + (int) (devnum - slice) +
                                    " indx");
            break;
          }
        }
      }

      if (!found)
        output_lines.addElement(Format.f("%-50s", name) +
                                Format.f(" %-12s ", instance) + " -1 -1 nfs?");
    }


    if (common.get_debug(common.FAKE_LIBDEV))
    {
      for (int i = 0; i < output_lines.size(); i++)
      {
        common.ptod("output:" + output_lines.elementAt(i));
      }
    }

    return output_lines;
  }


  /**
   * Translate 'cxtxdxsx' or 'cxtxdxpx' to a relative partition number.
   */
  private static String noslice = "";
  private static String prefix = "";
  public static int translateSliceToNumber(String name)
  {
    /* If the name still contains /dev/rdsk, remove it all: */
    if (name.indexOf("/") != -1)
    {
      prefix = name.substring(0, name.lastIndexOf("/") + 1);
      name   = name.substring(name.lastIndexOf("/") + 1);
      //common.ptod("prefix: " + prefix);
      //common.ptod("name: " + name);
    }

    /* Check for 's' or 'p': */
    char identifier = name.charAt(name.length() - 2);
    if (identifier != 's' && identifier != 'p')
      return -1;

    /* First start with 's': */
    if (identifier == 's')
    {
      noslice = name.substring(0, name.length() - 3);
      //common.ptod("noslice: " + noslice);
      if (name.indexOf("s") != name.length() - 2)
        return -1;

      noslice = name.substring(0, name.length() - 2);
      //common.ptod("noslice: " + noslice);
      char slice = name.charAt(name.length() - 1);
      if (slice >= '0' && slice <= '9')
        return slice - 48;
      if (slice >= 'a' && slice <= 'f')
        return slice - 97 + 10;
    }

    /* Now do with 'p': */
    if (identifier == 'p')
    {
      if (name.indexOf("p") != name.length() - 2)
        return -1;

      noslice = name.substring(0, name.length() - 2);
      //common.ptod("noslice: " + noslice);
      char slice = name.charAt(name.length() - 1);
      if (slice >= 'g' && slice <= 'z')
        return slice - 97 + 10;
    }


    return -1;
  }
  public static String getLastName()
  {
    return prefix + noslice;
  }


  /**
   * Translate a line of the 'ls -l' output into a string that consists
   * of only the major and minor device number.
   */
  private static long xlateLS(String line)
  {
    String values[] = new String[12];
    int vals;

    line = line.trim().toLowerCase();
    StringTokenizer st = new StringTokenizer(line, " ", false);

    /* Copy the first n-strings to an array: */
    for (vals = 0; vals < values.length; vals++)
    {
      if (!st.hasMoreTokens())
        break;
      values[ vals ] = st.nextToken();
    }

    /* Is this a directory name? */
    if (vals < 1)
      common.failure("Invalid LS line contents: " + line);

    if (line.startsWith("/"))
      common.failure("Invalid LS line contents: " + line);

    /* Look for major/minor name: */
    if (vals < 8)
      common.failure("Invalid LS line contents: " + line);


    /* If no comma, then no major name: */
    String work = values[4];
    if (work.indexOf(",") == -1)
      common.failure("Invalid LS line contents: " + line);

    //common.ptod("test2");

    /*
    crw-------   1 root     root     200,100 Dec  6 15:36 /dev/vx/rdsk local
    crw-------   1 root     root     200,103 Dec  6 15:34 /dev/vx/rdsk var
    crw-------   1 root     root     200,101 Dec  6 15:33 /dev/vx/rdsk globaldev0
    crw-------   1 root     root     200,  0 Nov  4 11:32 /dev/vx/rdsk rootvol
    crw-------   1 root     root     200,104 Jun 12  2002 /dev/vx/rdsk oracle
    crw-------   1 root     root     200,102 Jun  5  2002 /dev/vx/rdsk swapvol
    crw-------   1 oracle   dba      200,29052 Dec 10 14:15 /dev/vx/rdsk/elogexdg partition1G_32
    crw-------   1 oracle   dba      200,29054 Dec 10 14:15 /dev/vx/rdsk/elogexdg partition1G_34
    */
    /* If comma last byte, then major/minor not concatenated: */
    if (work.charAt(work.length() -1) == ',')
    {
      if (vals < 9)
        common.failure("Invalid LS line contents: " + line);

      Integer num = Integer.valueOf(work.substring(0, work.length() -1));
      long major  = num.intValue();
      num         = Integer.valueOf(values[5]);
      long minor  = num.intValue();
      return(major << 32) + minor;
    }
    else
    {
      Integer num = Integer.valueOf(work.substring(0, work.indexOf(",") ));
      long major   = num.intValue();
      num         = Integer.valueOf(work.substring(work.indexOf(",") + 1 ));
      long minor   = num.intValue();
      return(major << 32) + minor;
    }
  }


  /**
   * For errors: print iostat left and right to logfile.html
   */
  public static void printIostat()
  {
    common.ptod("");
    common.ptod("");
    common.ptod("The translation of a proper device name (/dev/rdsk/cxtxdxsx) to a Kstat        ");
    common.ptod("instance name is done by combining the outputs of '/usr/bin/ls -rlL /dev/rdsk',");
    common.ptod("'/usr/bin/iostat -xpX', and '/usr/bin/iostat -xpXn', extracting device         ");
    common.ptod("major and minor names and kstat instance names.                                ");
    common.ptod("It appears that one or more device names could not be translated this way.     ");
    common.ptod("If this device name indeed does not show in the iostat output then proper      ");
    common.ptod("kstat statistics for this device are not available. This could be considered  ");
    common.ptod("a Solaris bug.                                                                 ");
    common.ptod("");
    common.ptod("See file 'logfile.html' for a copy of the iostat output");
    common.ptod("");

    /* First determine maximum length of 'left': */
    int max = 0;
    for (int i = 0; i < left_lines.length; i++)
      max = Math.max(max, left_lines[i].length());

    String mask = "%-" + max + "s";

    common.plog("");
    common.plog(Format.f(mask, "Output of '" + IO_STAT + "'") +
                "  |  " +
                Format.f(mask, "Output of '" + IO_STAT + "n'"));
    common.plog(Format.f(mask, "") + "  |  " + Format.f(mask, ""));

    for (int i = 0; i < left_lines.length; i++)
    {
      common.plog(Format.f(mask, left_lines[i]) +
                  "  |  " +
                  Format.f(mask, right_lines[i]).trim());
    }
  }



  public static void printAll()
  {

    /* First determine maximum length of 'left': */
    int max = 0;
    for (int i = 0; i < left_lines.length; i++)
      max = Math.max(max, left_lines[i].length());

    String mask = "%-" + max + "s";

    common.ptod("");
    common.ptod(Format.f(mask, "Output of '" + IO_STAT + "'") +
                "  |  " +
                Format.f(mask, "Output of '" + IO_STAT + "n'"));
    common.ptod(Format.f(mask, "") + "  |  " + Format.f(mask, ""));

    for (int i = 0; i < left_lines.length; i++)
    {
      common.ptod(Format.f(mask, left_lines[i]) +
                  "  |  " +
                  Format.f(mask, right_lines[i]).trim());
    }

  }
}


