package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import Utils.*;

public class InstanceXlate
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private static String   IO_STAT         = "/usr/bin/iostat -xp";
  private static String[] left_lines      = null;
  private static String[] right_lines     = null;


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
    Vector output_lines = new Vector(256, 0);


    /* In theory it is possible for there to be a slight difference   */
    /* in the length of the output of either command, caused by a new */
    /* device being added in the middle of things.                    */
    /* That should be resolved in five tries:                         */
    do
    {
      /* Make sure we start with an empty array: */
      left_lines  = new String[0];
      right_lines = new String[0];

      if (attempts++ > 5)
        common.failure("failed to get a stable configuration listing");

      /* Get all the LEFT data for instance names: */
      if ((left_lines = getIostatData("")) == null)
        continue;

      /* Get all the RIGHT data for device names names: */
      if ((right_lines = getIostatData("n")) == null)
        continue;

    } while (left_lines.length != right_lines.length);



    if (common.get_debug(common.FAKE_LIBDEV))
    {
      left_lines  = Fget.readFileToArray("iostat_left");
      right_lines = Fget.readFileToArray("iostat_right");
    }


    /* Match left and right (ignore column headers): */
    for (int i = 2; i < right_lines.length; i++)
    {
      String linel    = left_lines[i];
      String liner    = right_lines[i];
      String instance = linel.substring(0, linel.indexOf(" "));
      String name     = liner.substring(liner.lastIndexOf(" ") + 1);

      /* Solaris $%#@#$%%^&&^%$#-up again: */
      if (name.trim().length() == 0)
        continue;

      /* These are the only ones that I recognize right now: */
      if (!(instance.startsWith("sd")  ||
            instance.startsWith("ssd") ||
            instance.startsWith("blkdev")  ||  // NVME? David Carlson 03/11/14
            instance.startsWith("flmdisk") ||
            instance.startsWith("ramdisk") ||
            instance.startsWith("dad") ||
            instance.startsWith("st")  ||
            instance.startsWith("vdc") ||
            instance.startsWith("xdf") ||
            instance.startsWith("xvf") ||
            instance.startsWith("zvblk") ||
            instance.startsWith("cmdk") ||   // For XVM?
            instance.startsWith("nfs")))
      {
        //common.ptod("simulateLibdev unknown: " + instance);
        continue;
      }

      if (name.indexOf(".fp") != -1)  // mpxio code obsolete
        name = name.substring(0, name.indexOf(".fp"));

      if (instance.startsWith("nfs"))
        output_lines.addElement(name + " " + instance);
      else
        output_lines.addElement("/dev/rdsk/" + name + " " + instance);

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

  private static String[] getIostatData(String right)
  {
    /* If we don't reuse, just issue command: */
    if (!common.get_debug(common.REUSE_IOSTAT))
    {
      OS_cmd ocmd = OS_cmd.execute(IO_STAT + right);
      if (!ocmd.getRC())
        return null;
      return ocmd.getStdout();
    }

    /* We reuse. If we have a file, return: */
    String file = ClassPath.classPath("reuse_iostat" + right + ".txt");
    if (Fget.file_exists(file))
      return Fget.readFileToArray(file);

    /* We don't have a file. Execute command, store and return: */
    OS_cmd ocmd = OS_cmd.execute(IO_STAT + right);
    if (!ocmd.getRC())
      return null;

    Fput fp = new Fput(file);
    String[] lines = ocmd.getStdout();
    for (int i = 0; i < lines.length; i++)
      fp.println(lines[i]);
    fp.close();

    return lines;
  }
}


