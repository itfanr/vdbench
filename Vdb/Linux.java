package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import Utils.Fget;
import Utils.OS_cmd;
import Utils.ClassPath;
import Utils.Format;


/**
 * Code related to Linux cpu statistics.
 */
public class Linux
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static long    ticks_per_second;
  private static boolean first_time = true;


  public static Kstat_cpu getCpuStats()
  {
    boolean rc;
    if (first_time)
    {
      first_time = false;
      if (common.onZLinux())
        rc = getTickCount390();
      else
        rc = getTickCount();

      if (!rc)
        return null;
    }

    Kstat_cpu kc = new Kstat_cpu();

    try
    {

      String[] lines = Fget.readFileToArray("/proc/stat");
      if (lines.length < 1)
      {
        disable("No data found in /proc/stat. Linux data collection disabled");
        return null;
      }

      /* Count the amount of cpus: */
      int cpus = -1;
      for (int i = 0; i < lines.length; i++)
      {
        if (lines[i].startsWith("cpu"))
          cpus++;
      }

      String[] split = lines[0].split(" +");
      kc.cpu_user    = Long.parseLong(split[1]) * 100 * ticks_per_second / cpus;
      kc.cpu_kernel  = Long.parseLong(split[3]) * 100 * ticks_per_second / cpus;
      kc.cpu_kernel += Long.parseLong(split[6]) * 100 * ticks_per_second / cpus;
      kc.cpu_kernel += Long.parseLong(split[7]) * 100 * ticks_per_second / cpus;

      kc.cpu_total   = kc.cpu_user + kc.cpu_kernel;
      kc.cpu_total  += Long.parseLong(split[2]) * 100 * ticks_per_second / cpus; // nice
      kc.cpu_total  += Long.parseLong(split[4]) * 100 * ticks_per_second / cpus; // idle
      kc.cpu_total  += Long.parseLong(split[5]) * 100 * ticks_per_second / cpus; // iowait
      kc.cpu_total  += Long.parseLong(split[7]) * 100 * ticks_per_second / cpus; // steal

      //common.ptod("kc.cpu_total: " + kc.cpu_total);
    }

    catch (Exception e)
    {
      disable("Unexpected exception parsing Linux Cpu statistics. Collection disabled");
      common.ptod(e);
    }

    return kc;
  }


  /**
   * Get the CPU tick count.
   */
  private static boolean getTickCount()
  {
    String arch = System.getProperty("os.arch");
    if (arch.equals("ppc64") || arch.equals("ppc64"))
    {
      ticks_per_second = 100;
      common.ptod("getTickCount(): set to 100 for PPC");
      return true;
    }

    ticks_per_second = Native.getTickCount();
    if (ticks_per_second <= 0)
    {
      common.ptod("Error retrieving tick count: %d. Setting to 100", ticks_per_second);
      ticks_per_second = 100;
      return true;
    }

    common.plog("ticks_per_second: " + ticks_per_second);

    return true;
  }

  private static boolean getTickCount390()
  {
    String arch = System.getProperty("os.arch");

    OS_cmd ocmd = new OS_cmd();
    if (arch.equals("s390"))
      ocmd.addText(ClassPath.classPath("linux/linux_clock_390_32"));
    else if (arch.equals("s390x"))
      ocmd.addText(ClassPath.classPath("linux/linux_clock_390_64"));
    else
      common.failure("Invalid S390 architecture: " + arch);

    ocmd.setStdout();
    ocmd.setStderr();
    ocmd.execute();

    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();
    if (stdout.length != 1)
    {
      for (int i = 0; i < stdout.length; i++)
        common.ptod("linux_clock stdout: " + stdout[0]);
      for (int i = 0; i < stderr.length; i++)
        common.ptod("linux_clock stderr: " + stderr[0]);
      disable("Unexpected return values from 'linux_clock'; Linux processing disabled");
      return false;
    }

    String[] split = stdout[0].split(" +");
    if (split.length != 2)
    {
      disable("Unexpected return values from 'linux_clock'; Linux processing disabled");
      return false;
    }

    ticks_per_second = Long.parseLong(split[1]);
    common.plog("ticks_per_second: " + ticks_per_second);

    return true;
  }

  /**
   * There was an error_stream. Print info and disable Linux.
   */
  private static void disable(String txt)
  {
    common.ptod("");
    common.ptod(txt);
    common.ptod("");


    String[] lines = Fget.readFileToArray("/proc/stat");
    common.plog("/proc/stat:");
    for (int i = 0; i < lines.length; i++)
      common.plog(lines[i]);
  }


  public static long getLinuxSize(String in_name)
  {
    String rawname = in_name;
    String line = null;
    try
    {
      rawname = Kstat_data.translateSoftLink(rawname);
      String[] lines = Fget.readFileToArray("/proc/partitions");
      for (int i = 0; i < lines.length; i++)
      {
        line = lines[i].trim();
        if (line.length() == 0)
          continue;
        if (line.startsWith("major"))
          continue ;

        String[] split = line.split(" +");
        if (split.length != 4)
          continue;

        if (("/dev/" + split[3]).equalsIgnoreCase(rawname))
        {
          /* It took me a while to figure out that blocksize on Linux is not */
          /* always 512. There are some exceptions, but it appears that the  */
          /* default is 1024 bytes.                                          */
          long blocks = Long.parseLong(split[2]);
          return blocks * 1024;
        }
      }
    }

    catch (Exception e)
    {
      common.ptod("Unexpected Exception while looking for Linux raw lun size.");
      common.ptod("line: " + line);
      common.ptod("Returning raw lun size of zero.");
      common.ptod(e);
    }

    common.ptod("Unable to find lun size for %s (%s): ", in_name, rawname);
    return 0;
  }


  public static void main(String[] args)
  {
    long size = getLinuxSize(args[0]);
    common.ptod("size: %,12d bytes", size);
    common.ptod("size: %.3fg %.3fg", (size / 1000000000.), (size / (1024*1024*1024.)));
  }
}

