package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import Utils.ClassPath;
import Utils.Fget;


/**
 * This class contains information about Replay that needs to be passed back and
 * forth from Master to Slaves.
 */
public class ReplayInfo implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private boolean replay     = false;
  private int     repeat     = 1;
  private String  replay_filename;
  private String  split_directory;
  private double  replay_adjust;
  private boolean duplication = false;
  private long    stagger     = 0;
  private boolean compress   = true;

  private long    low_start_filter  = 0;
  private long    high_start_filter = Long.MAX_VALUE;
  private long    lba_fold_size     = Long.MAX_VALUE;

  private HashMap <Long, ReplayDevice> device_map = new HashMap(64);

  private Vector  group_list = new Vector(16, 0);



  private static  ReplayInfo info = new ReplayInfo();



  public static ReplayInfo getInfo()
  {
    return info;
  }
  public static void setInfo(ReplayInfo i)
  {
    info = i;
  }

  public static void setReplay()
  {
    if (!info.replay)
    {
      info.replay = true;
      info.findFilter();
    }
  }
  public static boolean isReplay()
  {
    return info.replay;
  }
  public static int getRepeatCount()
  {
    return info.repeat;
  }
  public static HashMap getDeviceMap()
  {
    return info.device_map;
  }
  public static long getLowFilter()
  {
    return info. low_start_filter;
  }
  public static long getHighFilter()
  {
    return info.high_start_filter;
  }
  public static long getFoldSize()
  {
    return info.lba_fold_size;
  }
  public static Vector getGroupList()
  {
    return info.group_list;
  }

  /**
   * Return ALL devices, real devices and duplicates
   * Sort by device number
   *
   * This method may not be static because InfoFromHost.possibleReplayInfo()
   * will call this when using the copy of ReplayInfo RETURNED from the slaves,
   * and therefore NOT the current local copy residing in the Master.
   */
  public Vector <ReplayDevice> getDeviceList()
  {
    Long[] devnumbers = (Long[]) device_map.keySet().toArray(new Long[0]);
    java.util.Arrays.sort(devnumbers);
    Vector device_list = new Vector(devnumbers.length);
    for (int i = 0; i < devnumbers.length; i++)
      device_list.add(device_map.get(devnumbers[i]));
    return device_list;
  }

  public static Vector <ReplayDevice> getNodupDevs()
  {
    Long[] devnumbers = (Long[]) info.device_map.keySet().toArray(new Long[0]);
    java.util.Arrays.sort(devnumbers);
    Vector device_list = new Vector(devnumbers.length);
    for (int i = 0; i < devnumbers.length; i++)
    {
      ReplayDevice rdev = info.device_map.get(devnumbers[i]);
      if (rdev.getDuplicateNumber() == 0)
      {
        //common.ptod("getNodupDevs: " + rdev.getDevString() + " " + rdev.getDuplicateNumber());
        device_list.add(rdev);
      }
    }
    return device_list;
  }

  public static Vector <ReplayDevice> getDupDevs()
  {
    Long[] devnumbers = (Long[]) info.device_map.keySet().toArray(new Long[0]);
    java.util.Arrays.sort(devnumbers);
    Vector device_list = new Vector(devnumbers.length);
    for (int i = 0; i < devnumbers.length; i++)
    {
      ReplayDevice rdev = info.device_map.get(devnumbers[i]);
      if (rdev.getDuplicateNumber() != 0)
      {
        //common.ptod("getNodupDevs: " + rdev.getDevString() + " " + rdev.getDuplicateNumber());
        device_list.add(rdev);
      }
    }
    return device_list;
  }



  /**
   * Store the replay parameters.
   *
   * There can be 'n' parameters: the first one always is the replay file
   * name, but the others can be a mix.
   *
   * Replay=(flatfile.bin.gz,repeat=n,split=x,duplication=y,stagger=n)
   * - repeat=n:      how often to repeat this replay run
   * - split=x:       split directory
   * - duplication=y: Use Replay duplication
   * - stagger=n:     for duplicates, how many milliseconds to stagger start
   *   time
   * - compress=no   (do not gzip).
   *
   * This method could be static, but I am too lazy to change the code... :-)
   */
  public void parseParameters(String[] names)
  {
    /* The first parameter is always the replay file: */
    replay_filename = names[0];

    if (!new File(replay_filename).exists())
      common.failure("storeParameters(names): replay file name does not exist: " +
                     replay_filename);

    /* Set the default split directory: */
    split_directory = new File(replay_filename).getAbsoluteFile().getParent();

    /* If we have multiple parameters: */
    if (names.length > 1)
    {
      for (int i = 1; i < names.length; i++)
      {
        String parm = names[i].trim();
        String[] split = parm.split("=");
        if (split.length != 2)
          common.failure("Invalid Replay parameter. Must be xx=yy: " + parm);

        if ("repeat".startsWith(split[0]))
          repeat = Integer.parseInt(split[1]);

        else if ("split".startsWith(split[0]))
          split_directory = split[1];

        else if ("duplication".startsWith(split[0]))
          duplication = split[1].toLowerCase().startsWith("y");

        else if ("stagger".startsWith(split[0]))
          stagger = Long.parseLong(split[1]) * 1000;

        else if ("compress".startsWith(split[0]))
          compress = split[1].toLowerCase().startsWith("y");

        else
          common.failure("Unknown Replay parameter: " + parm);
      }
    }

    /* Make sure the split directory is there, create it if needed: */
    File dirptr = new File(split_directory);
    if (dirptr.exists() && !dirptr.isDirectory())
      common.failure("Replay file target directory %s exists but is not a directory",
                     split_directory);
    else if (!dirptr.exists() && !dirptr.mkdir())
      common.failure("Replay file target directory %s can not be created",
                     split_directory);

  }

  public static String getReplayFile()
  {
    return info.replay_filename;
  }

  public static String getSplitDirectory()
  {
    return info.split_directory;
  }

  public static void setAdjustValue(double d)
  {
    info.replay_adjust = d;
  }
  public static double getAdjustValue()
  {
    return info.replay_adjust;
  }

  public static boolean duplicationNeeded()
  {
    return info.duplication;
  }
  public static long getStagger()
  {
    return info.stagger;
  }
  public static boolean compress()
  {
    return info.compress;
  }


  /**
   * Read file 'replay_filter'. This will allow you to set a beginning and end
   * time stamp for inoput data selection.
   * It also allows for a folding of the maximum lba, allowing testing against
   * smaller luns.
   *
   * This information has been moved from 'replay_filter.txt' to using the
   * MiscParms 'misc=' parameter.
   */
  private void findFilter()
  {
    ArrayList <String[]> misc = MiscParms.getMiscellaneous();

    for (String[] array : misc)
    {
      for (String parm : array)
      {
        String[] split = parm.trim().split("=");

        if (parm.startsWith("low_start_filter") && split.length == 2)
          low_start_filter = Long.parseLong(split[1]) * 1000000;

        else if (parm.startsWith("high_start_filter") && split.length == 2)
          high_start_filter = Long.parseLong(split[1]) * 1000000;

        else if (parm.startsWith("lba_fold_size_mb") && split.length == 2)
          lba_fold_size = Long.parseLong(split[1]) * 1024 * 1024;

        /* Anything 'else' is possibly not meant for ME, so I can not display an error! */
        else
          continue;

        common.ptod("ReplayInfo.findFilter() 'misc=' input found: " + parm);

      }
    }
  }
}
