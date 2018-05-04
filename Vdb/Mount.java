package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.File;
import java.io.Serializable;
import java.util.*;

import Utils.*;


/**
 * Code to handle mount requests, including the creation of mount points.
 */
public class Mount implements Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String[] requests;



  public Mount(String[] lines)
  {
    requests = lines;

    for (int i = 0; i < requests.length; i++)
    {
      String line = requests[i];
      String[] split = line.trim().split(" +");
      if (split.length < 2)
        common.failure("'hd=xxx,mount=\"special [options[ mountpoint\" must contain " +
                       "a minimum of two blank separated fields: " + line);
    }
  }


  /**
   * Mount if needed.
   * The request[] list above contains 'complete' mount commands, where
   * there are mutliple blank separated fields:
   * - last field:           mountpoint.
   * - second to last field: special.
   *
   * The requested command is passed on 'asis', which means that the 'mount'
   *   command can be a script or something to handle special cases?
   */
  public void mountIfNeeded()
  {
    mountIfNeeded(null);
  }
  public void mountIfNeeded(String new_options)
  {
    if (common.onWindows())
    {
      common.failure("Running on Windows. No 'mount' command available.");
      return;
    }

    /* Split the command to pick up special, mountpoint and options: */
    for (int i = 0; i < requests.length; i++)
    {
      String[] split = requests[i].trim().split(" +");

      /* The last is always mount point: */
      String mountpoint = split[split.length-1];

      /* The second to last is always special: */
      /* The rest is options which we ignore: */
      String special = split[split.length-2];

      /* The first is the 'mount' command, or equivalent: */
      String mount = split[0];

      /* The rest inbetween (except 'mount') is options: */
      String options = "";
      for (int j = 1; j < split.length - 2; j++)
        options += split[j] + " ";

      /* Create mountpoint if needed: */
      createMountpoint(mountpoint);

      /* Unmount if needed: */
      if (isMountpointActive(special, mountpoint))
        doUnMount(mountpoint);

      /* Now mount using whatever options have been given: */
      if (new_options == null || new_options.equals("reset"))
        doMount(mount + " " + options + " " + special + " " + mountpoint);
      else
        doMount(mount + " " + new_options + " " + special + " " + mountpoint);
    }
  }



  private void createMountpoint(String mountpoint)
  {
    File fptr = new File(mountpoint);
    if (fptr.exists() && fptr.isFile())
      common.failure("Mountpoint exists, but is a file, not a directory: " + mountpoint);

    if (fptr.exists())
      return;

    if (!fptr.mkdirs())
      common.failure("Unable to create mountpoint: " + mountpoint);

    SlaveJvm.sendMessageToConsole("Created mountpoint: " + mountpoint);
  }


  private boolean isMountpointActive(String special, String mountpoint)
  {
    String[] lines = Fget.readFileToArray("/etc/mnttab");

    for (int i = 0; i < lines.length; i++)
    {
      String line = lines[i];

      /* Line contains tab characters? */
      line = common.replace(line, "\t", " ");

      String[] split = line.split(" +");
      if (split.length < 4)
      {
        for (int j = 0; j < split.length; j++)
          common.ptod("split: " + j + " " + split[j]);

        common.failure("Expecting at least 4 substrings in /etc/mnttab file: " + line);
      }

      if (split[0].equalsIgnoreCase(special) && split[1].equals(mountpoint))
        return true;
    }

    return false;
  }

  /**
   * Always unmount 'forced'.
   * It should not be needed, but I ran into some cases where the unmount failed
   * because somehow the system thought it was still busy.
   */
  private void doUnMount(String mountpoint)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("umount -f " + mountpoint);

    ocmd.execute();

    boolean rc = ocmd.getRC();
    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();

    if (stdout.length + stderr.length > 0)
    {
      common.plog("Unmount command output for " + mountpoint + ":");
      for (int i = 0; i < stdout.length; i++)
        common.plog("stdout: " + stdout[i]);
      for (int i = 0; i < stderr.length; i++)
        common.plog("stderr: " + stderr[i]);
    }

    if (!rc)
      common.failure("Unmount of file system failed. See above messages. ");
  }

  private void doMount(String line)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(line);
    ocmd.execute();

    boolean rc = ocmd.getRC();
    String[] stdout = ocmd.getStdout();
    String[] stderr = ocmd.getStderr();

    if (stdout.length + stderr.length > 0)
    {
      common.plog("Mount command output for " + line + ":");
      for (int i = 0; i < stdout.length; i++)
        common.plog("stdout: " + stdout[i]);
      for (int i = 0; i < stderr.length; i++)
        common.plog("stderr: " + stderr[i]);
    }

    if (!rc)
      common.failure("Mount of file system failed. See above (" +
                     (stdout.length + stderr.length) + ") messages. ");

    /* Just run the 'mount' command for reporting: */
    String[] split = line.trim().split(" +");
    String mountpoint = split[split.length-1];
    ocmd = new OS_cmd();
    ocmd.addText("mount | grep " + mountpoint);
    ocmd.execute();

    rc = ocmd.getRC();
    stdout = ocmd.getStdout();
    stderr = ocmd.getStderr();

    if (stdout.length + stderr.length > 0)
    {
      common.plog("Mount command output for " + line + ":");
      for (int i = 0; i < stdout.length; i++)
        common.plog("stdout: " + stdout[i]);
      for (int i = 0; i < stderr.length; i++)
        common.plog("stderr: " + stderr[i]);
    }

  }
}
