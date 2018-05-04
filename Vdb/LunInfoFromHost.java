package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.HashMap;
import java.util.Vector;

import Utils.Fget;
import Utils.OS_cmd;




/**
 * This class communicates between master and slaves to get information
 * like:
 * - does lun/file exist
 * - lun/file size
 * - Kstat info
 * - etc
 */
public class LunInfoFromHost implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  protected String  lun;
  protected String  original_lun;   /* The lun name not modified by unix2windows() */
  protected String  sd_instance;    /* Instance specified in sd=,...instance= */
  protected String  host_name;
  protected boolean lun_exists          = false;
  protected boolean parent_exists       = false;
  protected boolean open_for_write      = false;
  protected long    lun_size;
  protected long    end_lba;              /* From SD size= parameter */
  protected boolean read_allowed        = false;
  protected boolean write_allowed       = false;
  protected boolean error_opening       = false;
  protected String  kstat_instance;
  protected Vector  kstat_error_messages = new Vector(0, 0);
  protected String  soft_link           = null;

  /* For verification of proper SD Concatenation: */
  protected boolean marker_needed       = false;
  protected boolean marker_found        = false;
  protected long    marker_tod          = 0;
  protected long    marker_sd_num       = 0;


  protected void mismatch(SD_entry sd)
  {
    common.ptod("sd="   + sd.sd_name +
                ",host=" + host_name +
                ",lun="  + lun);
    common.ptod("Lun exists:     " + lun_exists);
    common.ptod("Lun size:       " + lun_size);
    common.ptod("Read  access:   " + read_allowed);
    common.ptod("Write access:   " + write_allowed);
    common.ptod("Open for write: " + open_for_write);
  }



  public void getRawInfo()
  {

    long handle = Native.openFile(lun);
    if (handle == -1)
    {
      lun_exists    = false;
      read_allowed  = false;
      write_allowed = false;
      error_opening = true;
    }

    else
    {
      /* Check for read access: */
      lun_exists    = true;
      parent_exists = true;
      read_allowed  = true;
      error_opening = false;

      if (common.onLinux())
        lun_size = Linux.getLinuxSize(lun);
      else
        lun_size = Native.getSize(handle, lun);

      Native.closeFile(handle);

      if (open_for_write)
      {
        /* Check for write access: */
        handle = Native.openFile(lun, 1);
        if (handle == -1)
          write_allowed = false;
        else
        {
          write_allowed = true;
          Native.closeFile(handle);
        }
      }
    }
  }



  public void getFileInfo()
  {
    /* Regular file system file is much easier: */
    File fptr   = new File(lun);
    File parent = fptr.getParentFile();
    if (parent == null || !parent.exists())
      parent_exists = lun_exists = false;

    else
    {
      parent_exists = true;
      lun_exists    = fptr.exists();
    }

    /* If this lun is a soft link, Java won't think the file exists. */
    /* Therefore, if 'ls -l' output starts with 'l', I'll accept */
    /* it as being there and see if anything happens later on: */
    boolean is_a_file = fptr.isFile();
    if (!is_a_file)
    {
      if (!common.onWindows())
      {
        OS_cmd ocmd     = OS_cmd.execute("ls -l " + lun);
        String[] stdout = ocmd.getStdout();
        if (stdout != null && stdout.length > 0)
        {
          for (int j = 0; j < stdout.length; j++)
          {
            if (stdout[j].startsWith("l"))
            {
              String[] split = stdout[j].split(" +");
              soft_link = split[split.length-1];
              is_a_file = true;
            }
          }
        }
      }
    }

    if (lun_exists && is_a_file)
    {
      read_allowed  = fptr.canRead();
      write_allowed = fptr.canWrite();
      lun_size      = fptr.length();
      long handle        = Native.openFile(lun);
      if (handle == -1)
        error_opening = true;
      else
        Native.closeFile(handle);
    }
  }

  public static void main(String[] args)
  {
    /* Needed in case any Native.xxx ends up doing a PTOD: */
    Native.allocSharedMemory();

    String fname = args[0];

    if (fname.startsWith("/dev/") || fname.startsWith("\\\\.\\"))
    //if (fname.startsWith("/dev/"))
    {
      LunInfoFromHost linfo = new LunInfoFromHost();
      linfo.lun             = fname;
      linfo.getRawInfo();
      common.ptod("linfo.lun_size: " + linfo.lun_size);
    }

    else
    {

      LunInfoFromHost linfo = new LunInfoFromHost();
      linfo.lun             = fname;
      linfo.getFileInfo();
      common.ptod("linfo.lun_size: " + linfo.lun_size);
    }
  }
}


