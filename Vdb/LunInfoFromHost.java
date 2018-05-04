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

import java.io.*;
import java.util.HashMap;
import java.util.Vector;

import Utils.Semaphore;
import Utils.Fget;




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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  protected String  lun;
  protected String  original_lun;   /* The lun name not modified by unix2windows() */
  protected String  sd_instance;    /* Instance specified in sd=,...instance= */
  protected String  host_name;
  protected boolean lun_exists          = false;
  protected boolean parent_exists       = false;
  protected boolean open_for_write      = false;
  protected long    lun_size;
  protected boolean read_allowed        = false;
  protected boolean write_allowed       = false;
  protected boolean error_opening       = false;
  protected String  kstat_instance;
  protected Vector  kstat_error_messages = new Vector(0, 0);


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
}


