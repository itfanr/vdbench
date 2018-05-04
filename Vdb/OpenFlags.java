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

/**
 * Code related to 'openflags' parameters.
 * This also now includes 'close' flag.
 *
 */
public class OpenFlags implements java.io.Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String[] parm_list = new String[0];
  private boolean  flags_translated = false;


  /* These are flags that are directly passed to the Unix open() function: */
  private        int openflags        = 0;
  public  static int WINDOWS_DIRECTIO = 0x000001;  /* Hardcoded in vdbwin2k.c */

  private static int SOLARIS_SYNC     = 0x000010;
  private static int SOLARIS_DSYNC    = 0x000040;
  private static int SOLARIS_RSYNC    = 0x008000;

  private static int LINUX_DSYNC      = 0x010000; // These three are synonyms
  private static int LINUX_RSYNC      = 0x010000;
  private static int LINUX_SYNC       = 0x010000;
  private static int LINUX_DIRECT     = 0x004000; // does not work for partitions!!!
                                                  // 0x4000 is actually O_NONBLOCK
                                                  // O_DIRECT = 0x40000 !!
                                                  //

  /* These are options to be used for other reasons: */
  private int       otherflags       = 0;
  public static int FSYNC_ON_CLOSE   = 0x000001;
  public static int SOL_DIRECTIO     = 0x000002;
  public static int SOL_DIRECTIO_OFF = 0x000004;
  public static int SOL_CLEAR_CACHE  = 0x000008;



  public OpenFlags()
  {
  }
  public OpenFlags(String[] parms)
  {
    parm_list = parms;
  }

  public Object clone()
  {
    try
    {
      OpenFlags of = (OpenFlags) super.clone();
      of.parm_list = (String[])  parm_list.clone();
      return of;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public int getOpenFlags()
  {
    if (!flags_translated)
      translateFlags();
    return openflags;
  }

  public boolean isOther(int mask)
  {
    if (!flags_translated)
      translateFlags();
    return (otherflags & mask) != 0;
  }

  /**
   * Translate an array (usually just one) of open flags to an int.
   *
   * For performance reasons this should be changed to do it only ONCE, and not
   * for each OPEN request.
   *
   * We want to run this only ONCE to eliminate reporting-scanning for each
   * OpenFile().
   */
  private synchronized void translateFlags()
  {
    int temp_open_flags  = 0;
    int temp_other_flags = 0;

    if (common.onSolaris())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        if (tmp.equals("o_dsync"))
          temp_open_flags |= SOLARIS_DSYNC;
        else if (tmp.equals("o_rsync"))
          temp_open_flags |= SOLARIS_RSYNC;
        else if (tmp.equals("o_sync"))
          temp_open_flags |= SOLARIS_SYNC;
        else if (tmp.startsWith("0x"))
          temp_open_flags |= hexFlags(tmp);

        else if (tmp.equals("fsync"))
          temp_other_flags |= FSYNC_ON_CLOSE;
        else if (tmp.equals("directio"))
          temp_other_flags |= SOL_DIRECTIO;
        else if (tmp.equals("directio_off"))
          temp_other_flags |= SOL_DIRECTIO_OFF;
        else if (tmp.equals("clear_cache"))
          temp_other_flags |= SOL_CLEAR_CACHE;

        else
          common.failure("Invalid 'openflags=' parameter for Solaris: " + parm);
      }
    }

    else if (common.onWindows())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        if (tmp.equals("directio"))
          temp_open_flags |= WINDOWS_DIRECTIO;

        else
          common.failure("Invalid 'openflags=' parameter for Windows: " + parm);
      }
    }

    else if (common.onLinux())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        if (tmp.equals("o_dsync"))
          temp_open_flags |= LINUX_DSYNC;
        else if (tmp.equals("o_rsync"))
          temp_open_flags |= LINUX_RSYNC;
        else if (tmp.equals("o_sync"))
          temp_open_flags |= LINUX_SYNC;
        else if (tmp.equals("o_direct"))
          temp_open_flags |= LINUX_DIRECT;
        else if (tmp.startsWith("0x"))
          temp_open_flags |= hexFlags(tmp);

        else if (tmp.equals("fsync"))
          temp_other_flags |= FSYNC_ON_CLOSE;

        else
          common.failure("Invalid 'openflags=' parameter for Linux: " + parm);
      }
    }

    else if (parm_list.length > 0)
      common.failure("'openflags=' parameter is only valid when the target "+
                     "system is Solaris, Linux or Windows");

    if ((temp_other_flags & SOL_DIRECTIO)     != 0 &&
        (temp_other_flags & SOL_DIRECTIO_OFF) != 0)
      common.failure("'openflags=directio' and 'openflags=directio_off' are mutually exclusive");

    otherflags       = temp_other_flags;
    openflags        = temp_open_flags;
    flags_translated = true;

    if (common.get_debug(common.PRINT_OPEN_FLAGS))
      common.ptod(this);
  }

  private static int hexFlags(String parm)
  {
    try
    {
      return Integer.parseInt(parm.substring(2), 16);
    }
    catch (Exception e)
    {
      common.ptod("Exception parsing openflags=" + parm);
      common.failure(e);
    }
    return 0;
  }

  public boolean equals(Object obj)
  {
    OpenFlags oflags = (OpenFlags) obj;
    if (!flags_translated)
      translateFlags();
    if (!oflags.flags_translated)
      oflags.translateFlags();

    return openflags == oflags.openflags && otherflags == oflags.otherflags;
  }

  public String toString()
  {
    if (!flags_translated)
      translateFlags();
    return String.format("OpenFlags: 0x%08x OtherFlags: 0x%08x",
                         openflags, otherflags);
  }
}


