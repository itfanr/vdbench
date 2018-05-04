package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String[] parm_list = new String[0];
  private boolean  flags_translated = false;


  /* These are flags that are directly passed to the Unix open() function: */
  private        int openflags        = 0;
  public  static int WINDOWS_DIRECTIO = 0x000001;  /* Hardcoded in vdbwin2k.c */

  /* These are options to be used for other reasons: */
  private int       otherflags       = 0;
  public static int FSYNC_ON_CLOSE   = 0x000001;
  public static int SOL_DIRECTIO     = 0x000002;
  public static int SOL_DIRECTIO_OFF = 0x000004;
  public static int SOL_CLEAR_CACHE  = 0x000008;

  /*On the appliance
    zfs set sync=disabled <internal_filesystem_name>

    Use "zfs list" to find the internal filesystem name for your share.

    From kenneth.straver@oracle.com 07/22/14
    (Native ZFS):
    To set a zfs filesystem to as close to directio as possible, the following can be set :

    zfs set primarycache=none <file_system_name> <<--- turns off cache for this file system
    zfs set sync=always <file_system_name> <<--- performs a sync to disk on each transaction
  */


  public OpenFlags()
  {
  }
  public OpenFlags(String[] parms, double[] numerics)
  {
    ArrayList <String> newparms = new ArrayList(8);
    for (String parm : parms)
      newparms.add(parm);

    if (numerics != null)
    {
      for (double parm : numerics)
        newparms.add("" + (int) parm);
    }

    parm_list = newparms.toArray(new String[0]);
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
    return(otherflags & mask) != 0;
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
    String arch          = System.getProperty("os.arch");

    if (common.onSolaris())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        if (     tmp.equals("o_dsync"))      temp_open_flags  |= 0x000040;
        else if (tmp.equals("o_rsync"))      temp_open_flags  |= 0x008000;
        else if (tmp.equals("o_sync"))       temp_open_flags  |= 0x000010;
        else if (tmp.startsWith("0x"))       temp_open_flags  |= hexFlags(tmp);
        else if (tmp.equals("fsync"))        temp_other_flags |= FSYNC_ON_CLOSE;
        else if (tmp.equals("directio"))     temp_other_flags |= SOL_DIRECTIO;
        else if (tmp.equals("directio_off")) temp_other_flags |= SOL_DIRECTIO_OFF;
        else if (tmp.equals("clear_cache"))  temp_other_flags |= SOL_CLEAR_CACHE;
        else if (tmp.equals("clearcache"))   temp_other_flags |= SOL_CLEAR_CACHE;

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

        if (tmp.equals("directio")) temp_open_flags |= WINDOWS_DIRECTIO;

        else
          common.failure("Invalid 'openflags=' parameter for Windows: " + parm);
      }
    }

    else if (common.onMac())
    {
      if (parm_list.length > 1)
        common.failure("Currently only ONE openflag accepted for MAC. #flags: " + parm_list.length);

      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i].toLowerCase();

        if (parm.equals("f_nocache"))
          temp_open_flags  = 48;
        else if (parm.equals("directio"))
          temp_open_flags  = 48;
        else if (common.isNumeric(parm))
          temp_open_flags  = Integer.parseInt(parm);
        else
          common.failure("Invalid 'openflags=' parameter for MAC: " + parm);
      }
    }

    else if (common.onLinux())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        // o_driect/directio are 0x40000 on PPC

        /* This received 04/22/2016 from Forum: */
        if (arch.equals("aarch64"))
        {
          if (     tmp.equals("o_dsync"))   temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_rsync"))   temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_sync"))    temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_direct"))  temp_open_flags  |= 0x10000;
          else if (tmp.equals("directio"))  temp_open_flags  |= 0x10000;
          else if (tmp.startsWith("0x"))    temp_open_flags  |= hexFlags(tmp);
          else if (tmp.equals("fsync"))     temp_other_flags |= FSYNC_ON_CLOSE;

          else
            common.failure("Invalid 'openflags=' parameter for Linux: " + parm);
        }

        /* This received 07/14/2016 from Pravin Kudav */
        else if (arch.equals("ppc64"))
        {
          if (     tmp.equals("o_dsync"))   temp_open_flags  |= 0x101000;
          else if (tmp.equals("o_rsync"))   temp_open_flags  |= 0x101000;
          else if (tmp.equals("o_sync"))    temp_open_flags  |= 0x101000;
          else if (tmp.equals("o_direct"))  temp_open_flags  |= 0x20000;
          else if (tmp.equals("directio"))  temp_open_flags  |= 0x20000;
          else if (tmp.startsWith("0x"))    temp_open_flags  |= hexFlags(tmp);
          else if (tmp.equals("fsync"))     temp_other_flags |= FSYNC_ON_CLOSE;

          else
            common.failure("Invalid 'openflags=' parameter for Linux: " + parm);
        }

        else
        {
          if (     tmp.equals("o_dsync"))   temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_rsync"))   temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_sync"))    temp_open_flags  |= 0x01000;
          else if (tmp.equals("o_direct"))  temp_open_flags  |= 0x004000;
          else if (tmp.equals("directio"))  temp_open_flags  |= 0x004000;
          else if (tmp.startsWith("0x"))    temp_open_flags  |= hexFlags(tmp);
          else if (tmp.equals("fsync"))     temp_other_flags |= FSYNC_ON_CLOSE;

          else
            common.failure("Invalid 'openflags=' parameter for Linux: " + parm);
        }

      }
    }

    else if (common.onAix())
    {
      for (int i = 0; i < parm_list.length; i++)
      {
        String parm = parm_list[i];
        String tmp  = parm.toLowerCase();

        if (     tmp.equals("o_dsync"))    temp_open_flags  |= 0x00400000;
        else if (tmp.equals("o_rsync"))    temp_open_flags  |= 0x00200000;
        else if (tmp.equals("o_sync"))     temp_open_flags  |= 0x00000010;
        else if (tmp.equals("o_direct"))   temp_open_flags  |= 0x08000000;
        else if (tmp.equals("directio"))   temp_open_flags  |= 0x08000000;
        else if (tmp.startsWith("0x"))     temp_open_flags  |= hexFlags(tmp);

        else
          common.failure("Invalid 'openflags=' parameter for AIX: " + parm);
      }
    }

    else if (parm_list.length > 0)
      common.failure("'openflags=' parameter is only valid when the target "+
                     "system is Solaris, Linux, Windows, MAC or AIX. ");

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


