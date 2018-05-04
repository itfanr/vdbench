package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;


/**
 * Errno translation.
 *
 * Vdbench has a few private errno values, while the remainder of the errno
 * values can be found in errno.h, or using the strerror() function call.
 *
 * However, since strerror() does not return acronyms like 'ENOENT' I decided to
 * hard code them all, but two different lists, one for Solaris and one for
 * Linux.
 */
public class Errno
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static final String vdbench_table[] = {


    /* These values also reside in JNI code! */
    /* The '60003' message is hardcoded in DvPost() */
    "  797 BAD_READ_RETURN  Read was successful, but data buffer contents not changed     ",
    "  798 INCORRECT_SIZE   Vdbench determined that not enough bytes were read or written ",
    "  799 ERRNO_ZERO       ERRNO contained zero after i/o error       ",
    "60003 60003            A Data Validation error was discovered     "


  };


  /**
   * Translate errno to a text string.
   */
  static public String xlate_errno(long errno)
  {
    /* First see if this code it a Vdbench error: */
    for (String line : vdbench_table)
    {
      String[] split = line.trim().split(" +");
      if (Integer.parseInt(split[0]) == errno)
      {
        return String.format("%s: '%s'", split[1],
                             line.substring(line.indexOf(split[1]) + split[1].length()).trim());
      }
    }

    /* This is not one created by Vdbench. Call others: */
    if (common.onWindows())
    {
      String msg = Native.getWindowsErrorText((int) errno);
      if (msg != null)
        return String.format("Windows error %d: '%s'", errno, msg);
      else
        return "Unknown Windows error code: " + errno;
    }

    else if (common.onSolaris())
      return ErrnoSolaris.xlateErrno(errno);

    else if (common.onLinux())
      return ErrnoLinux.xlateErrno(errno);

    else if (common.onMac())
      return ErrnoLinux.xlateErrno(errno);

    else
    {
      common.ptod("Errno translation: No errno translation table available for this OS. " +
                  "Using Linux table");
      return ErrnoLinux.xlateErrno(errno);
    }
  }


  public static void main(String args[])
  {
    //for (int i = 0; i < 256; i++)
    //{
    //  // Unix only returns the text, not the acronym.
    //  // I therefore decided to NOT do this. (for now?)
    //  String ret = Native.getErrorText(i);
    //  if (ret != null)
    //    common.ptod("ret: %3d %s", i, ret);
    //}
    //

    String txt = xlate_errno(Integer.parseInt(args[0]));
    common.ptod("txt: " + txt);
  }
}


