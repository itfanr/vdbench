package Utils;

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

import java.util.Vector;
import java.util.zip.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * This class reads data lines from an regular or GZIP file.
 */
public class Fput
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String      full_name        = null;
  private PrintWriter pw               = null;
  private int         pw_files_created = 0;

  private static Vector open_files = new Vector (8, 0);


  public Fput(String dirname, String fname)
  {
    this(dirname + File.separator + fname, false);
  }
  public Fput(String fname)
  {
    this(fname, false);
  }
  public Fput(String dirname, String fname, boolean append)
  {
    this(dirname + File.separator + fname, append);
  }
  public Fput(String fname, boolean append)
  {
    full_name = fname;
    open_files.add(this);

    if (fname.startsWith("null"))
      common.failure("null file name");

    try
    {
      if (!fname.endsWith("-"))
      {
        File fptr = new File(fname);
        boolean exist = fptr.exists();
        FileOutputStream ofile = new FileOutputStream(fptr, append);
        pw = new PrintWriter(new BufferedOutputStream(ofile));
        //common.plog("Created file: " + fptr.getAbsolutePath());
      }
      else
      {
        pw = new PrintWriter(System.out);
        //common.plog("Created file: " + fname);
      }

      chmod(fname);

      if (++pw_files_created % 50 == 0)
        common.ptod("Created " + pw_files_created + " report files; continue processing.");
    }

    catch (Exception e)
    {
      common.failure(e);
    }

    /* An HTML file gets a little extra: */
    if (fname.indexOf(".html") != -1)
    {
      pw.println("<pre>");
      //common.chk_error(pw);
    }
  }


  /**
   * Close output file
   */
  public void close()
  {
    if (!open_files.remove(this))
      common.failure("TRying to close an Fput() file that is not open");


    try
    {
      if (pw != null)
      {
        pw.close();
        pw = null;
      }
    }
    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public static void closeAll()
  {
    while (open_files.size() > 0)
      ((Fput) open_files.firstElement()).close();
  }

  public String getName()
  {
    return full_name;
  }

  /**
   * Write line to file.
   * html files are immediately flushed because it is possible that I will
   * be looking at it during the run!
   */
  public void println(String line)
  {
    if (pw == null)
    {
      common.ptod("");
      common.ptod("Fput.println(): file was closed already.");
      common.ptod("Fput.println(): " + line);
      common.failure("Fput.println(): file was closed already.");
    }
    pw.println(line);

    if (full_name.endsWith("html"))
      pw.flush();
    //common.chk_error(pw);
  }

  public void flush()
  {
    pw.flush();
  }

  /**
   * Chmod command.
   * Since windows has no protection whatsoever, we ignore the request
   */
  public static void chmod(String fname)
  {
    /* fiddling with stdin and stdout causes problems: */
    if (fname.endsWith("-"))
      return;

    if (!common.onWindows())
      OS_cmd.execute("chmod 777 " + fname, false);
  }


  public static void main (String args[]) throws IOException
  {
  }
}




