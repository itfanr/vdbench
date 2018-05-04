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
import Utils.Format;

/**
 * This class reads data lines from an regular or GZIP file.
 */
public class Fget
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private BufferedReader br          = null;
  private File           fptr        = null;
  private long           filesize    = 0;
  private long           bytesread   = 0;
  private long           early_eof   = Long.MAX_VALUE;
  private String         fname       = null;

  static String sep = System.getProperty("file.separator");


  /**
   * Open input file name
   */
  public Fget(String dir, String fname)
  {
    this(dir + sep + fname);
  }
  public Fget(String fname_in)
  {
    fname = fname_in;
    if (fname.endsWith("-"))
      br = new BufferedReader(new InputStreamReader(System.in));

    else
    {

      fptr = new File(fname);
      try
      {
        if (fptr.getName().toLowerCase().endsWith(".gz"))
          br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fptr)))));
        else if (fptr.getName().toLowerCase().endsWith(".jz1"))
          br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fptr)))));
        else
        {
          if (!fname.endsWith("stdin") && !fname.endsWith("_"))
          {
            br = new BufferedReader(new FileReader(fptr));
            filesize = fptr.length();
          }
          else
          {
            br = new BufferedReader(new InputStreamReader(new BufferedInputStream(System.in)));
          }
        }
      }


      catch (Exception e)
      {
        common.failure(e);
      }
    }
  }

  /**
   * Open input File pointer
   */
  public Fget(File fin)
  {

    try
    {
      fptr = fin;
      if (fptr.getName().toLowerCase().endsWith(".gz"))
        br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fptr)))));
      else if (fptr.getName().toLowerCase().endsWith(".jz1"))
        br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fptr)))));
      else
      {
        br = new BufferedReader(new FileReader(fptr));
        filesize = fptr.length();
      }
    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public void force_eof(long bytes)
  {
    early_eof = bytes;
  }


  public String getName()
  {
    return fname;
  }

  public static boolean fileRename(String parent, String oldname, String newname)
  {
    File oldf = new File(parent,oldname);
    File newf = new File(parent,newname);
    boolean ret = oldf.renameTo(newf);
    if (!ret)
      common.ptod("Fget.fileRename(): rename failed. " + oldf.getName() + " ===> " + newf.getName());
    return ret;
  }


  /**
   * Place a separator at the end of a directory name.
   */
  public static String separator(String dir)
  {
    /* Accept either an existing unix or windows separator: */
    if (!dir.endsWith("/") && !dir.endsWith("\\"))
      dir += sep;
    return dir;
  }

  /**
   * Return one line from input file
   */
  public String get()
  {
    /* Already EOF? */
    if (br == null)
      return null;

    File fnew = null;
    try
    {
      String line = br.readLine();
      if (line == null)
      {
        close();
        return null;
      }

      bytesread += line.length();
      return line;
    }

    catch (Exception e)
    {
      e.printStackTrace();
      common.ptod("Exception with file name: " + fnew.getName());
      common.failure(e);
    }
    return null;
  }


  /**
   * Service routine to help read a (not too huge) flatfile into memory.
   */
  public static Vector read_file_to_vector(String parent, String filename)
  {
    return read_file(new File(parent, filename));
  }
  public static Vector read_file_to_vector(String filename)
  {
    return read_file(new File(filename));
  }
  public static String[] readFileToArray(String dir, String filename)
  {
    return readFileToArray(new File(dir, filename));
  }
  public static String[] readFileToArray(String filename)
  {
    return readFileToArray(new File(filename));
  }
  private static String[] readFileToArray(File fptr)
  {
    Vector lines = read_file(fptr);
    String[] array = new String[lines.size()];
    for (int i = 0; i < lines.size(); i++)
      array[i] = (String) lines.elementAt(i);

    return array;
  }

  private static Vector read_file(File fptr)
  {
    Vector output = new Vector(64,0);
    if (!fptr.exists())
    {
      common.plog("read_file_to_vector(): file not found: " + fptr.getAbsolutePath());
      return output;
    }

    Fget fg = new Fget(fptr);

    /* Read all lines and store in vector: */
    while (true)
    {
      String line = fg.get();
      if (line == null)
        break;
      output.addElement(line);
    }
    fg.close();

    return output;
  }

  public String pct_read()
  {
    if (filesize == 0)
      return "n/a";
    return Format.f("%3d", bytesread * 100 / filesize);
  }

  public String get_parent()
  {
    return fptr.getAbsoluteFile().getParent();
  }


  public static String get_parent(String fname)
  {
    return new File(fname).getAbsoluteFile().getParent() + sep;
  }


  public void close()
  {
    //common.ptod("fget.close(): ");
    //Thread.currentThread().dumpStack();


    try
    {
      if (br != null)
      {
        br.close();
        br = null;
      }
    }
    catch (Exception e)
    {
      common.failure(e);
    }
  }

  /**
   * Check for existence of directory
   */
  public static boolean dir_exists(String dirname)
  {
    File dir = new File(dirname);
    if (!dir.exists())
      return false;
    return dir.isDirectory();
  }
  public static boolean dir_exists(String parent, String dirname)
  {
    File dir = new File(parent, dirname);
    if (!dir.exists())
      return false;
    return dir.isDirectory();
  }

  /**
   * Check to see if a file exists.
   * If the file exists, but it is a directory name, return false.
   */
  public static boolean file_exists(String parent, String fname)
  {
    //if (!parent.endsWith(sep))
    //  common.failure("Directory name not terminated by a separator: " + parent);

    boolean ret = new File(parent, fname).exists();
    if (ret)
      return (!dir_exists(parent, fname));
    return ret;
  }
  public static boolean file_exists(String fname)
  {
    boolean ret = new File(fname).exists();
    if (ret)
      return (!dir_exists(fname));
    return ret;
  }


  public static void file_delete(String fname)
  {
    new File(fname).delete();
  }
  public static void file_delete(String dir, String fname)
  {
    new File(dir, fname).delete();
  }


  public static void main(String[] args)
  {
    boolean rc = file_exists("/home/hv104788/vdbench/solaris/config.sh");
    common.ptod("rc: " + rc);
  }

}




