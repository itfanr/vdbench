package Utils;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

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
    /* fname some times ends up with double separators. Fix it: */
    full_name = fname;
    full_name = full_name.replace(File.separator + File.separator, File.separator);

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
      common.failure("Trying to close an Fput() file that is not open");


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

  public static String getTmpDir()
  {
    String ret = System.getProperty("java.io.tmpdir");
    if (!ret.endsWith(File.separator))
      ret += File.separator;
    return ret;
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
  public void print(String format, Object ... args)
  {
    if (args.length > 0)
      pw.print(String.format(format, args));
    else
      pw.print(format);
  }
  public String  println(String format, Object ... args)
  {
    return println(String.format(format, args));
  }
  public String println(String line)
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

    return  line;
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
    if (common.onWindows())
      return;

    /* fiddling with stdin and stdout causes problems: */
    if (fname.endsWith("-"))
      return;

    if (common.onSolaris())
      Vdb.Native.chmod(fname);

    else
      OS_cmd.execute("chmod 777 " + fname, false);
  }

  // See: http://www.devx.com/Java/Article/22018/1954
  // for future reference.
  public static String createTempFileName(String one, String two)
  {
    return createTempFile(one, two).getAbsolutePath();
  }
  public static String createTempFileName(String two)
  {
    return createTempFile("swat", two).getAbsolutePath();
  }
  public static File createTempFile(String one, String two)
  {
    try
    {
      File fptr = File.createTempFile(one, two);
      fptr.deleteOnExit();
      return fptr;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }



  public static void main (String args[]) throws IOException
  {
  }
}




