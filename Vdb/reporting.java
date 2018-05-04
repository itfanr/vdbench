package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.lang.String;
import java.util.Vector;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.*;
import Utils.*;


/**
 * This class contains simple support methods for printing column layouts.
 * This code should be replaced with tnfe/flatfile.java
 */
public class reporting
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  String title1;        /* First title line text for this column              */
  String title2;        /* Second column                                      */
  String format;        /* Format for column printing                         */
  String format_title;  /* Format for title printing                          */
  int    width;         /* width of column                                    */

  reporting()
  {
  }

  /**
   * Set up report titles and layouts.
   * Receive two title strings and a format string with '*'.
   * The '*' in the format string will be replaced with the MAXIMUM length
   * of either title strings. This will then be used for printing.
   * When a title starts with a '-', the title will be left adjusted,
   * otherwise it will be always right adjusted.
   *
   * If title1 contains '.' or '-.', the title will not be printed and
   * the caller is responsible to have a string in the previous title
   * that is long enough to also include the space taken up by this title.
   */
  reporting(String t1, String t2, String form, int w)
  {
    int index;
    boolean left = false;

    /* Copy width of column: */
    width = w;

    /* Copy title for column: */
    if (t1.startsWith("-"))
    {
      title1 = t1.substring(1);
      left = true;
    }
    else
      title1 = t1;
    if (t2.startsWith("-"))
    {
      title2 = t2.substring(1);
      left = true;
    }
    else
      title2 = t2;

    /* Create the format strings. Add one extra required space: */
    format = new String();
    index = form.indexOf('*', 0);
    format = form.substring(0, index);
    format = " " + format + width;
    format = format + form.substring(index+1);

    format_title = new String();
    if (left)
      format_title = " %-" + width + "s";
    else
      format_title = " %" + width + "s";

  }

  /**
   * Print titles as defined above.
   */
  public static void report_header(PrintWriter pw, reporting rep[])
  {
    int i;
    String line1 = "\n            ";  // accomodate 12 blanks for tod
    String line2 = "            ";  // accomodate 12 blanks for tod

    /* Print first header line with column titles: */
    for (i = 0; rep[i] != null; i++)
    {
      if (rep[i].title1.compareTo(".") != 0)
        line1 = line1 + Format.f(rep[i].format_title, rep[i].title1 );
    }

    /* Print second header line with column titles: */
    for (i = 0; rep[i] != null; i++)
    {
      line2 = line2 + Format.f(rep[i].format_title, rep[i].title2 );
    }

    common.println(line1, pw);
    common.println(line2, pw);
  }

  /**
   * Close report file
   */
  public void report_close(PrintWriter pw)
  {
    common.println(" ", pw);
    common.println(" ", pw);

    pw.close();
  }

  /**
   * Print one column.
   */
  public String report(int number)
  {
     return Format.f(this.format, number );
  }

  public String report(long number)
  {
    return Format.f(this.format, number );
  }

  public String report(double number)
  {
    return Format.f(this.format, number );
  }

  public String report(String number)
  {
    return Format.f(this.format, number );
  }



  /**
   * Create a new directory after deleting all the files in it.
   * If there is a subdirectory in the list, we won't delete it.
   */
  public static String rep_mkdir(String newdir)
  {
    int i;
    String separator = System.getProperty("file.separator");
    boolean output_dir_plus = false;

    if (newdir.length() == 0)
      common.failure("No proper output directory name specified using '-o' parameter.");

    /* A '+' at the end increments the directory number: */
    if (newdir.substring(newdir.length() -1).startsWith("+"))
    {
      newdir = newdir.substring(0, newdir.length() -1);
      output_dir_plus = true;
    }

    /* '.tod' at the end adds a date/tod: */
    if (newdir.length() > 5 && newdir.substring(newdir.length() -4).startsWith(".tod"))
    {
      DateFormat df = new SimpleDateFormat( "yyMMdd.HHmmss" );
      Date d = new Date();
      newdir = newdir.substring(0, newdir.length() -4) + "." + df.format( d );
    }

    /* If the directory does not exist, just create it: */
    File dir = new File(newdir);
    if (!output_dir_plus && !dir.exists())
    {
      if (!dir.mkdir())
        common.failure("Unable to create output directory: " + newdir);

      /* Make sure directory has all permissions: */
      if (common.onSolaris())
        OS_cmd.executeCmd("chmod 777 " + newdir);

      common.ptod("Created output directory '" + dir.getAbsolutePath() + "'");

      return dir.getAbsolutePath();
    }

    /* If the directory exists and we don't need name change, delete files: */
    if (!output_dir_plus)
    {
      /* List all the filenames we have here: */
      String[] filenames = dir.list();

      /* If there is something to delete, delete it: */
      if (filenames != null)
      {
        for (i = 0; i < filenames.length; i++)
        {
          if (filenames[i].endsWith(".html"        ) ||
              filenames[i].endsWith(".bin"         ) ||
              filenames[i].endsWith("swat_mon.txt" ) ||
              filenames[i].endsWith("swat_mon.bin" ) )
          {
            File del = new File(newdir + separator + filenames[ i ]);
            //System.out.println("deleting " + del.getAbsolutePath());
            del.delete();
          }
        }
      }
      return dir.getAbsolutePath();
    }

    /* Increment directory name until we find a non-existing one: */
    for (i = 1; i < 999; i++)
    {
      File maybe = new File(newdir + Format.f("%03d", i));
      if (!maybe.exists())
        break;
    }
    if (i == 999)
      common.failure("Directories '" + newdir + "1' thru 999 already exist. " +
                     "Directory limit reached");

    /* Create new directory: */
    newdir = newdir + Format.f("%03d", i);
    dir = new File(newdir);
    if (!dir.mkdir())
      common.failure("Unable to create output directory: " + newdir);

    common.ptod("Created output directory '" + dir.getAbsolutePath() + "'");
    return dir.getAbsolutePath();
  }
}


