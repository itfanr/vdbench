package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.io.Serializable;
import java.util.*;

import Utils.*;


public class LS
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private   static int       loop_protector = 0;
  private   static ArrayList detail_data    = new  ArrayList(32768);
  private   static LsData[]  data           = null;
  protected static boolean   reuse          = false;
  protected static boolean   sort_size      = false;
  protected static boolean   limit_output   = false;
  private   static boolean   no_recursive   = false;
  protected static int       limit_lines    = Integer.MAX_VALUE;
  private   static int       dir_count      = 0;
  private   static boolean   depth          = false;
  private   static int       how_deep       = 0;

  protected static long      minimum_report = 0;

  public static void main(String[] args) throws Exception
  {
    Getopt g = new Getopt(args, "rnsl:d:m:", 1);
    g.print("ls");
    if (!g.isOK())
    {
      g.print("ls");
      common.ptod("");
      common.ptod("Usage: lll [-n] [-s] [-l nn] [-d nn] [-m nn] directory");
      common.ptod("-n:    No looking at sub directories");
      common.ptod("-s:    Sort output descending by file size");
      common.ptod("-r:    Reuse directory information saved earlier");
      common.ptod("-m nn: Minimum reporting. Skip output < nn bytes (k/m/g/t)");
      common.ptod("-l nn: Line count: report on the first 'nn' lines");
      common.ptod("-d nn: Depth. This reports totals_report of 'nn' directories deep");
      common.ptod("");
      common.failure("Parameter scan error");
    }


    String dirname = ".";
    if (g.get_positionals().size() != 0)
      dirname = g.get_positional();
    if (dirname.endsWith(File.separator))
      dirname = dirname.substring(0, dirname.lastIndexOf(File.separator));

    reuse        = g.check('r');
    no_recursive = g.check('n');
    sort_size    = g.check('s');
    if (g.check('l'))
      limit_lines  = (int) g.get_long();
    if (depth = g.check('d'))
      how_deep = (int) g.get_long();
    if (g.check('m'))
      minimum_report = common.parseSize(g.get_string());

    if (sort_size && limit_lines == Integer.MAX_VALUE)
      limit_lines = 20;

    if (!reuse)
      System.err.print("One '.' per directory scanned: ");


    /* Start scanning all directories: */
    readDirectories(dirname);

    /* Pick up all the files we have and sort if needed: */
    data = (LsData[]) detail_data.toArray(new LsData[0]);
    Arrays.sort(data);
    for (int i = 0; depth && i < data.length; i++)
      DirData.addFile(data[i], how_deep);

    /* Report the total file size: */
    long total = 0;
    long files = 0;
    for (int i = 0; i < data.length; i++)
    {
      total += data[i].size;
      files++;
    }

    if (!reuse)
      System.out.println(String.format("Total file size: %,d (%s); Directories: %d; Files: %d",
                                       total, xlateSize(total), dir_count, files));


    /* Now print out all (or a limited amount of) detail: */
    if (!depth)
      printDetail();
    else
      DirData.printDirectories();
  }

  /**
   * Read the requested directory structure, or reuse earlier saved one.
   */
  private static void readDirectories(String dirname)
  {
    if (reuse)
    {
      detail_data = (ArrayList) common.serial_in(Fput.getTmpDir() + "vdbench_ls.tmp");
      return;
    }
    /* Start scanning all directories: */
    long start = System.currentTimeMillis();
    recursive(dirname);
    long end   = System.currentTimeMillis();

    if (end - start > 10000)
      System.err.println("\n" + dir_count + " directories in " + (end - start) / 1000 + " seconds.");
    else
      System.err.println();
    if (!reuse)
      common.serial_out(Fput.getTmpDir() + "vdbench_ls.tmp", detail_data);
  }

  private static void recursive(String dirname)
  {
    if (dirname.contains("///"))
    {
      common.ptod("Some kind of recursive error is going on: " + dirname);
      return;
    }
    String line = null;

    try
    {
      if (dir_count++ % 100 == 0)
        System.err.println("\nScanning: " + dirname);
      loop_protector++;

      if (dir_count % 25 == 0)
      {
        if (dir_count % 100 == 0)
          System.err.print(String.format("(%d) (%d)", dir_count, detail_data.size()));
        else
          System.err.print(String.format("(%d)", dir_count));
      }
      else
        System.err.print(".");

      if (loop_protector > 20)
        common.failure("Is there a loop here? " + dirname);
      OS_cmd ocmd = new OS_cmd();
      if (common.onWindows())
        ocmd.addText("ls -ln");

      else if (common.onLinux())
        ocmd.addText("ls -l");  // or /bin/ls?

      else
        ocmd.addText("/usr/bin/ls -l");

      ocmd.addText(dirname);
      ocmd.execute(false);

      //common.ptod("ocmd: " + ocmd.getCmd());

      String[] lines = ocmd.getStdout();
      for (int i = 0; i < lines.length; i++)
      {
        line = lines[i].trim();
        //common.ptod("line: " + line);
        String[] split = line.trim().split(" +");
        if (line.startsWith("total"))
          continue;
        if (line.endsWith(":"))
          continue;

        String prefix = "";
        for (int x = 0; x < split.length && x < 8; x++)
          prefix += split[x] + " ";
        prefix = prefix.trim();

        String name = "";
        for (int x = 8; x < split.length; x++)
          name += split[x] + " ";
        name = name.trim();

        //common.ptod("line: " + line);
        //common.ptod("prefix: " + prefix + " ||||||||| " + name);

        /* If this is a directory, scan it, unless it is a link: */
        if (line.startsWith("d") && !no_recursive)
          recursive(dirname + File.separator + name);

        else if (line.startsWith("l"))
          System.err.println("\nlink: " + dirname + " " + name);

        else if (line.startsWith("c"))
        {
          //common.ptod("Skipping: " + line);
          continue;
        }

        else
        {
          //common.ptod("line: " + line);
          LsData lsd = new LsData(prefix, dirname, name, line);
          detail_data.add(lsd);
        }
      }

      loop_protector--;
    }

    catch (Exception e)
    {
      System.err.println();
      System.err.println("line: " + line);
      common.failure(e);
    }
  }


  private static void printDetail()
  {
    /* Now print out all (or a limited amount of) detail: */
    for (int i = 0; i < data.length; i++)
    {
      if (-- limit_lines > 0)
      {
        if (data[i].size >= LS.minimum_report)
          System.out.println(data[i].getDetail());
      }
    }
  }

  public static String xlateSize(long size)
  {
    long   KB = 1024l;
    long   MB = 1024l*1024l;
    long   GB = 1024l*1024l*1024l;

    String sz;
    if (size < KB*10)
      sz = String.format("%,d", size);
    else if (size < MB*10)
      sz = String.format("%,dk", size / KB);
    else  if (size < GB*10)
      sz = String.format("%,dm", size / MB);
    else
      sz = String.format("%,dg", size / GB);

    return sz;
  }
}

/**
 * A class instance is created for each single file name found, containing the
 * output as it will be printed.
 */
class LsData implements Comparable, Serializable
{
  String dirname;
  String fname;
  String file_stuff;
  long   size;

  public LsData(String ls_output, String dirname_in, String fname_in, String line)
  {
    dirname = dirname_in;
    fname   = fname_in;
    String[] split = ls_output.split(" +");
    try
    {
      if (split.length < 5)
      {
        common.ptod("Invalid data: " + ls_output);
        return;
      }
      size = Long.parseLong(split[4]);
      String sz = LS.xlateSize(size);

      if (!common.onWindows())
      {
        file_stuff = String.format("%-12s %3s %-8s %-8s %,16d %6s %3s %2s %5s ",
                                   split[0], split[1], split[2], split[3], size, sz,
                                   split[5], split[6], split[7]);
      }
      else
        file_stuff = String.format("%10s %3s %-4s %-4s %,12d %6s %3s %2s %5s ",
                                   split[0], split[1], split[2], split[3], size, sz,
                                   split[5], split[6], split[7]);
    }
    catch (Exception e)
    {
      common.ptod("dirname:    " + dirname);
      common.ptod("fname:      " + fname);
      common.ptod("file_stuff: " + file_stuff);
      common.ptod("ls_output:  " + ls_output);
      common.ptod("line:       " + line);
      common.failure(e);
    }
  }

  public String getDetail()
  {
    return file_stuff + dirname + File.separator + fname;
  }

  public int compareTo(Object obj)
  {
    LsData ls = (LsData) obj;

    if (!LS.sort_size)
    {
      String tmp1 = dirname + File.separator + fname;
      String tmp2 = ls.dirname + File.separator + ls.fname;
      return tmp1.compareToIgnoreCase(tmp2);
    }

    long delta = ls.size - size;
    if (delta > 0)
      return 1;
    else if (delta < 0)
      return -1;
    else
      return 0;
  }
}

class DirData  implements Comparable
{
  String dirname;
  long   dir_total;
  long   file_count;

  static HashMap <String, DirData> dir_list = new HashMap(256);
  static StringBuffer work = new StringBuffer(1024);

  static String splitter = common.onWindows() ? "\\\\+" : "/+";


  /**
   * From the complete file name, pick as many file separators as have been
   * requested. Use that as a key to get a combined list of file names and
   * directories to report.
   */
  public static void addFile(LsData lsd, int how_deep)
  {
    /* Separate the file's directory name into pieces: */
    String   fname = lsd.dirname + File.separator;
    String[] split = fname.split(splitter);
    work.setLength(0);
    //common.ptod("split.length: " + fname + " " + split.length);
    for (int i = 0; i < how_deep+1 && i < split.length; i++)
    {
      if (i == 0)
        work.append(split[i]);
      else
        work.append(File.separator + split[i]);
    }

    String dir_short = work.toString();

    DirData dd = dir_list.get(dir_short.toString());
    if (dd == null)
    {
      dd         = new DirData();
      dd.dirname = dir_short;
      dir_list.put(dir_short, dd);
      //common.ptod("adding dir_short: %d %s %s", split.length, dir_short, fname);
    }

    dd.dir_total += lsd.size;
    dd.file_count ++;

    //if (lsd.dirname.startsWith(".\\solaris") )
    //{
    //  common.ptod("dir_short: "  + dir_short);
    //  common.ptod("lsd.dirname " + lsd.dirname + " " + lsd.fname);
    //}

  }

  public static void printDirectories()
  {
    DirData[] dds = (DirData[]) dir_list.values().toArray(new DirData[0]);
    Arrays.sort(dds);
    common.ptod("%12s %8s %8s   %s" , "Size", "Size", "Files", "Directory name");
    for (int i = 0; i < dds.length && i < LS.limit_lines; i++)
    {
      DirData dd = dds[i];
      if (dd.dir_total >= LS.minimum_report)
      {
        String sz  = LS.xlateSize(dd.dir_total);
        common.ptod("%12d %8s %8d   %s", dd.dir_total, sz, dd.file_count, dd.dirname);
      }
    }
  }

  public int compareTo(Object obj)
  {
    DirData dd = (DirData) obj;
    if (!LS.sort_size)
      return dirname.compareToIgnoreCase(dd.dirname);

    long delta = dd.dir_total - dir_total;
    if (delta > 0)
      return 1;
    else if (delta < 0)
      return -1;
    else
      return 0;

    //return(int) (dd.dir_total - dir_total);
  }
}
