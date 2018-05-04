package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */



import java.io.File;
import java.util.HashMap;

import Utils.Getopt;
import Utils.OS_cmd;

/**
 * Run jmap a few times, and report the increments in values of what we're
 * seeing.
 *
 * This class does all the number crunching.
 *
 *
 *   http://stackoverflow.com/questions/1087177/what-do-those-strange-class-names-in-a-java-heap-dump-mean
 *
 *
 *      Element Type        Encoding
 *        boolean             Z
 *        byte                B
 *        char                C
 *        class or interface  Lclassname;
 *        double              D
 *        float               F
 *        int                 I
 *        long                J
 *        short               S
 *
 *
 */
public class Jmap
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static HashMap <String, JmapStuff> map = new HashMap(1024);
  private static long total_heap = 0;

  public static void main (String args[])
  {
    long sleep     = 5;
    long count     = 1;
    long min_bytes = 1 * 1024 * 1024;

    map.clear();

    /* Start with removing the first argument: */
    String[] nargs = new String[args.length - 1];
    System.arraycopy(args, 1, nargs, 0, nargs.length);
    args = nargs;

    Getopt g = new Getopt(args, "fgpm:k:l:s:d:", 1);
    g.print("Jmap");

    if (!g.isOK() || args.length == 0 || g.get_positionals().size() != 1)
    {
      common.ptod("Usage: ./vdbench jmap [-s nn] [-l nn] [-k nn] [-p] pid");
      common.ptod("       '-l nn' : loop nn times, default one");
      common.ptod("       '-s nn' : sleep nn seconds, default 5");
      common.ptod("       '-k nn' : minimum total kb for data type, default 1024k");
      common.ptod("       '-m nn' : minimum total mb for data type, default 1024k");
      common.ptod("       '-p'    : Create jmap dump");
      return;
    }

    long pid = g.get_pos_long(0);
    if (g.check('s'))
      sleep = g.get_long();
    if (g.check('l'))
      count = g.get_long();
    if (g.check('k'))
      min_bytes = g.get_long() * 1024l;
    if (g.check('m'))
      min_bytes = g.get_long() * 1024l * 1024l;

    //if (min_bytes == 0)
    //{
    //  min_bytes = 2048;
    //  common.ptod("Settings min_bytes to " + min_bytes);
    //}



    /* Some times they add 'jre' at the end: */
    String home = System.getProperty("java.home");
    if (home.endsWith("/jre") || home.endsWith("\\jre"))
      home = home.substring(0, home.length() - 4);


    OS_cmd ocmd = new OS_cmd();
    ocmd.addQuot(home + File.separator + "bin" + File.separator + "jmap");
    if (g.check('f'))
      ocmd.addText("-F");

    if (g.check('g'))
      ocmd.addText("-histo:live " + pid);
    else
      ocmd.addText("-histo " + pid);


    ocmd.execute(true);
    ocmd.printStderr();

    /* First pass: */
    String[] lines = ocmd.getStdout();
    for (int i = 0; i < lines.length; i++)
    {
      String line = lines[i].trim();

      if (line.length() == 0)
        continue;
      if (line.indexOf(":") == -1)
        continue;
      JmapStuff jm = new JmapStuff(line);
      map.put(jm.name, jm);
    }


    int loop = 0;
    while (true)
    {
      int linecount = 0;
      long total = 0;

      /* If we do only one pass, reuse the 'first pass' output: */
      if (count > 1)
      {
        ocmd = new OS_cmd();
        ocmd.addQuot(home + File.separator + "bin" + File.separator + "jmap");
        ocmd.addText("-histo " + pid);
        ocmd.execute(false);
        ocmd.printStderr();
        if (!ocmd.getRC())
          break;
      }

      lines = ocmd.getStdout();
      for (int i = 0; i < lines.length; i++)
      {
        String line = lines[i].trim();
        //common.ptod("line: " + line);

        if (line.indexOf("Total") != -1)
        {
          //common.ptod("line: " + line);
          common.ptod("Total bytes: %d mb", (total / 1048576));
          common.ptod("");
          total_heap = total;
          break;
        }

        if (line.indexOf(":") == -1)
          continue;

        /* Store the new data: */
        JmapStuff jm  = new JmapStuff(line);
        JmapStuff jmo = map.get(jm.name);
        map.put(jm.name, jm);

        if (jmo == null)
          continue;

        jm.max_bytes = Math.max(jm.bytes,     jmo.max_bytes);
        jm.max_inst  = Math.max(jm.instances, jmo.max_inst);
        total += jm.bytes;

        if (jm.bytes < min_bytes)
          continue;

        if (linecount++ ==0)
        {
          common.ptod("");
          common.ptod("%14s %8s %10s %11s %10s %8s %s\n",
                      "bytes",
                      "mb",
                      "count",
                      "new_count",
                      "instance",
                      "delta_mb", "");
        }


        common.ptod("%,14d %8d %,10d %,11d %,10d %8d %s",
                    jm.bytes,
                    jm.bytes/1048576,
                    jm.instances,
                    jm.instances-jmo.instances,
                    (jm.bytes / jm.instances),
                    (jm.bytes-jmo.bytes) / 1048576,
                    jm.name);

        //common.ptod("instances: %8d max:%8d new: %8d " +
        //            " bytes: %12d (%5d) %4dmb max: %8d new: %8d",
        //            jm.instances,
        //            (jm.max_inst == jm.instances) ? 0: jm.max_inst,
        //            jm.instances-jmo.instances,
        //            jm.bytes, (jm.bytes / jm.instances),
        //            jm.bytes/1048576,
        //            (jm.max_bytes == jm.bytes) ? 0 : jm.max_bytes,
        //            jm.bytes-jmo.bytes,
        //            jm.name);
      }

      /* Next pass: */
      if (++loop >= count)
        break;
      common.sleep_some(sleep * 1000);
    }

    /* Create dump file if needed: */
    if (g.check('p'))
    {
      String fname = getJmapFile();
      String parms = String.format("-dump:format=b,file=%s %d", fname, pid);

      ocmd = new OS_cmd();
      ocmd.addQuot(home + File.separator + "bin" + File.separator + "jmap");
      ocmd.addText(parms);
      ocmd.execute();
      ocmd.printStderr();
      ocmd.printStdout();
      common.ptod("Run 'jhat %s', and then enter 'http://localhost:7000' in your browser.", fname);
    }
  }

  private static String getJmapFile()
  {
    String[] files = new File(".").list();
    int highest_number = 0;
    for (int i = 0; i < files.length; i++)
    {
      String file = files[i];
      if (file.equals("jdump"))
        continue;
      if (!file.startsWith("jdump"))
        continue;
      int fnum = Integer.parseInt(file.substring(5));
      highest_number = Math.max(highest_number, fnum);
    }
    return String.format("jdump%03d", highest_number + 1);
  }

  public static void runJmap()
  {
    String[] args = new String[]
    {
      "xx", common.getProcessIdString(), "-m0"
    };
    Jmap.main(args);
  }

  public static void runJmapDump()
  {
    String[] args = new String[]
    {
      "xx", common.getProcessIdString(), "-p", "-m0"
    };

    /* Make sure we get rid of garbage first, because it looks as if jhat */
    /* con not distinguish between junk and real stuff: */
    for (int i = 0; i < 10; i++)
      System.gc();

    Jmap.main(args);
  }

  public static long getTotalHeap()
  {
    return total_heap;
  }

}

class JmapStuff
{
  long   instances;
  long   bytes;
  long   max_inst;
  long   max_bytes;
  String name;

  public JmapStuff(String line)
  {
    String[] split = line.split(" +");
    if (split.length != 4)
      common.failure("Requiring 4 fields: " + line);

    instances = Long.parseLong(split[1]);
    bytes     = Long.parseLong(split[2]);
    name      = split[3];

    if (name.length() > 24 && name.indexOf(".") != -1)
      name = name.substring(name.lastIndexOf("."));
  }
}

