package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

/**
 * Program to assist in the calculation of memory needs.
 */
public class Estimate
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

  public static void main(String[] args)
  {
    display("");
    display("Vdbench memory estimation ");
    display("");

    sdCalculation();

    //String type = query("What type of test, sd or fsd?");
    //if (type.startsWith("s"))
    //  sdCalculation();
    //else
    //   For fsds: 64 bytes per filename
    //  fsdCalculation();
  }

  private static void sdCalculation()
  {
    String ret = null;

    long sd_count   = common.parseSize(query("How many SDs?"));
    long sd_threads = common.parseSize(query("Number of threads per SD?"));
    long max_xfer   = common.parseSize(query("Largest xfersize (k/m)?"));

    boolean dv = query("Will you be using Data Validation? (y/n)").startsWith("y");
    if (!dv)
    {
      display("");
      display("Estimated native memory needs for data buffers: #sds * #threads * max_fersize * 2");
      display("Estimated native memory needs for data buffers: %4d * %8d * %,11d * 2 = %,d (%s)",
              sd_count, sd_threads, max_xfer,
              (2 * sd_count * sd_threads * max_xfer),
              FileAnchor.whatSize(2 * sd_count * sd_threads * max_xfer));
      display("");
    }
    else
    {
      long lun_size = common.parseSize(query("Lun size per SD (k/m/g/t)?"));
      long min_xfer = common.parseSize(query("Smallest xfersize (k/m)?"));

      display("");
      display("Estimated native memory needs for data buffers: #sds * #threads * max_fersize * 2");
      display("Estimated native memory needs for data buffers: %4d * %8d * %,11d * 2 = %,d (%s)",
              sd_count, sd_threads, max_xfer,
              (2 * sd_count * sd_threads * max_xfer),
              FileAnchor.whatSize((2 * sd_count * sd_threads * max_xfer)));
      display("");

      long total_size = sd_count * lun_size;
      display("");
      display("Estimated total lun size: %,d (%s) ",
              total_size, FileAnchor.whatSize(total_size));

      display("Estimated native memory needs for Data Validation table: "+
              "one byte per smallest xfersize: %,d (%s) ",
              total_size / min_xfer, FileAnchor.whatSize(total_size / min_xfer));
      display("");

      // maybe add 1 bit for dedup flipflop and unique map?
      // flipflopmap only when not doing DV.

    }
  }

  private static void display(String format, Object ... args)
  {
    String text = String.format(format, args);
    System.out.println(text);
  }
  private static String query(String format, Object ... args)
  {
    String text = String.format(format, args);
    text = String.format("%-40s ===> ", text);
    System.out.print(text);

    try
    {
      String ret = br.readLine();
      if (ret.startsWith("q"))
      {
        System.out.println();
        System.out.println("Quitting upon request");
        System.out.println();
        System.exit(0);
      }

      return ret;
    }
    catch (Exception e)
    {
      common.failure(e);
      return null;
    }
  }

}
