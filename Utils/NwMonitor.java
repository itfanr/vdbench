package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.text.*;


/**
 * Very primitve network performance monitor. Built to prove that performance
 * sucks!!!!
 */
class NwMonitor
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  public static void main(String[] args)
  {
    if (args.length != 2)
      common.failure("Usage: NwMonitor seconds target_directory");

    String target_dir = args[1];
    long sleep = Long.parseLong(args[0]) * 1000;

    String filename_for_write = "monitor.";
    if (common.onWindows())
      filename_for_write += "windows";
    else
      filename_for_write += "solaris";

    DateFormat df = new SimpleDateFormat("EEEE, MMMM dd yyyy, HH:mm:ss.SSS" );


    if (args.length > 0)
      sleep = Long.parseLong(args[0]) * 1000;


    long next = System.currentTimeMillis();
    next = (next + sleep -1 ) / sleep * sleep + 1;

    while (true)
    {
      /* Write a small file and report the duration: */
      long now = System.currentTimeMillis();
      if (next - now > 1)
        common.sleep_some_no_int(next - now);

      /* This is the timed loop: */
      long start = System.currentTimeMillis();

      /* Write about 8k: */
      Fput fp = new Fput(target_dir, filename_for_write);
      for (int i = 0; i < 153; i++)
        fp.println("The quick brown fox jumps over the lazy network: " + i);
      fp.close();

      long end = System.currentTimeMillis();
      long write_duration = end - start;

      /* Do a 'ping' and see how long it takes: */
      start = System.currentTimeMillis();
      String host = "sm-ubrm-01.central.sun.com";
      OS_cmd.execute("ping -n1 " + host, false);
      end = System.currentTimeMillis();
      long ping_duration = end - start;


      System.out.println(df.format(new Date(next)) +
                         Format.f(" Elapsed: %7d ms;", (write_duration)) +
                         Format.f(" Ping " + host + ": %7d ms", (ping_duration)) );

      /* Calculate next sample time: */
      now = System.currentTimeMillis();
      next = (now + sleep -1 ) / sleep * sleep + 1;
    }
  }
}


