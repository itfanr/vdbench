package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.*;
import java.util.*;
import Utils.Format;
import Utils.Fget;


class Build_replay
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  public static void main(String[] args)
  {
    //String dir = "p:\\tnf_traces\\juniper\\0217\\trace4a\\output";
    String dir = ".";
    String line = null;
    double total_rate = 0;
    int    total_size = 0;
    String devicename = null;

    File fdir = new File(dir);
    String[] files = fdir.list();

    /* Calculate total i/o rate for all files combined: */
    for (int f = 0; f < files.length; f++)
    {
      long   device_number = 0;
      double rate          = 0;
      double max_lba       = 0;
      double GB            = 1024*1024*1024;

      if (!files[f].startsWith("d_"))
        continue;

      Fget fg = new Fget(dir, files[f]);

      while ((line = fg.get()) != null)
      {
        line = line.trim();
        if (line.length() == 0)
          continue;


        StringTokenizer st = new StringTokenizer(line);

        /* Pick up device name: */
        if (line.indexOf("Statistics for:") != -1)
        {
          st.nextToken();
          st.nextToken();
          devicename = st.nextToken();
          continue;
        }

        /* Look for device number: */
        if (line.indexOf("Device number:") != -1)
        {
          for (int i = 0; i < 2; i++) st.nextToken();
          device_number = Long.parseLong(st.nextToken());
        }

        /* Look for max lba: */
        if (line.indexOf("highest:") != -1)
        {
          for (int i = 0; i < 6; i++) st.nextToken();
          max_lba = Double.parseDouble(st.nextToken());
        }

        /* Get i/o rate: */
        String label = st.nextToken();
        if (label.equalsIgnoreCase("reads:"))
          rate += Double.parseDouble(st.nextToken());
        else if (label.equalsIgnoreCase("writes:"))
          rate += Double.parseDouble(st.nextToken());
        else
          continue;
      }

      /* Only select devices requested: */
      boolean found = false;
      for (int i = 0; i < args.length; i++)
      {
        if (devicename.startsWith(args[i]))
        {
          found = true;
          break;
        }
      }
      if (!found)
        continue;


      /* Create one SD parameter: */
      int rounded_max = (int) ((max_lba * GB + GB - 1) / GB);
      String card = "sd=sd" + device_number + ",lun=/dev/rdsk/xxx" +
                    Format.f(",size=%03dg", rounded_max) +
                    ",replay=" + device_number;
      System.out.println(Format.f("%-70s", card) +
                         Format.f(" * Device %-12s", devicename) +
                         Format.f(" iops: %7.2f ", rate));

      total_rate += rate;
      total_size += rounded_max;


      fg.close();
    }


    System.out.println("*total_rate: " + total_rate);
    System.out.println("*total_size: " + total_size + "g");

  }


  public static void oldmain(String[] args)
  {
    String dir = "p:\\tnf_traces\\juniper\\0217\\trace4a\\output";
    String line = null;
    double total_rate = 0;


    /* Calculate total i/o rate for all files combined: */
    for (int f = 0; f < args.length; f++)
    {
      long   device_number = 0;
      double rate          = 0;
      double max_lba       = 0;
      double GB            = 1024*1024*1024;

      Fget fg = new Fget(dir, args[f] + ".html");

      while ((line = fg.get()) != null)
      {
        line = line.trim();
        if (line.length() == 0)
          continue;

        StringTokenizer st = new StringTokenizer(line);

        /* Look for device number: */
        if (line.indexOf("Device number:") != -1)
        {
          for (int i = 0; i < 2; i++) st.nextToken();
          device_number = Long.parseLong(st.nextToken());
        }

        /* Look for max lba: */
        if (line.indexOf("highest:") != -1)
        {
          for (int i = 0; i < 6; i++) st.nextToken();
          max_lba = Double.parseDouble(st.nextToken());
        }


        String label = st.nextToken();
        if (label.equalsIgnoreCase("reads:"))
          rate += Double.parseDouble(st.nextToken());
        else if (label.equalsIgnoreCase("writes:"))
          rate += Double.parseDouble(st.nextToken());
        else
          continue;
      }

      common.ptod("sd=sd" + device_number + ",lun=/dev/rdsk/xxx" +
                  ",size=" + (int) ((max_lba * GB + GB - 1) / GB) + "g" +
                  ",replay=" + device_number +
                  Format.f("     * Device iops: %.2f ", rate));

      total_rate += rate;


      fg.close();
    }


    //common.ptod("total_rate: " + total_rate);

    double total_skew = 0;

    /* Now interpret the data again for more detailed stuff: */
    for (int f = 0; f < args.length; f++)
    {
      double  rate = 0;
      double  seek_pct = -1;
      boolean read_flag = false;
      long    device_number = 0;

      Fget fg = new Fget(dir, args[f] + ".html");

      boolean xfer_found = false;

      while ((line = fg.get()) != null)
      {
        StringTokenizer st = new StringTokenizer(line);

        /* Get rid of empty lines: */
        line = line.trim();
        if (line.length() == 0)
          continue;

        /* 'label' is the first field on a line: */
        String label = st.nextToken();

        /* Look for device number: */
        if (label.equals("Device"))
        {
          st.nextToken();
          device_number = Long.parseLong(st.nextToken());
          continue;
        }

        /* In the 'reads' line, pick up the i/o rate: */
        if (label.equalsIgnoreCase("reads:"))
        {
          read_flag = true;
          rate = Double.parseDouble(st.nextToken());
          for (int i = 0; i < 11; i++) st.nextToken();
          seek_pct = Double.parseDouble(st.nextToken());
          common.ptod("");
          common.ptod("wd=default,sd=sd" + device_number +
                      ",rdpct=100" +
                      ",seekpct=" + seek_pct);

          //common.ptod("reads: " + rate + " " + seek_pct);
          continue;
        }

        /* In the 'writes' line, pick up the i/o rate: */
        if (label.equalsIgnoreCase("writes:"))
        {
          read_flag = false;
          rate = Double.parseDouble(st.nextToken());
          for (int i = 0; i < 11; i++) st.nextToken();
          seek_pct = Double.parseDouble(st.nextToken());
          common.ptod("");
          common.ptod("wd=default,sd=sd" + device_number +
                      ",rdpct=0" +
                      ",seekpct=" + seek_pct);

          //common.ptod("writes: " + rate + " " + seek_pct);
          continue;
        }

        /* If we have not found a seek% yet, don't look for xfersize: */
        if (seek_pct < 0)
          continue;


        /* In the 'xfer' line, skip the next line and remember: */
        if (line.startsWith("xfer") )
        {
          xfer_found = true;
          fg.get();
          continue;
        }

        /* Throw everything away unless we are looking at the xfersize distribution: */
        if (!xfer_found)
          continue;

        /* Mark the end of the distribution: */
        if (label.startsWith("Sequential") ||
            label.startsWith("Reads:") ||
            label.startsWith("Writes:") )
        {
          xfer_found = false;
          continue;
        }

        /* We now have the transfersize distribution: */
        st = new StringTokenizer(line);
        for (int i = 0; i < 2; i++) st.nextToken();
        int xfersize = Integer.parseInt(st.nextToken());
        xfersize = ((xfersize + 511) / 512) * 512;

        double pct = Double.parseDouble(st.nextToken());

        if (pct < 1)
          continue;

        double xfersize_rate = pct * rate / 100;
        //common.ptod("xfersize_rate: " + xfersize_rate);

        double skew = xfersize_rate * 100 / total_rate;
        total_skew += skew;

        /* Print the wd: */
        line  = "wd=wd" + device_number + ((read_flag) ? "_r_" : "_w_");
        line += "" + xfersize;
        line += ",xfersize=" + xfersize;
        line += Format.f(",skew=%.2f", skew);
        //line += "      " + total_skew;

        common.ptod(line);
      }
    }

    common.ptod("");
    common.ptod("*Total i/o rate observed: " + total_rate);
    common.ptod("*Total skew% passed: " + total_skew);
  }
}
