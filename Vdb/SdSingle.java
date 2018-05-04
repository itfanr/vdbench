package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.*;

import Utils.Fget;
import Utils.Getopt;


/**
 * This program reads a Vdbench output directory looking for all results of
 * tests done using 'sd=setsofX'.
 *
 * For each SD found in logfile.html this program collects all run average info
 * from the respective sdX.html files, and then reports those devices that
 * perform worst than 'x%' of the average for all devices. First however the
 * first 2 slowest devices are removed before we calculate that average.
 *
 * See 'usage' below.
 */
public class SdSingle
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";


  private static void usage(String txt)
  {
    common.ptod("");
    common.ptod("Usage: ./vdbench Vdb.SdSingle [-p nn] [-r nn] [-a] [-f] [-t type] vdbench_output_directory");
    common.ptod("       -r nn: Default 2. How many of the slowest devices to remove to calculate average.");
    common.ptod("       -p nn: Default 5. Any device not performing within 'nn' percent of the average will be reported");
    common.ptod("       -a:    Display all devices, including those devices not removed.");
    common.ptod("       -t     Type: allows 'resp', 'iops', and 'max'. Default 'iops'");
    common.ptod("       -f     Full reporting, sorted by SD");
    common.ptod("");
    common.ptod("Note that this program only works for Vdbench runs using 'sd=setsofN'");
    common.ptod("");
    common.failure(txt);
  }

  public static void main(String[] args)
  {
    int remove_slowest = 2;
    int drop_pct       = 5;
    boolean disp_all   = false;
    String vdb_directory;
    String type = "iops";

    Getopt getopt = new Getopt(args, "far:p:d:t:", 1);
    if (!getopt.isOK() || getopt.get_positionals().size() == 0)
      usage("Parameter scan error");

    disp_all = getopt.check('a');
    if (getopt.check('r'))
      remove_slowest = (int) getopt.get_long();
    if (getopt.check('p'))
      drop_pct = (int) getopt.get_long();
    if (getopt.check('t'))
    {
      type = getopt.get_string();
      if (!type.equals("resp") && !type.equals("max") && !type.equals("iops"))
        common.failure("'-t' only allows 'resp', 'iops', and 'max'. Default 'iops'");
    }

    vdb_directory = getopt.get_pos_string(0);

    HashMap <String, RunData> sd_map = new HashMap(64);

    /* Create a list of sds: */
    /* This requires all sdnames to start with 'sd': */
    String[] lines = Fget.readFileToArray(vdb_directory, "logfile.html");
    for (String line : lines)
    {
      if (line.contains("sd=sd") && line.contains(",lun=") && line.contains("lun size"))
      {
        String[] split = line.trim().split(" +");
        if (split.length < 2)
          continue;
        split = split[1].split(",+");
        RunData run  = new RunData();
        run.sd_name  = split[0].substring(3);
        run.dev_name = split[1].substring(4);

        /* Read the SDs html file: */
        String[] sdlines = Fget.readFileToArray(vdb_directory, run.sd_name + ".html");
        for (String sdline : sdlines)
        {
          if (sdline.contains("avg_"))
          {
            split = sdline.split(" +");
            run.iops = Double.parseDouble(split[2]);
            run.resp = Double.parseDouble(split[6]);
            run.max  = Double.parseDouble(split[9]);

            /* Only add the device when we found detail data. */
            /* This allows me to look at output that is still running. */
            sd_map.put(run.sd_name, run);
            break;
          }
        }

      }
    }


    //   System.exit(777);



    /* Go through all SDs: */
    RunData[] run_array = sd_map.values().toArray(new RunData[0]);
    ArrayList <RunData> runs = new ArrayList(256);
    for (RunData run : run_array)
      runs.add(run);

    /* Sort the runs on iops: */
    RunData.which_sort = type;
    Collections.sort(runs);

    // reporting
    //for (RunData run : runs)
    //  common.ptod(run);



    common.ptod("");
    common.ptod("Removing the %d worst '%s' devices from calculated averages:", remove_slowest, type);
    for (int j = 0; j < remove_slowest; j++)
    {
      RunData data = runs.get(j);
      common.ptod(data);
    }


    /* Calculate average iops using all but the first two: */
    double total_iops = 0;
    double total_resp = 0;
    double total_max  = 0;
    int    total_count = 0;
    for (int j = remove_slowest; j < runs.size(); j++)
    {
      RunData data = runs.get(j);
      total_iops  += data.iops;
      total_resp  += data.resp;
      total_max   += data.max;
      total_count ++;
    }


    double cutoff_value;
    double avg_value;
    if (type.equals("iops"))
    {
      avg_value    = total_iops / total_count;
      cutoff_value = avg_value * (100 - drop_pct) / 100;
    }
    else if (type.equals("resp"))
    {
      avg_value    = total_resp / total_count;
      cutoff_value = avg_value * (100 + drop_pct) / 100;
    }
    else
    {
      avg_value    = total_max  / total_count;
      cutoff_value = avg_value * (100 + drop_pct) / 100;
    }

    String low_high = (type.equals("iops")) ? "lowest" : "highest";

    common.ptod("");
    common.ptod("The average %s is %8.3f (the %d %s of %d ignored)",
                type, avg_value, remove_slowest, low_high, runs.size());

    int slowest = 0;
    if (remove_slowest > 0)
    {
      common.ptod("");
      common.ptod("*******************************************************************************************");
      common.ptod("These are the devices that are %d%% different from the average '%s' of %.3f "+
                  "(cutoff is %.3f)",
                  drop_pct, type, avg_value, cutoff_value);

      String preserve_sort = RunData.which_sort;

      RunData.which_sort = type;
      Collections.sort(runs);

      for (RunData run : runs)
      {
        if (type.equals("iops"))
        {
          run.value_used = run.iops;
          if (drop_pct > 0 && run.iops < cutoff_value)
          {
            slowest++;
            run.dropped    = true;
            common.ptod(run);
          }
        }
        else if (type.equals("resp"))
        {
          run.value_used = run.resp;
          if (drop_pct > 0 && run.resp > cutoff_value)
          {
            slowest++;
            run.dropped    = true;
            common.ptod(run);
          }
        }
        else
        {
          run.value_used  = run.max;
          if (drop_pct > 0 && run.max > cutoff_value)
          {
            slowest++;
            run.dropped = true;
            common.ptod(run);
          }
        }
      }

      if (slowest == 0)
        common.ptod("No slow devices found");
      common.ptod("*******************************************************************************************");
    }

    if (!disp_all)
    {
      common.ptod("");
      common.ptod("Remaining devices not displayed; add -a execution parameter to also display those.");
    }

    else
    {
      RunData.which_sort = type;
      Collections.sort(runs);

      common.ptod("");
      common.ptod("These are the remaining devices, sorted by '%s':", type);
      int seqno = 1;
      for (int j = 0; j < runs.size(); j++)
      {
        RunData run = runs.get(j);
        if (!run.dropped)
          common.ptod(String.format("(%3d) %s (%5.1f%% of average %.3f)",
                                    seqno++, run,
                                    run.value_used * 100 / avg_value,
                                    avg_value));
      }
    }


    if (getopt.check('f'))
    {
      RunData.which_sort = "name";
      Collections.sort(runs);

      common.ptod("");
      common.ptod("These are the remaining devices, sorted by '%s':", type);
      int seqno = 1;
      for (int j = 0; j < runs.size(); j++)
      {
        RunData run = runs.get(j);
        //if (!run.dropped)
          common.ptod(String.format("(%3d) %s (%5.1f%% of average %.3f)",
                                    seqno++, run,
                                    run.value_used * 100 / avg_value,
                                    avg_value));
      }
    }
  }


}


/**
 * This class describes a set of data related to an SD name created because of
 * sd=setsofX.
  */
class RunData implements Comparable
{
  String sd_name;
  String dev_name;
  int    count;   /* for totals only */
  double iops;
  double resp;
  double max;
  double value_used;
  boolean dropped = false;

  static String which_sort = "tbd";

  public int compareTo(Object obj)
  {
    RunData data = (RunData) obj;
    if (which_sort.equals("iops"))
    {
      if (iops < data.iops)
        return -1;
      if (iops > data.iops)
        return 1;
      else
        return 0;
    }

    else if (which_sort.equals("resp"))
    {
      if (resp > data.resp)
        return -1;
      if (resp < data.resp)
        return 1;
      else
        return 0;
    }

    else if (which_sort.equals("max"))
    {
      if (max > data.max)
        return -1;
      if (max < data.max)
        return 1;
      else
        return 0;
    }

    else if (which_sort.equals("name"))
    {
      int one = Integer.parseInt(sd_name.substring(2));
      int two = Integer.parseInt(data.sd_name.substring(2));
      return one - two;
    }


    common.failure("Invalid 'which_sort': " + which_sort);
    return 0;

  }

  public String toString()
  {
    return String.format("%-10s %-40s iops: %8.3f resp: %7.3f max: %8.3f",
                         sd_name, dev_name, iops, resp, max);
  }
}
