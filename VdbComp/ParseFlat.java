package VdbComp;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import Utils.common;
import Utils.Fget;

/**
 * Parse flatfile.html files look for 'avg_' intervals.
 */
public class ParseFlat
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Parse a flatfile.html
   *
   * @param fname  Complete file name for flatfile.html
   *
   * @return
   */
  public static void parseFlatFile(FlatFile ff, Vector all_runs)
  {
    String fname = ff.name;
    Fget fg         = new Fget(fname);
    String line     = null;
    int    comments = 0;

    /* Look for the first header: */
    while ((line = fg.get()) != null)
    {
      line = line.trim();
      if (line.startsWith("tod"))
        break;
      if (line.startsWith("*"))
        comments++;
    }

    /* Flatfile.html alas is also created in the multi-jvm h1,h2 subdirectory */
    /* we can recognize this because those files will not contain comments */
    if (line == null && comments == 0)
    {
      fg.close();
      return;
    }

    if (!line.startsWith("tod"))
    {
      fg.close();
      throw new CompException("Missing 'tod' label in file " + fname);
    }

    /* Pick up all column headers: */
    StringTokenizer st = new StringTokenizer(line);
    Vector headers = new Vector(st.countTokens());
    while (st.hasMoreTokens())
      headers.add(st.nextToken());

    /* Now pick up data for each 'avg_' run from the flatfile: */
    String  last_rd = null;
    Run last_run = null;
    while ((line = fg.get()) != null)
    {
      if (line.indexOf("Starting RD=") != -1)
      {
        last_rd = line;
        common.ptod("last_rd: " + last_rd);
        continue;
      }

      HashMap data = new HashMap();
      st = new StringTokenizer(line);
      int col = 0;
      while (st.hasMoreTokens())
      {
        String value = st.nextToken();
        //if (headers.elementAt(col).equals("Run"))
        //  common.ptod("value: " + value + " " + headers.elementAt(col));
        try
        {
          double number = Double.parseDouble(value);
          data.put(headers.elementAt(col++), new Double(number));
        }
        catch (NumberFormatException e)
        {
          data.put(headers.elementAt(col++), value);
        }
      }

      if (col != headers.size())
      {
        fg.close();
        String txt = "\nlast line: " + line + "\nheaders: " + headers.size() +
                     "\ndata: " + col;
        throw new CompException("Not enough data for all columns in file " + fname + txt);
      }

      /* Only save data for a run average: */
      Object interval = data.get("Interval");
      if (interval == null)
      {
        fg.close();
        throw new CompException("Missing 'Interval' column in file " + fname);
      }

      /* The plan was to store the run averages only when we had 'avg'         */
      /* in the interval. However, multi-jvm-host stopped putting 'avg'        */
      /* in the interval for the flatfile. Fixed in vdbench406.                */
      /* We now write out the previous run when the interval becomes '1' again */
      /*
      if (interval.startsWith("avg_"))
      {
        Run run = new Run(fname, data, last_rd);
        all_runs.add(run);
      }
      */

      if ((interval instanceof Double) &&
          ((Double) interval).doubleValue() == 1 && last_run != null)
      {
        all_runs.add(last_run);
        ff.runs.add(last_run);
        //common.ptod("last_run: " + all_runs.size() + " " + last_run.rd_name);
      }

      last_run = new Run(fname, ff.base_dir, data, last_rd);
    }

    /* Add the last process run at eof. This assumes that the last run was   */
    /* complete!! There is no way to check, but the only consequence will be */
    /* that the values of the last running interval will be seen as run      */
    /* averages Fine for now.                                                */
    if (last_run != null)
    {
      all_runs.add(last_run);
      ff.runs.add(last_run);
    }

    if (all_runs.size() == 0)
    {
      fg.close();
      throw new CompException("No valid run averages found in file " + fname);
    }


    fg.close();
  }


  /**
   * 'Temporary' functionality to parse summary.html for those files
   * where the 'Starting RD=' data is not in flatfile.html yet.
   */
  public static void parseSummary(Vector all_flats)
  {
    /* If we already have any run description, exit: */
    for (int i = 0; i < all_flats.size(); i++)
    {
      FlatFile ff = (FlatFile) all_flats.elementAt(i);

      for (int j = 0; j < ff.runs.size(); j++)
      {
        Run run = (Run) ff.runs.elementAt(j);
        if (run.run_description != null)
          return;
      }
    }


    /* Parse the summary.html files in the flatfile directories one at the time: */
    for (int i = 0; i < all_flats.size(); i++)
    {
      FlatFile ff = (FlatFile) all_flats.elementAt(i);

      /* Get all "Starting RD" lines for this one summary.html: */
      String summ = new File(ff.name).getParent() + File.separator + "summary.html";
      if (!new File(summ).exists())
        throw new CompException("File does not exist: " + summ);
      //common.ptod("summ: " + summ);
      Fget fg             = new Fget(summ);
      Vector descriptions = new Vector(8, 0);
      String line         = null;

      while ((line = fg.get()) != null)
      {
        if (line.indexOf("Starting RD") != -1)
        {
          descriptions.add(line);
          //common.ptod("line: " + descriptions.size() + " " + all_flats.size() + " " + line);
        }
      }
      fg.close();


      /* We now have all these 'Starting RD' and must put them back in the runs: */
      for (int j = 0; j < ff.runs.size(); j++)
      {
        Run run = (Run) ff.runs.elementAt(j);
        String description = (String) descriptions.elementAt(j);
        StringTokenizer st = new StringTokenizer(description, "=; ");

        boolean found = false;
        while (st.hasMoreTokens())
        {
          String token = st.nextToken();
          if (token.equals("RD"))
          {
            found = true;

            /* Remove html info if needed: */
            String name = st.nextToken();
            if (name.indexOf("</b>") != -1)
              name = name.substring(0, name.indexOf("</b>"));

            if (!name.equals(run.rd_name))
              throw new CompException("Unmatching RD names in file " + summ +
                                  ": " + run.rd_name + "/" + name);

            run.run_description = description;
            break;
          }
        }

        if (!found)
          throw new CompException("No proper RD name found in " + description);

      }
    }
  }

  public static void main(String[] args)
  {
    //Vector all_runs = new Vector(8, 0);
    //String fname = "C:\\wlcomp\\dir1\\flatfile.html";
    //parseFile(fname, all_runs);
  }
}

class FlatFile
{
  String name;
  String base_dir;
  Vector runs = new Vector(8, 0);

  public FlatFile(String name, String base_dir)
  {
    this.name     = name;
    this.base_dir = base_dir;
  }
}
