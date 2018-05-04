package VdbComp;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
//import Utils.common;
import Utils.Fget;
import Vdb.common;

/**
 * Contains data for each run
 */
public class Run
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  String  rd_name;
  String  flatfile_name;
  String  base_dir;
  HashMap flatfile_data;
  String  run_description;
  HashMap forxx_values;

  public Run(String fn, String base, HashMap map, String rd)
  {
    flatfile_name   = fn;
    flatfile_data   = map;
    run_description = rd;
    base_dir        = base;

    /* Pick up the run name: */
    rd_name = (String) map.get("Run");
    if (rd_name == null)
      new CompException("No proper 'Run' column found in file " + fn);
  }


  /**
   * Parse for 'forxx' values, returning a HashMap with each found value
   */
  public void parseForxxValues(HashMap all_keywords)
  {
    forxx_values = new HashMap();
    if (run_description == null)
      return;

    if (run_description.indexOf("For loops: None") != -1)
      return;

    if (run_description.indexOf("For loops:") == -1)
      return;
      //throw new CompException("Unable to find proper 'loops:' value: \n" + run_description);

    /* Skip until 'loops'.                                            */
    StringTokenizer st = new StringTokenizer(run_description, " <");
    while (!st.nextToken().contains("loops:"));

    /* Starting Vdbench 5.00 there are TWO occurences of 'for loops:' */
    //if (run_description.startsWith("<"))
    //  while (st.hasMoreTokens() && !st.nextToken().contains("loops:"));

    /* We now have the 'forxxx=nnnn' pairs: */
    while (st.hasMoreTokens())
    {
      String pair = st.nextToken();
      if (pair.startsWith("/b"))
        break;

      if (pair.indexOf("=") == -1)
        throw new CompException("No '=' value inside of forxx pair: \n" + run_description);
      String keyword = pair.substring(0, pair.indexOf("="));
      String value   = pair.substring(pair.indexOf("=") + 1);
      //common.ptod("keyword: " + keyword + " " + value);

      try
      {
        double number;
        if (value.endsWith("k"))
          number = 1024 * Double.parseDouble(value.substring(0, value.length() - 1));
        else if (value.endsWith("m"))
          number = 1024 * 1024 * Double.parseDouble(value.substring(0, value.length() - 1));
        else if (value.endsWith("g"))
          number = 1024 * 1024 * 1024 * Double.parseDouble(value.substring(0, value.length() - 1));
        else if (keyword.equals("iorate") && value.equals("max"))
          number = 999988;
        else
          number = Double.parseDouble(value);

        //common.ptod("keyword: " + keyword);
        forxx_values.put(keyword, new Double(number));
        all_keywords.put(keyword, keyword);
      }
      catch (NumberFormatException e)
      {
        common.ptod(e);
        throw new CompException("Invalid numerics in forxx pair: \n" + run_description);
      }
    }
  }


  /**
   * Compare forxx values.
   * A run is only identical to an other run if all the parameters are identical.
   *
   * However, this can only be checked for forxx, not for other parameters
   * in wd and sd.
   */
  public void compareForxxValues(Run run)
  {
    if (run_description == null && run.run_description == null)
      return;

    if (run_description == null || run.run_description == null)
      throw new CompException("Mismatch in 'forxx' values for run '" +
                              rd_name + "' in files " + flatfile_name + " vs. " +
                              run.flatfile_name);


    Iterator it1 = forxx_values.entrySet().iterator();
    Iterator it2 = run.forxx_values.entrySet().iterator();
    while (it1.hasNext())
    {
      Map.Entry e1 = (Map.Entry) it1.next();
      Map.Entry e2 = (Map.Entry) it2.next();
      if (!e1.getKey().equals(e2.getKey()))
        throw new CompException("'forxx' values are not in the same order for run '" +
                                rd_name + "' in files " + flatfile_name + " vs. " +
                                run.flatfile_name);
      if (!e1.getValue().equals(e2.getValue()))
        throw new CompException("'forxx' values are not in the same values for run '" +
                                rd_name + "' in files " + flatfile_name + " vs. " +
                                run.flatfile_name);
    }
  }

  public String getSubDir()
  {
    String parent = new File(flatfile_name).getParent();

    String dir = parent.substring(base_dir.length());

    if (dir.length() == 0)
      return "./";

    else
      return dir;
  }
}

