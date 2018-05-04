package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.lang.String;
import java.lang.Double;
import java.util.*;
import java.text.NumberFormat;
import Utils.Fget;


/**
 * This class contains methods used to translate input parameters.
 */
public class Vdb_scan
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public String keyword;                      /* Keyword found in parameter */
  public String[] alphas   = new String[256]; /* Array of alpha parameters  */
                                              /* (length is correct)        */
  public double[] numerics = new double[512]; /* Array of numeric parms     */
  /* (length is correct)        */
  public int    alpha_count;                  /* Number of alpha parms      */
  public int    num_count;                    /* number of numeric parms    */

  static BufferedReader br;
  static String  line = null;
  static StringTokenizer st;
  static PrintWriter copy_file = null;

  static String list_of_parameters[] = new String[32768];
  static int    parms_read = 0;
  static int    parms_used = 0;

  private static void parm_error(String msg)
  {
    common.ptod("");
    common.ptod("Parameterscan error. See also output file 'parmscan.html' for details.");
    common.ptod(msg, Vdbmain.parms_report);
    Vdbmain.usage();
    common.failure(msg);
  }

  /**
   * Read parameter files to 'parms' table.
   * When there are leftover arguments from the execution parameters we will use
   * them in stead.
   */
  static void Vdb_scan_read(Vector parm_files, boolean direct)
  {

    for (int i = 0; i < parm_files.size(); i++)
    {
      String fname = (String) parm_files.elementAt(i);
      //common.ptod("Reading parameter file: " + fname);
      File parmfile = new File(fname);
      if (!parmfile.exists())
        parm_error("Requested parameter file does not exist: " + fname);



      /* Open the file for reading: */
      if (direct)
        copy_file.println("\n* Contents of parameter file: " + parmfile.getAbsolutePath() + "\n");
      else
        copy_file.println("\n* WD parameter read from file: " + parmfile.getAbsolutePath() + "\n");

      try
      {
        Fget fg = new Fget(parmfile);
        String line;
        while ((line = fg.get()) != null)
        {
          copy_file.println(line.replace('@',' '));

          line = line.trim();

          if (line.startsWith("eof"))
            break;

          if (line.startsWith("/")   ||
              line.startsWith("#")   ||
              line.startsWith("*")   ||
              line.length() == 0)
            continue;

          if (line.startsWith("include="))
          {
            readIncludeFile(line, fname);
            continue;
          }
          else
            list_of_parameters[parms_read++] = line;

        }
        fg.close();
      }

      catch (Exception e)
      {
        Vdbmain.usage();
        common.failure(e);
      }
    }
  }


  /**
   * Process an 'include=' parameter, adding whatever we find in that file.
   */
  private static void readIncludeFile(String include_line, String previous)
  {
    try
    {
      String[] split = include_line.split("=");
      if (split.length != 2)
        common.failure("'include=' parameter mustChange have only one subparameter: " + include_line);

      String include     = split[1];

      /* If the included file name does not contain a file separator, prefix */
      /* it with the parent directory of the --current-- paremeter file:     */
      String prev_parent = new File(previous).getParent();
      if (include.indexOf(File.separator) == -1)
        include = prev_parent + File.separator + include;

      include = new File(include).getAbsolutePath();

      if (!new File(include).exists())
        common.failure("include= file does not exist: " + include);

      copy_file.println("\n* Contents of include file: " + include + "\n");

      Fget fg = new Fget(include);
      String line;
      while ((line = fg.get()) != null)
      {
        copy_file.println(line.replace('@',' '));

        line = line.trim();

        if (line.startsWith("eof"))
          break;

        if (line.startsWith("include="))
          common.failure("Using the 'include=' parameter recusrively is not allowed: " + line);

        if (line.startsWith("/")   ||
            line.startsWith("#")   ||
            line.startsWith("*")   ||
            line.length() == 0)
          continue;

        list_of_parameters[parms_read++] = line;
      }
      fg.close();

      copy_file.println("\n* Continuing with original file: " + previous + "\n");
    }

    catch (Exception e)
    {
      Vdbmain.usage();
      common.failure(e);
    }
  }

  /**
   * Parameters entered from command line: make usable for scan routines:
   */
  static void xlate_command_line(String args[], int index)
  {
    /* Each remaining parameter will be treated as if it had come */
    /* from 'parmfile':                                           */
    common.ptod("Command line 'parmfile' input. All remaining command line " +
                "parameters will be treated as if coming from '-fparmfile'");

    parms_read = 0;
    for (index++; index < args.length; index++)
    {

      /* Replace 0x0a with blanks, then trim, then replace blanks with ?*/
      String line = args[index];
      line = line.replace('\n', ' ');
      line = line.trim();
      line = line.replace(' ', '?');

      /* Replace all consecutive blanks with one comma: */
      char carr[] = line.toCharArray();
      for (int i = 0; i < carr.length; i++)
      {

        if ((int) carr[i] < 0x20)
          carr[i] = '?';

        if (carr[i] == '?')
        {
          carr[i] = ',';
          for (int j = i+1; j < carr.length ; j++, i++)
          {
            if (carr[j] != '?')
              break;
          }
        }
      }

      list_of_parameters[parms_read++] = line.valueOf(carr);
      if (copy_file != null)
        copy_file.println(list_of_parameters[parms_read-1]);

    }
  }


  static boolean sd_found = false;
  static boolean wd_found = false;
  static boolean rd_found = false;
  static boolean first    = true;


  /**
   * Method to translate a parameter file that has one line per parameter
   * to something that is more readable.
   */
  public static void parms_print(String str)
  {
    if (false)
    {
      if (str == null)
      {
        copy_file.println();
        return;
      }

      if (first)
      {
        copy_file.println();
        copy_file.println("*Contents of complete parameterfile translated from a");
        copy_file.println("*possible 'one parameter per line' format to a more readable format:");
        copy_file.println();
        first = false;
      }

      str = str.replace('@', ' ');
      str = str.replace('^', ',');
      str = str.replace('{', '=');
      str = str.replace('}', '-');

      /* if (str.indexOf(" ") != -1 ||
          str.indexOf(",") != -1 ||
          str.indexOf("=") != -1 ||
          str.indexOf("-") != -1)
        str = "\"" + str + "\""; */

      if (!sd_found)
      {
        if (str.toLowerCase().startsWith("sd"))
          sd_found = true;
        else
        {
          copy_file.println(str);
          return;
        }
      }

      if (!wd_found)
      {
        if (str.toLowerCase().startsWith("sd"))
        {
          copy_file.println();
          copy_file.print(str);
          return;
        }

        if (str.toLowerCase().startsWith("wd"))
          wd_found = true;
      }

      if (!rd_found)
      {
        if (str.toLowerCase().startsWith("wd"))
        {
          copy_file.println();
          copy_file.print(str);
          return;
        }

        if (str.toLowerCase().startsWith("rd"))
        {
          rd_found = true;
          copy_file.println();
          copy_file.print(str);
          return;
        }
      }

      if (str.toLowerCase().startsWith("rd"))
      {
        copy_file.println();
        copy_file.print(str);
      }
      else
        copy_file.print("," + str);
    }
  }


  /**
   * Return the length of the parameter arrays.
   *
   * Note: though in parms_array_resize() the length of the arrays is set
   * to the proper size, there still is some dependency on the num_count and
   * alpha_count fields. I therefore can't return the numerics.length.
   * Have not taken the time to figure out why.
   */
  public int getNumCount()
  {
    return num_count;
  }
  public int getAlphaCount()
  {
    return alpha_count;
  }

  /**
   * Read one parameter from the input parameter file.
   * <pre>
   * If a new line starts with 'eof' processing stops.
   * Lines starting with '#' or '*' in column one are comment lines.
   * A line starting with 'eof' forces end-of-file.
   *
   * Parameters in the input have a required syntax of:
   *      keyword=value
   * or   keyword=[value1,value2,...value-n]
   *
   * The value can be ascii or numeric. If multiple values are specified, all
   * have to be of the same type. Numeric values will be stored as floating
   * point variables. Numeric values ending with 'k', 'm', 'g' will be
   * multiplied with the correspoinding amount of 1024's or 1000's.
   *
   * Allow 6m to be interpreted as 6 minutes.
   */
  public static String parms_get()
  {
    int i;
    String work;
    String work1;
    String value;
    String token;
    boolean paren;

    /* Get a line from the input if needed: */
    if (line == null)
    {
      if (parms_used == parms_read)
        return null;
      line = list_of_parameters[parms_used++];

      common.ptod("line: " + line, Vdbmain.parms_report);
      ////copy_file.println(line.replace('@',' '));

      /* For quotes we must temporarily replace blanks: */
      line = specialize(line);
      //common.ptod("linea: " + line);

      /* If parameter ends with a comma, concatenate the next line: */
      while (line.endsWith(","))
      {
        if (parms_used == parms_read)
          break;
        String nline = list_of_parameters[parms_used++];
        line += specialize(nline);
        //common.ptod("lineb: " + line);
      }

      st = new StringTokenizer(line, ",= ()", true);
    }



    /* Start with a clean slate: */
    value = "";
    paren = false;
    while (st.hasMoreTokens())
    {
      token = st.nextToken();
      if (token == null)
      {
        line = null;
        return value;
      }

      if (token.equals("("))
      {
        paren = true;
        value = value + token;
        continue;
      }
      if (token.equals(")") && paren)
      {
        paren = false;
        value = value + token;
        continue;
      }
      if (token.equals(")") && !paren)
      {
        parm_error("Unmatched Parenthesis ");
      }
      if (token.equals(",") && paren)
      {
        value = value + token;
        continue;
      }
      if (token.equals(",") )
      {
        if (!st.hasMoreTokens())
          line = null;
        return value;
      }
      if (token.equals(" ") )
      {
        line = null;
        return value;
      }
      if (token.equals("?") )
        continue;

      value = value + token;
    }
    line = null;
    return value;
  }

  /**
   * Code needed for the InsertHost() code that repeats each parameter that
   * contains $host or #host.
   */
  public static String completedHostRepeat(String[] new_lines)
  {
    /* Replace the array containing all the parameters: */
    list_of_parameters = new_lines;

    /* Force parameters scanning to start again with the line that it */
    /* had started before InsertHosts was called:                     */
    line = null;
    parms_used--;

    /* Pick up the new amount of parameter lies that we now have: */
    parms_read = list_of_parameters.length;

    /* Print the parameters as seen AFTER the change: */
    copy_file.println(" ");
    copy_file.println("*");
    copy_file.println("* Parameters after $host and #host values were replaced:");
    copy_file.println("*");
    copy_file.println(" ");
    for (int i = 0; i < new_lines.length; i++)
      copy_file.println(new_lines[i]);

    /* Now read the previously started and possibly changed parameter line: */
    return parms_get();
  }

  /**
   * Hide (for now) things like quotes and embedded blanks.
   */
  private static String specialize(String nline)
  {

    /* For quotes we must temporarily replace blanks: */
    for (int i = 0; i < nline.length(); i++)
    {
      /* Look for starting quote: */
      if (nline.charAt(i) == '"')
      {
        /* Remove starting quote: */
        nline = nline.substring(0, i) + nline.substring(i+1);

        /* Look for ending quote: */
        for (int j = i+1; j < nline.length(); j++)
        {
          if (nline.charAt(j) == '"')
          {
            /* Remove ending quote: */
            nline = nline.substring(0, j) + nline.substring(j+1);
            break;
          }

          /* Replace blank with '@': */
          if (nline.charAt(j) == ' ')
            nline = nline.substring(0, j) + "@" + nline.substring(j+1);

          /* Replace comma with '^': */
          if (nline.charAt(j) == ',')
            nline = nline.substring(0, j) + "^" + nline.substring(j+1);

          /* Replace '=' with '{': */
          if (nline.charAt(j) == '=')
            nline = nline.substring(0, j) + "{" + nline.substring(j+1);

          /* Replace '-' with '}': */
          if (nline.charAt(j) == '-')
            nline = nline.substring(0, j) + "}" + nline.substring(j+1);
        }
      }
    }

    /* Terminate line after first blank: */
    if (nline.indexOf(" ") != -1)
      nline = nline.substring(0, nline.indexOf(" "));

    return nline;
  }

  public long getLong()
  {
    return (long) numerics[0];
  }
  public int getInt()
  {
    return (int) numerics[0];
  }
  public double getDouble()
  {
    return numerics[0];
  }

  /**
   * Check the input parameter and determine whether it is ascii or numeric.
   * Parameters will be stored as such. k/m/g translations are done.
   */
  public void parms_alfa_or_num(String rest, int index)
  {

    Double dbl;

    /* Remove '@' if needed (was used to allow blanks within quotes): */
    rest = rest.replace('@', ' ');

    /* Remove '^' if needed (was used to allow commas within quotes): */
    rest = rest.replace('^', ',');

    /* Remove '{' if needed (was used to allow '=' within quotes): */
    rest = rest.replace('{', '=');

    /* Remove '}' if needed (was used to allow '-' within quotes): */
    rest = rest.replace('}', '-');

    common.ptod("parm: " + rest, Vdbmain.parms_report);

    double kilo, mega, giga, terra;

    /* Default binary: */
    kilo = 1024; mega = kilo * kilo; giga = mega * kilo; terra = giga * kilo;

    /* Handle requests for minutes: */
    if (keyword.startsWith("el"))   // elapsed
      mega = 60;
    else if (keyword.startsWith("in"))   // interval
      mega = 60;
    else if (keyword.startsWith("pa"))   // pause
      mega = 60;
    else if (keyword.startsWith("wa"))   // pause
      mega = 60;
    else if (keyword.startsWith("reset"))   // ????
      mega = 60;

    try
    {
      dbl = Double.valueOf(rest);
      numerics[index] = dbl.doubleValue();
      num_count++;
    }

    catch (NumberFormatException e)
    {
      String tail, front;
      double kmg;

      /* If last character is a 'k/m/g/%', try again with numeric: */
      /* (Allow for kb, mb, gb):                                   */
      tail = rest.substring(rest.length() - 1).toLowerCase();
      if (tail.equals("b") && rest.length() > 2)
      {
        tail  = rest.substring(rest.length() - 2, rest.length() - 1).toLowerCase();
        front = rest.substring(0, rest.length() - 2).toLowerCase();
      }
      else
        front = rest.substring(0, rest.length() - 1).toLowerCase();

      if (tail.compareTo("k") == 0)
        kmg = kilo;
      else if (tail.compareTo("m") == 0)
        kmg = mega;
      else if (tail.compareTo("g") == 0)
        kmg = giga;
      else if (tail.compareTo("t") == 0)
        kmg = terra;
      else if (tail.compareTo("h") == 0)
        kmg = 3600;
      else if (tail.compareTo("%") == 0)
        kmg = -1;
      else
      {

        //if (rest.lastIndexOf("-") > 0)
        //  parm_error("No '-' allowed in aphanumeric parameter: " + rest);

        alphas[index] = rest;
        alpha_count++;

        if (alpha_count != 0 && num_count != 0)
          parm_error("Mix of alpha and numeric values not allowed: (" +
                     keyword + ") " + rest );

        return;
      }

      /* Kilo, mega, and gigabytes maybe: */
      try
      {
        dbl = Double.valueOf(front);
        numerics[index] = dbl.doubleValue() * kmg;
        num_count++;
      }

      /* It was k/m/g/%, but there was some other garbage: */
      catch (NumberFormatException f)
      {
        alphas[index] = rest;
        alpha_count++;
      }
    }

    //if (alpha_count != 0 && num_count != 0)
    //  if (alpha_count != 0 && num_count != 0)
    //    parm_error("Mix of alpha and numeric values not allowed: (" +
    //               keyword + ") " + rest );
  }

  /**
   * Resize numeric and alpha parameter arrays to its minimum size
   */
  void parms_array_resize()
  {
    double[] nums = new double[num_count];
    System.arraycopy(numerics, 0, nums, 0, num_count);
    numerics = nums;

    String[] alfas = new String[alpha_count];
    System.arraycopy(alphas, 0, alfas, 0, alpha_count);
    alphas = alfas;
  }

  /**
   * Split parameter contents from 'keyword=[xxx]'
   * Parameters are stored in Vdb_scan.keyword and array alphas[] or numerics
   */
  public static Vdb_scan parms_split(String input)
  {
    StringTokenizer tk;
    String token, rest;
    Vdb_scan prm = new Vdb_scan();
    double val;
    int index = 0;
    boolean range = false;
    double v1, v2, v3;

    //common.ptod("input: " +input);

    /* Look for keyword first: */
    common.ptod("keyw: " + input, Vdbmain.parms_report);
    tk = new StringTokenizer(input, "=");
    token = tk.nextToken();
    if (token == null)
      parm_error("Expecting 'keyword=' in parameter: " + input);
    prm.keyword = token.toLowerCase();

    /* Minimum 2 characters required: */
    if (prm.keyword.length() < 2)
      parm_error("Keyword must contain a minimum of 2 characters: " + input);

    /* There must be more data after the '=': */
    if (!tk.hasMoreTokens())
      parm_error("Expecting value after 'keyword=' in parameter: " + input);

    if ( (rest = tk.nextToken()) == null)
      parm_error("Expecting value after 'keyword=' in parameter: " + input);

    /* There may not be more data: */
    if (tk.hasMoreTokens())
      parm_error("Too many keyword parameters: " + input);

    /* If the rest of the parameters do not start with '(', just pick parameter: */
    if (!rest.startsWith("(") )
    {
      prm.parms_alfa_or_num(rest, index);
      prm.parms_array_resize();
      return prm;
    }


    /* Parenthesis, single parameters are done. Loop until ")": */
    if (prm.keyword.equalsIgnoreCase("host") || prm.keyword.equals("hd"))
      tk = new StringTokenizer(rest, ",()", true);
    else
      tk = new StringTokenizer(rest, ",()-", true);

    while (tk.hasMoreTokens())
    {
      token = tk.nextToken();

      if ( (token.compareTo("(") == 0) ||
           (token.compareTo(")") == 0) ||
           (token.compareTo(",") == 0) )
        continue;


      /* A dash means a range.: */
      if (token.compareTo("-") == 0)
      {
        if (prm.getNumCount() > 0)
        {
          prm.numerics[index++] = Var_parms.VAR_DASH_N;
          prm.num_count++;
        }
        else
        {
          prm.alphas[index++] = Var_parms.VAR_DASH_A;
          prm.alpha_count++;
        }

        range = true;
        continue;
      }

      else if (range && token.compareTo("d") == 0)
      {
        prm.parms_alfa_or_num("" + Var_parms.VAR_DOUBLE_N, index++);
        range = false;
        continue;
      }
      else
      {
        prm.parms_alfa_or_num(token, index++);
      }
    }

    Var_parms.handle_var(prm);

    prm.parms_array_resize();
    return prm;
  }


  public String toString()
  {
    String txt = "Vdb_scan:";
    txt += " keyword: " + keyword;
    txt += " alpha: " + alpha_count;
    for (int i = 0; i < alpha_count; i++)
      txt += " " + alphas[i];
    txt += " num: " + num_count;
    for (int i = 0; i < num_count; i++)
      txt += " " + numerics[i];

    return txt;
  }
}





