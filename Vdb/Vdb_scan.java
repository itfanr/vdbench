package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public String keyword;                      /* Keyword found in parameter */
  public String[] alphas   = new String[512]; /* Array of alpha parameters  */
                                              /* (length is correct)        */
  public double[] numerics = new double[10240]; /* Array of numeric parms     */

  public ArrayList <String> raw_values = new ArrayList(4);
  public String   raw_value = null;


  /* (length is correct)        */
  public int    alpha_count;                  /* Number of alpha parms      */
  public int    num_count;                    /* number of numeric parms    */

  static BufferedReader br;
  static String  line = null;
  static StringTokenizer st;
  static PrintWriter parm_html = null;

  static String list_of_parameters[] = new String[32768];
  static int    parms_read = 0;
  static int    parms_used = 0;

  static boolean external_parms_used = false;

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
        parm_html.println("\n* Contents of parameter file: " + parmfile.getAbsolutePath() + "\n");
      else
        parm_html.println("\n* WD parameter read from file: " + parmfile.getAbsolutePath() + "\n");

      try
      {
        Fget fg = new Fget(parmfile);
        String line;
        while ((line = fg.get()) != null)
        {
          /* Possible substitution: */
          String original = line;
          line = InsertHosts.substituteLine(original);

          /* Any line starting with '*d ' will not be suppressed with debug=99: */
          if (common.get_debug(common.IGNORE_PARM_COMMENT) && line.startsWith("*d "))
            line = line.substring(3).trim() + " /* '*d ' accepted */";

          /* If line unchanged, just print it, otherwise print both: */
          if (line.equals(original))
            parm_html.println(line.replace('[',' '));
          else
          {
            external_parms_used = true;
            parm_html.println(original.replace('[',' '));
            parm_html.println("    variable substitution: " + line.replace('[',' '));
          }

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
    external_parms_used = true;
    try
    {
      String[] split = include_line.split("=");
      if (split.length != 2)
        common.failure("'include=' parameter mustChange have only one subparameter: " + include_line);

      String include = split[1];

      /* If the included file name does not contain a file separator, prefix */
      /* it with the parent directory of the --current-- parameter file:     */
      String prev_parent = new File(previous).getParent();
      if (include.indexOf(File.separator) == -1)
        include = prev_parent + File.separator + include;
      include = new File(include).getAbsolutePath();

      /* if the file does not exist, try the CURRENT directory: */
      if (!Fget.file_exists(include))
      {
        String prev = include;
        include = new File(split[1]).getAbsolutePath();
        if (!Fget.file_exists(include))
        {
          common.ptod("Attempted to find include file '%s'", prev);
          common.ptod("Attempted to find include file '%s'", include);
          common.failure("include=%s file not found", split[1]);
        }
      }

      parm_html.println("\n* Contents of include file: " + include + "\n");

      Fget fg = new Fget(include);
      String line;
      while ((line = fg.get()) != null)
      {
        parm_html.println(line.replace('[',' '));

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

        line = InsertHosts.substituteLine(line);

        list_of_parameters[parms_read++] = line;
      }
      fg.close();

      parm_html.println("\n* Continuing with original file: " + previous + "\n");
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
    common.ptod("Entering workload parameters from the command line, e.g. ");
    common.ptod("./vdbench - sd=sd1,lun=xxxx,.....");
    common.ptod("is no longer supported. Use of a parameter file is now required");
    common.failure("Use of parameter file is now required.");
  }


  static boolean sd_found = false;
  static boolean wd_found = false;
  static boolean rd_found = false;
  static boolean first    = true;


  /**
   * Method to translate a parameter file that has one line per parameter
   * to something that is more readable.
   */
  public static void obsolete_parms_print(String str)
  {
    if (false)
    {
      if (str == null)
      {
        parm_html.println();
        return;
      }

      if (first)
      {
        parm_html.println();
        parm_html.println("*Contents of complete parameterfile translated from a");
        parm_html.println("*possible 'one parameter per line' format to a more readable format:");
        parm_html.println();
        first = false;
      }

      str = str.replace('[', ' ');
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
          parm_html.println(str);
          return;
        }
      }

      if (!wd_found)
      {
        if (str.toLowerCase().startsWith("sd"))
        {
          parm_html.println();
          parm_html.print(str);
          return;
        }

        if (str.toLowerCase().startsWith("wd"))
          wd_found = true;
      }

      if (!rd_found)
      {
        if (str.toLowerCase().startsWith("wd"))
        {
          parm_html.println();
          parm_html.print(str);
          return;
        }

        if (str.toLowerCase().startsWith("rd"))
        {
          rd_found = true;
          parm_html.println();
          parm_html.print(str);
          return;
        }
      }

      if (str.toLowerCase().startsWith("rd"))
      {
        parm_html.println();
        parm_html.print(str);
      }
      else
        parm_html.print("," + str);
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
  public void mustBeNumeric()
  {
    if (num_count == 0)
      common.failure("Numeric parameter required for '%s='.", keyword);
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

      /* Use the specified override from execution parameters: */
      if (line.trim().startsWith("rd") &&
          !line.trim().endsWith(",")   &&
          Vdbmain.rd_parm_extras.length() != 0)
      {
        /* First remove everything beyond the first blank, then add: */
        String[] split = line.trim().split(" +");
        line = split[0] + "," + Vdbmain.rd_parm_extras;
        common.ptod("Adding ',%s' to each one-line RD parameter.", Vdbmain.rd_parm_extras);
        common.ptod("Added: " + Vdbmain.rd_parm_extras, Vdbmain.parms_report);
        common.ptod("Extra: " + line,                   Vdbmain.parms_report);
      }

      /* For quotes we must temporarily replace blanks: */
      line = specialize(line);

      /* If parameter ends with a comma, concatenate the next line: */
      while (line.endsWith(","))
      {
        if (parms_used == parms_read)
          break;
        String nline = list_of_parameters[parms_used++];
        line += specialize(nline);
      }

      st = new StringTokenizer(line, ",= ()", true);
    }

    /* Start with a clean slate: */
    value = "";
    paren = false;
    int parens = 0;
    while (st.hasMoreTokens())
    {
      token = st.nextToken();
      //common.ptod("parens: " + parens + " " + token);
      if (token == null)
      {
        line = null;
        return value;
      }

      if (token.equals("("))
      {
        paren = true;
        parens++;
        value = value + token;
        continue;
      }
      if (token.equals(")") && parens > 0)
      {
        paren = false;
        parens--;
        value = value + token;
        continue;
      }
      if (token.equals(")") && parens == 0)
      {
        parm_error("Unmatched Parenthesis ");
      }
      if (token.equals(",") && parens > 0)
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
  public static String completedHostRepeat()
  {
    /* Force parameters scanning to start again with the line that it */
    /* had started before InsertHosts was called:                     */
    line = null;
    parms_used--;

    /* Pick up the new amount of parameter lines that we now have: */
    parms_read = list_of_parameters.length;

    /* Print the parameters as seen AFTER the change: */
    parm_html.println(" ");
    parm_html.println("*");
    parm_html.println("* Parameters after $host and #host values were replaced:");
    parm_html.println("*");
    parm_html.println(" ");
    for (int i = 0; i < list_of_parameters.length; i++)
      parm_html.println(list_of_parameters[i]);

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

          /* Replace blank with '[': */
          if (nline.charAt(j) == ' ')
            nline = nline.substring(0, j) + "[" + nline.substring(j+1);

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
    return(long) numerics[0];
  }
  public int getInt()
  {
    return(int) numerics[0];
  }
  public int[] getIntArray()
  {
    int[] array = new int[ numerics.length ];
    for (int i = 0; i < numerics.length; i++)
      array[i] = (int) numerics[i];
    return array;
  }
  public double getDouble()
  {
    return numerics[0];
  }
  public String getString()
  {
    return alphas[0];
  }

  /**
   * Check the input parameter and determine whether it is ascii or numeric.
   * Parameters will be stored as such. k/m/g translations are done.
   */
  public void parms_alfa_or_num(String rest, int index)
  {
    /* Remove '[' if needed (was used to allow blanks within quotes): */
    rest = rest.replace('[', ' ');

    /* Remove '^' if needed (was used to allow commas within quotes): */
    rest = rest.replace('^', ',');

    /* Remove '{' if needed (was used to allow '=' within quotes): */
    rest = rest.replace('{', '=');

    /* Remove '}' if needed (was used to allow '-' within quotes): */
    rest = rest.replace('}', '-');

    common.ptod("parm: " + rest, Vdbmain.parms_report);

    /* Ran into a case where a value started with a numeric, and ended with */
    /* a 'd', e.g. 4600d. This was seen as a numeric instead of an alpha.   */
    /* For all you know we could get a host name like hd=100g ????          */
    // Need to fix this some day. Maybe forcing any non-numeric field to
    // start with an alpha character?

    /* 'Replay' parameters are all alpha: */
    if (keyword.equals("replay") || keyword.equals("histogram") || keyword.startsWith("misc"))
    {
      not_numeric(rest, index);
      return;
    }

    if (keyword.equals("iorate") && rest.equals("max"))
      rest = "" + RD_entry.MAX_RATE;

    try
    {
      numerics[index] = Double.parseDouble(rest);
      num_count++;
    }

    catch (NumberFormatException e)
    {
      not_numeric(rest, index);
    }
  }

  private void not_numeric(String rest, int index)
  {
    String tail, front;
    double kmg = 1;

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
    else if (keyword.startsWith("wa"))   // warmup
      mega = 60;
    else if (keyword.startsWith("reset"))   // ????
      mega = 60;


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

    /* Some parameters may not want numerics to be recognized: */
    /* (hd=5220h is text, not hours :-)                        */
    if (keyword.equalsIgnoreCase("histogram") ||
        keyword.equalsIgnoreCase("host")      ||
        keyword.equalsIgnoreCase("hd")        ||
        keyword.equalsIgnoreCase("system") )
    {
      alphas[index] = rest;
      alpha_count++;
      return;
    }

    else
    {
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
          parm_error("Mix of alpha and numeric values not allowed: (" + keyword + ") " + rest );

        return;
      }
    }

    /* Kilo, mega, and gigabytes maybe: */
    try
    {
      Double dbl = Double.valueOf(front);
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


  /**
   * Resize numeric and alpha parameter arrays to its minimum size
   */
  void parms_array_resize()
  {
    String[] alfas = new String[alpha_count];
    System.arraycopy(alphas, 0, alfas, 0, alpha_count);
    alphas = alfas;

    double[] nums = new double[num_count];
    System.arraycopy(numerics, 0, nums, 0, num_count);
    numerics = nums;
  }

  /**
   * Split parameter contents from 'keyword=[xxx]'
   * Parameters are stored in Vdb_scan.keyword and array alphas[] or numerics[]
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

    if (input == null)
      common.failure("Unexpected EOF in parameter file.");

    /* Look for keyword first: */
    common.ptod("keyw: " + input, Vdbmain.parms_report);
    String[] split = input.split("=");

    if (split.length == 1)
      parm_error("Expecting 'keyword=' in parameter: " + input);

    prm.keyword = split[0].toLowerCase();

    if (prm.keyword.equals("dedup"))
    {
      prm.raw_value = input;
      return prm;
    }

    //common.ptod("keywordxxx: " + prm.keyword);
    //common.ptod("input: " + input);

    /* Minimum 2 characters required: */
    if (prm.keyword.length() < 2)
      parm_error("Keyword must contain a minimum of 2 characters: " + input);

    rest = input.substring(prm.keyword.length() + 1);


    /* If the rest of the parameters do not start with '(', just pick parameter: */
    if (!rest.startsWith("(") )
    {
      prm.raw_values.add(rest);
      prm.parms_alfa_or_num(rest, index);
      prm.parms_array_resize();
      return prm;
    }


    /* Parenthesis, single parameters are done. Loop until ")": */
    if (prm.keyword.equalsIgnoreCase("host") ||
        prm.keyword.equalsIgnoreCase("hd")   ||
        prm.keyword.startsWith("misc")   ||
        prm.keyword.equalsIgnoreCase("replay"))
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

      /* Some keywords do their own parsing.                                               */
      /* The main reason for this is that this 14-year old parser is so                    */
      /* ugly that I am stuck with not easily allowing a mix of alpha and numeric          */
      /* parameters which started causing problems for fileselect.                         */
      /* And ultimately I think it would be better anyway to slowly get rid of this parser.*/
      if (prm.keyword.equals("fileselect") || "seekpct".startsWith(prm.keyword) )
      {
        prm.raw_values.add(token);
        continue;
      }

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


  /**
   * Method to accept information like 128k, 110g, etc
   */
  public static long extractLong(String arg)
  {
    try
    {
      long value;
      if (arg.endsWith("k"))
        value = Long.parseLong(arg.substring(0, arg.length() - 1)) * 1024l;
      else if (arg.endsWith("m"))
        value = Long.parseLong(arg.substring(0, arg.length() - 1)) * 1024 * 1024l;
      else if (arg.endsWith("g"))
        value = Long.parseLong(arg.substring(0, arg.length() - 1)) * 1024 * 1024 * 1024l;
      else if (arg.endsWith("t"))
        value = Long.parseLong(arg.substring(0, arg.length() - 1)) * 1024 * 1024 * 1024* 1024l;
      else
        value = Long.parseLong(arg);

      return value;
    }
    catch (Exception e)
    {
      common.ptod("Unable to extract numeric value from '%s'", arg);
      common.failure(e);
      return 0;
    }
  }


  public static double[] extractDoubles(String arg)
  {
    /* Remove parenthesis if needed: */
    if (arg.startsWith("(") && arg.endsWith(")"))
      arg = arg.substring(1, arg.length() - 1);

    String[] split = arg.split(",+");
    double[] doubles = new double[ split.length ];
    for (int i = 0; i < split.length; i++)
      doubles[i] = extractDouble(split[i]);

    return doubles;
  }

  public static double extractDouble(String arg)
  {
    try
    {
      double value;
      if (arg.endsWith("k"))
        value = Double.parseDouble(arg.substring(0, arg.length() - 1)) * 1024l;
      else if (arg.endsWith("m"))
        value = Double.parseDouble(arg.substring(0, arg.length() - 1)) * 1024 * 1024l;
      else if (arg.endsWith("g"))
        value = Double.parseDouble(arg.substring(0, arg.length() - 1)) * 1024 * 1024 * 1024l;
      else if (arg.endsWith("t"))
        value = Double.parseDouble(arg.substring(0, arg.length() - 1)) * 1024 * 1024 * 1024* 1024l;
      else
        value = Double.parseDouble(arg);

      return value;
    }
    catch (Exception e)
    {
      common.ptod("Unable to extract double value from '%s'", arg);
      common.failure(e);
      return 0;
    }
  }

  public static int extractInt(String arg)
  {
    long value = extractLong(arg);
    if (value > Integer.MAX_VALUE)
      common.failure("Expecting integer value, receiving something larger: " + value);
    return(int) value;
  }

  /**
   * This code expects a pair of keyword=xxx parameters.
   * The first of the pair has already been interpreted by the caller, it is the
   * second one we want.
   *
   * Basically, this code does the 'pair' checking and aborts if it is not a
   * pair.
   */
  public static String extractpair(String arg)
  {
    String[] split = arg.trim().split("=+");
    if (split.length != 2)
      common.failure("Vdbench parameter scan: expecting a pair of variables" +
                     " in the form of keyword=xxx: '%s'", arg);

    return split[1];
  }

  /**
   * Better? parser:
   *
   * dedup=(flipflop=hotsets,ratio=3.5,hotsets=(2,2,10,4))
   *
   * returning:
   * flipflop=hotsets
   * ratio=3.5
   * hotsets=(2,2,10,4)
   *
   */
  public static ArrayList <String> splitRawParms(String raw_in)
  {
    boolean debug   = false;
    String  keyword = null;

    if (debug)
    {
      common.ptod("");
      common.ptod("raw_in: " + raw_in);
    }

    String raw = raw_in;
    ArrayList <String> data = new ArrayList(8);
    if (!raw.contains("="))
      common.failure("splitRawParms: expecting '=': " + raw);

    String[] split   = raw.split("=", 2);

    for (String sp : split)
      if (debug) common.ptod("split: " + sp);

    if (split.length != 2)
      common.failure("splitRawParms: invalid data: " + raw);

    /* Remove the keyword from the raw input: */
    raw = split[1];
    if (debug) common.ptod("raw: " + raw);

    /* Remove possible parenthesis at begin/end: */
    if (raw.startsWith("(") && raw.endsWith(")"))
      raw = raw.substring(1, raw.length() - 1);


    /* Split into xxxx= pieces: */
    while (raw.length() > 0)
    {
      if (debug) common.ptod("");
      if (debug) common.ptod("raw1: " + raw);
      split     = raw.split("=", 2);
      keyword   = split[0];
      raw       = split[1];
      if (debug) common.ptod("keyword: " + keyword);
      if (debug) common.ptod("raw2: " + raw);
      int comma = raw.indexOf(",");
      int paren = raw.indexOf("(");
      if (debug) common.ptod("comma: " + comma);
      if (debug) common.ptod("paren: " + paren);

      /* No parenthesis: only one parameter (left): */
      if (comma == -1 && paren == -1)
      {
        String tmp = keyword + "=" + split[1];
        if (debug) common.ptod("tmp1: " + tmp);
        data.add(tmp);
        break;
      }

      /* Starts with an open paren: just get everything till the ending paren: */
      else if (paren == 0)
      {
        if (!split[1].contains(")"))
          common.failure("splitRawParms: Missing closing paren: " + raw);
        String value = split[1].substring(0, split[1].indexOf(")"));
        if (debug) common.ptod("value: " + value);
        raw = raw.substring(raw.indexOf(value) + value.length() + 1);
        if (debug) common.ptod("raw3: " + raw);

        String tmp = keyword + "=" + value + ")";
        if (debug) common.ptod("tmp4: " + tmp);
        data.add(tmp);
      }

      /* A comma is BEFORE a parenthesis: go only until the comma: */
      else if (comma < paren)
      {
        String value = split[1].substring(0, comma);
        String tmp = keyword + "=" + value;
        if (debug) common.ptod("tmp2: " + tmp);
        data.add(tmp);
        raw = raw.substring(comma + 1);
      }

      /* The parenthesis comes first, get stuff until the closingh paren: */
      else if (paren < comma)
      {
        String value = split[1].substring(0, comma);
        String tmp = keyword + "=" + value;
        if (debug) common.ptod("tmp3: " + tmp);
        data.add(tmp);
        raw = raw.substring(comma + 1);
      }

      else
      {
        for (String dat : data)
          common.ptod("dat: " + dat);
        common.failure("splitRawParms:unknown state parsing: " + raw_in);
      }


      //common.ptod("split: " + split[0]);
      //common.ptod("split: " + split[1]);
    }

    for (String dat : data)
      if (debug) common.ptod("dat: " + dat);

    return data;

  }

  public static void main(String[] args)
  {
    double tmp = Double.parseDouble(args[0]);
    common.ptod("tmp: " + tmp);
  }
}





