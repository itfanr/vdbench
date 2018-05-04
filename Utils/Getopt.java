package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import java.lang.*;

public class Getopt
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  private Vector  parm_data   = new Vector(32, 0);
  private Vector  positionals = new Vector(8, 0);
  private boolean success     = false;
  private char    last_check;
  private int     last_index;



  public Getopt(String[] args, String parse_list, int positionals_allowed)
  {
    /* Don't fiddle with 'debug on' when piping data, e.g. Atobin() */
    boolean debug = false;
    if (debug) common.ptod("new getopt()\n");

    /* Parse_list must end with a blank. Just force it: */
    parse_list += " ";

    /* Due to the constant messing with blank embedded file names on windows */
    /* whose quotes get parsed out somehwere, I created a second set of      */
    /* 'quotes', "{"  and "}". If you see them, pick up everything inserting */
    /* the blank again:                                                      */
    Vector new_args = new Vector(64, 0);
    loop:
    for (int i = 0; i < args.length; i++)
    {
      if (debug) common.ptod("args[i]: " + args[i]);

      if (args[i].startsWith("{") && args[i].endsWith("}"))
        args[i] = args[i].substring(1, args[i].length() -1);

      else if (args[i].startsWith("{"))
      {
        String newarg = args[i].substring(1) + " ";
        for (i++; i < args.length; i++)
        {
          if (!args[i].endsWith("}"))
            newarg += args[i] + " ";
          else
          {
            newarg += args[i].substring(0, args[i].length() - 1);
            //common.ptod("newarg: " + newarg);
            new_args.addElement(newarg);
            continue loop;
          }
        }
      }

      new_args.addElement(args[i]);
      if (debug) common.ptod("newargs args[i]: " + args[i]);
    }


    /* Convert argument list into a Vector: */
    for (int i = 0; i < new_args.size(); i++)
    {
      String arg = (String) new_args.elementAt(i);
      if (debug) common.ptod("xxx: " + arg);
      Parm_data pd = new Parm_data();
      pd.input = arg.trim();
      parm_data.addElement(pd);
    }

    /* Scan through the Vector looking for options */
    /* (options can be added in the middle) */
    for (int i = 0; i < parm_data.size(); i++)
    {
      Parm_data pd = (Parm_data) parm_data.elementAt(i);
      String arg = pd.input;
      if (debug) common.ptod("arg: >>" + arg + "<<");

      /* Allow for a single '-': */
      if (arg.equals("-"))
        arg = " -";

      if (!arg.startsWith("-"))
      {
        if (debug) common.ptod("positionals.size(): " + positionals.size());
        if (debug) common.ptod("positionals_allowed: " + positionals_allowed);
        if (positionals.size() == positionals_allowed)
          common.failure("Unexpected argument found in execution parameters: " + arg);
        if (arg.equals(" -"))
          arg = "-";
        positionals.addElement(arg);
        if (debug) common.ptod("added pos: " + arg);
        parm_data.remove(pd);
        i--;
        continue;
      }

      pd.opt = arg.charAt(1);
      int index = parse_list.indexOf(pd.opt);

      /* Invalid? */
      if (index == -1)
      {
        System.err.println();
        System.err.println("invalid parameter: " + pd.opt);
        System.err.println();
        return;
      }

      else if (parse_list.charAt(index + 1) != ':')
      {
        /* Options without parameters. If there are, then these are */
        /* parameters too!  (eg -xyz which is the same as -x -y -z) */
        if (arg.length() > 2)
        {
          /* Replace current option: */
          pd.input = arg.substring(0,2);

          /* Insert a new parameter in Vector (e.g. -yz) */
          Parm_data pdn = new Parm_data();
          pdn.input     = "-" + arg.substring(2);
          parm_data.insertElementAt(pdn, i+1);
          //System.err.println("added: " + pdn.input);
          pd.opt = arg.charAt(1);
        }
      }

      /* Options with parameter: */
      else
      {
        /* If there are is no parameter, get it from the next argument: */
        if (arg.length() == 2)
        {
          /* If this was the last parameter, give up: */
          //System.err.println("i: " + i);
          //System.err.println("parm: " + parm_data.size());
          if (i == parm_data.size() -1)
          {
            System.err.println("Sub parameter required for option '-" + pd.opt + "'");
            return;
          }
          Parm_data pdn = (Parm_data) parm_data.elementAt(i+1);
          pd.input += pdn.input;
          parm_data.remove(pdn);
          i--;
        }
        else
        {
          /* There is a parameter: */
          pd.parameter = arg.substring(2);
        }
      }
    }

    /* '-d' parameters are immediately processed: */
    if (check('d'))
    {
      int d;
      while ((d = (int) get_next()) != -1)
        common.set_debug(d);
    }

    /* Ok status: */
    success = true;
  }


  /**
   * Report parameters found.
   * Write to System.err so that these messages don't get piped to the next program.
   */
  public void print(String title)
  {
    for (int i = 0; i < parm_data.size(); i++)
    {
      Parm_data pd = (Parm_data) parm_data.elementAt(i);
      System.err.println(common.tod() + " " + title + " execution parameter:  '-" +
                         pd.opt +
                         ((pd.parameter == null) ? "'" : " " + pd.parameter + "'"));
    }

    for (int i = 0; i < positionals.size(); i++)
    {
      System.err.println(common.tod() + " " + title + " positional parameter: '" +
                         (String) positionals.elementAt(i) + "'");
    }

  }

  public Vector <String> get_positionals()
  {
    return positionals;
  }

  public String get_positional()
  {
    return get_positional(0);
  }
  public String get_positional(int index)
  {
    /* If we go too far, return null: */
    if ( index >= positionals.size())
      return null;

    return(String) positionals.elementAt(index);
  }

  public String get_pos_string(int index)
  {
    if (positionals.size() <= index)
      common.failure("Not enough positional execution parameters, looking for parameter #" + index);

    return(String) positionals.elementAt(index);
  }

  public long get_pos_long(int index)
  {
    if (positionals.size() <= index)
      common.failure("Not enough positional execution parameters, looking for parameters #" + index);

    String pos = (String) positionals.elementAt(index);
    return Long.parseLong(pos);
  }


  /**
   * Check existence of requested parameter.
   * If true, next get_string() will return value for this parameter..
   */
  public boolean check(char opt)
  {
    last_check = opt;
    for (last_index = 0; last_index < parm_data.size(); last_index++)
    {
      Parm_data pd = (Parm_data) parm_data.elementAt(last_index);
      if (pd.opt == opt)
        return true;
    }
    return false;
  }


  /**
   * Return value referenced with last check()
   */
  public String get_string()
  {
    Parm_data pd = (Parm_data) parm_data.elementAt(last_index);
    if (pd.opt == last_check)
      return pd.parameter;

    common.failure("No parameter value present for -" + last_check);
    return null;
  }

  /**
   * Return specificaly requested parameter value.
   */
  public String get_string(char opt)
  {
    if (!check(opt))
      common.failure("getopt.get_string(): non-existing parameter: -" + opt);

    return get_string();
  }


  public double get_double()
  {
    Parm_data pd = (Parm_data) parm_data.elementAt(last_index);
    if (pd.parameter == null)
      common.failure("No input parameter needed for -" + last_check);
    try
    {
      return Double.parseDouble(pd.parameter);
    }
    catch (NumberFormatException e)
    {
      common.failure("Parameter value for -" + last_check + " must be numeric");
    }

    common.failure("No parameter value present for -" + last_check);
    return 0;
  }

  public int get_int()
  {
    return (int) get_long();
  }
  public long get_long()
  {
    //System.err.println("last_index: " + last_index);
    Parm_data pd = (Parm_data) parm_data.elementAt(last_index);
    if (pd.parameter == null)
      common.failure("No input parameter needed for -" + last_check);
    try
    {
      //System.err.println(last_check + " returning: " + Long.parseLong(pd.parameter));
      return Long.parseLong(pd.parameter);
    }
    catch (NumberFormatException e)
    {
      common.failure("Parameter value for -" + last_check + " must be numeric");
    }

    common.failure("No parameter value present for -" + last_check);
    return 0;
  }


  public long get_next()
  {
    /* If we are already done, just return: */
    if (last_index >= parm_data.size())
      return -1;

    Parm_data pd = (Parm_data) parm_data.elementAt(last_index);
    if (pd.parameter == null)
      common.failure("No input parameter needed for -" + last_check);
    try
    {
      /* Look for next parameter : */
      for (last_index++; last_index < parm_data.size(); last_index++)
      {
        Parm_data pdn = (Parm_data) parm_data.elementAt(last_index);
        if (pdn.opt == last_check)
          break;
      }
      return Long.parseLong(pd.parameter);
    }
    catch (NumberFormatException e)
    {
      common.failure("Parameter value for -" + last_check + " must be numeric");
    }

    common.failure("No parameter value present for -" + last_check);
    return 0;
  }


  /**
   * Method to accept information like 128k, 110g, etc
   * getopt.check('x') has to be called first
   */
  public  long extractLong()
  {
    String arg = get_string();
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
      else if (arg.endsWith("p"))
        value = Long.parseLong(arg.substring(0, arg.length() - 1)) * 1024 * 1024 * 1024 * 1024* 1024l;
      else
        value = Long.parseLong(arg);

      return value;
    }
    catch (Exception e)
    {
      Vdb.common.ptod("Unable to extract numeric value from '%s'", arg);
      common.failure(e);
      return 0;
    }
  }

  public int extractInt()
  {
    long value = extractLong();
    if (value > Integer.MAX_VALUE)
      common.failure("Expecting integer value, receiving something larger: " + value);
    return(int) value;
  }


  public static void main(String[] args)
  {
    long number = 0;
    String parse_list = "l: o: r s: e: v: a: d: "; // make sure it ends on blank!

    Getopt gopt = new Getopt(args, parse_list, 1);
    if (!gopt.success)
      common.failure("parameterscan failure");

    if (gopt.check('d'))
    {
      while ((number = gopt.get_next()) != -1)
        System.err.println("ddd: " + number);
    }
    gopt.print("main");

    if (gopt.check('a')) System.err.println("number: " + gopt.get_long());
    if (gopt.check('l')) System.err.println("number: " + gopt.get_long());

    Vector nod = gopt.get_positionals();
    for (int i = 0; i < nod.size(); i++)
      System.err.println("names: " + (String) nod.elementAt(i));
  }

  public boolean isOK()
  {
    return success;
  }
}

class Parm_data
{
  char   opt       = 0;
  String parameter = null;
  String input     = null;
}

