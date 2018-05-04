package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.Arrays;

import Utils.ClassPath;

/**
 * Miscellaneous parameters.
 * Thse all need to be first in the parameter files.
 */
public class MiscParms
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public static boolean  maintain_run_totals = false;
  public static String[] unix2windows        = null;
  public static int      formatxfersize      = 0;
  public static boolean  create_anchors      = false;
  public static boolean  format_sds          = false;
  public static boolean  do_not_format_sds   = false;

  public static String[] aux_parms           = null;

  private static ArrayList <String[]> miscellaneous = new ArrayList(4);

  /**
   * Read Host information.
   */
  static String readParms()
  {
    String str = Vdb_scan.parms_get();
    if (str == null)
      common.failure("Early EOF on input parameters");

    try
    {
      while (true)
      {
        Vdb_scan prm = Vdb_scan.parms_split(str);

        if (prm.keyword.equals("rg")  ||
            prm.keyword.equals("sd")  ||
            prm.keyword.equals("fsd") ||
            prm.keyword.equals("wd")  ||
            prm.keyword.equals("fwd") ||
            prm.keyword.equals("rd")  ||
            prm.keyword.equals("hd")  ||
            prm.keyword.equals("host"))
          break;

        if ("compression".equals(prm.keyword))
        {
          double ratio = 100. / prm.getDouble();
          common.ptod("*");
          common.ptod("********************************************************************************");
          common.ptod("* compression= parameter replaced by compratio=. This no longer is a percentage.");
          common.ptod("* compression= has been converted to 'compratio=%.2f'", ratio                    );
          common.ptod("********************************************************************************");
          common.ptod("*");

          Patterns.setDefaultCompressionRatio(ratio);
          Validate.setCompressionRatio(ratio);
        }

        if ("compratio".startsWith(prm.keyword))
        {
          Patterns.setDefaultCompressionRatio(prm.numerics[0]);
          Validate.setCompressionRatio(prm.numerics[0]);
        }

        else if ("compressionseed".startsWith(prm.keyword))
        {
          if (prm.getNumCount() > 0)
            DV_map.compression_seed = (long) prm.numerics[0];
          else
          {
            if (prm.alphas[0].equals("tod"))
              DV_map.compression_seed = System.currentTimeMillis();
            else
              common.failure("Invalid 'compressionseed=' parameter: " + prm.alphas[0]);
          }
        }

        else if (prm.keyword.startsWith("dedup"))
          Dedup.dedup_default.parseDedupParms(prm, true);

        else if ("port".startsWith(prm.keyword))
          SlaveSocket.setMasterPort((int) prm.numerics[0]);

        else if ("pattern".startsWith(prm.keyword))
          Patterns.parsePattern(prm.alphas);

        else if ("data_errors".startsWith(prm.keyword) || "dataerrors".startsWith(prm.keyword))
          Validate.parseDataErrors(prm);

        else if ("swat".startsWith(prm.keyword))
        {
          common.failure("The 'swat=' parameter is no longer supported");
          if (prm.getAlphaCount() == 1)
            new SwatCharts(prm.alphas[0], ClassPath.classPath("swatcharts.txt"));
          else
            new SwatCharts(prm.alphas[0], prm.alphas[1]);
        }

        else if ("validate".startsWith(prm.keyword))
          Validate.parseValidateOptions(prm);

        else if (prm.keyword.equals("journal"))
          Validate.parseJournalOptions(prm);

        else if (prm.keyword.equals("startcmd") || prm.keyword.equals("start_cmd"))
          Debug_cmds.starting_command.storeCommands(prm.alphas);

        else if (prm.keyword.equals("endcmd") || prm.keyword.equals("end_cmd"))
          Debug_cmds.ending_command.storeCommands(prm.alphas);

        else if (prm.keyword.equals("heartbeat_error"))
          HeartBeat.heartbeat_error = new Debug_cmds().storeCommands(prm.alphas);

        else if (prm.keyword.equals("debug"))
        {
          if (prm.getNumCount() == 0)
            common.failure("'debug=' requires a numeric value");
          common.set_debug((int) prm.numerics[0]);
        }

        else if (prm.keyword.equals("parm="))   // already used in vdbench main
          continue;                             // This appears obsolete

        // This may not work any longer
        else if ("unix2windows".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() != 2)
            common.failure("'unix2windows=' requires two subparameters, e.g. 'unix2windows=(/mnt,c:\\)");
          unix2windows = prm.alphas;
        }

        else if ("formatxfersize".startsWith(prm.keyword))
          formatxfersize = (int) prm.numerics[0];

        else if ("create_anchors".startsWith(prm.keyword))
          create_anchors = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("formatsds".startsWith(prm.keyword))
        {
          if (prm.alphas[0].toLowerCase().startsWith("y"))
            format_sds = true;
          else if (prm.alphas[0].toLowerCase().startsWith("n"))
            do_not_format_sds = true;
          else
            common.failure("Invalid contents for formatsds=" + prm.alphas[0]);
        }

        else if ("report".startsWith(prm.keyword))
          Report.parseParameters(prm.alphas);

        else if ("report_run_totals".startsWith(prm.keyword))
          maintain_run_totals = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("histogram".startsWith(prm.keyword))
        {
          if (prm.alpha_count == 2)
            BucketRanges.setOption(prm.alphas[0], prm.alphas[1]);
          else
            new BucketRanges(prm.alphas);
        }

        else if ("psrset".startsWith(prm.keyword))
        {
          if (!common.onSolaris())
            common.failure("'psrset=' option is only supported on Solaris");
          Validate.setPsrset(prm.getIntArray());
        }

        else if ("auxreport".startsWith(prm.keyword))
          aux_parms = prm.alphas;

        else if ("monitor".startsWith(prm.keyword))
          Reporter.monitor_file = prm.alphas[0];

        else if ("concatenatesds".startsWith(prm.keyword))
        {
          if (prm.alphas[0].toLowerCase().startsWith("y"))
            Validate.setSdConcatenation();
          else if (!prm.alphas[0].toLowerCase().startsWith("n"))
            common.failure("Invalid contents for concatenatesds=%s; only value "+
                           "allowed is 'yes' or 'no'", prm.alphas[0]);
        }

        else if ("messagescan".startsWith(prm.keyword))
          addMiscKeyParameters("messagescan", prm.alphas);

        else if ("abort_failed_skew".startsWith(prm.keyword))
          Validate.setSkewAbort(prm.numerics[0]);

        else if ("misc".startsWith(prm.keyword))
          addMiscParameters(prm.alphas);

        else if (prm.keyword.equals("pattern_buffer"))
          Validate.setPatternMB((int) prm.numerics[0]);

        else if (prm.keyword.equals("showlba"))
          Validate.setShowLba(prm.alphas[0].toLowerCase().startsWith("y"));

        else if (prm.keyword.equals("loop"))
          scanLoopParameter(prm);

        else
          common.failure("Unknown keyword: " + prm.keyword);

        str = Vdb_scan.parms_get();
      }
    }

    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }

    if (Dedup.dedup_default != null)
      Dedup.dedup_default.checkDedupParms();

    Patterns.checkPattern();

    if (common.get_debug(common.SIMULATE))
      Vdbmain.simulate = true;

    if (Validate.sdConcatenation() && format_sds)
      common.failure("The 'formatsds=' and 'concatenatesds' parameters are mutually exclusive.");

    if (Validate.sdConcatenation() && Validate.isValidate())
      common.failure("Data Validation and SD concatenation are mutually exclusive.");

    //if (Validate.sdConcatenation() && Dedup.isDedup())
    //  common.failure("Dedup and SD concatenation are mutually exclusive.");

    return str;
  }


  /**
   * Miscellaneous: any String array passed on by using the 'misc=(xx,yyy,...)'
   * parameter.
   * This has been created to have some 'free format' way to pass information to
   * slaves, e.g. 'replay_filter'-type stuff.
   *
   * Things just got too complicated passing individual info from the Master to
   * the Slaves.
   */
  public static ArrayList <String[]> getMiscellaneous()
  {
    return miscellaneous;
  }
  public static void setMiscellaneous(ArrayList misc)
  {
    miscellaneous = misc;
    //common.where(8);
    //common.ptod("miscellaneous: " + miscellaneous.size());
  }

  /**
   * These three methods store and retrieve info, but optionally prefixed by a
   * keyword, e.g. "messagescan"
   *
  *  If the first parameter (the key) already exists, add the remainder to the
  *  old array.
   */
  public static void addMiscParameters(String[] parms)
  {
    /* First look for a previous set with the keyword: */
    String keyword = parms[0];
    for (int search = 0; search < miscellaneous.size(); search++)
    {
      /* if we find a match, just add the new parameters: */
      String[] old = miscellaneous.get(search);
      if (old[0].equalsIgnoreCase(keyword))
      {
        ArrayList <String> list = new ArrayList(old.length * 2);
        for (String oparm : old)
          list.add(oparm);
        for (int i = 1; i < parms.length; i++)
          list.add(parms[i]);

        //common.where();
        //for (String el : list)
        //  common.ptod("el: " + el);

        String[] new_parms = list.toArray(new String[0]);
        miscellaneous.set(search, new_parms);
        return;
      }
    }

    /* If this is a new keyword, just add all the parms: */
    miscellaneous.add(parms);
    //for (String parm : parms)
    //  common.ptod("Added MISC parameter: " + parm);
  }

  private static void addMiscKeyParameters(String keyword, String[] parms)
  {
    for (String[] prev : miscellaneous)
    {
      if (prev[0].equals(keyword))
        common.failure("MiscParms.addMiscKeyParameters: duplicate keyword not allowed: '%s'", keyword);
    }

    ArrayList <String> list = new ArrayList(8);
    list.add(keyword);
    for (String parm : parms)
      list.add(parm);

    addMiscParameters(list.toArray(new String[0]));
  }


  /**
   * Return an array of parameters whose first value starts with the key.
   */
  public static String[] getKeyParameters(String key)
  {
    for (String[] parms : miscellaneous)
    {
      if (parms[0].equalsIgnoreCase(key))
        return parms;
    }
    return null;
  }

  /**
   * Return an array of parameters whose first TWO values starts with the key.
   */
  public static String[] getKeyParameters(String key1, String key2)
  {
    for (String[] parms : miscellaneous)
    {
      if (parms.length > 1)
      {
        if (parms[0].equalsIgnoreCase(key1) && parms[1].equalsIgnoreCase(key2))
          return parms;
      }
    }
    return null;
  }

  /**
   *
   * loop=nn parameter.
   * loop=10  (times)
   * loop=10s
   * loop=10m
   * loop=10h
   *
   * ('loop=' has already been stripped off)
  */
  private static void scanLoopParameter(Vdb_scan prm)
  {
    String parm = prm.raw_values.get(0);

    Vdbmain.loop_all_runs = true;

    String number = parm;

    int multiplier = 1;
    if (number.endsWith("s"))
    {
      multiplier = 1;
      number = number.substring(0, number.length() -1);
    }
    else if (number.endsWith("m"))
    {
      multiplier = 60;
      number = number.substring(0, number.length() -1);
    }
    else if (number.endsWith("h"))
    {
      multiplier = 3600;
      number = number.substring(0, number.length() -1);
    }
    else if (!common.isNumeric(number))
    {
      common.failure("Expecting no parameter or a numeric paramater after '-l': " + parm);
    }
    else
      Vdbmain.loop_count = Long.parseLong(number);

    if (Vdbmain.loop_count == Long.MAX_VALUE)
    {
      Vdbmain.loop_duration = Long.parseLong(number) * multiplier;
      Vdbmain.loop_duration *= 1000;
    }
  }

  public static boolean shutDownAfterLoops(long first_start_tod)
  {
    if (!Vdbmain.loop_all_runs)
      return true;

    Vdbmain.loops_done++;
    if (Vdbmain.loop_count != Long.MAX_VALUE)
    {
      if (Vdbmain.loops_done >= Vdbmain.loop_count)
      {
        BoxPrint.printOne("Terminating loop run after %d loops", Vdbmain.loop_count);
        return true;
      }
      else
        return false;
    }

    if (System.currentTimeMillis() - first_start_tod > Vdbmain.loop_duration)
    {
      BoxPrint.printOne("Terminating loop run after %d seconds",
                  (System.currentTimeMillis() - first_start_tod) / 1000);
      return true;
    }

    return false;
  }

  public static void printLoopStart()
  {
    if (!Vdbmain.loop_all_runs)
      return;

    Status.printStatus("Starting loop " + (Vdbmain.loops_done+1), null);
    common.ptod("Starting loop " + (Vdbmain.loops_done+1));
  }
}


