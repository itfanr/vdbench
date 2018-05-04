package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;


/**
 * This class contains all information obtained from the FSD parameters:
 * 'File System Definition".
 */
class FsdEntry implements Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public  String    name        = null;;

  public  double[]  filesizes   = new double[] { 4096 };

  public FileAnchor anchor      = null;
  public String     dirname     = null;
  public String     jnl_dir_name = null;

  public int        width       = 1;
  public int        depth       = 1;
  public int        files       = 10;
  public String     dist        = "bottom";

  public boolean    cleanup_old = false;

  public OpenFlags  open_flags  = new OpenFlags();

  public int        fsdcount = 0;        /* How often to repeat the last FSD  */
  public int        fsdstart = 0;        /* With what to start counting       */

  public long       working_set       = 0;

  public long       total_size       = Long.MAX_VALUE;

  public boolean    shared        = false;
  public boolean    create_rw_log = false;

  public Dedup      dedup         = null;

  public static     int    max_fsd_name = 0;

  private static    Vector fsd_list = new Vector(16);
  private static    FsdEntry dflt   = new FsdEntry();


  public Object clone()
  {
    try
    {
      FsdEntry fsd   = (FsdEntry)  super.clone();
      fsd.filesizes  = (double[])  filesizes.clone();
      fsd.open_flags = (OpenFlags) open_flags.clone();
      if (dedup != null)
        fsd.dedup      = (Dedup)     dedup.clone();

      return fsd;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public static Vector <FsdEntry> getFsdList()
  {
    return fsd_list;
  }
  public static String[] getFsdNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      names.put(fsd.name, fsd);
    }

    return (String[]) names.keySet().toArray(new String[0]);
  }


  /**
   * Read File System Definition input and interpret and store parameters.
   */
  static String readParms(String first)
  {

    String str = first;
    Vdb_scan prm;
    FsdEntry fsd = null;

    try
    {


      while (true)
      {
        prm = Vdb_scan.parms_split(str);

        if (prm.keyword.equals("rd") ||
            prm.keyword.equals("wd") ||
            prm.keyword.equals("fwd") ||
            prm.keyword.equals("sd"))
          break;


        if (prm.keyword.equals("fsd"))
        {
          Vdbmain.setFwdWorkload();

          if (prm.alphas[0].equals("default")  )
            fsd = dflt;
          else
          {
            /* Don't allow duplicates: */
            for (int i = 0; i < fsd_list.size(); i++)
            {
              fsd = (FsdEntry) fsd_list.elementAt(i);
              if (fsd.name.equalsIgnoreCase(prm.alphas[0]) &&
                  fsd.fsdcount == 0)
                common.failure("Duplicate fsd name: " + fsd.name);
            }

            fsd      = (FsdEntry) dflt.clone();
            fsd.name = prm.alphas[0];
            fsd_list.addElement(fsd);

            max_fsd_name = Math.max(max_fsd_name, fsd.name.length());

            if (Vdbmain.sd_list.size() > 0)
              common.ptod("'fsd' and 'sd' parameters are mutually exclusive");

            if (Validate.isRealValidate() && fsd.name.length() > 8)
              common.failure("For Data Validation an FSD name may be only 8 " +
                             "characters or less: " + fsd.name);
          }
        }


        else if ("anchor".startsWith(prm.keyword))
          fsd.dirname = prm.alphas[0];

        else if ("journal".startsWith(prm.keyword))
          fsd.jnl_dir_name = prm.alphas[0];

        else if ("host".startsWith(prm.keyword) || prm.keyword.equals("hd"))
          common.failure("No 'host=' parameter is allowed for a File System Definition (FSD)\n" +
                         "An FSD is unique and can be used on only one host at the time.\n" +
                         "Specify the 'host=' parameter in the File system Workload Definition (FWD)\n" +
                         "to target this FSD towards a specific host.");

        else if ("width".startsWith(prm.keyword))
          fsd.width = (int) prm.numerics[0];

        else if ("depth".startsWith(prm.keyword))
          fsd.depth = (int) prm.numerics[0];

        else if ("sizes".startsWith(prm.keyword))
        {
          fsd.filesizes = prm.numerics;
          if (prm.num_count == 0)
            common.failure("No NUMERIC parameters specified for 'sizes='");
          //if (Validate.isDedup() && prm.num_count > 1)
          //  common.failure("Variable file sizes not allowed with Dedup until further notice.");
        }

        else if ("shared".startsWith(prm.keyword))
          fsd.shared = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("wss".startsWith(prm.keyword) || "workingsetsize".startsWith(prm.keyword))
        {
          fsd.working_set = (long) prm.numerics[0];
          if (prm.num_count > 1)
            fsd.working_set /= (long) prm.numerics[1];
        }


        else if ("total_size".startsWith(prm.keyword) || "totalsize".startsWith(prm.keyword))
        {
          fsd.total_size = (long) prm.numerics[0];
          if (prm.num_count > 1)
            fsd.total_size /= (long) prm.numerics[1];
          if (fsd.total_size < 0)
            common.failure("A percentage value as a 'totalsize=' parameter is NOT " +
                           "allowed for an FSD; only for an RD.");
        }

        else if ("files".startsWith(prm.keyword))
          fsd.files = (int) prm.numerics[0];

        else if ("distribution".startsWith(prm.keyword))
        {
          if (prm.alphas[0].equals("bottom"))
            fsd.dist = prm.alphas[0];
          else if (prm.alphas[0].equals("all"))
            fsd.dist = prm.alphas[0];
          else
            common.failure("FSD distribution, invalid value: " + prm.alphas[0]);
        }

        else if ("cleanup".startsWith(prm.keyword))
        {
          common.failure("'cleanup=' parameter is obsolete");
        }

        else if ("openflags".startsWith(prm.keyword))
          fsd.open_flags = new OpenFlags(prm.alphas, prm.numerics);

        else if (prm.keyword.equals("log"))
          fsd.create_rw_log = prm.alphas[0].toLowerCase().startsWith("y");

        else if ("count".startsWith(prm.keyword))
        {
          if (prm.getNumCount() != 2)
            common.failure("'count=(start,count)' parameter requires two values");
          fsd.fsdstart = (int) prm.numerics[0];
          if (prm.getNumCount() > 1)
          {
            fsd.fsdcount = (int) prm.numerics[1];
            if (fsd.fsdcount <= 0)
              common.failure("'count=(start,count)' parameter requires two "+
                             "values of which the second value must be greater than zero.");
          }
        }

        // I am reading here that if there is no default dedup it is technically
        // possible for some FSDs to ask for NO Dedup while others run WITH?

        // shortcut: no support yet for variable Dedup
        else if (prm.keyword.startsWith("dedup"))
        {
          common.failure("FSD specific dedup parameters: some time in the future");
          //fsd.dedup.parseDedupParms(prm);
        }

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

    /* If we did not see any fsd parameters we just fell through: */
    if (fsd_list.size() == 0)
      return str;

    /* Do some extra checking: */
    checkParameters();
    checkJournals();
    handleFsdCount(fsd_list);
    //checkAnchorDirectories();
    Dedup.checkFsdDedup();
    finalizeSetup();

    return str;
  }


  /**
   * Check contents of these parameters.
   */
  private static void checkParameters()
  {
    /* Directory name required: */
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      if (fsd.dirname == null)
        common.failure("Directory name required for fsd=" + fsd.name);
    }

    /* Directories may not be parents or children of each other: */
    int logs_requested = 0;
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);

      if (fsd.create_rw_log)
        logs_requested++;

      for (int j = i+1; j < fsd_list.size(); j++)
      {
        FsdEntry fsd2 = (FsdEntry) fsd_list.elementAt(j);
        if (fsd.dirname.equals(fsd2.dirname))
        {
          //common.failure("Directory (anchor) names used in fsd parameters must " +
          //               "be different: " + fsd.name + "/" + fsd2.name);
        }

        else
        {
          if ((fsd.dirname + File.separator).startsWith(fsd2.dirname  + File.separator) ||
              (fsd2.dirname + File.separator).startsWith(fsd.dirname + File.separator))
          {
            common.ptod("");
            common.ptod("fsd=" + fsd.name + ",dir=" + fsd.dirname);
            common.ptod("fsd=" + fsd2.name + ",dir=" + fsd2.dirname);
            common.failure("Directory (anchor) names used in fsd parameters may not be parents " +
                           "or children of each other: ");
          }
        }
      }
    }


    if (common.get_debug(common.CREATE_READ_WRITE_LOG) && logs_requested == 0)
      common.failure("Requesting read/write log, but no 'log=yes' found as FSD parameter");
    if (common.get_debug(common.CREATE_READ_WRITE_LOG) && !Validate.isRealValidate())
      common.failure("Requesting read/write log, but not using Data Validation");



    /* Check file size parameters: */
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);

      // Can be set by forsizes=
      if (fsd.filesizes.length == 0)
        continue;

      /* Having a value of zero in the second field means "create average": */
      if (fsd.filesizes.length == 2 && fsd.filesizes[1] == 0)
        continue ;

      if (fsd.files <= 0)
        common.failure("'files=' parameter must be greater than zero");

      /* Too many problems, like 'ls' failing with 'Not enough space', */
      /* but also problems in java LISTING those directories: */
      if (fsd.files > 10000000 && fsd.files % 1000000 != 777)
      {
        common.failure("'anchor=%s,files=%d' File count is limited to 10 million "+
                       "in a single directory.", fsd.dirname, fsd.files);
      }

      if (fsd.filesizes.length == 1)
        continue;

      if (fsd.filesizes.length % 2 != 0)
      {
        common.ptod("");
        common.ptod("fsd=" + fsd.name + ": 'filesizes=' parameter must either");
        common.ptod("contain a single value, or values defined in pairs, where");
        common.ptod("in each pair the first value contains a file size, and the");
        common.ptod("second value contains a percentage of distribution for this");
        common.ptod("file size. Percentages must add up to 100%");
        common.failure("Parameter error");
      }

      double total = 0;
      for (int j = 0; j < fsd.filesizes.length; j+=2)
      {
        /* Correct some possible fractions used, e.g. 6.6m: */
        fsd.filesizes[j] = (long) fsd.filesizes[j];
        total += fsd.filesizes[j+1];
      }

      if ((int) total != 100)
      {
        common.ptod("");
        common.ptod("fsd=" + fsd.name + ": 'filesizes=' parameter must either");
        common.ptod("contain a single value, or values defined in pairs, where");
        common.ptod("in each pair the first value contains a file size, and the");
        common.ptod("second value contains a percentage of distribution for this");
        common.ptod("file size. Percentages must add up to 100%");
        common.failure("Parameter error");
      }

    }
  }


  private static void checkJournals()
  {
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      //common.ptod("fsd.jnl_file_name: " + fsd.jnl_file_name);
      if (!Jnl_entry.isRawJournal(fsd.jnl_dir_name))
        continue;

      for (int j = i; j < fsd_list.size(); j++)
      {
        FsdEntry fsd1 = (FsdEntry) fsd_list.elementAt(j);
        if (!Jnl_entry.isRawJournal(fsd1.jnl_dir_name))
          continue;
        if (fsd1.jnl_dir_name.equals(fsd.jnl_dir_name))
          common.failure("When using a raw device for journaling every FSD needs "+
                         "his own raw journal device: journal=" + fsd.jnl_dir_name);
      }
    }
  }


  /**
   * Finalize whatever setup stuff must be done for Filesystem Workload definitions
   */
  public static void finalizeSetup()
  {
    /* Go through all FSDs (even if they are not all used by an RD */
    /* (maybe in the future we will bypass those)                  */
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);

      fsd.anchor = FileAnchor.newFileAnchor(fsd);
    }
  }


  public static void markKstatActive()
  {
    if (!common.onSolaris())
      return;

    //if (!SlaveJvm.isWdWorkload())
    //  return;

    /* First get kstat info for each FSD: */
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      fsd.anchor.getKstatForAnchor();
      if (fsd.anchor.devxlate_list == null)
      {
        common.ptod("fsd=" + fsd.name + ": Not all Kstat information available.");
        return;
      }
    }

    /* Now go ahead and activate reporting for the kstat instances: */
    //for (int i = 0; i < fsd_list.size(); i++)
    //{
    //  FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
    //  Devxlate.set_kstat_active(fsd.anchor.devxlate_list);
    //}
    //Devxlate.create_active_list();
  }


  /**
   * To facilitate playing with a (not too) large amount of different
   * fsds there is the count= parameter, which will take each FSD and
   * clone it count=(nn,mm) times.
   * FSD names and anchor names will each be suffixed with mm++, e.g.
   * fsd=sd,anchor=/dir,count=(5,1) results in fsd1-5 and file1-5
   */
  private static void handleFsdCount(Vector fsd_list)
  {
    boolean found = false;
    do
    {
      found = false;
      for (int i = 0; i < fsd_list.size(); i++)
      {
        FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
        for (int j = 0; j < fsd.fsdcount; j++)
        {
          FsdEntry fsd2 = (FsdEntry) fsd.clone();
          fsd2.name    += (j + fsd.fsdstart);
          fsd2.dirname += (j + fsd.fsdstart);

          fsd2.fsdcount = 0;
          fsd_list.add(fsd2);
          max_fsd_name = Math.max(max_fsd_name, fsd2.name.length());
          found = true;
          common.plog("'fsd=" + fsd.name +
                      ",count=(start,count)' added " + fsd2.name + " " +
                      fsd2.dirname);
        }

        if (fsd.fsdcount > 0)
          fsd_list.remove(fsd);
      }
    } while (found);


    /* Look for duplicate names now: */
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      for (int j = i+1; j < fsd_list.size(); j++)
      {
        FsdEntry fsd2 = (FsdEntry) fsd_list.elementAt(j);
        if (fsd2.name.equals(fsd.name))
          common.failure("Duplicate FSD names not allowed: " + fsd.name);
      }
    }
  }


  public static Object findFsd(String name)
  {
    for (int i = 0; i < fsd_list.size(); i++)
    {
      FsdEntry fsd = (FsdEntry) fsd_list.elementAt(i);
      if (fsd.name.equals(name))
        return fsd;
    }

    common.failure("fsd=" + name + " not found");
    return null;
  }
}


/**
 * Sort Fsds by name, allowing correct order for fsd1 and fsd12. Can use either
 * a String as input, or FsdEntry.
 */
class FsdSort implements Comparator
{
  private static int bad_sds = 0;
  public int compare(Object o1, Object o2)
  {
    String sd1;
    String sd2;

    /* We can handle both SDs and Strings: */
    if (o1 instanceof FsdEntry)
    {
      sd1 = ((FsdEntry) o1).name;
      sd2 = ((FsdEntry) o2).name;
    }
    else
    {
      sd1 = (String) o1;
      sd2 = (String) o2;
    }

    /* Get everything until we hit a numeric: */
    String char1 = getLetters(sd1);
    String char2 = getLetters(sd2);

    /* If these pieces don't match, just do alpha compare: */
    if (!char1.equalsIgnoreCase(char2))
      return sd1.compareToIgnoreCase(sd2);

    String r1 = null;
    String r2 = null;
    try
    {
      /* Get the remainder (numeric?) portion of both values: */
      r1 = sd1.substring(char1.length());
      r2 = sd2.substring(char2.length());

      /* If the results is not numberic, again do alpha compare: */
      if (!common.isNumeric(r1) || !common.isNumeric(r1))
        return sd1.compareToIgnoreCase(sd2);

      /* Just subtract these numbers and return: */
      int num1 = Integer.parseInt(r1);
      int num2 = Integer.parseInt(r2);
      return num1 - num2;
    }

    /* Any problem, report five of them: */
    catch (Exception e)
    {
      if (bad_sds++ < 5)
      {
        common.ptod("r1: char1: %s char2: %s sd1: %s sd2: %s r1: %s r2: %s",
                    char1, char2, sd1, sd2, r1, r2);
        common.ptod(e);
      }
    }

    /* Anything down here: just do an alpha compare: */
    return sd1.compareToIgnoreCase(sd2);
  }

  private String getLetters(String sd)
  {
    String char1 = "";
    for (int i = 0; i < sd.length(); i++)
    {
      char ch = sd.charAt(i);
      if (!Character.isLetter(ch))
        break;
      char1 += new Character(ch).toString();
    }

    return char1;
  }
}
