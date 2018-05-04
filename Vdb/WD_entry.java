package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.*;


/**
  * This class handles Workload Definition (WD) parameters.
  */
public class WD_entry implements Cloneable, Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String   wd_name = "default";/* Name for this Workload Definition           */
  String   wd_sd_name;         /* Name of Storage Definitions used            */
  String[] sd_names = null;    /* SD names requested, will be xlated to wd_sd_name */
  String[] host_names = new String[] { "*"};

  SD_entry concat_sd = null;

  int      wd_threads      = 0;

  int      stream_count = 0;

  int      priority  = Integer.MAX_VALUE;
  double   wd_iorate = 0;
  double   skew;               /* io rate skew (curve may change this)        */
  double   skew_original = 0;  /* io rate skew this WD gets.                  */
  double   skew_observed;      /* From wg.report_wg_iocount. For curve        */
  boolean  valid_skew_obs = false;

  boolean  hotband_used = false;
  double   lowrange  = -1;     /* Starting and                                */
  double   highrange = -1;     /* Ending search range within SD               */

  double   poisson_midpoint = 0;
  double   seekpct = 100;      /* How often do we need a seek?                */
                               /* 100: pure reandom                           */
                               /*   0: pure sequential                        */
                               /*  -1: seek=eof, pure sequential              */
  long     stride_min = -1;    /* Minimum to add when generating a random lba */
  long     stride_max = -1;    /* Maximum for same.                           */

  double   rhpct = 0;          /* Read hit % obtained from WD                 */
  double   whpct = 0;          /* Write hit % obtained from WD                */

  double[] xf_table = {4096};  /* Distribution table                          */
  double   readpct = 100;      /* Read percentage for this WD                 */

  long     total_io_done;      /* Used to set skew for curve runs             */

  boolean  one_slave_warning_given = false;

  OpenFlags  open_flags = null;

  String[] user_class_parms = null;

  private static boolean any_hotbands_requested = false;

  static   WD_entry dflt = new WD_entry();
  static   int max_wd_name = 0;



  /**
   * Make sure this is a deep clone!
   */
  public Object clone()
  {
    try
    {
      WD_entry wd = (WD_entry) super.clone();

      if (sd_names   != null) wd.sd_names   = (String[]) sd_names.clone();
      if (host_names != null) wd.host_names = (String[]) host_names.clone();
      if (xf_table   != null) wd.xf_table   = (double[]) xf_table.clone();
      if (open_flags != null) wd.open_flags = (OpenFlags) open_flags.clone();

      return wd;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }


  public void setSkew(double sk)
  {
    skew = sk;
  }
  public double getSkew()
  {
    return skew;
  }


  /**
   * Check to see if a duplicate WD exists. Not allowed.
   */
  static void wd_check_duplicate(Vector wd_list, String new_wd)
  {
    WD_entry wd;
    int i;

    for (i = 0; i < wd_list.size(); i++)
    {
      wd = (WD_entry) wd_list.elementAt(i);
      if (wd.wd_name.compareTo(new_wd) == 0)
        common.failure("duplicate WD names not allowed: " + new_wd);
    }
  }



  /**
   * Read Workload Definition input and interpret and store parameters.
   */
  static String readParms(Vector wd_list, String first)
  {

    String str = first;
    Vdb_scan prm;
    WD_entry wd = null;

    try
    {
      while (true)
      {
        prm = Vdb_scan.parms_split(str);

        if (prm.keyword.equals("fwd") ||
            prm.keyword.equals("rd"))
          break;

        if (prm.keyword.equals("wd"))
        {
          if (prm.alphas[0].equals("default")  )
            wd = dflt;
          else
          {
            wd = (WD_entry) dflt.clone();
            wd.wd_name = prm.alphas[0];
            wd_check_duplicate(wd_list, wd.wd_name);
            wd_list.addElement(wd);
            max_wd_name = Math.max(max_wd_name, wd.wd_name.length());

            /* Replay now uses the Vdbench API: */
            if (ReplayInfo.isReplay())
              wd.user_class_parms = new String[]
              {
                "Vdb.ReplayGen"
              };

            if (Vdbmain.isFwdWorkload())
              common.ptod("'fwd' and 'wd' parameters are mutually exclusive");
          }
        }


        else if (prm.keyword.equals("sd"))
        {
          if (prm.alpha_count == 0)
            common.failure("No parameters defined for 'wd=%s,sd=: %s", wd.wd_name, str);
          wd.sd_names   = prm.alphas;
          wd.wd_sd_name = wd.sd_names[0]; // in case there is just one sd
        }

        else if ("skew".startsWith(prm.keyword))
          wd.skew_original = prm.numerics[0];

        else if ("xfersize".startsWith(prm.keyword))
          wd.parseXfersize(prm);

        else if ("readpct".startsWith(prm.keyword) || "rdpct".startsWith(prm.keyword))
          wd.readpct = (int) prm.numerics[0];

        else if ("writepct".startsWith(prm.keyword) || "wrpct".startsWith(prm.keyword))
          wd.readpct = 100 - (int) prm.numerics[0];

        else if ("range".startsWith(prm.keyword))
        {
          wd.lowrange = prm.numerics[0];
          if (prm.getNumCount() > 1)
            wd.highrange = prm.numerics[1];
          else
            common.failure("'range=' parameter must be specified with a "+
                           "beginning and ending range, e.g. 'range=(10,20)'");
          //common.where();
          //ConcatSds.abortIf("'range=' parameter may not be used");
        }

        else if ("hotband".startsWith(prm.keyword))
        {
          any_hotbands_requested = true;
          wd.hotband_used = true;
          wd.lowrange = prm.numerics[0];
          if (prm.getNumCount() > 1)
            wd.highrange = prm.numerics[1];
          else
            common.failure("'hotband=' parameter must be specified with a "+
                           "beginning and ending range, e.g. 'hotband=(10,20)'");
        }

        else if ("seekpct".startsWith(prm.keyword))
          wd.parseSeekpct(prm);

        else if ("stride".startsWith(prm.keyword))
          wd.parseStride(prm);

        else if ("priority".startsWith(prm.keyword))
        {
          wd.priority = prm.getInt();
          if (wd.priority == 0)
            common.failure("Workload priority may not be zero");
        }

        else if ("iorate".startsWith(prm.keyword))
        {
          wd.wd_iorate = prm.getDouble();
          if (prm.num_count > 1)
            common.failure("The 'iorate' parameter for a Workload Definition (WD) "+
                           "may contain only one value");
        }

        else if ("rhpct".startsWith(prm.keyword))
        {
          ConcatSds.abortIf("'rhpct=' parameter may not be used.");
          wd.rhpct = prm.numerics[0];
        }

        else if ("whpct".startsWith(prm.keyword))
        {
          ConcatSds.abortIf("'whpct=' parameter may not be used.");
          wd.whpct = prm.numerics[0];
        }

        else if ("host".startsWith(prm.keyword) || prm.keyword.equals("hd"))
          wd.host_names = prm.alphas;

        else if ("openflags".startsWith(prm.keyword))
          wd.open_flags = new OpenFlags(prm.alphas, prm.numerics);

        else if (prm.keyword.equals("user"))
          wd.user_class_parms = prm.alphas;

        else if ("threads".startsWith(prm.keyword))
        {
          wd.wd_threads = (int) prm.numerics[0];
          if (!Validate.sdConcatenation())
            common.failure("Workload Definition (WD) level thread= parameter may "+
                           "only be used together with 'concatenatesds=yes'");
        }

        else if ("streams".startsWith(prm.keyword))
        {
          prm.mustBeNumeric();
          wd.stream_count = (int) prm.numerics[0];
          if (wd.stream_count < 2)
            common.failure("'streams=' parameter must specific a minimum of two streams");
        }

        else
          common.failure("Unknown keyword: " + prm.keyword);

        str = Vdb_scan.parms_get();

        // this may happen if there are no RD's specified for journal recovery
        if (str == null)
          break;

        //  common.failure("Early EOF on input parameters");
      }

      checkParameters(wd_list);
    }

    catch (Exception e)
    {
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.ptod(e);
      common.failure("Exception during reading of input parameter file(s).");
    }

    return str;
  }


  /**
   * xfersize= parameter. Either a single xfersize, a distribution list with
   * xfersizes and percentages, or a three-part xfersize with min, max, align
   * coded to allow for a random xfersize between min and max with an xfersize
   * on an 'align' boundary.
   */
  private void parseXfersize(Vdb_scan prm)
  {
    double[] xf = xf_table = prm.numerics;

    if (xf.length == 3)
    {
      if (xf[0] >= xf[1])
        common.failure("xfersize=(min,max,align): 'xfersize=(%d,%d,%d)' invalid contents",
                       xf[0], xf[1], xf[2]);
      if (xf[2] == 0 || xf[2] %512 != 0)
        common.failure("xfersize=(min,max,align): 'align' must be non-zero and "+
                       "a multiple of 512: " + xf[2]);
    }

    else if (xf.length > 1)
    {
      if (xf.length % 2 != 0)
        common.failure("Xfersize distribution list must be in pairs, e.g. " +
                       "xfersize=(1k,25,2k,25,4k,50)");

      double cumpct = 0;
      for (int i = 0; i < prm.numerics.length; i++)
      {
        if (i % 2 == 1)
          cumpct += xf[i];

        if (i % 2 == 0 && xf[i] % 512 != 0)
          common.failure("data transfer size not multiple of 512 bytes: " + (int) xf[i]);
      }
      int tmp = (int) Math.round(cumpct);
      if (tmp != 100)
        common.failure("Xfersize distribution does not add up to 100: " + tmp);
    }
  }

  private void parseSeekpct(Vdb_scan prm)
  {
    for (String parm : prm.raw_values)
    {
      if ("sequential".startsWith(parm))
        seekpct = 0;

      else if (parm.equalsIgnoreCase("eof"))
        seekpct = -1;

      else if ("random".startsWith(parm))
        seekpct = 100;

      else if ("poisson".startsWith(parm))
      {
        seekpct = 100;
        poisson_midpoint = 3;
      }

      else if (common.isDouble(parm))
      {
        if (poisson_midpoint == 0)
          seekpct = Double.parseDouble(parm);
        else
          poisson_midpoint = Double.parseDouble(parm);
      }

      else
        common.failure("Invalid contents for 'seekpct=': " + parm);
    }

    //common.ptod("seekpct: " + seekpct);
    //common.ptod("poisson_midpoint: " + poisson_midpoint);
  }

  private void parseStride(Vdb_scan prm)
  {
    if (prm.getAlphaCount() > 0)
      common.failure("stride= allows only numeric values");

    if (prm.getNumCount() != 2)
      common.failure("stride= requires two parameters: 'stride=(min,max)'");

    stride_min = (long) prm.numerics[0];
    stride_max = (long) prm.numerics[1];

    if (stride_max < stride_min)
      common.failure("stride=(min,max) max must be larger than stride minimum");
  }

  /**
   * Find Host names requested by this WD
   */
  public ArrayList <Host> getSelectedHosts()
  {
    ArrayList <Host> hosts_found = new ArrayList(8);

    /* Look for all hosts requested by this WD: */
    for (int i = 0; i < host_names.length; i++)
    {
      int number_found = 0;

      /* Scan all hosts, looking for a match with the requested name: */
      Vector hosts = Host.getDefinedHosts();
      for (int j = 0; j < hosts.size(); j++)
      {
        Host host = (Host) hosts.elementAt(j);

        if (common.simple_wildcard(host_names[i], host.getLabel()))
        {
          number_found++;
          hosts_found.add(host);
        }
      }

      if (number_found == 0)
        common.failure("Could not find host=" + host_names[i] + " for wd=" + wd_name);
    }

    /* If we did not find any hosts we are in trouble: */
    if (hosts_found.size() == 0)
    {
      for (int i = 0; i < host_names.length; i++)
      {
        common.ptod(host_names[i]);
        common.failure("Could not find correct host= for WD=" + wd_name);
      }
    }

    return hosts_found;
  }


  /**
   * Find SD entries requested for a specific Workload Definition (WD).
   * Create a Vector list of these SDs used.
   */
  public Vector getSdsForHost(Host host)
  {
    Vector sds_found = new Vector(64,0);

    /* First clear all active flags. We need this to assure that if an */
    /* SD is specified twice for this WD we signal it:                 */
    SD_entry.clearAllActive();

    /* Look for all SD entries requested by this WD: */
    for (String sdname : sd_names)
    {
      boolean sd_match_for_name = false;

      /* Scan all SDs, looking for a match with the requested name: */
      for (SD_entry sd : Vdbmain.sd_list)
      {
        if (!sd.concatenated_sd && common.simple_wildcard(sdname, sd.sd_name) )
        {
          sd_match_for_name = true;

          /* Only used this SD if it has been defined for this host: */
          if (!host.doesHostHaveSd(sd))
            continue;

          if (sd.isActive())
            common.failure("SD=" + sd.sd_name + " is requested more than once " +
                           "in wd=" + wd_name);

          sd.setActive();
          sds_found.addElement(sd);

          //common.ptod("getSdsForHost added host: " + host.getLabel() + " sd.sd_name: " + sd.sd_name);
        }
      }

      // This code must stay inactive for the caller to realize that he does
      // not get this sd!
      if (!sd_match_for_name)
        common.failure("Could not find sd=" + sdname +
                       " for wd=" + wd_name + ",host=" + host.getLabel());
    }

    return sds_found;
  }

  private static void checkParameters(Vector wd_list)
  {
    for (int i = 0; i < wd_list.size(); i++)
    {
      WD_entry wd = (WD_entry) wd_list.elementAt(i);
      if (wd.skew_original != 0 && wd.wd_iorate != 0)
        common.failure("Workload Definition (WD): 'iorate=' and 'skew=' are mutually exclusive");
    }

    if (Dedup.any_hotsets_requested && !any_hotbands_requested)
      BoxPrint.printOne("When requesting 'deduphotsets=' the use of 'hotbands=' is highly recommended");
  }


  /**
   * Make sure that if ANY iorate or priority is used, EACH workload has a
   * priority set, with the priority of the workloads without an iorate being
   * lower (numerically higher).
   */
  public static void checkPriorities(Vector wds)
  {
    HashMap prios_map = new HashMap(32);
    for (int i = 0; i < wds.size(); i++)
    {
      WD_entry wd = (WD_entry) wds.elementAt(i);
      if (wd.priority != Integer.MAX_VALUE)
        prios_map.put(new Integer(wd.priority), null);
    }

    boolean any_priority = false;
    for (int i = 0; i < wds.size(); i++)
    {
      WD_entry wd = (WD_entry) wds.elementAt(i);
      if (wd.priority != Integer.MAX_VALUE || wd.wd_iorate != 0)
        any_priority = true;
      if (wd.wd_iorate != 0 && wd.priority == Integer.MAX_VALUE)
        common.failure("A workload with iorate= also must have  priority= specified.");
    }
    if (any_priority && wds.size() != prios_map.size())
      common.failure("When a workload priority= or iorate= parameter is used, " +
                     "ALL workloads must have a (different) priority defined");


    /* Create a list of priorities of all workloads using iorate=: */
    HashMap iorate_map = new HashMap(32);
    for (int i = 0; i < wds.size(); i++)
    {
      WD_entry wd = (WD_entry) wds.elementAt(i);
      if (wd.wd_iorate != 0)
        iorate_map.put(new Integer(wd.priority), null);
    }
    Integer[] iorate_users = (Integer[]) iorate_map.keySet().toArray(new Integer[0]);
    Arrays.sort(iorate_users);

    /* Create a list of priorities of all workloads NOT using iorate=: */
    HashMap no_iorate_map = new HashMap(32);
    for (int i = 0; i < wds.size(); i++)
    {
      WD_entry wd = (WD_entry) wds.elementAt(i);
      if (wd.wd_iorate == 0)
        no_iorate_map.put(new Integer(wd.priority), null);
    }
    Integer[] no_iorate_users = (Integer[]) no_iorate_map.keySet().toArray(new Integer[0]);
    Arrays.sort(no_iorate_users);

    /* Check all iorate= workloads making sure their priority is higher than non-iorate: */
    if (no_iorate_users.length > 0)
    {
      int lowest_non_io = no_iorate_users[ 0 ].intValue();
      for (int i = 0; i < iorate_users.length; i++)
      {
        //common.ptod("lowest_non_io: " + lowest_non_io);
        //common.ptod("iorate_users[i].intValue(): " + iorate_users[i].intValue());
        if (iorate_users[i].intValue() >= lowest_non_io)
          common.failure("Workloads specifying iorate= must have a higher priority than " +
                         "workloads that do NOT specify iorate=");
      }
    }
  }
}

