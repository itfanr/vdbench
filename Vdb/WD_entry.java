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

import java.io.Serializable;
import java.lang.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

import Oracle.OracleParms;


/**
  * This class handles Workload Definition (WD) parameters.
  */
public class WD_entry implements Cloneable, Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String   wd_name = "default";/* Name for this Workload Definition           */
  String   wd_sd_name;         /* Name of Storage Definitions used            */
  String[] sd_names = null;    /* SD names requested, will be xlated to wd_sd_name */
  String[] host_names = new String[] { "*"};

  int      priority  = Integer.MAX_VALUE;
  double   wd_iorate = 0;
  double   skew;               /* io rate skew (curve may change this)        */
  double   skew_original = 0;  /* io rate skew this WD gets.                  */
  double   skew_observed;      /* From wg.report_wg_iocount. For curve        */
  boolean  valid_skew_obs = false;
  double   lowrange  = -1;     /* Starting and                                */
  double   highrange = -1;     /* Ending search range within SD               */
  double   seekpct = 100;      /* How often do we need a seek?                */

  double   rhpct = 0;          /* Read hit % obtained from WD                 */
  double   whpct = 0;          /* Write hit % obtained from WD                */

  int      xfersize = 4096;    /* Data transfersize                           */
  double[] xf_table;           /* Distribution table                          */
  double   readpct = 100;      /* Read percentage for this WD                 */

  long     total_io_done;      /* Used to set skew for curve runs             */

  String[] generate_parameters = null;
  boolean  seq_workload_warning_given = false;

  OpenFlags  open_flags = null;

  static   WD_entry dflt = new WD_entry();



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
      if (open_flags != null)
        wd.open_flags   = (OpenFlags) open_flags.clone();

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
        if (str.equalsIgnoreCase("oracle_parameters"))
        {
          str = OracleParms.readParms();
          continue;
        }

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

            if (Vdbmain.isFwdWorkload())
              common.ptod("'fwd' and 'wd' parameters are mutually exclusive");
          }
        }


        else if (prm.keyword.equals("sd"))
        {
          wd.sd_names   = prm.alphas;
          wd.wd_sd_name = prm.alphas[0]; // in case there is just one sd
        }

        else if ("skew".startsWith(prm.keyword))
          wd.skew_original = prm.numerics[0];

        else if ("xfersize".startsWith(prm.keyword))
        {
          if (prm.numerics.length == 1)
            wd.xfersize = (int) prm.numerics[0];
          else
          {
            double cumpct = 0;
            wd.xfersize = -1;
            wd.xf_table = new double[prm.numerics.length];
            for (int i = 0; i < prm.numerics.length; i++)
            {
              if (i % 2 == 1)
                cumpct += prm.numerics[i];
              wd.xf_table[i] = prm.numerics[i];

              if (i % 2 == 0 && wd.xf_table[i] % 512 != 0)
                common.failure("data transfer size not multiple of 512 bytes: " + (int) wd.xf_table[i]);
            }
            if ((int) cumpct != 100)
              common.failure("Xfersize distribution does not add up to 100");
          }
        }

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
        }

        else if ("seekpct".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() > 0)
          {
            if (SD_entry.isTapeTesting() && !prm.alphas[0].equals("eof"))
              common.ptod("'seekpct=' other than 'seekpct=eof' is ignored for a tape drive.");

            else
            {
              if ("sequential".startsWith(prm.alphas[0]))
                wd.seekpct = 0;

              else if (prm.alphas[0].equalsIgnoreCase("eof"))
                wd.seekpct = -1;

              else if ("random".startsWith(prm.alphas[0]))
                wd.seekpct = 100;

              else
                common.failure("Invalid contents for 'seekpct=': " + prm.alphas[0]);
            }
          }

          else
            wd.seekpct = prm.numerics[0];   /* Negative %% means: stop after EOF */
        }

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
          wd.rhpct = prm.numerics[0];

        else if ("whpct".startsWith(prm.keyword))
          wd.whpct = prm.numerics[0];

        else if ("host".startsWith(prm.keyword) || prm.keyword.equals("hd"))
          wd.host_names = prm.alphas;

        else if ("generate".startsWith(prm.keyword))
        {
          wd.generate_parameters = prm.alphas;
          wd.seekpct = -1;   // Causes IO_task to stop after last i/o.
        }

        else if ("openflags".startsWith(prm.keyword))
          wd.open_flags = new OpenFlags(prm.alphas);

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
      common.failure("Exception during reading of input parameter file(s).");
    }

    return str;
  }



  /**
   * Find Host names requested by this WD
   */
  public String[] getSelectedHostNames()
  {
    Vector hosts_found = new Vector(8, 0);

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
          hosts_found.add(host.getLabel());
          //common.ptod("host_names[i]: " + host_names[i]);
          //common.ptod("getSelectedHostNames: " + host.getLabel());
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

    return(String[]) hosts_found.toArray(new String[0]);
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
    for (int j = 0; j < Vdbmain.sd_list.size(); j++)
      ((SD_entry) Vdbmain.sd_list.elementAt(j)).setActive(false);

    /* Look for all SD entries requested by this WD: */
    boolean sd_match_for_name = false;
    boolean sd_match_for_host = false;
    for (int i = 0; i < sd_names.length; i++)
    {
      /* Scan all SDs, looking for a match with the requested name: */
      for (int j = 0; j < Vdbmain.sd_list.size(); j++)
      {
        SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(j);

        if (common.simple_wildcard(sd_names[i], sd.sd_name) )
        {
          sd_match_for_name = true;

          /* Only used this SD if it has been defined for this host: */
          if (host.getLunNameForSd(sd.sd_name) == null)
            continue;

          if (sd.isActive())
            common.failure("SD=" + sd.sd_name + " is requested more than once " +
                           "in wd=" + wd_name);

          sd.setActive(true);
          sds_found.addElement(sd);
          sd_match_for_host = true;
        }
      }

      // This code must stay inactive for the caller to realize that he does
      // not get this sd!
      if (!sd_match_for_name)
        common.failure("Could not find sd=" + sd_names[i] +
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
  }

  public void checkTapeParms()
  {
    if (!SD_entry.isTapeTesting())
      return;

    if (seekpct > 0)
    {
      common.ptod("checkTapeParms(): 'wd=" + wd_name + "' forced to use 'seekpct=eof' "+
                  "because of tape testing");
      seekpct = -1;
    }

    if (readpct != 0 && readpct != 100)
      common.failure("When tape drives are present, all workloads must define "+
                     "'rdpct=0' or 'rdpct=100");
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
        common.ptod("lowest_non_io: " + lowest_non_io);
        common.ptod("iorate_users[i].intValue(): " + iorate_users[i].intValue());
        if (iorate_users[i].intValue() >= lowest_non_io)
          common.failure("Workloads specifying iorate= must have a higher priority than " +
                         "workloads that do NOT specfy iorate=");
      }
    }
  }
}

