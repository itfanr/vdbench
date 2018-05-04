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
 * This class contains all information obtained from the FWD parameters:
 * 'Filesystem Workload Definition".
 */
class FwdEntry implements Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public  String    fwd_name      = "default";
  public  String    fsd_names[]   = new String[0];
  public  String[]  host_names    = new String[] { "*"};
  public  String    target_fsd    = null;

  public  double[]  xfersizes     = new double[] { 4096 };

  public  boolean   select_random = false;
  public  boolean   select_once   = false;
  public  double    poisson_skew  = 0;

  public  boolean   sequential_io = true;
  public  boolean   del_b4_write  = false;
  public  boolean   file_sharing  = false;
  public  long      stopafter     = Long.MAX_VALUE;
  public  double    skew          = 0;
  public  int       threads       = 1;
  public  double    readpct       = -1;

  public  OpenFlags open_flags    = null;

  private int       operation     = Operations.getOperationIdentifier("read");

  public  static int max_fwd_name = 0;

  private static    Vector fwd_list = new Vector(8);
  public  static    boolean  format_fwd_found = false;
  public  static    FwdEntry recovery_fwd = null;
  public  static    FwdEntry dflt     = new FwdEntry();

  public  static    FwdEntry format_fwd = new FwdEntry();
  static
  {
    format_fwd.xfersizes = new double[] {128*1024};  // equal to dedupunit=
    format_fwd.threads   = 8;
  };


  public Object clone()
  {
    try
    {
      FwdEntry fwd   = (FwdEntry) super.clone();
      fwd.fsd_names  = (String[]) fsd_names.clone();
      fwd.xfersizes  = (double[]) xfersizes.clone();
      if (open_flags != null)
        fwd.open_flags = (OpenFlags) open_flags.clone();

      return fwd;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  /**
   * Read Filesystem Workload Definition input and interpret and store parameters.
   */
  static String readParms(String first)
  {

    String str = first;
    Vdb_scan prm;
    FwdEntry fwd = null;

    try
    {


      while (true)
      {
        prm = Vdb_scan.parms_split(str);

        if (prm.keyword.equals("rd") || prm.keyword.equals("wd"))
          break;


        if (prm.keyword.equals("fwd"))
        {
          Vdbmain.setFwdWorkload();

          if (prm.alphas[0].equals("default")  )
            fwd = dflt;
          else
          {
            /* Don't allow duplicates: */
            for (int i = 0; i < fwd_list.size(); i++)
            {
              fwd = (FwdEntry) fwd_list.elementAt(i);
              if (fwd.fwd_name.equalsIgnoreCase(prm.alphas[0]))
                common.failure("Duplicate FWD name: " + fwd.fwd_name);
            }

            fwd          = (FwdEntry) dflt.clone();
            fwd.fwd_name = prm.alphas[0];

            /* Two special workloads: recover and format: */
            if (fwd.fwd_name.equals(Jnl_entry.RECOVERY_RUN_NAME))
              recovery_fwd = fwd;

            else if (fwd.fwd_name.equals("format"))
            {
              format_fwd           = fwd;
              format_fwd_found     = true;

              /* This is needed because otherwise 'fwd=default' value will be used: */
              format_fwd.xfersizes = new double[] { 128*1024 };
              common.plog("'fwd=format' will be used only for 'format=' workloads.");
              common.plog("Only the 'threads=' and 'openflags=' and the first 'xfersize=' will be used.");
            }

            else
            {
              max_fwd_name = Math.max(max_fwd_name, fwd.fwd_name.length());
              fwd_list.add(fwd);
            }

            if (Vdbmain.isWdWorkload())
              common.ptod("'fwd' and 'wd' parameters are mutually exclusive");

            if (Operations.getOperationIdentifier(fwd.fwd_name) >= 0)
              common.failure("fwd name may not match a valid operation: fwd=" + fwd.fwd_name);
          }
        }


        else if (prm.keyword.equals("fsd"))
          fwd.fsd_names = prm.alphas;

        else if ("readpct".startsWith(prm.keyword) || "rdpct".startsWith(prm.keyword))
          fwd.readpct = prm.numerics[0];

        else if ("stopafter".startsWith(prm.keyword))
          fwd.stopafter = (long) prm.numerics[0];

        else if ("operation".startsWith(prm.keyword))
        {
          if (prm.getAlphaCount() > 1)
            common.failure("'fwd=" + fwd.fwd_name + ",operations=' accepts only ONE parameter.");
          fwd.operation = Operations.getOperationIdentifier(prm.alphas[0]);
          if (fwd.operation == -1)
            common.failure("Unknown operation: " + prm.alphas[0]);
        }

        else if ("fileselect".startsWith(prm.keyword))
          fwd.parseFileSelect(prm.raw_values);

        else if ("fileio".equals(prm.keyword))
          fwd.fileIoParameters(prm);

        else if ("xfersizes".startsWith(prm.keyword))
        {
          fwd.xfersizes = prm.numerics;

          double cumpct = 0;
          for (int i = 0; i < prm.numerics.length; i++)
          {
            if (i % 2 == 1)
              cumpct += prm.numerics[i];
          }
          if (prm.numerics.length > 1 && (int) cumpct != 100)
            common.failure("Xfersize distribution does not add up to 100");
        }

        else if ("skew".startsWith(prm.keyword))
        {
          fwd.skew = prm.numerics[0];
          Host.noMultiJvmForFwdSkew();
        }

        else if ("threads".startsWith(prm.keyword))
          fwd.threads = (int) prm.numerics[0];

        else if ("target".startsWith(prm.keyword))
          fwd.target_fsd = prm.alphas[0];

        else if ("host".startsWith(prm.keyword) || prm.keyword.equals("hd"))
        {
          fwd.host_names = prm.alphas;
          for (int i = 0; i < fwd.host_names.length; i++)
          {
            if (Host.findHost(fwd.host_names[i]) == null)
              common.failure("Unable to find requested host (wildcards not allowed): host=" + fwd.host_names[i]);
          }
        }

        else if ("openflags".startsWith(prm.keyword))
          fwd.open_flags = new OpenFlags(prm.alphas, prm.numerics);

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

    /* If we did not see any FWD parameters we just fall through: */
    if (fwd_list.size() == 0)
      return str;

    /* Do some extra checking: */
    checkParameters();

    return str;
  }


  /**
   * Check contents of these parameters.
   */
  private static void checkParameters()
  {
    /* Check 'delete' parameters: */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.sequential_io && fwd.del_b4_write && fwd.readpct != -1)
        common.failure("'fileselect=(sequential,delete)'. 'rdpct=' and 'delete' parameters are mutually exclusive");
    }

    /* Check xfer size parameters: */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.xfersizes.length == 0)
        common.failure("'xfersizes=' parameter is required");
    }

    /* Check operation parameters: */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.operation == -1)
        common.failure("'operation=' parameter is required for each fwd");
    }

    /* Check for fsds: */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.fsd_names.length == 0)
        common.failure("fwd parameter requires at least one 'fsd='");
    }

    /* fileselect=seq if fileio=shared is used: */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.file_sharing && fwd.select_random)
        common.failure("'fileio=(random,shared)' and 'fileselect=random' are mutually exclusive");
    }

    /* If 'target' is specified, operation=copy/move is required */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.target_fsd != null)
      {
        if (fwd.operation != Operations.MOVE && fwd.operation != Operations.COPY)
          common.failure("'target=' may only be specified together with 'operation=move' "+
                         "or 'operation=copy'");
      }
    }

    /* If 'copy/move' is specified, target= is required */
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      if (fwd.target_fsd != null)
      {
        if (fwd.operation == Operations.MOVE || fwd.operation == Operations.COPY)
        {
          if (fwd.target_fsd == null)
            common.failure("'target=' parameter is required with 'operation=move' "+
                           "or 'operation=copy'");
        }
      }
    }
  }




  public int getOperation()
  {
    return operation;
  }
  public void setOperation(int op)
  {
    operation = op;
  }




  public boolean compareXfersize(FwdEntry fwd)
  {
    if (xfersizes.length != fwd.xfersizes.length)
      return false;

    for (int i = 0; i < xfersizes.length; i++)
    {
      if (xfersizes[i] != fwd.xfersizes[i])
        return false;
    }

    return true;
  }


  public String printXfersize()
  {
    String line = "";
    for (int i = 0; i < xfersizes.length; i++)
      line += xfersizes[i] + " ";
    return line;
  }



  /**
   * Get a list of FSDs that are requested for this FWD
   */
  public FsdEntry[] findFsdNames(RD_entry rd)
  {
    HashMap fsds = new HashMap(16);

    /* Scan the complete list of FSDs looking for the ones I need: */
    for (int i = 0; i < fsd_names.length; i++)
    {
      boolean found = false;
      for (int k = 0; k < FsdEntry.getFsdList().size(); k++)
      {
        FsdEntry fsd = (FsdEntry) FsdEntry.getFsdList().elementAt(k);

        if (common.simple_wildcard(fsd_names[i], fsd.name))
        {
          fsds.put(fsd, fsd);
          //common.ptod("findFsdNames(): added fsd for rd=" + name + " " + fsd.name);
          found = true;
        }
      }

      if (!found)
        common.failure("Could not find fsd=" + fsd_names[i] + " for FWD=" + fwd_name);
    }

    return(FsdEntry[]) fsds.values().toArray(new FsdEntry[0]);
  }

  public static Vector <FwdEntry> getFwdList()
  {
    return fwd_list;
  }

  public static String[] getFwdNames()
  {
    HashMap names = new HashMap(64);
    for (int i = 0; i < fwd_list.size(); i++)
    {
      FwdEntry fwd = (FwdEntry) fwd_list.elementAt(i);
      names.put(fwd.fwd_name, fwd);
    }

    return(String[]) names.keySet().toArray(new String[0]);
  }


  private void fileIoParameters(Vdb_scan prm)
  {
    if ("random".startsWith(prm.alphas[0]))
    {
      sequential_io = false;
      if (prm.getAlphaCount() > 1)
      {
        if ("shared".startsWith(prm.alphas[1]))
        {
          if (Validate.isRealValidate())
            common.failure("'fileio=(random,shared)' may not be used with Data Validation");
          file_sharing = true;
        }
        else
          common.failure("Invalid 'fileio' parameter contents: " + prm.alphas[1]);
      }
    }

    else if ("sequential".startsWith(prm.alphas[0]))
    {
      sequential_io = true;
      if (prm.getAlphaCount() > 1)
      {
        if ("delete".startsWith(prm.alphas[1]))
          del_b4_write = true;
        else
          common.failure("Invalid 'fileio' parameter contents: " + prm.alphas[1]);
      }
    }
    else
      common.failure("Invalid 'fileio' parameter contents: " + prm.alphas[0]);
  }

  /**
   * fileselect=
   *
   * - random
   * - sequential
   * - once
   * - xxxxx
   * - average=nn
   */
  private void parseFileSelect(ArrayList <String> parms)
  {
    for (String parm : parms)
    {
      if ("random".startsWith(parm))
        select_random = true;

      else if ("sequential".startsWith(parm))
        select_random = false;

      else if (parm.equals("once"))
        select_once = true;

      else if (parm.equals("skewed"))  // renamed to 'poisson' after first tests
      {
        select_random = true;
        poisson_skew = 3;
      }

      else if (parm.equals("poisson"))
      {
        select_random = true;
        poisson_skew = 3;
      }

      else if (parm.startsWith("midpoint=")) // also for compatibility
      {
        String[] split = parm.split("=");
        if (split.length != 2)
          common.failure("Invalid 'fileselect' parameter contents: " + parm);
        if (!common.isNumeric(split[1]))
          common.failure("Invalid 'fileselect' parameter contents: " + parm);

        select_random = true;
        poisson_skew = Double.parseDouble(split[1]);
        if (poisson_skew <= 0)
          common.failure("Invalid 'fileselect' parameter contents: " + parm);
      }

      else if (common.isDouble(parm))
        poisson_skew = Double.parseDouble(parm);

      else
        common.failure("Invalid 'fileselect' parameter contents: " + parm);
    }
  }
}
