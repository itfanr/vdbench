package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

/**
 * This class optionally compares the way that work was spread over slaves to
 * the user specified 'misc=(work,rd,....' input.
 *
 * For debugging.
 */
public class CompareWorkInfo
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";




  /**
   * Hopefully a helping hand with testing 'what goes where':
   *
   * Compare current generated work with the input:
   *
   *     misc=(work,rd1,slave,wd,sd,rd2,slave,wd,sd,......)
   *
   * Request this data to be generated:
   *     misc=work
   *
   */
  public static String[] generateDebugInfo(RD_entry rd)
  {
    String[] work = MiscParms.getKeyParameters("work");

    if (!common.get_debug(common.GENERATE_WORK_INFO) && work == null)
      return null;

    //for (String w : work)
    //  common.ptod("w: " + w);

    ArrayList <String> data = new ArrayList(8);
    data.add(String.format("misc=(work,%s,", rd.rd_name));


    //
    // For now: this only handles SD workloads where it is most important.
    //

    /* Get all workloads and sort info by slave/wd/sd: */
    RD_entry.printWgInfo2("Sort order: slave");
    ArrayList <WG_entry> wgs = WG_entry.sortWorkloads(RD_entry.getAllWorkloads(), "slave");
    for (int i = 0; i < wgs.size(); i++)
    {
      WG_entry wg = wgs.get(i);
      data.add(String.format("%s,%s,%s,%d%s",
                             wg.getSlave().getLabel(),
                             wg.wd_used.wd_name,
                             wg.sd_used.sd_name,
                             rd.getSdThreadsUsedForSlave(wg.sd_used.sd_name, wg.getSlave()),
                             (i == wgs.size() - 1) ? ")" : ","));
    }

    //for (String d : data)
    //  System.out.println(d);

    return data.toArray(new String[0]);
  }

  public static boolean debugCompareNeeded()
  {
    String[] work = MiscParms.getKeyParameters("work");
    if (work == null)
      return false;
    if (work.length > 1)
      return true;
    return false;
  }
  public static boolean debugOutputNeeded()
  {
    //if (common.get_debug(common.GENERATE_WORK_INFO))
    //  return true;

    String[] work = MiscParms.getKeyParameters("work");
    if (work == null)
      return false;
    if (work.length == 1)
      return true;
    return false;
  }


  /**
   * Simple compare: generate the 'new' info, and compare it with what we found
   * in the 'misc=' set of parameters
   *
   * Remember: the threadcount is the TOTAL for the SD on a slave, not the
   * thread count for the WD. (which does not exist).
   */
  public static void compareWorkInfo(RD_entry rd)
  {
    String[] old_info = null;
    String[] new_info = null;
    try
    {
      old_info = MiscParms.getKeyParameters("work", rd.rd_name);
      if (old_info == null)
        common.failure("compareWorkInfo: no info found for rd=%s", rd.rd_name);

      /* Translate the old info to look like the new info: */
      /* (easier on the eyes): */
      ArrayList <String> parms = new ArrayList(8);
      parms.add(String.format("misc=(work,%s,", old_info[1]));
      int FIELDS = 4;
      for (int i = 2; i < old_info.length; i += FIELDS)
      {
        String line = String.format("%s,%s,%s,%s%s",
                                    old_info[ i + 0 ],
                                    old_info[ i + 1 ],
                                    old_info[ i + 2 ],
                                    old_info[ i + 3 ],
                                    (i + FIELDS == old_info.length) ? ")" : ",");
        //common.ptod("line: " + line);
        parms.add(line);
      }

      old_info = parms.toArray(new String[0]);

      new_info = generateDebugInfo(rd);

      if (old_info.length != new_info.length)
      {
        printInfo(old_info, "Info found in parameter file 'misc=' parameter:");
        printInfo(new_info, "Info generated during run:");
        common.failure("compareWorkInfo: mismatch in element count");
      }

      for (int i = 0; i < old_info.length; i++)
      {
        if (!old_info[i].equals(new_info[i]))
        {
          common.ptod("compareWorkInfo: mismatch old: %s", old_info[i]);
          common.ptod("compareWorkInfo: mismatch new: %s", new_info[i]);
          printInfo(old_info, "Info found in parameter file 'misc=' parameter:");
          printInfo(new_info, "Info generated during run:");
          common.failure("compareWorkInfo: mismatch in element compare, element %d", i);
        }
      }
    }
    catch (Exception e)
    {
      if (old_info != null)
        printInfo(old_info, "Info found in parameter file 'misc=' parameter:");
      if (new_info != null)
        printInfo(new_info, "Info generated during run:");
      common.failure(e);
    }

  }

  private static void printInfo(String[] info, String txt)
  {
    common.ptod("compareWorkInfo: %s", txt);
    common.ptod("compareWorkInfo: elements: %d", info.length);
    for (int i = 0; i < info.length; i++)
      common.ptod("                 element %2d: %s", i, info[i]);
  }
}
