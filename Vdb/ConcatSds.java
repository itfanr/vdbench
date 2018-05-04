package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;


/**
 * This class contains information and code related to concatenated SDs
 */
public class ConcatSds
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Look through all Workload Definitions and Run Definitions and create a
   * concatenated SD (csd) whenever needed.
   * No duplicates allowed, just reuse, which could happen for instance when:
   * - wd=wd1,sd=(sd1,sd2,concat),....
   * - wd=wd2,sd=(sd1,sd2,concat),....
   *
   * todo:
   * - why did I not see var adm lines at the end of an aborted run????sj1
   * - differing lun names across hosts.
   *
   * - new file is not recognized on remote host! or, no format inserted to
   *   create it?
   *
   *
   * - no mix of normal and concat sds allowed.
   * - check openflags, like openflags=directio etc.
   * - range
   * - access block zero
   * - DV
   * - Dedupratio?
   * - compratio?
   * - rhpct + whpct, hitarea not allowed
   * - test: sd1,sd2 vs. sd2,sd1 both for wd and rd.
   * - replay
   * - do I handle file format properly?
   *   Possibly: it will just write to all Sds as part of a seekpct=eof to the
   *   csd.
   * - windows rewinds needs to be taken care of, because of removal of special
   *   windows open. Or get rid of tape!!!!!!!
   * - close all files: need to close the real sds, not the csd
   * - unused SDs should not be reported?
   * - stride stuff
   * - stream stuff
   *
   * - user_class_parms
   * - wg_iorate
   * - priority
   * - what to do about pure sequential, running only on one host?
   * - forxxx stuff.
   * - streamcontext with and without concat.
   * - SD overlap: ignore, drop to prvious or advance to next? Same with lba0?
   * - need a WD report and SD reports!
   * - will FSD statistics still work with JNI?
   *
   *
   * - done:
   * - removed: all tape references.
   * - need to test threads vs. multiple workloads.
   * - size=?
   * - sd.win_handles removed
   * - proper amount of threads per host?
   * - may not use multi_io because of sequential!
   * - do I need to sendEOF to all SDs?
   * - how about with and without waiter?
   * - createLunInfoFromHost: allow Kstat instance name to be picked up from SD
   *   Maybe remove this or just leave it in case we need it again?
   * - sd.open for write?
   * - need to test rd=xx,sd=
   * - with multiple WDs, code does not properly recognize 'open for write'.
   */




  public static void createConcatSds(Vector <WD_entry> wds, Vector <RD_entry> rds)
  {
    /* Create concatenated SDs for all WDs: */
    for (WD_entry wd : wds)
    {
      wd.concat_sd         = createOneConcatSd(wd.sd_names, wd.wd_name);
      wd.concat_sd.threads = wd.wd_threads;
    }

    /* If RD specified SDs then create an RD-specific concatenated SD: */
    for (RD_entry rd : rds)
    {
      if (rd.sd_names != null )
        rd.concat_sd = createOneConcatSd(rd.sd_names, "rd=" + rd.rd_name);
    }
  }


  /**
   * We have received size info for each real SD, now use that to calculate the
   * total concatenated size.
   */
  public static void calculateSize()
  {
    Vector <SD_entry> sdlist = Vdbmain.sd_list;
    for (int c = 0; c < sdlist.size(); c++)
    {
      SD_entry csd = sdlist.get(c);
      if (!csd.concatenated_sd)
        continue;

      /* We now have a csd, look for all its real SDS: */
      csd.end_lba = 0;
      String long_sdname = "";
      for (int r = 0; r < csd.sds_in_concatenation.size(); r++)
      {
        SD_entry real_sd = csd.sds_in_concatenation.get(r);
        if (real_sd.concatenated_sd)
          continue;
        csd.end_lba += real_sd.end_lba;
        long_sdname += real_sd.sd_name + " ";
      }
      common.plog("Total size is %s for concatenated sd=%s consisting of sd=(%s)",
                  FileAnchor.whatSize(csd.end_lba),
                  csd.sd_name,
                  common.replace(long_sdname.trim(), " ", ","));
    }
  }

  /**
   * If we are concatenating and do not have an rd threads= then all WDs must
   * have a threads= parameter:
   */
  public static void checkForWdThreads(RD_entry rd)
  {
    if (!Validate.sdConcatenation())
      return;

    if (rd.current_override.getThreads() != For_loop.NOVALUE)
      return;

    Vector <WD_entry> wdlist = Vdbmain.wd_list;
    for (int i = 0; i < wdlist.size(); i++)
    {
      WD_entry wd = wdlist.get(i);
      if (wd.wd_threads == 0)
        common.failure("wd=%s: for SD concatenation each WD must request a threads= " +
                       "value, unless overridden by the RD threads= or forthreads= parameter",
                       wd.wd_name);
    }
  }


  /**
   * Set up a logical end-lba for each SD within a concatenated SD_entry.
   * This value will be used in a binary search so that a concatenated lba can
   * be translated to a real lba.
   */
  public static void calculateLbaRanges(WG_entry wg)
  {
    long starting_lba = 0;

    /* Loop through all of the SDs establishing relative starting lba: */
    for (SD_entry sd : wg.sds_in_concatenation)
    {
      /* If we have set an offset before, it better be the same: */
      if (sd.csd_start_lba >= 0 && sd.csd_start_lba != starting_lba)
      {
        common.failure("SD concatenation: an SD (%s) may not be reused in a "+
                       "non-matching concatenation.", sd.sd_name);
      }

      sd.csd_start_lba = starting_lba;
      sd.csd_end_lba   = starting_lba + sd.end_lba - 1;
      starting_lba      = starting_lba + sd.end_lba;
      //common.ptod("sd.csd_end_lba: " + sd.csd_end_lba);

      sd.trackSdXfersizes(wg.getXfersizes());

    }
  }


  /**
   * Create a concatenated SD_entry, allowing duplicates.
   *
   * Duplicates are needed to allow a concatenated set of SDs to be used more
   * than once in a run, e.g. wd1+wd2 both having the same concatenation.
   */
  private static SD_entry createOneConcatSd(String[] selection, String wd_name)
  {
    /* Count the amount of csds we already have to create the CSD#: */
    int csds = 0;
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      if (Vdbmain.sd_list.get(i).concatenated_sd)
        csds++;
    }

    SD_entry csd        = new SD_entry();
    csd.concatenated_sd = true;
    csd.concat_wd_name  = wd_name;
    csd.sd_name         = String.format("concat#%02d", (csds+1));

    /* Though concatenated SDs don't allow Data Validation, we need something here: */
    csd.sd_name8        = "concatxx";

    /* These must be in the order defined in the parameter file.   */
    /* This allows user to decide 'first sd1 then sd2',or reverse. */
    csd.sds_in_concatenation = new ArrayList(Arrays.asList(getSdsOrdered(selection)));
    Vdbmain.sd_list.add(csd);
    SD_entry.max_sd_name = Math.max(SD_entry.max_sd_name, csd.sd_name.length());

    String txt = "";
    for (SD_entry sd : csd.sds_in_concatenation)
    {
      if (sd.concatenated_sd)
        common.failure("Oops: concatenated SD included in concatenation");
      txt += sd.sd_name + " ";
    }

    String wd_mask    = String.format("wd=%%-%ds ",  WD_entry.max_wd_name);
    common.plog("Created concatenated SD %s for " + wd_mask + "sd=(%s)",
                csd.sd_name, wd_name, txt.trim().replace(" " , ","));

    return csd;
  }


  /**
   * Modify the requested i/o to a concatenated SD to fit within the proper REAL
   * SD.
   * Note that any block straddling two SDs will be changed to use the first
   * block on the --following-- (or first) SD instead.
   * This may lead with sequential i/o to one block being read twice, but that
   * may be cleaner than just skipping the i/o completely.
   *
   * BTW: as of today, access_block_zero NEVER will be true.
   */
  public static SD_entry translateConcatToRealSd(Cmd_entry       cmd,
                                                 SD_entry        concat_search_key,
                                                 ConcatLbaSearch search_method,
                                                 boolean         access_block_zero)
  {
    long org_lba = cmd.cmd_lba;
    ArrayList <SD_entry> real_sds = cmd.cmd_wg.sds_in_concatenation;

    //common.ptod("cmd.cmd_lba: %,12d", cmd.cmd_lba);

    /* Use a dummy SD_entry to create a search key value: */
    concat_search_key.csd_end_lba = cmd.cmd_lba; // - cmd.cmd_xfersize;
    int index = Collections.binarySearch(real_sds,
                                         concat_search_key,
                                         search_method);

    /* Recalculate proper index after search (see java doc): */
    if (index < 0)
      index = (index+1) * -1;
    cmd.sd_ptr = real_sds.get(index);

    /* If the block straddles: */
    if (cmd.cmd_lba + cmd.cmd_xfersize -1 > cmd.sd_ptr.csd_end_lba)
    {
      /* Go to the next (or first) SD: */
      index       = (index+1) % real_sds.size();
      cmd.sd_ptr  = real_sds.get(index);
      cmd.cmd_lba = cmd.sd_ptr.csd_start_lba;

      common.ptod("lba=%,12d xfersize=%d to concatenated sd=%s; lba straddles two SDs. Switched to lba=0 on sd=",
                  org_lba, cmd.cmd_xfersize, cmd.concat_sd.sd_name, cmd.sd_ptr.sd_name);
    }

    /* If we're not allowed to access lba=0 on an SD, skip one: */
    if (!access_block_zero && cmd.cmd_lba == cmd.sd_ptr.csd_start_lba)
    {
      cmd.cmd_lba += cmd.cmd_xfersize;

      common.ptod("Not allowed to access sd=%s,lba=0. Switched to lba=%d",
                  cmd.sd_ptr.sd_name, cmd.cmd_xfersize);
    }


    /* Adjust the lba to be relative to the start of this real SD: */
    cmd.cmd_lba -= cmd.sd_ptr.csd_start_lba;

    /* Set JNI index to the proper WG/SD combination: */
    cmd.jni_index = cmd.cmd_wg.jni_index_list.get(index).jni_index;

    if (cmd.cmd_lba < 0)
    {
      common.ptod("org_lba:     %,d", org_lba);
      common.ptod("cmd.cmd_lba: %,d", cmd.cmd_lba);
      common.ptod("index:       " + index);
      common.ptod("real_sds:    %,d", real_sds.size());
      common.failure("Negative lba. ");
    }

    return cmd.sd_ptr;
  }

  /**
   * Get a list of SD names that are requested, either specifically or using
   * wild cards.
   */
  private static String[] getSdNames(String[] selection)
  {
    HashMap <String, SD_entry> sd_map = getSdMap(selection);
    return(String[]) sd_map.keySet().toArray(new String[0]);
  }

  /**
   * Get a list of SDs that are requested, either specifically or using wild
   * cards.
   */
  private static SD_entry[] obsolete_getSds(String[] selection)
  {
    HashMap <String, SD_entry> sd_map = getSdMap(selection);
    SD_entry[] sds = (SD_entry[]) sd_map.values().toArray(new SD_entry[0]);
    return sds;
  }

  /**
   * Get a list of all SDs requested.
   * This map must be in the order that the user specified the sds, e.g. sd1,sd2
   * or sd2,sd1.
   * Duplicates are illegal.
   *
   * We therefore can not use a HashMap to do this!
   */
  private static SD_entry[] getSdsOrdered(String[] selection)
  {
    ArrayList <SD_entry> sds = new ArrayList(64);
    HashMap   <String, Object> duplicates = new HashMap(64);

    for (int i = 0; i < selection.length; i++)
    {
      boolean sd_match_for_name = false;
      for (int j = 0; j < Vdbmain.sd_list.size(); j++)
      {
        SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(j);

        if (!sd.concatenated_sd && common.simple_wildcard(selection[i], sd.sd_name) )
        {
          sd_match_for_name = true;
          if (duplicates.put(sd.sd_name, sd) != null)
            common.failure("'sd=%s' has been requested more than once using 'sd=%s' selection",
                           sd.sd_name, selection[i]);
          sds.add(sd);
        }
      }

      if (!sd_match_for_name)
        common.failure("Could not find sd=" + selection[i]);
    }

    return(SD_entry[]) sds.toArray(new SD_entry[0]);
  }


  /**
   * Get a map of all SDs requested
   */
  private static HashMap <String, SD_entry> getSdMap(String[] selection)
  {
    HashMap <String, SD_entry> sd_map = new HashMap(16);
    for (int i = 0; i < selection.length; i++)
    {
      boolean sd_match_for_name = false;
      for (int j = 0; j < Vdbmain.sd_list.size(); j++)
      {
        SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(j);

        if (common.simple_wildcard(selection[i], sd.sd_name) )
        {
          sd_match_for_name = true;
          if (sd_map.get(sd.sd_name) != null)
            common.failure("sd=%s has been requested multiple times", sd.sd_name);

          sd_map.put(sd.sd_name, sd);
        }
      }

      if (!sd_match_for_name)
        common.failure("Could not find sd=" + selection[i]);
    }

    return sd_map;
  }


  /**
   * These two methods are a quick way to say 'with SD concatenation you may
   * ...'
   */
  public static void abortIf(String txt)
  {
    if (Validate.sdConcatenation())
      common.failure("SD concatenation: %s", txt);
  }
  public static void abortNotIf(String txt)
  {
    if (!Validate.sdConcatenation())
      common.failure("SD concatenation: %s", txt);
  }
}


/**
 * Binary search through a list of concatenated SDs, looking for the proper real
 * SD to do the i/o to.
 */
class ConcatLbaSearch implements Comparator
{
  public int compare(Object o1, Object o2)
  {
    SD_entry sd1 = (SD_entry) o1;
    SD_entry sd2 = (SD_entry) o2;

    long delta = sd1.csd_end_lba - sd2.csd_end_lba;
    int rc;
    if (delta == 0)
      rc = 0;
    if (delta < 0)
      rc = -1;
    else
      rc = 1;

    //common.ptod("compare %,12d %,12d %3d", sd1.csd_end_lba, sd2.csd_end_lba, rc);

    return rc;
  }
}

