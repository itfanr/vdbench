package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

/**
 * This class handles the manipulation of an index used to communicate between
 * java and JNI related to performance statistics.
 *
 * In JNI statistics are maintained in an array of (currently) 10240 'struct
 * workload' elements.
 *
 * Each element within the original implementation of Vdbench represented a
 * unique WG_entry. With there being one SD per WG this created a tie-back from
 * WG to SD.
 *
 * Starting the introduction of SD concatenation we no longer have this tie-back
 * WG/SD relationship because one WG now gets to use multiple SDs. We therefore
 * needed a mechanism to translate WG and SD to the above mentioned struct
 * Workload array element.
 *
 */
public class JniIndex
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  public int    jni_index;
  public String sd_name;
  public String wd_name;

  public SdStats dlt_stats = new SdStats();  /* Passed statistics from slave to master */
  public SdStats old_stats = new SdStats();  /* Only created on the slave              */


  /**
   * Create the indexes for each SD in use on this slave.
   */
  public static void createIndexes(Work work)
  {
    int current_index = 0;

    /* Without concatenation we need only one JniIndex per WG: */
    if (!Validate.sdConcatenation())
    {
      for (int i = 0; i < work.wgs_for_slave.size(); i++)
      {
        WG_entry wg = (WG_entry) work.wgs_for_slave.get(i);
        wg.jni_index_list = new ArrayList(1);

        JniIndex jnx  = new JniIndex();
        jnx.jni_index = current_index++;
        jnx.sd_name   = wg.sd_used.sd_name;
        jnx.wd_name   = wg.wd_name;

        wg.jni_index_list.add(jnx);
      }
    }

    else
    {

      /* With concatenation we need one JniIndex per WG per SD: */
      for (int i = 0; i < work.wgs_for_slave.size(); i++)
      {
        WG_entry wg       = (WG_entry) work.wgs_for_slave.get(i);
        wg.jni_index_list = new ArrayList(wg.sds_in_concatenation.size());

        for (int s = 0; s < wg.sds_in_concatenation.size(); s++)
        {
          SD_entry sd = wg.sds_in_concatenation.get(s);

          JniIndex jnx  = new JniIndex();
          jnx.jni_index = current_index++;
          jnx.sd_name   = sd.sd_name;
          jnx.wd_name   = wg.wd_name;

          wg.jni_index_list.add(jnx);
        }
      }
    }

    int LIMIT = 10240;
    if (current_index >= LIMIT)
    {
      common.ptod("There is a hardcoded limit of %d statistics table elements.", LIMIT);
      common.ptod("One per SD per WD");
      common.failure("Maximum current jni_index value is %d, it currently is %d. Contact Henk",
                     LIMIT,
                     current_index);
    }
  }
}


