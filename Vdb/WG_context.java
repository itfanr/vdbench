package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.*;

import Utils.Format;


/**
 * This contains context information for determining low and high seek address
 * for cache hits or misses.
 */
class WG_context implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  long      next_seq = -1;      /* Next sequential seek address               */
  long      last_lba;           /* To assist with next sequential io          */
  long      seek_low;           /* Low seek address for wg                    */
  long      seek_high;          /* High seek address for wg                   */
  long      seek_width;         /* Seek width (high - low)                    */
  long      max_xfersize;       /* maximum xfersize for this WG               */

  boolean   crossing_range_boundary = false;

  HotBand     hotband               = null;

  public Object clone()
  {
    try
    {
      return super.clone();
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }



  /**
   * Send the necessary information to JNI so that it can run the workload
   */
  public static void setup_jni_context(ArrayList <WG_entry> wg_list)
  {
    RD_entry rd = RD_entry.next_rd;

    Native.allocSharedMemory();

    index_map = new HashMap(16);

    for (int i = 0; i < wg_list.size(); i++)
    {
      WG_entry wg = (WG_entry) wg_list.get(i);
      SD_entry sd = (SD_entry) wg.sd_used;
      if (sd.sd_name8 == null)
        common.failure("Missing 8-digit internal SD name for sd=%s", sd.sd_name);

      if (wg.hcontext == null)
        common.failure("hcontext is missing for " + wg.wg_name);
      if (wg.mcontext == null)
        common.failure("mcontext is missing for " + wg.wg_name);

       //common.ptod("wg.seekpct             : " + wg.seekpct            );
       //common.ptod("wg.readpct             : " + wg.readpct            );
       //common.ptod("wg.rhpct               : " + wg.rhpct              );
       //common.ptod("wg.whpct               : " + wg.whpct              );
       //common.ptod("wg.hcontext.seek_low   : " + wg.hcontext.seek_low  );
       //common.ptod("wg.hcontext.seek_high  : " + wg.hcontext.seek_high );
       //common.ptod("wg.hcontext.seek_width : " + wg.hcontext.seek_width);
       //common.ptod("wg.mcontext.seek_low   : " + wg.mcontext.seek_low  );
       //common.ptod("wg.mcontext.seek_high  : " + wg.mcontext.seek_high );
       //common.ptod("wg.mcontext.seek_width : " + wg.mcontext.seek_width);
       //common.ptod("sd.fhandle             : " + sd.fhandle            );
       //common.ptod("sd.threads_used        : " + sd.threads_used       );
       //common.plog("sd.lun                 : " + sd.lun                );
       //common.plog("sd.sd_name             : " + sd.sd_name            );


      /* Conventional SDs: */
      if (!Validate.sdConcatenation())
      {
        JniIndex jni = wg.jni_index_list.get(0);
        //wg.print_sizes();

        Native.setup_jni_context(jni.jni_index,
                                 sd.sd_name8,
                                 jni.old_stats.read_hist.getJniBucketArray(),
                                 jni.old_stats.write_hist.getJniBucketArray());
        checkIndexMap(jni.jni_index);
      }

      /* Concatenated SDs: */
      else
      {
        for (int s = 0; s < wg.sds_in_concatenation.size(); s++)
        {
          SD_entry sd2 = wg.sds_in_concatenation.get(s);
          JniIndex jni = wg.jni_index_list.get(s);

          Native.setup_jni_context(jni.jni_index,
                                   sd.sd_name8,
                                   jni.old_stats.read_hist.getJniBucketArray(),
                                   jni.old_stats.write_hist.getJniBucketArray());
          checkIndexMap(jni.jni_index);
        }
      }
    }

    /* A very quick and dirty experiment to resolve i/o coalescing issues on windows? */
    if (common.get_debug(common.TIMEBEGINPERIOD))
    {
      long rc = Native.allocBuffer(-1);
      common.ptod("TIMEBEGINPERIOD return code: " + rc);
      if (rc == 97)
        common.failure("Windows timeBeginPeriod() call failed");
    }
  }

  /**
   * Just double checking to make sure we do not pass the same index twice.
   */
  private static HashMap <Integer, Integer> index_map = null;
  private static void checkIndexMap(int index)
  {
    if (index_map.get(index) != null)
      common.failure("JNI index %d already exists", index);
    index_map.put(index, index);
  }
}


