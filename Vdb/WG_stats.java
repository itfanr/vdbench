package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;

public class WG_stats
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";



  private static Histogram static_read_hist  = new Histogram("default");
  private static Histogram static_write_hist = new Histogram("default");
  private static Histogram static_total_hist = new Histogram("default");

  /**
   * Get JNI statistics for each workload.
   * Delta statistics will be stored in wg.jni_index_list
   */
  public static void get_jni_statistics(boolean workload_done)
  {

    long tod = Native.get_simple_tod();
    SdStats  sd_stats = new SdStats();

    long[] read_hist  = static_read_hist.getJniBucketArray();
    long[] write_hist = static_write_hist.getJniBucketArray();

    /* JNI statistics are maintained per workload.  */
    /* One workload is always only one SD           */
    for (int i = 0; i < SlaveWorker.work.wgs_for_slave.size(); i++)
    {
      WG_entry wg = (WG_entry) SlaveWorker.work.wgs_for_slave.get(i);

      for (int s = 0; s < wg.jni_index_list.size(); s++)
      {
        JniIndex jni  = wg.jni_index_list.get(s);

        String line = Native.get_one_set_statistics(jni.jni_index,
                                                    read_hist,
                                                    write_hist);

        /* In 503 I opted to have statistics passed in ascii. It's much easier */
        /* and since it is done at worst once a second the overhead is little: */
        String[] split = line.trim().split(" +");
        sd_stats.last_ts     = tod;
        sd_stats.reads       = Long.parseLong(split[ 1  ]);
        sd_stats.r_resptime  = Long.parseLong(split[ 3  ]);
        sd_stats.r_resptime2 = Long.parseLong(split[ 5  ]);
        sd_stats.r_max       = Long.parseLong(split[ 7  ]);
        sd_stats.r_bytes     = Long.parseLong(split[ 9  ]);
        sd_stats.r_errors    = Long.parseLong(split[ 11 ]);

        sd_stats.writes      = Long.parseLong(split[ 13 ]);
        sd_stats.w_resptime  = Long.parseLong(split[ 15 ]);
        sd_stats.w_resptime2 = Long.parseLong(split[ 17 ]);
        sd_stats.w_max       = Long.parseLong(split[ 19 ]);
        sd_stats.w_bytes     = Long.parseLong(split[ 21 ]);
        sd_stats.w_errors    = Long.parseLong(split[ 23 ]);

        sd_stats.rtime       = Long.parseLong(split[ 25 ]);
        sd_stats.rlentime    = Long.parseLong(split[ 27 ]);


        /* Since histograms are reported ONLY after normal completion, */
        /* why even bother to send them to the master? With loads of   */
        /* SDs and workloads that's a heck of a lot of data.           */
        /*                                                             */
        /* Note: This results in us at this time not being able to     */
        /* distinguish between ios done during the warmup period.      */
        jni.dlt_stats = new SdStats();
        if (!workload_done)
          jni.dlt_stats.clearHistograms();

        else
        {
          /* Store the Histogram statistics: */
          sd_stats.read_hist  = new Histogram("default");
          sd_stats.write_hist = new Histogram("default");
          sd_stats.histogram  = new Histogram("default");
          sd_stats.read_hist .storeJniBucketArray(read_hist );
          sd_stats.write_hist.storeJniBucketArray(write_hist);

          /* JNI no longer creates a read+write histogram. Just create one: */
          sd_stats.histogram = (Histogram) sd_stats.read_hist.clone();
          sd_stats.histogram.accumBuckets(sd_stats.write_hist);
        }

        /* Change statistics to delta values: */
        jni.dlt_stats.stats_delta(sd_stats, jni.old_stats);
        jni.old_stats.stats_copy(sd_stats);
      }
    }
  }
}
