package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Random;
import java.text.DecimalFormat;
import java.io.*;

/*
 * HotBand.java
 *
 * Created on Jan 12 2012,
 *
 * Purpose of the program is to generate a record off set in a simulated cache
 * hit algorithm.
 *
 * history:
 * @author Steven.A.Johnson@oracle.com
 *
 *
 */
public class HotBand
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  int next_ran = 0;      // Keep track of next array to pull random number from.
  long BandSizeInMB = 0; // maintain size of band in MB.  MB = 1000^2


  static int DEBUG = 0;

  Random r[] = new Random[5] ;           // Array of random number generators

  DecimalFormat df1= new DecimalFormat("0000");
  DecimalFormat df2= new DecimalFormat("###0.00");

  private static boolean add_one_pct_to_writes = true;

  static long startTime, Start_Time;

  // Curve for cache access.  Runs out to 10 PB
  static long CacheSizeMB [] = {0, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
    16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304,
    8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
    1073741824L, 10737418240L};
  // Probability of access curve
  static double PBCurve [] = {0, 0.087217, 0.10101, 0.132206, 0.159946,
    0.201713, 0.262602, 0.32645, 0.366902, 0.386175, 0.436941, 0.482631,
    0.523752, 0.56076, 0.594068, 0.624045, 0.651024, 0.675305, 0.697159,
    0.716827, 0.734528, 0.750459, 0.764797, 0.777701, 0.789314, 0.799767,
    0.809174, 0.81764, 1};
  // Slope of line between cache size data points
  static double Slope [] = { 183.4511752, 1159.945628, 1025.778015,
    2307.172362, 3064.644852, 4204.331611, 8019.055699, 25313.90019, 106263.1067,
    80683.52498, 179296.7222, 398437.1604, 885415.912, 1967590.916, 4372424.257,
    9716498.348, 21592218.55, 47982707.89, 106628239.8, 236951643.9, 526559208.7,
    1170131575.0, 2600292389.0, 5778427530.0, 12840950067.0, 28535444594.0, 63412099098.0,
    1313220630.0, 10737418240.0};


  //    static sajstats filesz_stats;

  public static int pb_lookup(double z)
  {
    // Z is a random number between 0-1.
    // Look up which segment the slope calculation must come from
    int pb_mid, pb_low, pb_high; // for random number generator

    // Prime the high and low for binary search
    pb_low = 0;
    pb_high = PBCurve.length-1;
    // Perform a binary search till z is >= lower edge, and z < upper edge
    while (true)
    {

      pb_mid = (pb_low+pb_high)/2;
      if (DEBUG==1)
        System.out.println("pb_lookup: z= "+ z + " pb_low = "+ pb_low
                           + " pb_mid = "+ pb_mid + " pb_high = "+ pb_high );

      // Have we found
      if ((PBCurve[pb_mid] <= z) && (z < PBCurve[pb_mid+1]))
      {
        // Found the location Return pb_mid
        return(pb_mid);
      }
      else if (PBCurve[pb_mid] < z)
      {
        pb_low = pb_mid+1;
      }
      else
      {
        pb_high = pb_mid-1;
      }
    } // While (true)

  } // pb_lookup()

  public  HotBand (long size_in_bytes)
  {
    int i;
    // Incoming size of hot band in MB
    // do basic setup.
    Start_Time = startTime = System.currentTimeMillis();
    df1= new DecimalFormat("00000");
    df2= new DecimalFormat("###0.00");
    //	filesz_stats = new sajstats();
    for (i=0; i<r.length; i++)
    {
      if (common.get_debug(common.FIXED_HOTBAND_SEED))
        r[i] = new Random(0);
      else
        r[i] = new Random();
    }
    BandSizeInMB = size_in_bytes / 1000000l;  // remember size of band MB=1000^2

    if (BandSizeInMB == 0)
      common.failure("Minimum hotband size of one megabyte is required. Size: %d bytes", size_in_bytes);

  }

  public  HotBand (long size_in_bytes, long seed)
  {
    int i;
    // vector of prime numbers to give unique seeds to each generator
    // more numbers than random generator array size for future expansion
    long seedtbl[] ={0,313,667,757,827,907,977,997};

    // don't really do anything for the creation right now.
    Start_Time = startTime = System.currentTimeMillis();
    df1= new DecimalFormat("00000");
    df2= new DecimalFormat("###0.00");
    //	filesz_stats = new sajstats();

    // create list of random number generators with seeds defined.
    for (i=0; i<r.length; i++)
    {
      r[i] = new Random(seed + seedtbl[i]);
    }
    BandSizeInMB = size_in_bytes / 1000000l;  // remember size of band MB=1000^2
    //
    if (BandSizeInMB == 0)
      common.failure("Minimum hotband size of 1,000,000 bytes is required. Size: %d bytes", size_in_bytes);


  } // HotBand (size, seed)



  /**
   * The Hotband code adds an extra one percent offset to the lba that is
   * generated.
   * This is done to allow writes to step away from the hottest portion of the
   * hotband.
   *
   * This of course gets in the way when using Dedup and hot dedup sets
   * which are in the beginning of the LUN, which prompted the recommendation
   * that for these workloads we use hotbands in the first place.
   *
   * In other words: this will only be called when using Dedup plus hot bands.
   */
  public static void doNotAddOnePct()
  {
    add_one_pct_to_writes = false;
  }



  public long get_rec_index(int rec_size_in_bytes, boolean read)
  {
    double ran_double;
    int index;
    long kb_offset, num_recs_in_band;
    long rec;

    // Find the next record to be accessed in cache.
    // Select the next random number stream
    next_ran = (next_ran+1) % r.length;

    // Flip a coin to select a record
    ran_double = r[next_ran].nextDouble();
    index = pb_lookup(ran_double);

    // Calculate MB byte offset
    if (ran_double - PBCurve[index] < 0) System.err.println("get_rec_index: ** ran_double < PBCurve["+index+"] ran_double = "+ ran_double + "  PBCurve[index] = " + PBCurve[index] );
    kb_offset =(long) (((ran_double - PBCurve[index]) * Slope[index])
                       + CacheSizeMB[index]) *1000L;
    // Convert KB offset into a record offset
    rec = ((kb_offset  * 1000L * 1000L)/rec_size_in_bytes)/(1000L * 1000L);
    if (DEBUG == 1 && rec < 0) System.out.println("get_rec_index: ** Out of range  rec= "+ rec + "  kb_offset = " + kb_offset );
    //if (DEBUG == 1 ) System.out.println("get_rec_index: ** Out of range  rec= "+ rec + "  kb_offset = " + kb_offset );

    // Make sure record is in the range of the size of the hot band
    // Calculate the number of record in the hot band
    num_recs_in_band = (long)(BandSizeInMB *1000L*1000L/rec_size_in_bytes);


    //long tmp1 = rec % num_recs_in_band;
    //long tmp2 = (rec + num_recs_in_band / 100) % num_recs_in_band;
    //common.ptod("read: %8d write %8d delta %8d #blocks %8d", tmp1, tmp2, tmp2 - tmp1, num_recs_in_band);

    // Take the record number a mod it by the number of records in the band to keep it in range
    if (!read)
    {
      if (add_one_pct_to_writes)
        rec += num_recs_in_band / 100;
    }
    rec = rec % num_recs_in_band;
    if (DEBUG == 1)
    {
      System.out.println("get_rec_index: rec= "+ rec + " ran_double = "+ran_double + " Num_recs_in_band = " + num_recs_in_band );
    }
    return(rec);
  }  // get_rec_index




  /**
   * @param args the command line arguments
   */
  public  static void main(String[] args)
  {

    HotBand hotband ;

    // Check to see if the right number of parms is passed in
    if (args.length < 3)
    {
      // if not, let user know how to use the program.
      System.out.println(" usage:    java HotBand size_mb xfersize number_of_loops  <seed>");
      System.out.println(" example : java HotBand 200 4096 250");
      System.exit(-1);
    }
    System.err.println("HotBand program to simulate cache hits Version 0.001");
    System.err.println("\nCreated by Steven.A.Johnson@Oracle.com \n");

    long lun_size = Long.parseLong(args[0]) * 1000000l;
    int  xfersize = Integer.parseInt(args[1]);
    int  loop     = Integer.parseInt(args[2]);
    int  blocks   = (int) (lun_size / xfersize);
    if (args.length == 4)
    {
      long seed;

      seed = Integer.parseInt(args[3]) ;
      hotband = new HotBand(lun_size, seed);


    }
    else
    {
      hotband = new HotBand(lun_size);
    }

    int[] counters = new int[ blocks ];

    // test code
    for (int i = 0; i < loop; i++)
    {
      long block = hotband.get_rec_index(xfersize, false);
      //System.out.println(block);
      counters[ (int) block ] ++;
    }


    common.ptod("blocks: " + blocks);
    for (int i = 0; i < 50; i++)
      common.ptod("block: %4d %6d %5.2f",
                  i,
                  counters[i],
                  counters[i] * 100. / loop);
  }



}
