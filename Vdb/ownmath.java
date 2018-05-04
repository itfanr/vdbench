package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.lang.Math;
import java.util.Random;

public class ownmath
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  //public double max = 0.0;
  //public double min = 0;
  //public long count = 0;
  //public double sum = 0.0;
  //public double sumsqr = 0.0;
  //public boolean firsttime = true;
  //public int distrib[] = new int[200];
  //public int overflow_cnt= 0;
  //public int last_position;
  static int next = 1; /* give it a seed */
  static Random r = new Random();


  public static double exponential(double average)
  {
    //
    // return an exponential distribution based on
    // incoming average.

    double  d;

    /* This line changed to eliminate possible very low values for a double */
    /* that could cause elongated wait times.                               */
    /* We do not need the huge precission provided by a double anyway       */
    d = r.nextDouble();

    /* Too low a value may generate infinite waits */
    if (d < 0.00000001)
      d = 0.00000001;

    //d = Math.random();

    return( (-(Math.log(d))*(average)));
  }

  /**
   * Round a number to the lowest multiple of 'unit'.
   * If unit is zero, return 0.
   */
  public static long round_lower(long number, long unit)
  {
    return unit == 0 ? 0 : number / unit * unit;
  }

  public static double uniform(double min, double max)
  {
    //
    // return a uniform distribution based on
    // incoming min and max number..

    double  d;

    d = r.nextDouble();
    return( (d*(max-min))+min);
  }


  public static int  rand()
  /*
   *	return a pseudo random number [0 - (2**15) - 1] (0..32767)
   */
  {

    next *= 1103515245;
    next += 12345;
    return((next >> 16) & 0x7FFF);
  }
  public static double  zero_to_one()
  /*
   *	return a *FAST* pseudo random number [0 - 1)
   * only returns 32768 unique random numbers.  Do Not use as high precision!
   */
  {
    return(rand() / 32768.0);
  }


  public static void  seed( int seed)
  /*
   *	Set the random number generator seed
   */
  {
    next =  seed;

    /* Zero is an not a valid seed value for the generator */
    if (seed == 0) next = 1;

  }

  public static double stddev(int n, long sumsqr, long sum)
  {
    if (n <= 1)
      return 0.0;

    if (((double)n*(double)(sumsqr)) < ((double)(sum)*(double)(sum)))
      return 0.0;

    return Math.sqrt((((double)n*(double)(sumsqr))-((double)(sum)*(double)(sum)))/((double)n*((double)n-1.0)));
  }



  /*
   * Generate a number based upon an poisson distribution
   * max_value - the max value to generate.
   */
  private static Random p_random = new Random(0); // for now use fixed seed:

  private static long experiment = 0;
  private static long last_100k  = 0;
  private static long offset     = 0;
  public  static long distPoisson(long max_value, double midpoint)
  {
    /* pick a number from Zero to 1 */
    double rand = p_random.nextDouble();

    /* This is the big knob for skewing the file selection                          */
    /* average number picked is 1/3 of the number of files                          */
    /* so half the access is over 1/3 of the file, other half over 2/3s of the file */
    double average = max_value / midpoint;

    if (rand == 0.0)
      return 0;


    long lvalue = ((long) (rand == 0.0 ? 0 : ((long)(-(Math.log(rand))*(average)))) % max_value);

    return lvalue;

    //    // experiment++;
    //    // if (experiment % 100000 == 0)
    //    // {
    //    //   rand        = p_random.nextDouble();
    //    //   offset  = (long) (rand * max_value);
    //    // }
    //    // lvalue      = (lvalue + offset) % max_value;
    //
    //    long middle = max_value / 2;
    //
    //    long gaussian = ((long) (p_random.nextGaussian() * max_value ));
    //
    //    //common.ptod("lvalue: " + lvalue);
    //
    //    //lvalue = Math.abs(lvalue) % max_value;
    //    //lvalue     = ((long) (rand.nextGaussian() * max_value ));
    //
    //    common.ptod("gaus: %,8d %,8d %,8d %,8d", gaussian, middle, max_value,
    //                Math.abs((middle + gaussian)) % max_value);
    //
    //    return Math.abs((middle + gaussian)) % max_value;
  }

  public static void main(String[] args)
  {
    int    file_count     = Integer.parseInt(args[0]);
    long   loops          = Integer.parseInt(args[1]) * 1000000l;
    String stars          = "**************************************************" +
                            "**************************************************";


    for (int arg = 2; arg < args.length; arg++)
    {
      int    range          = Integer.parseInt(args[arg]);
      int[]  counters       = new int[ file_count ];
      long   random_zeroes  = 0;
      long   negatives      = 0;
      int    max_count      = 0;


      int misses   = 0;

      long report = 50 * 1000 * 1000l;


      for (long index = 1; index < loops +1 ; index++)
      {
        long result = distPoisson(file_count, range);

        if (result == Long.MAX_VALUE)
        {
          random_zeroes++;
          continue;
        }

        if (result < 0)
        {
          negatives++;
          continue;
        }

        if (counters[ (int) result ] == 0)
          misses++;

        counters[ (int) result ]++;

        if ((index % report) == 0)
        {
          long speed   = 100;
          long bytes   = misses * 4096l;
          double mb    = bytes / 1024 / 1024.;
          double gb    = bytes / 1024 / 1024 / 1024.;
          common.ptod("4k Files done: %5d million; "+
                      "hits: %,15d; "+
                      "hit%%: %6.2f "+
                      "unique_gb: %7.2f "+
                      "speed: %3d mb/sec "+
                      "elapsed: %5d",
                      index / 1000000,
                      (index - misses),
                      ((index - misses) * 100.) / index,
                      gb,
                      speed,
                      (int) (mb / speed)
                     );
        }
      }

      common.ptod("range:         %,12d ", range);
      common.ptod("loops:         %,12d ", loops);
      common.ptod("negatives:     %,12d ", negatives);
      common.ptod("random_zeroes: %,12d ", random_zeroes);

      /* Find highest count: */
      for (int i = 0; i < file_count; i++)
        max_count = Math.max(max_count, counters[i]);

      double cum_pct = 0;
      for (int i = 0; i < file_count; i++)
      {
        if (i > 30)
          break;
        int count  = counters[i];
        double pct = count * 100. / loops;
        cum_pct   += pct;

        double chars = count * 100 / max_count;
        String st    = stars.substring(0, (int) (chars * stars.length() / 100.));

        String txt = String.format("[%3d]: %8d  %5.2f%%  %6.2f%% %s", i, count,
                                   pct, cum_pct, st);
        System.out.println(txt);
      }
    }

  }
}

