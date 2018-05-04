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

import java.lang.Math;
import java.util.Random;

public class ownmath
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

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
}


