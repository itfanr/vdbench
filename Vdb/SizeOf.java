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



/**
  * The common class contains some general service methods
  *
  * Warning: some calls from code in the Utils package to similary named methods
  * here will NOT actually use the code below!
  * Need to prevent that some day.
  */
public class SizeOf
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";




  public static void main(String args[]) throws Exception
  {
    sizeof(args);
  }

  /**
   * sizeof
   *
   * Minimum 8 bytes for any even empty instance.
   **/
  public static void sizeof(String args[]) throws Exception
  {
    int loops = 1000000;
    if (args.length > 0)
      loops = Integer.parseInt(args[0]) * 1000000;
    Object[] sink = new Object[loops];

    /* Make sure we have no old garbage: */
    System.gc();
    System.gc();
    double used_at_start = Runtime.getRuntime().totalMemory() -
                           Runtime.getRuntime().freeMemory();


    for (int i = 0; i < loops; i++)
    {
      sink[i] = new Kstat_data();
    }


    System.gc();
    System.gc();
    double used_at_end = Runtime.getRuntime().totalMemory() -
                         Runtime.getRuntime().freeMemory();

    common.ptod("used_at_start: " + used_at_start / 1048576.);
    common.ptod("used_at_end:   " + used_at_end / 1048576.);
    common.ptod("used_at_end:   " + (used_at_end - used_at_start));
    common.ptod("estimated size per instance: " + (int) ((used_at_end - used_at_start) / loops));

    /* This code is here to assure that GC can not clean up the sink array yet: */
    long dummy = 0;
    for (int i = 0; i < loops; i++)
      dummy += sink[i].hashCode();
  }
}
