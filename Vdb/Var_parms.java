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

import java.io.*;
import java.lang.String;
import java.util.StringTokenizer;

/**
 * Handle range input parameters
 */
class Var_parms
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  static final int    VAR_DASH_N   =  -101;
  static final String VAR_DASH_A   = "-101a";
  static final int    VAR_DOUBLE_N =  -102;

  static void handle_var(Vdb_scan input)
  {
    if (input.getNumCount() > 0)
      handle_num(input);
    else
      handle_alpha(input);
  }


  private static void handle_num(Vdb_scan input)
  {
    double temp;
    double lastflag = 0;

    /* Allocate enough to handle this all: */
    double newd[] = new double[1024];
    double oldd[] = input.numerics;
    int    newix = 0;

    for ( int oldix = 0; oldix < input.getNumCount(); oldix++)
    {

      double oldval = oldd[oldix];

      /* A special value tells us what to do: */
      if ( oldval == VAR_DASH_N ||
           oldval == VAR_DOUBLE_N)
      {
        lastflag = oldval;
        continue;
      }

      /* A range is requested, either increment or double: */
      if (lastflag == VAR_DASH_N)
      {
        /* Starting 502, allow decremention doubling of values: */
        /* e.g. forx=(64k-1k,d)                                 */
        if (oldd[oldix+1] == VAR_DOUBLE_N)
        {
          /* Positive doubling: */
          if (newd[newix-1] < oldval)
          {
            for (temp = newd[newix-1] * 2; temp <= oldval; temp *= 2)
              newd[newix++] = temp;
          }

          /* Negative doubling: */
          else
          {
            for (temp = newd[newix-1] / 2; temp >= oldval; temp /= 2)
              newd[newix++] = temp;
          }
        }
        else
        {
          for (temp = newd[newix-1] + oldd[oldix+1]; temp <= oldval; temp += oldd[oldix+1])
            newd[newix++] = temp;
        }
        oldix++;
        lastflag = 0;
      }
      else
      {
        newd[newix++] = oldval;
        continue;
      }
    }
    input.numerics = newd;
    input.num_count = newix;
  }




  private static void handle_alpha(Vdb_scan input)
  {
    String lastflag = "";

    /* Allocate enough to handle this all: */
    String news[] = new String[1024];
    String olds[] = input.alphas;
    int    newix = 0;

    for ( int oldix = 0; oldix < input.alpha_count; oldix++)
    {

      String oldval = olds[oldix];
      //common.ptod("oldval: " + oldval + "/" + input.alpha_count);

      /* A special value tells us what to do: */
      if (oldval == VAR_DASH_A)
      {
        lastflag = oldval;
        continue;
      }

      /* A range is requested, increment: */
      if (lastflag.compareTo(VAR_DASH_A) == 0)
      {
        Integer temp;
        int prefixlen1 = split_name(news[newix-1]);
        int prefixlen2 = split_name(oldval);
        String prefix1 = news[newix-1].substring(0, prefixlen1);
        String prefix2 = oldval.substring(0, prefixlen2);

        //common.ptod("prefix1: " + prefix1 + "/" + news[newix-1].substring(prefixlen1));
        //common.ptod("prefix2: " + prefix2 + "/" + oldval.substring(prefixlen2));

        temp = Integer.valueOf(news[newix-1].substring(prefixlen1));
        int val1 = temp.intValue();
        temp = Integer.valueOf(oldval.substring(prefixlen2));
        int val2 = temp.intValue();


        if (prefix1.compareTo(prefix2) != 0)
          common.failure("Alpha portions of alphanumeric range values must be equal: "
                         + news[newix-1] + "/" + oldval);

        if (val1 >= val2)
          common.failure("Numeric portions of alphanumeric re values must be incremental: "
                         + news[newix-1] + "/" + oldval);

        for (int i = val1+1; i <= val2; i++)
        {
          news[newix++] = prefix1 + i;
        }

        //oldix++;
        lastflag = "";
      }
      else
      {
        news[newix++] = oldval;
        continue;
      }
    }
    input.alphas = news;
    input.alpha_count = newix;
  }



  public static int split_name(String in)
  {
    int i;
    String numbers = "0123456789";

    //common.ptod("in: " + in);

    /* Find where the last numeric digit ends: */
    for (i = in.length() - 1; i > 0; i--)
    {
      //common.ptod("i:  " + i + "///" + in.substring(i, i+1));
      if (numbers.indexOf(in.substring(i, i+1)) == -1)
        break;
    }

    //common.ptod("end i:  " + i);
    if (i < 0 || i == in.length()-1)
    {
      common.ptod("Parameter range value '" + in + "' must be alphanumeric, " +
                  "ending with a numeric value.");
      common.ptod("If this parameter however is not a range parameter, please enclose " +
                  "the parameter with double quotes.");
      common.ptod("");
      common.failure("Parameter range value '" + in + "' must be alphanumeric, " +
                     "ending with a numeric value.");
    }

    //common.ptod("num:  " + (i+1));


    return i+1;
  }
}




