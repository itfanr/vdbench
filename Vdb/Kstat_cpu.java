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

import Utils.Format;

/**
 * Kstat related data
 */
public class Kstat_cpu extends VdbObject implements java.io.Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  /* This has been accumulated from kstat or windows PDH counters: */
  long cpu_count;
  long cpu_total;
  long cpu_idle;
  long cpu_user;
  long cpu_kernel;
  long cpu_wait;
  long cpu_hertz;  /* For windows: QueryPerformanceFrequency  */
                   /* For Solaris: Number of ticks per second */


  void cpu_accum(Kstat_cpu in)
  {
    cpu_count       = in.cpu_count  ;
    cpu_total      += in.cpu_total  ;
    cpu_idle       += in.cpu_idle   ;
    cpu_user       += in.cpu_user   ;
    cpu_kernel     += in.cpu_kernel ;
    cpu_wait       += in.cpu_wait   ;
  }


  public void cpu_delta(Kstat_cpu nw, Kstat_cpu old)
  {
    cpu_count      = nw.cpu_count;
    cpu_total      = nw.cpu_total  - old.cpu_total  ;
    cpu_idle       = nw.cpu_idle   - old.cpu_idle   ;
    cpu_user       = nw.cpu_user   - old.cpu_user   ;
    cpu_kernel     = nw.cpu_kernel - old.cpu_kernel ;
    cpu_wait       = nw.cpu_wait   - old.cpu_wait   ;
    //common.ptod("kernel: " + cpu_kernel + " nw: " + nw.cpu_kernel + " old: " + old.cpu_kernel);
  }


  public void cpu_copy(Kstat_cpu in)
  {
    cpu_count      = in.cpu_count      ;
    cpu_total      = in.cpu_total      ;
    cpu_idle       = in.cpu_idle       ;
    cpu_user       = in.cpu_user       ;
    cpu_kernel     = in.cpu_kernel     ;
    cpu_wait       = in.cpu_wait       ;
  }


  public double user_pct()
  {
    return ( cpu_user * 100. / cpu_total);
  }

  public double kernel_pct()
  {
    return ( cpu_kernel * 100. / cpu_total);
  }

  public String toString()
  {
    String txt = "Kstat_cpu: ";
    txt += Format.f("count:  %6d ", cpu_count );
    txt += Format.f("total:  %6d ", cpu_total );
    txt += Format.f("idle:   %6d ", cpu_idle  );
    txt += Format.f("user:   %6d ", cpu_user  );
    txt += Format.f("kernel: %6d ", cpu_kernel);
    //txt += Format.f("wait:   %6d ", cpu_wait  );
    txt += Format.f("hertz:  %6d ", cpu_hertz );

    return txt;
  }
}
