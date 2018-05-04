package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import Utils.Format;

/**
 * Kstat related data
 */
public class Kstat_cpu implements java.io.Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

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
    return( cpu_user * 100. / cpu_total);
  }

  public double kernel_pct()
  {
    return( cpu_kernel * 100. / cpu_total);
  }

  public double getBoth()
  {
    return user_pct() + kernel_pct();
  }

  public String toString()
  {
    String fmt = "count: %3d total: %7d idle: %7d user: %7d kernel: %7d hertz: %7d";
    String txt = String.format(fmt, cpu_count, cpu_total, cpu_idle, cpu_user,
                               cpu_kernel, cpu_hertz);

    return txt;
  }
}
