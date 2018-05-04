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

import java.io.Serializable;
import Utils.Format;

/**
 * Kstat related data
 */
public class Kstat_data extends VdbObject implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  /* This has been taken directly from struct kstat_io in sys-kstat.h:        */
  long nread;                /* number of bytes read                          */
  long nwritten;             /* number of bytes written                       */
  long reads;                /* number of read operations                     */
  long writes;               /* number of write operations                    */
  long wlentime;             /* cumulative wait length*time product           */
  long rtime;                /* cumulative run (service) time                 */
  long rlentime;             /* cumulative run length*time product            */

  long totalio;              /* Sum of reads + writes (stored in jni)         */
  long tod;                  /* micro seconds                                 */
  long elapsed;              /* Duration of above statistics                  */

  int  devices = 0;          /* Used to remember how many different devices   */
                             /* are accumulated into these counters for rtime */

  /* This is stored upon arrival of the data on the master from the slave: */
  InstancePointer pointer = null;



  /**
   * Accumulate statistics
   */
  public void kstat_accum(Kstat_data add, boolean add_elapsed)
  {

    this.nread       += add.nread   ;
    this.nwritten    += add.nwritten;
    this.reads       += add.reads   ;
    this.writes      += add.writes  ;
    this.wlentime    += add.wlentime;
    this.rtime       += add.rtime   ;
    this.rlentime    += add.rlentime;
    this.totalio     += add.totalio ;

    if (add_elapsed)
    {
      this.elapsed += add.elapsed ;
      this.devices  = add.devices;
    }
    else
    {
      this.devices++;
      this.elapsed  = add.elapsed ;
    }
  }


  public double kstat_rate()
  {
    if (elapsed == 0)
      return 0;

    return(double)totalio * 1000000.0 / elapsed;
  }

  public double kstat_megabytes()
  {
    if (elapsed == 0)
      return 0;

    return(double)(nread + nwritten) * 1000000.0 / Report.MB / elapsed;
  }

  public double kstat_readpct()
  {
    if (totalio == 0)
      return 0;

    return(double)reads * 100.0 / totalio;
  }

  public double kstat_wait()
  {
    if (elapsed == 0 || totalio == 0)
      return 0;

    return this.kstat_ioswaiting() * 1000 / this.kstat_rate();
  }

  public double kstat_svctm()
  {
    if (elapsed == 0 || totalio == 0)
      return 0;

    return this.kstat_iosrunning() * 1000 / this.kstat_rate();
  }

  public double kstat_busy()
  {
    if (elapsed == 0)
      return 0;

    int division = (devices == 0) ? 1 : devices;

    return(double) rtime * 100 / elapsed / 1000 / division;
  }

  public double kstat_ioswaiting()
  {
    if (elapsed == 0)
      return 0;

    return(double) wlentime / elapsed / 1000;
  }

  public double kstat_iosrunning()
  {
    if (elapsed == 0)
      return 0;

    return(double) rlentime / elapsed / 1000;
  }

  public double kstat_bytes()
  {
    if (totalio == 0)
      return 0;

    return(double) (nread + nwritten) / totalio;
  }

  public String toString()
  {
    String txt = "";

    txt += Format.f("nread: %4d ",    nread   );
    txt += Format.f("nwritten: %4d ", nwritten);
    txt += Format.f("reads: %4d ",    reads   );
    txt += Format.f("writes: %4d ",   writes  );
    //txt += Format.f("wlentime: %4d ", wlentime);
    //txt += Format.f("rtime: %4d ",    rtime   );
    //txt += Format.f("rlentime: %4d ", rlentime);

    return txt;
  }
}

