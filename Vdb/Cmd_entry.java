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
import Oracle.OracleBlock;

/**
 * This class contains all information needed to start and complete an i/o
 * request.
 *
 * Warning: Don't attacxh this to VdbObject. finalize() is too expensive.
 */
public class Cmd_entry implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  long     delta_tod;          /* Relative tod that io needs to start         */
                               /* WT makes this tod to start                  */
  public long  cmd_lba;        /* lba within SD where i/o goes                */
  public long  cmd_xfersize;   /* Number of bytes for read/write              */

  public boolean  cmd_read_flag; /* True == read                              */
  boolean  cmd_hit;            /* Request must be a cache hitarea hit         */
  boolean  cmd_rand;           /* This is a random io                         */
  boolean  dv_pre_read;        /* flag indicating write changed to rewrite    */
  public   SD_entry sd_ptr;    /* Instance of SD used for this io             */

  int      dv_key;             /* Data validation key for read or write       */
                               /* Will be incremented after the read is       */
                               /* completed for the rewrite.                  */

  public WG_entry cmd_wg;      /* WG_entry that is requesting this cmd        */

  public Object   oblocks;     /* Pointer to who/what/where for oracle testing*/


  static Object cmd_lock = new Object();
  static boolean preallocated = false;


  /**
   * Allocate enough space to avoid running out of memory too fast when
   * allocation Cmd_entries.
   * Used to have a preallocated pool of Cmd_entries, but using new was more efficient.
   * Now making sure that there is 50MB worth of free space available so
   * that GC does not get called too often.
   */
  public static void cmd_create_pool()
  {
    int MB = 1024 * 1024;
    int memory = 0;
    int WANTED_MB = 128;
    int STEP      = 16;

    if (!preallocated)
    {
      for (memory = WANTED_MB; memory > 1; memory -= STEP)
      {
        try
        {
          //common.ptod("trying to allocate: " + memory);
          byte temp[] = new byte[memory * MB];
          break;
        }

        catch (Throwable t)
        {
          String bad_class = t.getClass().getName();

          if (bad_class.compareTo("java.lang.OutOfMemoryError") != 0)
            common.failure("Unknown failure during cmd_create_pool()");
        }
      }
      /*
      long total = Runtime.getRuntime().totalMemory() / MB;
      if (total < 256 - 5)
      {
        common.ptod("It is recommended to run with a minimum Java heap size of 256m");
        common.ptod("Run with 'java -Xmx256m -Xms256m' (minimum) " + total);
      }

      common.plog(Format.f("Preallocated %dk bytes for Cmd_entries", memory));
      */
    }

    preallocated = true;
  }



  public void cmd_print(SD_entry sd, String txt)
  {
    synchronized(cmd_lock)
    {
      common.ptod(txt +
                  //Format.f(" delta: %12d ", delta_tod) +
                  Format.f(" lba: %12d",   cmd_lba) +
                  Format.f(" xfer: %6d",   cmd_xfersize) +
                  " hit: " + cmd_hit +
                  " sd: " + sd.sd_name +
                  " read: " + cmd_read_flag +
                  " key: " + this.dv_key);
    }
  }


  /**
   * Note: for sequential the real next LBA is determined in JNI code.
   */
  static long last_block = -1;
  public void cmd_print_resp(SD_entry sd, String txt)
  {

    synchronized(cmd_lock)
    {
      /* Show sequentiality. Only works with threads=1 */
      if (last_block + cmd_xfersize != cmd_lba)
        txt += " s";
      else
        txt += "  ";
      last_block = cmd_lba;

      String rw = this.cmd_read_flag ? " read  " : " write ";
      common.ptod(txt + rw +
                  Format.f(" rand: %5s",    "" + cmd_rand) +
                  //Format.f(" lba: %10.2f *8",     (double) cmd_lba / 8192.) +
                  Format.f(" lba: %12d",     cmd_lba) +
                  Format.f(" xfer: %6d",     cmd_xfersize) +
                  Format.f(" dlta: %10.4f",  (double) (delta_tod - SlaveWorker.first_tod) / 1000000) +
                  " " + sd.sd_name + " key: " + dv_key);
    }
  }


  static double GB = 1024. * 1024. * 1024.;
  public String toString()
  {
    return
    sd_ptr.sd_name +
    Format.f(" %10.6f", (double) delta_tod / 1000000.) +
    Format.f(" %10.3fg", (double) cmd_lba / GB);
  }
}


