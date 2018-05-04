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
import java.util.*;
import java.text.*;
import Utils.Format;


/**
 * Debugging counters.
 *
 * After further review I noticed that everything here turned out to be
 * static, instead of per FwgEntry.
 * Maybe at some point in the future it could cause locking problems. but
 * for now just leave it asis.
 *
 * Because of this however, locking on the Blocked() instance is invalid
 * because we can compromise the --static-- counters!!!
 */
class Blocked implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /* These counters are accumulated individually per slave, and then       */
  /* transferred to the master via the java socket.                        */
  /* So, on the slave this is an active set of counters, but on the master */
  /* it is the sum of the last received statistics from all slaves         */
  private static long[] stat_counter;

  private static Vector labels = new Vector(16, 0);
  private static int counters  = 0;

  public static int FILE_CREATES        = 0;
  public static int DIRECTORY_CREATES   = 1;
  public static int FILE_DELETES        = 2;
  public static int DIRECTORY_DELETES   = 3;
  public static int READ_OPENS          = 4;
  public static int WRITE_OPENS         = 5;
  public static int DIR_BUSY_RMDIR      = 6;
  public static int DIR_BUSY_MKDIR      = 7;
  public static int DIR_DOES_NOT_EXIST  = 8;
  public static int DIR_EXISTS          = 9;
  public static int DIR_STILL_HAS_CHILD = 10;
  public static int DIR_STILL_HAS_FILES = 11;
  public static int FILE_BUSY           = 12;
  public static int FILE_MAY_NOT_EXIST  = 13;
  public static int FILE_MUST_EXIST     = 14;
  public static int FILE_NOT_FULL       = 15;
  public static int GET_ATTR            = 16;
  public static int MISSING_PARENT      = 17;
  public static int PARENT_DIR_BUSY     = 18;
  public static int SET_ATTR            = 19;
  public static int spare20             = 20;
  public static int FILE_CLOSES         = 21;
  public static int SKIP_BAD_BLOCKS     = 22;
  public static int SKIP_WRITE          = 23;
  public static int FILES_COPIED        = 24;
  public static int FILES_MOVED         = 25;
  public static int FILE_WRONG_SIZE     = 26;
  public static int BAD_FILE_SKIPPED    = 27;
  public static int FILE_IS_FULL        = 28;
  public static int DIR_CREATE_SHARED   = 29;
  public static int FILE_FILL_SHARED    = 30;
  public static int FILE_DELETE_SHARED  = 31;
  public static int DIR_DELETE_SHARED   = 32;
  public static int SKIP_BAD_FILE       = 33;
  public static int spare6              = 34;

  static
  {
    init(FILE_CREATES ,       "FILE_CREATES        Files created");
    init(DIRECTORY_CREATES,   "DIRECTORY_CREATES   Directories created");
    init(FILE_DELETES,        "FILE_DELETES        Files deleted");
    init(DIRECTORY_DELETES,   "DIRECTORY_DELETES   Directories deletes");
    init(READ_OPENS,          "READ_OPENS          Files opened for read activity");
    init(WRITE_OPENS,         "WRITE_OPENS         Files opened for write activity");
    init(DIR_BUSY_RMDIR,      "DIR_BUSY_RMDIR      Directory busy (rmdir)");
    init(DIR_BUSY_MKDIR,      "DIR_BUSY_MKDIR      Directory busy (mkdir)");
    init(DIR_DOES_NOT_EXIST,  "DIR_DOES_NOT_EXIST  Directory does not exist (rmdir)");
    init(DIR_EXISTS,          "DIR_EXISTS          Directory may not exist (yet)");
    init(DIR_STILL_HAS_CHILD, "DIR_STILL_HAS_CHILD Directory still has a child directory");
    init(DIR_STILL_HAS_FILES, "DIR_STILL_HAS_FILES Directory still has files");
    init(FILE_BUSY,           "FILE_BUSY           File busy");
    init(FILE_MAY_NOT_EXIST,  "FILE_MAY_NOT_EXIST  File may not exist (yet)");
    init(FILE_MUST_EXIST,     "FILE_MUST_EXIST     File does not exist (yet)");
    init(FILE_NOT_FULL,       "FILE_NOT_FULL       File has not been completely written");
    init(GET_ATTR,            "GET_ATTR            Getattr requests");
    init(MISSING_PARENT,      "MISSING_PARENT      Parent directory does not exist (yet)");
    init(PARENT_DIR_BUSY,     "PARENT_DIR_BUSY     Parent directory busy, waiting");
    init(SET_ATTR,            "SET_ATTR            Setattr requests");
    init(spare20,             "spare20             spare20");
    init(FILE_CLOSES,         "FILE_CLOSES         Close requests");
    init(SKIP_BAD_BLOCKS,     "SKIP_BAD_BLOCKS     A block marked bad has been skipped");
    init(SKIP_WRITE,          "SKIP_WRITE          Write after bad read was skipped");
    init(FILES_COPIED,        "FILES_COPIED        Files copied");
    init(FILES_MOVED,         "FILES_MOVED         Files moved");
    init(FILE_WRONG_SIZE,     "FILE_WRONG_SIZE     Target copy/move file must be same size");
    init(BAD_FILE_SKIPPED,    "BAD_FILE_SKIPPED    Too many errors on file");
    init(FILE_IS_FULL,        "FILE_IS_FULL        File is full");
    init(DIR_CREATE_SHARED,   "DIR_CREATE_SHARED   Shared directory already exists");
    init(FILE_FILL_SHARED,    "FILE_FILL_SHARED    Shared file already exists");
    init(FILE_DELETE_SHARED,  "FILE_DELETE_SHARED  Shared file delete failed");
    init(DIR_DELETE_SHARED,   "DIR_DELETE_SHARED   Shared directory delete failed");
    init(SKIP_BAD_FILE,       "SKIP_BAD_FILE       Not enough good blocks left in file");
  };


  private static BlockTrace[] trace = new BlockTrace[256];
  static
  {
    for (int i = 0; i < trace.length; i++)
      trace[i] = new BlockTrace();
  }
  private static int offset = 0;


  public Blocked()
  {
    stat_counter = new long[counters];
  }

  private static void init(int index, String label)
  {
    counters = index + 1;
    labels.addElement(label);
  };

  public static String getLabel(int index)
  {
    return(String) labels.elementAt(index);
  }


  /**
   * See above: because everything is static we can't use the Blcoked()
   * instance lock!
   */
  private static Object count_lock = new Object();
  public void count(int index)
  {
    synchronized(count_lock)
    {
      stat_counter[index] ++;

      BlockTrace bl = trace[ offset++ % trace.length ];
      bl.ts = System.currentTimeMillis();
      bl.operation = index;
    }
  }

  private static int only_once = 0;
  public static void printTrace()
  {
    DateFormat df = new SimpleDateFormat( "HH:mm:ss.SSS" );

    synchronized (common.ptod_lock)
    {
      if (only_once++ > 0)
        return;

      int index = offset;
      for (int i = 0; i < trace.length; i++)
      {
        BlockTrace bl = trace[ index++ % trace.length];
        String tod = df.format( new Date(bl.ts) );
        common.ptod("trace: " + tod + " " +
                    labels.elementAt(bl.operation));
      }
    }
  }

  public static long[] getCounters()
  {
    return stat_counter;
  }
  public static void resetCounters()
  {
    stat_counter = new long[counters];
  }

  public static void printAndResetCounters()
  {
    printCounters(stat_counter, common.stdout);

    resetCounters();
  }

  public static void printCountersToLog()
  {
    printCounters(stat_counter, common.log_html);
  }
  public static void printCounters(long[] counters, PrintWriter pw)
  {
    /* Only print if we have asked for FWD: */
    if (SlaveJvm.isThisSlave() && !SlaveJvm.isFwdWorkload())
      return;
    if (!SlaveJvm.isThisSlave() && !Vdbmain.isFwdWorkload())
      return;

    /* To avoid confusion, if all counters are zero, report zero: */
    int total = 0;
    for (int i = 0; i < counters.length; i++)
      total += counters[i];

    if (total == 0)
    {
      common.ptod("Miscellaneous statistics: All counters are zero", pw);
      return;
    }

    /* We don't want async messages to appear in the middle: */
    synchronized (common.ptod_lock)
    {
      common.ptod("", pw);
      common.ptod("Miscellaneous statistics:", pw);
      if (Vdbmain.isWorkloadDone())
        common.ptod("(These statistics do not include activity between the last "+
                    "reported interval and shutdown.)");

      for (int i = 0; i < counters.length; i++)
      {
        if (counters[i] != 0)
          common.ptod(Format.f("%-60s", (String) labels.elementAt(i) + ":") +
                      Format.f(" %7d", counters[i]) +
                      ((!SlaveJvm.isThisSlave()) ?
                       Format.f(" %7d/sec", (counters[i] / Report.getInterval() / Report.getIntervalDuration()))
                       : ""), pw);
      }
      common.ptod("", pw);
    }
  }

  public static void accumCounters(long[] newc)
  {
    if (stat_counter.length != newc.length)
      common.failure("accumCounters() unequal counter length: " + stat_counter.length + " " + newc.length);

    for (int i = 0; i < stat_counter.length; i++)
    {
      stat_counter[i] += newc[i];

      //common.ptod("stat: " + getLabel(i) + " " + newc[i]);
    }
  }
}

class BlockTrace extends VdbObject
{
  long ts;
  int operation;
}
