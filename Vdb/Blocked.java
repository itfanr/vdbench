package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  /* These counters are accumulated individually per slave, and then       */
  /* transferred to the master via the java socket.                        */
  /* So, on the slave this is an active set of counters, but on the master */
  /* it is the sum of the last received statistics from all slaves         */
  private static long[] stat_counter;

  private static ArrayList <String> labels = new ArrayList(16);

  public static int FILE_CREATES        = init("FILE_CREATES        Files created");
  public static int DIRECTORY_CREATES   = init("DIRECTORY_CREATES   Directories created");
  public static int FILE_DELETES        = init("FILE_DELETES        Files deleted");
  public static int DIRECTORY_DELETES   = init("DIRECTORY_DELETES   Directories deleted");
  public static int READ_OPENS          = init("READ_OPENS          Files opened for read activity");
  public static int WRITE_OPENS         = init("WRITE_OPENS         Files opened for write activity");
  public static int DIR_BUSY_RMDIR      = init("DIR_BUSY_RMDIR      Directory busy (rmdir)");
  public static int DIR_BUSY_MKDIR      = init("DIR_BUSY_MKDIR      Directory busy (mkdir)");
  public static int DIR_DOES_NOT_EXIST  = init("DIR_DOES_NOT_EXIST  Directory does not exist (rmdir)");
  public static int DIR_EXISTS          = init("DIR_EXISTS          Directory may not exist (yet)");
  public static int DIR_STILL_HAS_CHILD = init("DIR_STILL_HAS_CHILD Directory still has a child directory");
  public static int DIR_STILL_HAS_FILES = init("DIR_STILL_HAS_FILES Directory still has files");
  public static int FILE_BUSY           = init("FILE_BUSY           File busy");
  public static int FILE_MAY_NOT_EXIST  = init("FILE_MAY_NOT_EXIST  File may not exist (yet)");
  public static int FILE_MUST_EXIST     = init("FILE_MUST_EXIST     File does not exist (yet)");
  public static int FILE_NOT_FULL       = init("FILE_NOT_FULL       File has not been completely written");
  public static int GET_ATTR            = init("GET_ATTR            Getattr requests");
  public static int MISSING_PARENT      = init("MISSING_PARENT      Parent directory does not exist (yet)");
  public static int PARENT_DIR_BUSY     = init("PARENT_DIR_BUSY     Parent directory busy, waiting");
  public static int SET_ATTR            = init("SET_ATTR            Setattr requests");
  public static int SPARSE_CREATES      = init("SPARSE_CREATES      Sparse files created");
  public static int FILE_CLOSES         = init("FILE_CLOSES         Close requests");
  public static int SKIP_BAD_BLOCKS     = init("SKIP_BAD_BLOCKS     A block marked bad has been skipped");
  public static int SKIP_WRITE          = init("SKIP_WRITE          Write after bad read was skipped");
  public static int FILES_COPIED        = init("FILES_COPIED        Files copied");
  public static int FILES_MOVED         = init("FILES_MOVED         Files moved");
  public static int FILE_WRONG_SIZE     = init("FILE_WRONG_SIZE     Target copy/move file must be same size");
  public static int BAD_FILE_SKIPPED    = init("BAD_FILE_SKIPPED    Too many errors on file");
  public static int FILE_IS_FULL        = init("FILE_IS_FULL        File is full");
  public static int DIR_CREATE_SHARED   = init("DIR_CREATE_SHARED   Shared directory already exists");
  public static int DIR_WAIT_SHARED     = init("DIR_WAIT_SHARED     Shared directory check pending");
  public static int FILE_FILL_SHARED    = init("FILE_FILL_SHARED    Shared file already exists");
  public static int FILE_DELETE_SHARED  = init("FILE_DELETE_SHARED  Shared file delete failed");
  public static int DIR_DELETE_SHARED   = init("DIR_DELETE_SHARED   Shared directory delete failed");
  public static int SKIP_BAD_FILE       = init("SKIP_BAD_FILE       Not enough good blocks left in file");
  public static int ACCESS              = init("ACCESS              Access to file checked");

  private static int TRACE_LENGTH = 1024;  /* Must be power of two! */
  private static int TRACE_MASK   = TRACE_LENGTH -1;
  private static BlockTrace[] trace = new BlockTrace[TRACE_LENGTH];
  static
  {
    for (int i = 0; i < trace.length; i++)
      trace[i] = new BlockTrace();
  }
  private static long offset = 0;


  public Blocked()
  {
    stat_counter = new long[labels.size()];
  }

  private static int init(String label)
  {
    labels.add(label);
    return labels.size() - 1;
  };

  public static String getLabel(int index)
  {
    return labels.get(index);
  }


  /**
   * See above: because everything is static we can't use the Blocked() instance
   * lock!
   */
  private static Object count_lock = new Object();
  public void count(int index)
  {
    synchronized(count_lock)
    {
      stat_counter[index] ++;

      BlockTrace bl = trace[ (int) (offset++ & TRACE_MASK) ];
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

      long index = offset;
      for (int i = 0; i < trace.length; i++)
      {
        BlockTrace bl = trace[ (int) (index++ & TRACE_MASK) ];
        String tod = df.format( new Date(bl.ts) );
        common.ptod("trace: " + tod + " " +
                    labels.get(bl.operation));
      }
    }
  }

  public static long[] getCounters()
  {
    return stat_counter;
  }
  public static void resetCounters()
  {
    stat_counter = new long[labels.size()];
  }

  public static void printAndResetCounters()
  {
    printCounters(stat_counter, common.stdout);

    resetCounters();
  }

  public static void printCountersToLog()
  {
    if (stat_counter != null)
      printCounters(stat_counter, common.log_html);
  }
  private static void printCounters(long[] counters, PrintWriter pw)
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
        {
          if (SlaveJvm.isThisSlave())
            common.ptod("%-60s %,10d", labels.get(i) + ":", counters[i]);
          else
            common.ptod("%-60s %,10d %,10d/sec",
                        labels.get(i) + ":", counters[i],
                        counters[i] / Report.getInterval() / Report.getIntervalDuration());
        }
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

class BlockTrace
{
  long ts;
  int operation;
}
