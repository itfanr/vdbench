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
 * This class contains execution parameters used by Data Validation and
 * Journaling.
 * The values will be passed from the master to the slave as a single instance.
 *
 * I also added other 'data content' parameters here for simplicity.
 */
public class Validate implements java.io.Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private boolean journal_active     = false;
  private boolean journal_flush      = true;
  private boolean journal_recovery   = false;
  private boolean journal_rec_only   = false;
  private boolean journal_maponly    = false;
  private boolean journal_recovered  = false;

  private boolean validate           = false;
  private boolean validate_immed     = false;
  private boolean validate_nopreread = false;
  private boolean validate_time      = false;

  private int     force_error_after  = Integer.MAX_VALUE;
  private int     force_error_count  = 1;

  private int     maximum_dv_wait    = 0;
  private int     maximum_dv_errors  = 50;
  private String  dv_error_cmd       = null;
  private String  output_dir         = null;

  private double  compression        = -1;
  private long    compression_seed   = 0;

  private double  dedup_rate         = 0;      /* 0 means no deduping */
  private int     dedup_sets         = 0;
  private int     dedup_place        = 0;
  private long    dedup_seed         = 0;


  private static Validate options    = new Validate();


  /**
   * Store options that come from the master to the slave.
   */
  public static void storeOptions(Validate opt)
  {
    options = opt;
  }
  public static Validate getOptions()
  {
    return options;
  }

  public static void setForceError(int after, int count)
  {
    options.force_error_after = after;
    options.force_error_count = count;
  }
  public static boolean attemptForcedError()
  {
    return options.force_error_after != Integer.MAX_VALUE;
  }


  public static void setJournaling()
  {
    options.journal_active = true;
  }
  public static void setNoJournalFlush()
  {
    options.journal_flush = false;
  }
  public static void setJournalRecovery()
  {
    options.journal_recovery = true;
  }
  public static void setRecoveryOnly()
  {
    options.journal_rec_only = true;
  }

  /**
   * This will be reset to 'false' only when using the -l execution parameter to
   * allow for continued testing in a loop.
   **/
  public static void setJournalRecovered(boolean bool)
  {
    options.journal_recovered = bool;
  }
  public static void setValidate()
  {
    options.validate = true;
  }
  public static void setImmediateRead()
  {
    options.validate_immed = true;
  }
  public static void setNoPreRead()
  {
    options.validate_nopreread = true;
  }
  public static void setStoreTime()
  {
    options.validate_time = true;
  }
  public static void setMapOnly()
  {
    options.journal_maponly = true;
  }





  public static boolean isJournaling()
  {
    return options.journal_active;
  }
  public static boolean isJournalFlush()
  {
    return options.journal_flush;
  }
  public static boolean isJournalRecoveryActive()
  {
    return options.journal_recovery && !options.journal_recovered;
  }
  public static boolean isJournalRecovery()
  {
    return options.journal_recovery;
  }
  public static boolean isRecoveryOnly()
  {
    return options.journal_rec_only;
  }
  public static boolean isJournalRecovered()
  {
    return options.journal_recovered;
  }
  public static boolean isValidate()
  {
    return options.validate;
  }
  public static boolean isImmediateRead()
  {
    return options.validate_immed;
  }
  public static boolean isNoPreRead()
  {
    return options.validate_nopreread;
  }
  public static boolean isStoreTime()
  {
    return options.validate_time;
  }
  public static boolean isMapOnly()
  {
    return options.journal_maponly;
  }

  public static int getMaxErrorWait()
  {
    return options.maximum_dv_wait;
  }
  public static String getErrorCommand()
  {
    return options.dv_error_cmd;
  }
  public static int getMaxErrorCount()
  {
    return options.maximum_dv_errors;
  }

  public static void setOutput(String d)
  {
    options.output_dir = d;
  }
  public static String getOutput()
  {
    return options.output_dir;
  }

  public static void setCompression(double d)
  {
    options.compression = d;
  }
  public static void setCompSeed(long l)
  {
    options.compression_seed = l;
  }
  public static double getCompression()
  {
    return options.compression;
  }

  public static long getCompSeed()
  {
    return options.compression_seed;
  }

  public static void setDedupRate(double d)
  {
    options.dedup_rate = d;
  }
  public static double getDedupRate()
  {
    return options.dedup_rate;
  }
  public static int getDedupSets()
  {
    return options.dedup_sets;
  }
  public static int getDedupPlace()
  {
    return options.dedup_place;
  }
  public static void setDedupSeed(long l)
  {
    options.dedup_seed = l;
  }
  public static long getDedupSeed()
  {
    return options.dedup_seed;
  }

  /**
   * Method to force one error during data validation.
   *
   * This is kinda confusion, so here goes:
   * Using 'force_error_after' forces a Data Validation error.
   * Add to this common.DV_DEBUG_WRITE_ERROR, and this will be
   * replaced by a write error.
   */
  public static synchronized boolean forceError(SD_entry sd, FileEntry fe,
                                                long lba, long xfersize)
  {
    //common.ptod("options.force_error_after: " + options.force_error_after + " " + options.force_error_count);
    /* If we don't have any specified, just return: */
    if (options.force_error_after == Integer.MAX_VALUE)
      common.failure("You should always call attemptForcedError() before getting here.");

    /* If we still have to run for a while, return: */
    if (--options.force_error_after > 0)
      return false;

    /* We need to generate an error. Have we done enough already? */
    if (options.force_error_count == 0)
      return false;

    options.force_error_count--;

    /* Force an error: */
    String txt;
    if (sd != null)
      txt = String.format("Validate.force_error(): Error forced for "+
                          "sd=%s,lun=%s, lba 0x%08x", sd.sd_name, sd.lun, lba);
    else
      txt = String.format("Validate.force_error(): Error forced on file %s "+
                          "lba 0x%08x xfersize %d", fe.getName(), lba, xfersize);

    common.ptod(txt);
    common.ptod("options.force_error_after: " + options.force_error_after + " " + options.force_error_count);
    ErrorLog.sendMessageToMaster(txt);
    common.ptod("Amount of errors still to force: " + options.force_error_count);
    ErrorLog.sendMessageToMaster("Amount of errors still to force: " + options.force_error_count);

    return true;
  }

  /**
   * Parse the parameter file's journal options.
   * Journal implies validate=yes
   */
  public static void parseJournalOptions(Vdb_scan prm)
  {
    String[] parms = prm.alphas;
    for (int i = 0; i < parms.length; i++)
    {
      String parm = parms[i];

      if ("no".startsWith(parm))
      {
        options.journal_active = false;
        return;
      }

      Validate.setValidate();
      Validate.setJournaling();

      if ("yes".startsWith(parm))
        Validate.setJournaling();

      else if ("recover".startsWith(parm))
        Validate.setJournalRecovery();

      else if ("noflush".startsWith(parm))
        Validate.setNoJournalFlush();

      else if ("maponly".compareTo(parm) == 0)
        Validate.setMapOnly();

      else if ("only".compareTo(parm) == 0)
        Validate.setRecoveryOnly();

      else
        common.failure("Unknown keyword value for 'journal=': " + parm);
    }
  }


  public static void parseValidateOptions(Vdb_scan prm)
  {
    String[] parms = prm.alphas;
    for (int i = 0; i < parms.length; i++)
    {
      String parm = parms[i];

      if ("no".startsWith(parm))
      {
        options.validate = false;
        return;
      }

      Validate.setValidate();

      if ("yes".startsWith(parm))
        Validate.setValidate();

      else if ("read_after_write".startsWith(parm))
        Validate.setImmediateRead();

      else if ("no_preread".startsWith(parm))
        Validate.setNoPreRead();

      else if ("time".compareTo(parm) == 0)
        Validate.setStoreTime();

      else
        common.failure("Unknown keyword value for 'validate=': " + parm);
    }
  }

  public static void parseDataErrors(Vdb_scan prm)
  {
    if (prm.getNumCount() > 0)
    {
      options.maximum_dv_errors = (int) prm.numerics[0];
      if (prm.getNumCount() > 1)
        options.maximum_dv_wait = (int) prm.numerics[1];
    }

    else
    {
      options.maximum_dv_errors = 0;
      options.dv_error_cmd      = prm.alphas[0];
    }
  }

  public static void parseDedupParms(Vdb_scan prm)
  {
    if (prm.keyword.equals("dedupseed"))
      options.dedup_seed = prm.getLong();

    else if (prm.keyword.equals("deduprate"))
      options.dedup_rate = prm.getDouble();

    else if (prm.keyword.equals("dedupsets"))
      options.dedup_sets = prm.getInt();

    else if (prm.keyword.equals("dedupoffset"))
      options.dedup_place = prm.getInt();

    else
      common.failure("Invalid parameter: " + prm.keyword);

    if (options.dedup_place < 0 || options.dedup_place > 100)
      common.failure("Dedup place must be between 0 and 100: " + options.dedup_place);
    if (options.dedup_rate < 0 || options.dedup_rate > 100)
      common.failure("Dedup rate must be between 0 and 100: " + options.dedup_rate);
  }
}

