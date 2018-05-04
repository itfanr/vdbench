package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private boolean journal_active     = false;
  private boolean journal_flush      = true;
  private boolean journal_recovery   = false;
  private boolean journal_rec_only   = false;
  private boolean journal_maponly    = false;
  private boolean journal_recovered  = false;

  private boolean validate           = false;
  private boolean for_dedup          = false;
  private boolean validate_immed     = false;
  private boolean validate_nopreread = false;
  private boolean validate_time      = false;

  private boolean continue_old_map   = false;
  private boolean ignore_zero_reads  = false;
  private boolean skip_data_read     = false;

  private boolean ignore_pending     = false;

  private int     maximum_dv_wait    = 0;
  private int     maximum_dv_errors  = 50;
  private String  dv_error_cmd       = null;
  private String  output_dir         = null;

  private double  compression        = 1;
  private boolean compression_used   = false;
  private long    compression_seed   = 0;

  private int[]   psrset             = new int[0];

  private boolean sd_concatenation   = false;
  private double  abort_failed_skew  = Double.MAX_VALUE;

  private boolean dedup              = false;
  private int     dedup_unit         = 0;

  private boolean showlba            = false;

  private int patt_mb = 1;  /* Initial default. Probably should change */

  public static int FLAG_VALIDATE        = 0x0001;
  public static int FLAG_DEDUP           = 0x0002;
  public static int FLAG_COMPRESSION     = 0x0004;
  public static int FLAG_SPARE           = 0x0008;
  public static int FLAG_VALIDATE_NORMAL = 0x0010;
  public static int FLAG_VALIDATE_DEDUP  = 0x0020;
  public static int FLAG_VALIDATE_COMP   = 0x0040;
  public static int FLAG_USE_PATTERN_BUF = 0x0080;   // no longer needed/used
  public static int FLAG_NORMAL_READ     = 0x0100;
  public static int FLAG_PRE_READ        = 0x0200;
  public static int FLAG_READ_IMMEDIATE  = 0x0400;
  public static int FLAG_PENDING_READ    = 0x0800;
  public static int FLAG_PENDING_REREAD  = 0x1000;

  public static String REMOVE_DEVICE_OPTION = "remove_device";

  private static Validate options  = new Validate();


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

  /**
   * This is a generic 'all or nothing' dedup setting.
   * We don't allow some SDs with and some SDs without debug.
   * Of course, someone can specify dedupratio=1 to make that SD non-dedup
   * space-wise but not code-wise.
   */
  public static void setDedup()
  {
    options.dedup = true;
  }
  public static boolean isDedup()
  {
    return options.dedup;
  }
  public static void setDedupUnit(int u)
  {
    options.dedup_unit = u;
  }
  public static int getDedupUnit()
  {
    return options.dedup_unit;
  }

  public static void setValidateOptionsForDedup()
  {
    if (!isValidate())
    {
      setValidate();
      setValidateForDedup();
      setNoPreRead();
      options.ignore_zero_reads = true;
    }
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

  private long journal_max = Long.MAX_VALUE;
  public static void setJournalMax(long max)
  {
    options.journal_max = max;
  }
  public static long getMaxJournal()
  {
    return options.journal_max;
  }

  public static void setSkipRead()
  {
    options.skip_data_read = true;
  }
  public static boolean skipRead()
  {
    return options.skip_data_read;
  }

  public static void setIgnorePending()
  {
    options.ignore_pending = true;
  }
  public static boolean ignorePending()
  {
    return options.ignore_pending;
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
    if (Vdbmain.loop_all_runs && Validate.isJournalRecovery())
      common.failure("The '-l nnn' (loop) parameter is not allowed together with journal recovery.");
    options.validate = true;
  }
  public static void setValidateForDedup()
  {
    options.for_dedup = true;
  }


  /**
   * Return 'true' if this is data validation ONLY for dedup.
   * If REAL DV is active, return 'false'
   */
  public static boolean isValidateForDedup()
  {
    return options.for_dedup;
  }

  public static boolean isRealValidate()
  {
    return (isValidate() && !isValidateForDedup());
  }
  public static void setImmediateRead()
  {
    options.validate_immed = true;
  }

  public static void setNoPreRead()
  {
    options.validate_nopreread = true;
  }
  public static boolean isNoPreRead()
  {
    return options.validate_nopreread;
  }

  public static void setStoreTime()
  {
    options.validate_time = true;
  }
  public static boolean isStoreTime()
  {
    return options.validate_time;
  }

  public static void setMapOnly()
  {
    options.journal_maponly = true;
  }
  public static boolean isMapOnly()
  {
    return options.journal_maponly;
  }
  public static void setContinueOldMap()
  {
    options.continue_old_map = true;
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
  public static boolean isContinueOldMap()
  {
    return options.continue_old_map;
  }

  public static int getMaxErrorWait()
  {
    return options.maximum_dv_wait;
  }
  public static String getErrorCommand()
  {
    if (options.dv_error_cmd == null)
      return null;

    if (options.dv_error_cmd.equals(REMOVE_DEVICE_OPTION))
      return null;
    else
      return options.dv_error_cmd;
  }
  public static boolean removeAfterError()
  {
    if (options.dv_error_cmd == null)
      return false;

    if (options.dv_error_cmd.equals(REMOVE_DEVICE_OPTION))
      return true;
    else
      return false;
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

  public static void setCompressionRatio(double d)
  {
    options.compression = d;
    options.compression_used = true;
  }
  public static void setCompSeed(long l)
  {
    options.compression_seed = l;
  }
  public static double getCompressionRatio()
  {
    return options.compression;
  }
  public static boolean isCompressionRequested()
  {
    return options.compression_used;
  }

  // A lot of calls can be removed since we now ALWAYS have compression turned on
  // with as default compratio=1
  // I'll clean this up s o m e   t i m e
  public static boolean isCompression()
  {
    boolean rc = options.compression != -1;
    if (!rc)
      common.failure("Compression should ALWAYS be on");
    return options.compression != -1;
  }
  public static boolean ignoreZeroReads()
  {
    return options.ignore_zero_reads;
  }

  public static long getCompSeed()
  {
    return options.compression_seed;
  }

  public static void setPatternMB(int mult)
  {
    options.patt_mb = mult;
    if (mult <= 0)
      common.failure("Invalid 'pattern_buffer=%d' size, must be MB. ", mult);
  }
  public static int getPatternMB()
  {
    return options.patt_mb;
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

      if (parm.equals("no"))
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

      else if ("skip_read_all".startsWith(parm))
        Validate.setSkipRead();

      else if ("ignore_pending".startsWith(parm))
        Validate.setIgnorePending();

      else if (parm.startsWith("max="))
        Validate.setJournalMax(common.parseSize(parm.substring(4)));

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

      /* If we see 'no' anywhere, just turn it off, regardless of other options: */
      if ("no".startsWith(parm))
      {
        options.validate = false;
        return;
      }

      /* This parameter does NOT activate DV (dedup only for now) */
      if ("continue_old_map".startsWith(parm))
      {
        common.failure("'validate=continue' no longer supported");
        Validate.setContinueOldMap();
        return;
      }

      /* ANY validate option turns validation on: */
      Validate.setValidate();

      if ("yes".startsWith(parm))
        Validate.setValidate();

      else if ("read_after_write".startsWith(parm))
        Validate.setImmediateRead();

      else if ("no_preread".startsWith(parm))
        Validate.setNoPreRead();

      else if ("time".compareTo(parm) == 0)
        Validate.setStoreTime();

      else if ("ignore_zero_reads".startsWith(parm))
        options.ignore_zero_reads = true;

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

      /* Option to stop using SD after i/o error: */
      if (REMOVE_DEVICE_OPTION.startsWith(options.dv_error_cmd))
      {
        options.dv_error_cmd      = REMOVE_DEVICE_OPTION;
        options.maximum_dv_errors = 10000;
        common.ptod("'data_errors=%s' option requested. Max error count set to %d",
                    REMOVE_DEVICE_OPTION,
                    options.maximum_dv_errors);
      }
    }
  }

  /**
   * These flags determine what kind of data must be placed in each write buffer.
   * The flags are used in JNI.
   */
  public static int createDataFlag()
  {
    int flag = 0;
    String txt = "";

    /* Different Data Validation options: */
    if (Validate.isValidate())
    {
      flag |= FLAG_VALIDATE;
      txt  += "FLAG_VALIDATE ";
      if (Dedup.isDedup())
      {
        flag |= FLAG_VALIDATE_DEDUP;       // this flag not used in JNI, but,
                                           // though not used, the 'else' here
                                           // won't trigger!
        txt  += "FLAG_VALIDATE_DEDUP ";
      }
      else if (Validate.isCompression())
      {
        flag |= FLAG_VALIDATE_COMP;
        txt  += "FLAG_VALIDATE_COMP ";
      }
      else
      {
        flag |= FLAG_VALIDATE_NORMAL;
        txt  += "FLAG_VALIDATE_NORMAL ";
      }
    }

    if (Dedup.isDedup())
    {
      flag |= FLAG_DEDUP;
      txt  += "FLAG_DEDUP ";
    }

    else if (Validate.isCompression())
    {
      flag |= FLAG_COMPRESSION;
      txt  += "FLAG_COMPRESSION ";
    }

    //common.ptod("createDataFlag: %08x %s", flag, txt.toLowerCase());

    return flag;
  }


  public static void setSdConcatenation()
  {
    options.sd_concatenation = true;
  }
  public static boolean sdConcatenation()
  {
    return options.sd_concatenation;
  }

  public static void setPsrset(int[] set)
  {
    options.psrset = set;
  }
  public static int[] getPsrset()
  {
    return options.psrset;
  }

  public static void setSkewAbort(double limit)
  {
    options.abort_failed_skew = limit;
  }
  public static double getSkewAbort()
  {
    return options.abort_failed_skew;
  }

  public static void setShowLba(boolean bool)
  {
    options.showlba = bool;
  }
  public static boolean showLba()
  {
    return options.showlba;
  }
}

