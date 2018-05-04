package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

class OpCreate extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static boolean debug = common.get_debug(common.REPORT_CREATES);

  public OpCreate(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }



  /**
   * Create a file and then return with that file no longer busy.
   *
   * The Java create gives me 7 get, 2 lookup, 2 access, and 1 create
   * The JNI  create gives me 5 get, 2 lookup, 2 access, and 1 create
   *
   *
   * Note: when used for format, this doOperation() call ONLY returns when
   * all files have been done.
   * Of course 'done' appears to not always be correct.....
   */
  protected boolean doOperation()
  {
    boolean debug = false;

    // BTW: 'OpFormat', faking three different operations sees operation as read ????
    //common.ptod("fwg.getOperation(): " + Operations.getOperationText(fwg.getOperation()));

    // Note: 'findNonExistingFile' includes format=restart's 'file not full'
    FileEntry fe = findNonExistingFile();
    if (fe == null)
    {
      if (debug) common.ptod("doOperation1 false");
      return false;
    }

    //* Create the file and filler up: */
    long start = Native.get_simple_tod();
    afe = openForWrite(fe);

    /* For a 'create' operation all we need to do is close: */
    if (fwg.getOperation() == Operations.CREATE)
      afe.closeFile();

    /* Experiment creating sparse files */
    else if (SlaveWorker.work.format_run && common.get_debug(common.FILE_FORMAT_TRUNCATE))
    {
      long rc = Native.truncateFile(afe.getHandle(), afe.getFileEntry().getReqSize());
      if (rc != 0)
        common.failure("ftruncate of file %s for %,d bytes failed, error code %d",
                       afe.getFileEntry().getFullName(),
                       afe.getFileEntry().getReqSize(), rc);
      afe.closeFile();
      fwg.blocked.count(Blocked.SPARSE_CREATES);
      if (debug) common.ptod("doOperation2 true");
      return true;
    }

    else
    {
      /* Keep writing this file until it is full, but if the run is done */
      /* Do not finish the file: */
      while (true)
      {
        if (!doSequentialWrite(true))
          break;
        if (SlaveJvm.isWorkloadDone())
        {
          afe.closeFile();
          break;
        }
      }
    }

    /* File is closed by doSequentialWrite() */
    //fwg.blocked.count(Blocked.FILE_CREATES);
    FwdStats.count(Operations.CREATE, start);
    if (debug) common.ptod("doOperation3 true ");

    return true;
  }
}
