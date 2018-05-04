package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

class OpWrite extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public OpWrite(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }

  protected boolean doOperation()
  {
    /* First get a file to fiddle with: */
    if (afe == null)
    {
      FileEntry fe;

      /* If we are restarting a format we only look for incomplete files: */
      // this check only valid if we do not use 'opfill' for format.
      if (format_restart)
        fe = findFileToWrite(OUTPUT_FILE_NOT_COMPLETE);

      /* For sequential write, take any: */
      else if (fwg.sequential_io)
        fe = findFileToWrite(OUTPUT_FILE_EITHER);

      else
        fe = findFileToWrite(OUTPUT_FILE_MUST_EXIST);

      if (fe == null)
        return false;

      if (!fwg.sequential_io && !fe.exists())
        common.failure("Should exist? " + fe.getFullName());

      afe = openForWrite(fe);
    }

    if (fwg.sequential_io)
    {
      boolean rc = doSequentialWrite(false);
      return rc;
    }
    else
    {
      boolean rc = doRandomWrite();
      return rc;
    }
  }


  protected boolean doRandomWrite()
  {
    /* Get the next transfer size: */
    afe.xfersize = fwg.getXferSize();

    /* If we just did our quota, get an other file: */
    if (afe.done_enough)
    {
      //common.ptod("switching afe.blocks_done: " + afe.blocks_done + " " + afe.bytes_done + " " + afe.bytes_to_do);
      afe = afe.closeFile();
      FileEntry fe = findFileToWrite(OUTPUT_FILE_MUST_EXIST);
      if (fe == null)
        return false;
      afe = openForWrite(fe);

      /* Get the first transfer size: */
      afe.xfersize = fwg.getXferSize();
    }

    /* Set next random lba. If that is unsuccessful, force next call to switch file: */
    if (!afe.setNextRandomLba())
    {
      afe.done_enough = true;
      return true;
    }

    afe.writeBlock();

    /* Determine if we've done enough: */
    afe.checkEnough();

    return true;
  }
}
