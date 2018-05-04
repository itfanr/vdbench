package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

/**
 * operation=read processing.
 *
 * Note: Because of OpRead() and OpWrite() being used in tandom to handle
 * rdpct=xx requests, a file opened in Opread() can also be opened for WRITE.
 */
class OpRead extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private boolean this_is_OpReadWrite = false;

  public OpRead(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }

  public void usingOpReadWrite()
  {
    this_is_OpReadWrite = true;
  }

  protected boolean doOperation()
  {
    /* First get a file to fiddle with: */
    if (afe == null)
    {
      FileEntry fe = findFileToRead();
      if (fe == null)
      {
        return false;
      }

      if (this_is_OpReadWrite)
        afe = openForWrite(fe);
      else
        afe = openForRead(fe);
    }

    if (fwg.sequential_io)
      return doSequentialRead();
    else
      return doRandomRead();
  }


  protected boolean doSequentialRead()
  {
    /* Get the next transfer size: */
    afe.xfersize = fwg.getXferSize();

    /* If we just reached EOF, get an other file: */
    if (!afe.setNextSequentialRead() || afe.done_enough)
    {
      afe = afe.closeFile();

      FileEntry fe = findFileToRead();
      if (fe == null)
        return false;

      if (this_is_OpReadWrite)
        afe = openForWrite(fe);
      else
        afe = openForRead(fe);

      /* Get the first transfer size: */
      afe.xfersize = fwg.getXferSize();
      afe.setNextSequentialRead();
    }

    afe.readBlock();

    /* Determine if we've done enough: */
    afe.checkEnough();

    return true;
  }


  protected boolean doRandomRead()
  {
    /* Get the next transfer size: */
    afe.xfersize = fwg.getXferSize();

    /* If we just did our quota, get an other file: */
    if (afe.done_enough)
    {
      //common.ptod("switching afe.blocks_done: " + afe.blocks_done + " " + afe.bytes_done + " " + afe.bytes_to_do);
      afe = afe.closeFile();
      FileEntry fe = findFileToRead();
      if (fe == null)
        return false;

      if (this_is_OpReadWrite)
        afe = openForWrite(fe);
      else
        afe = openForRead(fe);

      /* Get the first transfer size: */
      afe.xfersize = fwg.getXferSize();
    }

    /* Set next random lba. If that is unsuccessful, force next call to switch file: */
    if (!afe.setNextRandomLba())
    {
      afe.done_enough = true;
      return true;
    }

    afe.readBlock();

    /* Determine if we've done enough: */
    afe.checkEnough();

    return true;
  }
}
