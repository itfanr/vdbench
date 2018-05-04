package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

class OpClose extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  public OpClose(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }



  protected boolean doOperation()
  {
    while (!SlaveJvm.isWorkloadDone())
    {
      FileEntry fe = fwg.anchor.getFile(fwg);

      if (!fe.setFileBusy())
      {
        block(Blocked.FILE_BUSY);
        continue;
      }

      if (fe.isBadFile())
      {
        block(Blocked.BAD_FILE_SKIPPED);
        fe.setUnBusy();
        continue;
      }

      if (!fe.exists())
      {
        fe.setUnBusy();
        block(Blocked.FILE_MUST_EXIST);

        continue;
      }

      /* We're finally happy: */
      afe = openForWrite(fe);

      afe = afe.closeFile();

      return true;
    }

    return false;
  }
}


