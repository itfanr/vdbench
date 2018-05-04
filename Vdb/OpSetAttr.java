package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;


class OpSetAttr extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public OpSetAttr(Task_num tn, FwgEntry fwg)
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

        if (!canWeGetMoreFiles(msg))
          return false;

        continue;
      }

      /* We're finally happy: */
      long now = System.currentTimeMillis();
      long tod = Native.get_simple_tod();
      File file_ptr = new File(fe.getFullName());

      if (!file_ptr.setLastModified(now + YEAR))
      {
        common.failure("Unable to do a setattr request for file: " + fe.getFullName());
      }

      FwdStats.count(Operations.SETATTR, tod);
      fwg.blocked.count(Blocked.SET_ATTR);

      fe.setUnBusy();

      return true;
    }

    return false;
  }


  private String[] msg =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to change attributes for a file, but there are no files",
    "available, and there are no threads currently active that can create new files."
  };
}
