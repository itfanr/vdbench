package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;


class OpGetAttr extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public OpGetAttr(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }


  protected boolean doOperation()
  {
    while (!SlaveJvm.isWorkloadDone())
    {
      FileEntry fe = fwg.anchor.getFile(fwg);
      if (fe == null)
        return false;

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
      long tod = Native.get_simple_tod();
      File file_ptr = new File(fe.getFullName());
      long mod = file_ptr.lastModified();
      FwdStats.count(Operations.GETATTR, tod);
      fwg.blocked.count(Blocked.GET_ATTR);

      fe.setUnBusy();

      return true;
    }

    return false;
  }


  private String[] msg =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to get attributes for a file, but there are no files",
    "available, and there are no threads currently active that can create new files."
  };
}
