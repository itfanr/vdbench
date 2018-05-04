package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

class OpDelete extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  public OpDelete(Task_num tn, FwgEntry fwg)
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

      if (!fe.setFileBusyExc())
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
        fe.cleanup();
        block(Blocked.FILE_MUST_EXIST);

        if (!canWeGetMoreFiles(msg))
          return false;

        continue;
      }

      /* The parent must be locked to allow proper maintenance */
      /* of the file count within that parent: */
      // Wrong: Parent is synchronized when count is updated!
      /*
      if (!fe.setParentBusy(true))
      {
        fe.cleanup();
        block(Blocked.PARENT_DIR_BUSY);
        continue;
      } */

      /* We're finally happy: */
      fe.deleteFile(fwg);

      fe.cleanup();

      return true;
    }

    return false;
  }

  private String[] msg =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to delete a file, but no files are available, and no",
    "threads are currently active creating new files."
  };
}
