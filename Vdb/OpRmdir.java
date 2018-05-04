package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

class OpRmdir extends FwgThread
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  public OpRmdir(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }

  /**
   * Create a directory.
   * Search for a directory who is missing a parent, and then
   * create the highest level missing parent.
   */
  protected boolean doOperation()
  {
    Directory dir = null;

    while (true)
    {
      if (SlaveJvm.isWorkloadDone())
        return false;

      dir = fwg.anchor.getDir(fwg.select_random, format);

      if (!dir.setBusy(true))
      {
        //common.ptod("dir1: " + dir.getFullName());
        block(Blocked.DIR_BUSY_RMDIR);
        continue;
      }

      if (!dir.exist())
      {
        dir.setBusy(false);

        if (!canWeGetMoreDirectories(msg2))
          return false;

        //common.ptod("dir2: " + dir.getFullName());
        block(Blocked.DIR_DOES_NOT_EXIST, dir.getFullName());
        continue;
      }

      if (dir.anyExistingChildren())
      {
        dir.setBusy(false);
        block(Blocked.DIR_STILL_HAS_CHILD, dir.getFullName());
        continue;
      }

      /* Can't delete directory if it still has some files: */
      if (dir.countFiles(0, null) != 0)
      {
        dir.setBusy(false);
        block(Blocked.DIR_STILL_HAS_FILES, dir.getFullName());

        if (!canWeGetMoreFiles(msg))
        {
          common.where();
          return false;
        }

        if (!canWeExpectFileDeletes(msg))
        {
          common.where();
          return false;
        }

        continue;
      }


      break;
    }

    /* Now do the work: */
    dir.deleteDir(fwg);
    dir.setBusy(false);

    return true;
  }

  private String[] msg =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to delete a directory, but the directory that we are",
    "trying to delete is not empty, and there are no threads currently",
    "active that can delete those files."
  };

  private String[] msg2 =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to delete a directory, but the directory that we are",
    "trying to delete does not exist, and there are no threads currently",
    "active that create new directories."
  };

}
