package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;



class OpMkdir extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  public OpMkdir(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }

  /**
   * Create a directory.
   */
  protected boolean doOperation()
  {
    Directory dir = null;

    while (true)
    {
      if (SlaveJvm.isWorkloadDone())
      {
        //common.where();
        return false;
      }

      dir = fwg.anchor.getDir(fwg.select_random, format);
      if (dir == null)
        return false;

      /* During a format we may give up as soon as the directory exists: */
      if (format && dir.exist())
      {
        //common.ptod("DIR_EXISTS: " + dir.buildFullName());
        block(Blocked.DIR_EXISTS);

        /* During a format when (depth == 1) already exists, THIS thread is done: */
        if (dir.getDepth() == 1)
          return false;

        if (!canWeGetMoreDirectories(msg))
          return false;

        continue;
      }

      /* Lock the directory: */
      if (!dir.setBusy(true))
      {
        block(Blocked.DIR_BUSY_MKDIR);
        //common.ptod("DIR_BUSY_MKDIR: " + dir.buildFullName());
        continue;
      }

      /* If the dir exists we can blow out here already: */
      if (dir.exist())
      {
        dir.setBusy(false);
        //common.where();
        block(Blocked.DIR_EXISTS);
        if (!canWeGetMoreDirectories(msg))
          return false;
        continue;
      }

      /* Need to check 'exists' again because now we're locked: */
      // duplicate!
      /*
      if (format && dir.exist())
      {
        //common.ptod("format: " + format + " " + dir.getFullName());
        dir.setBusy(false);
        block(Blocked.DIR_EXISTS);

        if (!canWeGetMoreDirectories(msg))
          return false;

        continue;
      } */


      /* We also must lock the parent because someone  */
      /* might just want to delete this parent:        */
      // wrong: if this directory has children left we won't, and the child is
      // locked, so it can not be removed!
      // But it does not have a child YET, since we're creating it here!
      if (!dir.getParent().setBusy(true))
      {
        block(Blocked.PARENT_DIR_BUSY, dir.getFullName());
        //common.ptod("dir  busy: " + dir.getFullName());
        dir.setBusy(false);
        continue;
      }

      /* With the parent locked, we can now be sure whether it exists or not: */
      // can we do this without the lock?
      if (!dir.getParent().exist())
      {
        dir.getParent().setBusy(false);
        dir.setBusy(false);
        block(Blocked.MISSING_PARENT, dir.getFullName());
        //common.ptod("mkdir missing parent: " + dir.getFullName());

        if (format)
          common.sleep_some_usecs(200);

        continue;
      }

      /* With the directory locked, we can now be sure whether it exists or not: */
      // wrong, why check exists again, we already did that above.
      // so we'll never hit this code.
      /*
      if (dir.exist())
      {
        dir.getParent().setBusy(false);
        dir.setBusy(false);

        if (!canWeGetMoreDirectories(msg))
          return false;

        //common.ptod("DIR_EXISTS: " + dir.buildFullName());
        block(Blocked.DIR_EXISTS, dir.getDirName());

        continue;
      } */

      break;
    }

    /* Now do the work: */
    if (dir.createDir())
      fwg.blocked.count(Blocked.DIRECTORY_CREATES);

    /* No locking required, since this (the parent) is still busy: */
    if (format)
      createChildren(dir.getChildren());

    dir.getParent().setBusy(false);
    dir.setBusy(false);


    return true;
  }

  private void createChildren(Directory[] children)
  {
    if (children == null)
      return;

    for (int i = 0; i < children.length; i++)
    {
      /* Lock this new child. The loop is in case an other threads is trying */
      /* to create this directory also, but he'll fail since the parent is busy: */
      while (!children[i].setBusy(true));
      if (children[i].createDir())
        fwg.blocked.count(Blocked.DIRECTORY_CREATES);

      createChildren(children[i].getChildren());

      /* Free up this child now: */
      children[i].setBusy(false);
    }
  }

  private String[] msg =
  {
    "Anchor: " + fwg.anchor.getAnchorName(),
    "Vdbench is trying to create a new directory, but all directories",
    "already exist and no threads are currently active deleting directories."
  };


}
