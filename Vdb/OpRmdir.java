package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


/*
 * Author: Henk Vandenbergh.
 */

class OpRmdir extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



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
