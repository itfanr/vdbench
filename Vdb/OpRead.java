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

/**
 * operation=read processing.
 *
 * Note: Because of OpRead() and OpWrite() being used in tandom to handle
 * rdpct=xx requests, a file opened in Opread() can also be opened for WRITE.
 */
class OpRead extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public OpRead(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
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

      afe = openFile(fe);
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
      afe = openFile(fe);

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
      afe = openFile(fe);

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
