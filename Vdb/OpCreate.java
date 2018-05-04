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

class OpCreate extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static boolean debug = common.get_debug(common.REPORT_CREATES);

  public OpCreate(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }



  /**
   * Create a file and then return with that file no longer busy.
   *
   * The Java create gives me 7 get, 2 lookup, 2 access, and 1 create
   * The JNI  create gives me 5 get, 2 lookup, 2 access, and 1 create
   */
  protected boolean doOperation()
  {
    FileEntry fe = findNonExistingFile();
    if (fe == null)
      return false;

    //* Create the file and filler up: */
    long start = Native.get_simple_tod();
    afe = openForWrite(fe);

    /* For a 'create' operation all we need to do is close: */
    if (fwg.getOperation() == Operations.CREATE)
      afe.closeFile();

    else
    {
      /* Keep writing this file until it is full: */
      while (true)
      {
        if (!doSequentialWrite(true))
          break;
      }
    }

    /* File is closed by doSequentialWrite() */
    fwg.blocked.count(Blocked.FILE_CREATES);
    FwdStats.count(Operations.CREATE, start);

    return true;
  }
}
