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

import java.util.Vector;

import Utils.Format;

/*
 * Author: Henk Vandenbergh.
 */

/**
 * Do a mixed read/write to a file.
 * OpRead and OpWrite either read or write a file, no mix.
 * This class will use an extra instance of OpRead and OpWrite to handle
 * reading and writing after OpReadWrite flips a coin.
 */
class OpReadWrite extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private OpRead    opread   = null;
  private OpWrite   opwrite  = null;
  private ActiveFile last_active_file = null;

  private static Signal signal;


  public OpReadWrite(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
    signal = new Signal(30);

    opread  = new OpRead(null, fwg);
    opwrite = new OpWrite(null, fwg);
  }



  /**
   * Decide whether to read or write and then pass on the request.
   */
  protected boolean doOperation() throws InterruptedException
  {
    /* Decide read or write? */
    boolean read_flag;

    if (fwg.readpct == 100)
      read_flag = true;
    else if (fwg.readpct == 0)
      read_flag = false;
    else if (ownmath.zero_to_one() * 100 < fwg.readpct)
      read_flag = true;
    else
      read_flag = false;


    /* The ActiveFile instance must be passed between the two, it must also */
    /* be stored in the current OpReadWrite instance so that at the end     */
    /* of the run any still open file can be close.                         */
    boolean rc;

    /* Call the requested type of operation. */
    if (read_flag)
    {
      opread.afe = last_active_file;
      rc = opread.doOperation();
      afe = last_active_file = opread.afe;
    }
    else
    {
      opwrite.afe = last_active_file;
      rc = opwrite.doOperation();
      afe = last_active_file = opwrite.afe;
    }

    return rc;
  }
}
