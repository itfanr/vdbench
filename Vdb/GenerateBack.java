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

import java.util.*;


/**
 * This class handles callback functions for an i/o request to be handed
 * back to Vdbench.
 */
class GenerateBack
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private SD_entry sd;
  private WG_entry wg;
  private Fifo wg_to_wt;
  private FifoList wt_to_iot;

  public GenerateBack(SD_entry sd, WG_entry wg)
  {
    this.sd        = sd;
    this.wg        = wg;
    this.wg_to_wt  = wg.wg_to_wt;
    this.wt_to_iot = sd.wt_to_iot;
  }


  public boolean isWorkloadDone()
  {
    return SlaveJvm.isWorkloadDone();
  }

  public long getLunSize()
  {
    return sd.end_lba;
  }


  /**
   * Send an i/o request to Vdbench to be queued up for
   * execution at the proper start_ts time.
   */
  public boolean scheduleRequest(long       start_ts,
                                 long       lba,
                                 int        xfersize,
                                 boolean    read_flag)
  {
    return queueRequest(start_ts, lba, xfersize, read_flag);
  }


  /**
   * Send an i/o request to Vdbench to be queued up for the i/o threads
   * immediately.
   */
  public boolean startRequest(long       lba,
                              int        xfersize,
                              boolean    read_flag)
  {
    return queueRequest(-1, lba, xfersize, read_flag);
  }

  /**
   * Send the i/o request to either the Waiter Task (WT_Task) or directly
   * to the I/O task (IO_Task)
   */
  private boolean queueRequest(long       start_ts,
                               long       lba,
                               int        xfersize,
                               boolean    read_flag)
  {
    if (lba == 0)
      common.failure("GenerateBack.storeRequest(): lba=0 may never be read or written");

    if (lba % 512 != 0)
      common.failure("GenerateBack.storeRequest(): lba must be multiple of 512: " + lba);

    if (xfersize % 512 != 0)
      common.failure("GenerateBack.storeRequest(): xfersize must be multiple of 512: " + xfersize);

    Cmd_entry cmd     = new Cmd_entry();
    cmd.delta_tod     = start_ts;
    cmd.cmd_lba       = lba;
    cmd.cmd_xfersize  = xfersize;
    cmd.cmd_read_flag = read_flag;
    cmd.cmd_rand      = true;
    cmd.sd_ptr        = sd;
    cmd.cmd_wg        = wg;

    /* Keep track of number of generated ios to handle EOF later: */
    wg.add_io(cmd);

    try
    {
      if (start_ts < 0)
        wt_to_iot.put(cmd, cmd.cmd_wg.getpriority());   /* Directly to the device */
      else
        wg_to_wt.put(cmd);    /* Through the waiter queue */
    }

    catch (InterruptedException e)
    {
      return false;
    }

    return true;
  }
}
