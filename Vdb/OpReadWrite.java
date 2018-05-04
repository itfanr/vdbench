package Vdb;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;

import Utils.Format;


/**
 * Do a mixed read/write to a file.
 * OpRead and OpWrite either read or write a file, no mix.
 * This class will use an extra instance of OpRead and OpWrite to handle
 * reading and writing after OpReadWrite flips a coin.
 */
class OpReadWrite extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  private OpRead    opread   = null;
  private OpWrite   opwrite  = null;
  private ActiveFile last_active_file = null;

  private static Signal signal;


  public OpReadWrite(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
    signal = new Signal(30);

    opread  = new OpRead(null, fwg);
    opread.usingOpReadWrite();
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
    /* of the run any still open file can be closed.                        */
    boolean rc;

    /* Call the requested type of operation. */
    if (read_flag)
    {
      opread.afe = last_active_file;
      rc = opread.doOperation();

      opread.consecutive_blocks = 0;
      opread.last_ok_request = System.currentTimeMillis();
      afe = last_active_file = opread.afe;
    }
    else
    {
      opwrite.afe = last_active_file;
      rc = opwrite.doOperation();

      opwrite.consecutive_blocks = 0;
      opwrite.last_ok_request = System.currentTimeMillis();
      afe = last_active_file = opwrite.afe;
    }

    return rc;
  }
}
