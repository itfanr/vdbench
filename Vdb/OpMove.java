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

import java.io.*;

class OpMove extends FwgThread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public OpMove(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }



  /**
   * Move files.
   *
   * The output file that we look for MUST have the correct size
   * because that determines the length of the DV_map.
   */
  protected boolean doOperation() throws InterruptedException
  {
    FileEntry input_fe;
    FileEntry output_fe;

    while (true)
    {
      /* Find a full file to move: */
      input_fe = findFileToRead();
      if (input_fe == null)
        return false;

      /* Get the target file name: */
      common.failure("xxx");  // there's some work to do!!
      output_fe = fwg.target_anchor.getRelativeFile(input_fe.getFileNoInList());

      /* If target file is busy, try an other one: */
      if (!output_fe.setBusy(true))
      {
        block(Blocked.FILE_BUSY);
        input_fe.setBusy(false);
        continue;
      }
      break;
    }

    /* First we need to delete the output file: */
    if (output_fe.exists())
      output_fe.deleteFile(fwg);

    /* Now start copying: */
    long tod = Native.get_simple_tod();
    ActiveFile input_afe  = openForRead(input_fe);
    ActiveFile output_afe = openForWrite(output_fe);


    /* Keep writing this file until we're done: */
    while (true)
    {
      /* Get the next transfer size: */
      input_afe.xfersize = fwg.getXferSize();

      /* If we just reached EOF, stop */
      if (!input_afe.setNextSequentialRead())
        break;

      input_afe.readBlock();

      /* Xfersize may have been truncated by setnextSequentialLba: */
      output_afe.xfersize = input_afe.xfersize;
      output_afe.next_lba = input_afe.next_lba;

      /* Get the list of keys for the output file: */
      if (Validate.isValidate())
        output_afe.getKeyMap().getKeysFromMap(output_afe.next_lba, output_afe.xfersize);

      /* Now go write the same blocked: */ // STILL NEED KEYS
      // the data will not really be copied with DV, a new block with the old keys
      // will be created!
      // by the way, the DV read of old data from the output_afe must use the old
      // DV keys, while the next write must use the just read_for_copy keys.
      // Is that really needed? Could we just use regular keys++???
      // To stay 'in sync' with the 'copy', use the OLD keys.
      //
      /* Decided it was cleaner to use the keys from the output file, which, */
      /* since it was just deleted, will all start with key=0: */
      output_afe.writeBlock();
    }

    //common.ptod("Moved: " + input_afe.fe.getName() + " " + output_afe.fe.getName());

    input_afe  = input_afe.closeFile(true);
    output_afe = output_afe.closeFile();

    fwg.blocked.count(Blocked.FILE_CREATES);
    FwdStats.count(operation, tod);
    fwg.blocked.count(Blocked.FILES_MOVED);

    return true;
  }
}
