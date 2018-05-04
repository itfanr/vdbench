package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;

class OpCopy extends FwgThread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public OpCopy(Task_num tn, FwgEntry fwg)
  {
    super(tn, fwg);
  }



  /**
   * Copy files.
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
      /* Find a full file to copy: */
      input_fe = findFileToRead();
      if (input_fe == null)
        return false;

      /* For file_select=seq, if the input file has been copied, we're done: */
      if (!fwg.select_random && input_fe.hasBeenCopied())
      {
        input_fe.setUnBusy();
        return false;
      }

      /* Get the target file name: */
      output_fe = fwg.target_anchor.getRelativeFile(input_fe.getFileNoInList());

      /* If target file is busy, try an other one: */
      if (!output_fe.setFileBusyExc())
      {
        block(Blocked.FILE_BUSY);
        input_fe.setUnBusy();
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

    /* Keep writing this file until it is full: */
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
        output_afe.getKeyMap().storeDataBlockInfo(output_afe.next_lba,
                                                  output_afe.xfersize,
                                                  output_afe.getAnchor().getDVMap());

      /* Now go write the same block: */ // STILL NEED KEYS
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

    input_fe.setCopied(true);
    input_afe  = input_afe.closeFile();
    output_afe = output_afe.closeFile();
    //fwg.blocked.count(Blocked.FILE_CREATES);
    FwdStats.count(operation, tod);
    fwg.blocked.count(Blocked.FILES_COPIED);

    return true;
  }
}
