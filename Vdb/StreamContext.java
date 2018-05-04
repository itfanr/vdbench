package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;


/**
 * This contains context information when allowing each i/o thread (IO_task())
 * to have his own sequential stream.
 * This was introduced to make it easier to find out a performance curve of
 * running 'n' sequential streams against the same file/lun.
 */
class StreamContext implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private String sd_name       = null;
  private long   low_lba       = 0;
  private long   next_lba      = -1;
  private long   high_lba      = 0;
  private long   stream_size   = 0;
  private int    which_context = 0;


  public StreamContext(SD_entry sd, int stream_count, int streamno)
  {
    /* Calculate size of each stream, truncated to xfersize: */
    stream_size  = sd.end_lba / stream_count;
    stream_size -= stream_size % sd.getMaxSdXfersize();

    int block0 = (sd.canWeUseBlockZero()) ? 0 : sd.getMaxSdXfersize();

    low_lba       = stream_size * streamno + block0;
    high_lba      = low_lba + stream_size - block0;
    next_lba      = -1;
    which_context = streamno;
    sd_name       = sd.sd_name;

    /* The true stream size must be lowered if we can't use block0: */
    // But it gets confusing when reported, so just leave it alone.
    //stream_size -= block0;

    if (high_lba > sd.end_lba)
    {
      common.ptod("stream_count: " + stream_count);
      common.ptod("streamno:     " + streamno);
      common.ptod("sd.end_lba:   " + sd.end_lba);
      common.ptod("high_lba:     " + high_lba);
      common.failure("Invalid StreamContext values");
    }
  }


  public String toString()
  {
    String txt = String.format("stream=%d: sd=%s; low: %8s; high: %8s size: %8s",
                               which_context,
                               sd_name,
                               FileAnchor.whatSize(low_lba),
                               FileAnchor.whatSize(high_lba),
                               FileAnchor.whatSize(stream_size));
    return txt;
  }


  /**
   * Get the next sequential lba.
   */
  public synchronized long getNextSequentialLba(Cmd_entry cmd)
  {
    /* First time setting: */
    if (next_lba < 0)
      next_lba = low_lba;

    /* If this new block won't fit, reset to the beginning: */
    if (next_lba + cmd.cmd_xfersize > high_lba)
    {
      /* Except for when doing seekpct=eof: */
      if (cmd.cmd_wg.seekpct < 0)
        return -1;
      next_lba = low_lba;
    }

    long lba_to_use = next_lba;

    /* Prepare for the next go-around: */
    next_lba += cmd.cmd_xfersize;

    //common.ptod("getNextSequentialLba: context: %2d %12d high_lba: %12d use_lba: %12d",
    //            which_context, low_lba, high_lba, lba_to_use);

    return lba_to_use;
  }
}


