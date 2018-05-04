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
import Utils.Bin;
import Utils.Format;
import Utils.printf;
import Utils.Flat_record;


/**
  * Replay replacement for WG_task().
  * A ReplayEntry is obtained from the in-memory Vector and it's
  * contents are sent to the Waiter Task (WT_task().
  *
  * There is only ONE ReplayRun thread running.
  */
public class ReplayRun extends Thread
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private Task_num tn;
  private SD_entry sd_used;

  static double replay_adjust = 1;

  static double GB = 1024. * 1024. * 1024.;
  static long   MB = 1024  * 1024;



  /**
   * Initialization of the Replay task.
   */
  public ReplayRun(Task_num tn_in, SD_entry sd)
  {
    tn = tn_in;
    sd_used = sd;
    tn.task_set_start_pending();
    setName("ReplayRun");
    ReplayDevice.clearMap();

    //common.ptod("starting ReplayRun: " + dev);
  }



  /**
   * Read Storage Definition input and interpret and store parameters.
   */
  static String readParms(String first)
  {
    String str = first;
    Vdb_scan prm;
    ReplayGroup rg = null;
    int seqno = 0;

    try
    {
      while (true)
      {
        /* This null check exists to allow cache-only run from T3_only: */
        if (str != null)
        {
          prm = Vdb_scan.parms_split(str);

          if (prm.keyword.equals("wd") ||
              prm.keyword.equals("fwd") ||
              prm.keyword.equals("sd") ||
              prm.keyword.equals("fsd") ||
              prm.keyword.equals("rd"))
            break;

          if (prm.keyword.equals("rg"))
          {
            //Vdbmain.setReplay(true);
            rg = new ReplayGroup(prm.alphas[0]);
          }

          else if ("devices".startsWith(prm.keyword))
          {
            if (prm.getNumCount() == 0)
              common.failure("Expecting parameters for 'devices='");
            for (int i = 0; i < prm.numerics.length; i++)
              rg.addDevice((long) prm.numerics[i]);
          }

          else
            common.failure("Unknown keyword: " + prm.keyword);
        }

        str = Vdb_scan.parms_get();
        if (str == null)
          return null;
      }

    }
    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Exception during reading of input parameter file(s).");
      common.ptod("Look at the end of 'parmscan.html' to identify the last parameter scanned.");
      common.failure("Exception during reading of input parameter file(s).");
    }

    return str;
  }

  /**
   * Read records for all requested devices into an in-memory table.
  * (Could be made smarter by not having to enlarge the table!!!)
   */
  public static void initializeTraceRun()
  {
    /* Since replay can only have one run, use the first RD_entry: */
    if (Vdbmain.rd_list.size() != 1)
      common.failure("A Replay run may have only ONE Run Definition (RD)");

    RD_entry rd  = (RD_entry) Vdbmain.rd_list.firstElement();
    String fname = RD_entry.replay_filename;
    if (fname == null)
      common.failure("No replay file specified");

    /* Lba's for each SD are rounded to 1mb: */
    roundLbas();

    /* Report the size of each replay device: */
    reportNumbers();

    /* Make sure that everything fits: */
    ReplayGroup.calculateGroupSizes();

    /* Create ReplayExtent instances fitting all replay devices in the proper SDs: */
    ReplayExtent.createExtents();

    /* During replay we need to keep track of when a replay device reaches the last i/o: */
    WG_entry.setSequentialFileCount(ReplayDevice.countUsedDevices());

    /* Mark the SDs needed active, also set proper thread count: */
    activateSDsForReplay(rd);

    /* Report observed iorate: */
    double secs = ((double) ReplayDevice.getLastTod()) / 1000000.;
    double iorate_found = (double) ReplayDevice.getTotalIoCount() / secs;
    printf pf = new printf("Replay selected i/o count: %d; traced elapsed time %.2f seconds; traced i/o rate: %.6f");
    pf.add(ReplayDevice.getTotalIoCount());
    pf.add(secs);
    pf.add(iorate_found);
    common.ptod(pf.print());

    /* Create an arrival time adjustment factor: */
    if (rd.iorate_req == 0)
      rd.iorate_req = iorate_found;
    replay_adjust = iorate_found / rd.iorate_req;
    common.plog(Format.f("Replay arrival time adjustment: %.8f", replay_adjust));
  }


  /**
   * Read the trace file into memory.
   * This 'guarantees' that the replay will not be delayed because of a possible
   * slow reading of the replay file and/or the expense of unzipping that file.
   */
  public static void readTraceFile(String fname)
  {

    boolean first_io     = true;
    long    begin_offset = 0;
    boolean fold = common.get_debug(common.REPLAY_FOLD);


    common.ptod("Reading replay file " + fname);
    common.memory_usage();
    Bin bin = new Bin(fname);
    bin.input();

    /* Read all binary records: */

    int count = 0;
    Flat_record frec = new Flat_record();
    while (bin.read_record())
    {
      if (count ++ == 10000 && common.get_debug(common.IGNORE_MISSING_REPLAY))
        break;
      frec.emport(bin);

      /* Get all fields that I want: */
      Replay_ent rent = new Replay_ent();

      rent.xfersize = frec.xfersize;
      rent.start    = frec.start;
      long device   = frec.device;
      rent.lba      = frec.lba;
      rent.read     = frec.flag == 1;
      rent.device   = device;

      //if (rent.start < replay_start)
      //   continue;
      //if (rent.start > replay_stop)
      //  break;

      if (fold)
        rent.lba = rent.lba % (8l*1024l*1024l);

      /* Some lba0 reads of 36 bytes have shown up. They are likely disguised */
      /* diagnostics reads. Xfersizes must be multiple of 512 bytes, so I am  */
      /* making sure that is the case.                                        */
      rent.xfersize = (rent.xfersize + 511) >> 9 << 9;

      /* Look for device: */
      ReplayDevice rpd = ReplayDevice.findDevice(device);

      rpd.records++;

      /* calculate stuff: */
      rpd.min_lba      = Math.min(rpd.min_lba,      rent.lba);
      rpd.max_lba      = Math.max(rpd.max_lba,      rent.lba+rent.xfersize);
      rpd.max_xfersize = Math.max(rpd.max_xfersize, rent.xfersize);

      if (rpd.reporting_only)
        continue;

      //System.out.println(first_tod + " ios: " + device + " " + rent.start + " " + rent.lba + " " + rent.xfersize + " " + rent.read);

      /* Subtract start time of first io (which allows us to ignore a period */
      /* of silence in the trace where the devices don't do anything)        */
      /* This caused some confusion when comparing a second by second replay */
      /* with the original tnfe detailed output. The replay was .3 seconds   */
      /* off, causing mismatch in second by second detail.                   */
      /* Now making sure we truncate the first tod to one second:            */
      if (first_io)
      {
        begin_offset = (rent.start / 1000000) * 1000000;
        first_io = false;
      }
      rent.start -= begin_offset;

      /* Add just created entry to the Replay array: */
      if (rpd.ios_table == null)
        rpd.ios_table = new Vector(1024);
      rpd.ios_table.add(rent);

      rpd.last_tod = rent.start;

      //System.out.println(begin_offset + " ios: " + device + " " + rent.start + " " + rent.lba + " " + rent.xfersize + " " + rent.read);
    }

    bin.close();
    common.memory_usage();
    common.ptod("Reading replay file completed: " + fname);
  }

  /**
   * Report statistics for the devices that were found
   */
  private static void reportNumbers()
  {
    /* Reporting: */
    boolean missing = false;
    Vector replay_list = ReplayDevice.getDeviceList();
    for (int i = 0; i < replay_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) replay_list.elementAt(i);

      //if (rdev.reporting_only)
      //  continue;

      if (rdev.records == 0)
      {
        missing = true;
        common.ptod("Replay device " + rdev.device_number + "; No replay records found");
        continue;
      }

      /* Because of the block0 problem (see run()) we must make sure that */
      /* the minimum length of the file is 4096 (the io to block1)         */
      /* Without this reading block1 will fail:                           */
      rdev.max_lba = Math.max(rdev.max_lba, 4096);

      common.ptod(Format.f("Replay %16d", rdev.device_number) +
                  Format.f("; ios: %7d", rdev.records) +
                  Format.f("; Lba: %6.3fg", (rdev.min_lba == Long.MAX_VALUE) ? 0 :
                           ((double) rdev.min_lba / GB)) +
                  Format.f(" - %9.3fg", ((double) rdev.max_lba / GB)) +
                  //Format.f("; Low lba: %6.3fg", ((double) rdev.low_lba / GB)) +
                  //Format.f("; High lba: %7.3fg", ((double) rdev.high_lba / GB)) +
                  Format.f("; Max xf: %6.1fk", ((double) rdev.max_xfersize / 1024.)) +
                  Format.f("; rg=%s", rdev.group.group_name));
    }

    if (missing && !common.get_debug(common.IGNORE_MISSING_REPLAY))
      common.failure("One or more requested devices found without replay records");

  }



  /**
   * Round high lba for each device to the next multiple one megabyte above
   * the maximum transfersize.
   * This eases splitting replay devices across SDs
   */
  private static void roundLbas()
  {
    /* First determine highest xfersize: */
    long max_xfer = 0;
    Vector replay_list = ReplayDevice.getDeviceList();
    for (int i = 0; i < replay_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) replay_list.elementAt(i);
      if (!rdev.reporting_only)
        max_xfer = Math.max(max_xfer, rdev.max_xfersize);
    }

    /* Round max xfersize: */
    long save_max = max_xfer;
    max_xfer += MB - 1;
    max_xfer  = max_xfer / MB * MB;

    /* Round high lba for each device: */
    for (int i = 0; i < replay_list.size(); i++)
    {
      ReplayDevice rdev = (ReplayDevice) replay_list.elementAt(i);
      if (!rdev.reporting_only)
      {
        rdev.max_lba += max_xfer - 1;
        rdev.max_lba  = (max_xfer !=0 ) ? rdev.max_lba / max_xfer * max_xfer : 0;
      }
    }

    common.ptod("");
    common.ptod("The high lba for each device has been rounded upwards to the next " +
                max_xfer + " bytes,");
    common.ptod("which is the maximum xfersize (" +
                save_max + ") rounded upwards to the next megabyte.");
    common.ptod("");

  }



  /**
   * Set the proper thread count and read/write access to use for each SD.
   */
  private static void activateSDsForReplay(RD_entry rd)
  {
    for (int i = 0; i < Vdbmain.sd_list.size(); i++)
    {
      SD_entry sd = (SD_entry) Vdbmain.sd_list.elementAt(i);
      rd.setThreadsUsedForSlave(sd, (Slave) SlaveList.getSlaveList().firstElement(), sd.threads);
      sd.open_for_write = true;
    }
  }

  /**
   * Replay task: runs in place of WG_task, but now selects ios from Vector.
   */
  public void run()
  {
    Cmd_entry cmd = new Cmd_entry();

    try
    {
      replay_adjust = SlaveWorker.work.replay_adjust;
      int found = 0;

      tn.task_set_start_complete();
      tn.task_set_running();

      table_scan:
      for (int idx = 0; idx < ReplayDevice.ios_table.size(); idx++)
      {
        if (SlaveJvm.isWorkloadDone())
          break;

        /* Get an i/o: */
        Replay_ent rent = (Replay_ent) ReplayDevice.ios_table.elementAt(idx);

        /* Find the proper ReplayDevice: */
        ReplayDevice rpd = ReplayDevice.findDevice(rent.device);

        /* When we locate the extent the xfersize may be adjusted if the */
        /* whole block does not fit. Keep looping until done.            */
        long bytes_done = 0;
        do
        {
          cmd               = new Cmd_entry();
          cmd.cmd_read_flag = rent.read;
          cmd.delta_tod     = (long) (rent.start * replay_adjust);
          cmd.cmd_xfersize  = rent.xfersize;
          cmd.cmd_hit       = false;

          /* Translate the lba into the proper SD and lba combination: */
          SD_entry sd       = rpd.findExtentForLba(cmd, rent.lba);
          cmd.cmd_wg        = sd.wg_for_sd;

          /* We now know which SD this io goes against. Since each of these */
          /* replayRun threads_used serves just one SD, throw away the rest: */
          if (sd != sd_used)
            continue table_scan;


          /* Don't touch first block: */
          /* When we have a replay device whose only reference is to block 0 */
          /* we never signal eof from WG_entry.subtract_io(). To avoid lots  */
          /* of new code let's just change the i/o to block 1                */
          if (cmd.cmd_lba == 0)
          {
            cmd.cmd_lba = cmd.cmd_xfersize = 4096;
            rent.xfersize = 4096;
          }


          /* keep track of how many ios are outstanding, so that we */
          /* later on after subtract_io() can trigger 'end of run': */
          sd.wg_for_sd.add_io(cmd);

          /* Send i/o to Waiter task: */
          try
          {
            sd.wg_for_sd.wg_to_wt.put(cmd);
          }
          catch (InterruptedException e)
          {
            break;
          }

          /* Keep track to see if the complete replay block is done: */
          rent.lba      += cmd.cmd_xfersize;
          rent.xfersize -= cmd.cmd_xfersize;

        } while ( rent.xfersize > 0);
      }

      /* Sequential EOF, write record to show eof: */
      try
      {
        cmd                  = new Cmd_entry();
        cmd.delta_tod        = Long.MAX_VALUE;
        cmd.sd_ptr           = sd_used;
        sd_used.wg_for_sd.seq_eof = true;
        sd_used.wg_for_sd.wg_to_wt.put(cmd);
      }

      // Can't do it this way since we have other ReplayRun threads still running!
      /* Sequential EOF, write record to show eof:
      try
      {
        for (int i = 0; i < SlaveWorker.sd_list.size(); i++)
        {
          SD_entry sd = (SD_entry) SlaveWorker.sd_list.elementAt(i);
          cmd                  = new Cmd_entry();
          cmd.delta_tod        = Long.MAX_VALUE;
          sd.wg_for_sd.seq_eof = true;
          sd.wg_for_sd.wg_to_wt.put(cmd);
        }
      }
      */

      catch (InterruptedException e)
      {
      }

      tn.task_set_terminating(0);
      //common.ptod("Ending Replay task ");

    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }
  }


  public static void main(String[] args)
  {
    long in = Long.parseLong(args[0]);


    long xfersize = (in + 511) >> 9 << 9;

    common.ptod("xfersize: " + in + " " + xfersize);


  }
}

class Replay_ent
{
  // Notes are about 'how to save memory if I keep insisting in having the
  // whole replay file in memory'.
  // This was originally done to avoid possible loss of performance during the
  // run caused by having to wait for the replay file block to be read.
  // However, this is in the ReplayRun thread, not in the  i/o thread, so as
  // long as there are i/o's in the fifos a little wait here should not hurt.
  // Besides, since we have to read the replay file once at the start of the
  // run, it probably is in file system cache anyway.
  //
  // So I think it would be OK to read the replay file for a second time.

  long    device;    // can be a byte if this points to an array of 256 devices.
  long    lba;
  long    start;     // if this was an int I could handle 4294 seconds replay
  int     xfersize;  // can be a short if this is in 512 bytes pieces.
  boolean read;

  // Remember, since this is tored in a Vector I need an other 8 bytes in the Vector.
  // Since this is per device I could move it to an array!
}




