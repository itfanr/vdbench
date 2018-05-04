package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;

import Utils.Fget;
import Utils.Format;


/**
 * ReplayRun contains some housekeeping code for Replay.
 */
public class ReplayRun
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Read Storage Definition input and interpret and store parameters.
   */
  static String readParms(String first)
  {
    String str = first;
    Vdb_scan prm;
    ReplayGroup rg = null;

    try
    {
      while (true)
      {
        /* This null check exists to allow cache-only run from T3_only: */
        if (str != null)
        {
          prm = Vdb_scan.parms_split(str);

          if (prm.keyword.equals("wd")  ||
              prm.keyword.equals("fwd") ||
              prm.keyword.equals("sd")  ||
              prm.keyword.equals("fsd") ||
              prm.keyword.equals("rd"))
            break;

          if (prm.keyword.equals("rg"))
          {
            ReplayInfo.setReplay();
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
  public static void setupTraceRun()
  {
    int runs = 0;
    for (int i = 0; i < Vdbmain.rd_list.size(); i++)
    {
      RD_entry rd = (RD_entry) Vdbmain.rd_list.elementAt(i);
      if (!rd.rd_name.startsWith(SD_entry.SD_FORMAT_NAME))
        runs++;
    }
    /* Since replay can only have one run, use the last RD_entry: */
    RD_entry rd  = (RD_entry) Vdbmain.rd_list.lastElement();

    /* Lba's for each SD are rounded to 1mb: */
    roundLbas();

    /* Report the size of each replay device: */
    ReplayDevice.reportNumbers();

    /* Make sure that everything fits: */
    ReplayGroup.calculateGroupSizes();

    /* Create ReplayExtent instances fitting all replay devices in the proper SDs: */
    ReplayExtent.createExtents();

    /* During replay we need to keep track of when a replay device reaches the last i/o: */
    // obsolete!
    WG_entry.setSequentialFileCount(ReplayDevice.countUsedDevices());

    /* Mark the SDs needed active, also set proper thread count: */
    activateSDsForReplay(rd);

    /* Report observed iorate: */
    double secs = ((double) ReplayDevice.getAllDevicesLastTod()) / 1000000.;
    double iorate_found = (double) ReplayDevice.getTotalIoCount() / secs;
    common.ptod("Replay selected i/o count: %,d; traced elapsed time %.2f seconds; traced i/o rate: %.6f",
                ReplayDevice.getTotalIoCount(), secs, iorate_found);

    /* Create an arrival time adjustment factor: */
    if (rd.iorate_req == 0)
      rd.iorate_req = iorate_found;
    ReplayInfo.setAdjustValue(iorate_found / rd.iorate_req);
    common.ptod(Format.f("Replay arrival time adjustment: %.8f", ReplayInfo.getAdjustValue()));
  }


  /**
   * Round high lba for each device to the next multiple one megabyte above
   * the maximum transfersize.
   * This eases splitting replay devices across SDs
   */
  private static void roundLbas()
  {
    long   MB = 1024 * 1024l;

    /* First determine highest xfersize: */
    long max_xfer = 0;
    Vector <ReplayDevice> all_device_list = ReplayInfo.getInfo().getDeviceList();
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = all_device_list.elementAt(i);
      if (!rdev.isReportingOnly())
        max_xfer = Math.max(max_xfer, rdev.getMaxXfersize());
    }

    /* Round max xfersize: */
    long save_max = max_xfer;
    max_xfer += MB - 1;
    max_xfer  = max_xfer / MB * MB;

    /* Round high lba for each device: */
    for (int i = 0; i < all_device_list.size(); i++)
    {
      ReplayDevice rdev = all_device_list.elementAt(i);
      if (!rdev.isReportingOnly())
      {
        long max_lba = rdev.getMaxLba();
        max_lba     += max_xfer - 1;
        rdev.setMaxLba((max_xfer != 0 ) ? (max_lba + save_max) / max_xfer * max_xfer : 0);
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
      rd.setThreadsUsedForSlave(sd.sd_name, (Slave) SlaveList.getSlaveList().firstElement(), sd.threads);
      sd.setOpenForWrite();
    }
  }

  /**
   * Utility to change a parameter file for duplication
   *
   *  sd=sd001,lun=/export/vm01/linux-flat.vmdk1,replay=4294967298
   */
  public static void main2(String[] args)
  {
    int      duplicates = Integer.parseInt(args[0]);
    String[] lines      = Fget.readFileToArray(args[1]);

    for (String line : lines)
    {
      line = line.trim();
      if (line.length() == 0)
        continue;
      if (!line.startsWith("sd="))
        continue;
      if (line.contains("default"))
        continue;

      //common.ptod("line: " + line);
      line = line.substring(0, line.indexOf(" "));
      String[] split = line.split(",+");

      for (int i = 0; i < duplicates; i++)
      {
        String tmp = String.format("%s_%02d,%s,%s%02d",
                                   split[0], i,
                                   split[1], split[2], i);
        System.out.println(tmp);
      }
    }
  }

  public static void main3(String[] args)
  {
    int      duplicates = Integer.parseInt(args[0]);
    String[] lines      = Fget.readFileToArray(args[1]);

    int fileno = 0;
    for (String line : lines)
    {
      line = line.trim();
      if (line.length() == 0)
        continue;
      if (!line.startsWith("sd="))
        continue;
      if (line.contains("default"))
        continue;

      /*sd=sd001_00,lun=/vm01/linux-flat.vmdk1,replay=429496729800
        sd=sd001_01,lun=/vm01/linux-flat.vmdk1,replay=429496729801

      */

      //common.ptod("line: " + line);
      line = line.substring(0, line.indexOf(" "));
      String[] split = line.split(",+");

      for (int i = 0; i < duplicates; i++)
      {
        String tmp = String.format("%s_%02d,lun=/vm%02d/linux-flat.vmdk%d,%s%02d",
                                   split[0], i,
                                   i + 1,
                                   ++fileno,
                                   split[2], i);
        System.out.println(tmp);
        if (fileno % 100 == 0)
          System.out.println();
      }
    }
  }

  /**
   * vdbench Vdb.ReplayRun boot/boot_devices.txt 0   Duplicate#
   */
  public static void main4(String[] args)
  {
    String[] lines     = Fget.readFileToArray(args[0]);
    int      duplicate = Integer.parseInt(args[1]);

    int sdno   = 1;
    //for (int dup = 0; dup <= duplicate; dup++)
    //{
      int vmdk_fileno = 1 + lines.length * duplicate;
      for (String line : lines)
      {
        long devno = Long.parseLong(line);            // linux01486-flat.vmdk
        String tmp = String.format("sd=sd%04d,lun=/vm%02d/linux0%d-flat.vmdk,replay=%d%02d",
                                   sdno++,
                                   ((vmdk_fileno - 1) % 4) + 1,
                                   vmdk_fileno,
                                   devno, duplicate);
        vmdk_fileno++;

        System.out.println(tmp);
      }

      System.out.println();
    //}
  }

  /**
   * vdbench Vdb.ReplayRun boot/boot_devices.txt 0   Duplicate#
   */
  public static void main5(String[] args)
  {
    String[] lines     = Fget.readFileToArray(args[0]);
    int      duplicate = Integer.parseInt(args[1]);

    int sdno   = 1;
    //for (int dup = 0; dup <= duplicate; dup++)
    //{
      int vmdk_fileno = 1 + lines.length * duplicate;
      for (String line : lines)
      {
        long devno = Long.parseLong(line);            // linux01486-flat.vmdk
        String tmp = String.format("sd=sd%04d,lun=/clone%05d/original_boot,replay=%d%02d",
                                   sdno++, vmdk_fileno,
                                   devno, duplicate);
        vmdk_fileno++;

        System.out.println(tmp);
      }

      System.out.println();
    //}
  }


  /**
   * vdbench Vdb.ReplayRun boot/boot_devices.txt 0   Duplicate#
   */
  public static void main(String[] args)
  {
    String[] lines = Fget.readFileToArray(args[0]);
    int      set   = Integer.parseInt(args[1]);

    int sdno           = 1;
    int starting_clone = 1 + lines.length * set / 4;

    for (int index = 0; index < lines.length; index++)
    {
      String line     = lines[index] ;
      long   devno    = Long.parseLong(line);
      String filesys  = String.format("fs%d", (index % 4) + 1);
      long   clone_no = starting_clone + (index / 4) ;
      //4294967298
      String tmp;
      long which = ( index % 4) + 1;
      tmp = String.format("sd=sd%04d,lun=/%s/clone%05d/original_boot,replay=%d  ",
                          sdno++,
                          filesys,
                          clone_no++,  //which, // clone_no++,    // code '1' to always use the same clone
                          devno);

      System.out.println(tmp);
    }

    System.out.println();
    //}
  }
}




