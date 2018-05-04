package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;


/**
 * This class helps us correct any problems with lun names and sd names not
 * matching across hosts.
 *
 * Examples:
 * - user specifies DIFFERENT lun names for an Sd:
 *   - 'accidentally', resulting in one SD using two different luns.
 *   - accidentally, the user specifies all proper lun names, but the order,
 *     which is important for Concatenation, is messaed up.
 *     One host can have sd1+sd2 concatenated, and an other host sd2+sd1
 *
 * The master writes markers in the last 4k of each lun.
 * Those markers then will be read on each slave, and the lun names will be
 * compared and corrected if discrepancies found.
 *
 * Marker content:
 * - relative SD number as found by SD_entry parameter parser
 * - Time of day in milliseconds when markers were written.
 */
public class ConcatMarkers
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static long marker_tod = 0;

  private static int MARKER1 = 0X434F4E43;  // CONC
  private static int MARKER2 = 0X4D41524B;  // MARK

  private static Host local_host = null;

  public static long getMarkerTod()
  {
    return marker_tod;
  }


  /**
   * The specified lun names for each host must match.
   * No host is allowed to have more or less lun names, anything else is an
   * error.
   */
  public static void checkLunCounts(HashMap <String, HashMap <String, LunInfoFromHost>> hosts_with_work)
  {
    /* Make sure all hosts have luns: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = Host.getDefinedHosts().get(i);
      HashMap <String, LunInfoFromHost> luns_on_host = hosts_with_work.get(host.getLabel());
      if (luns_on_host == null)
        error("ConcatMarkers: No lun definitions found for host=%s", host.getLabel());
    }


    /* Those lun counts must be the same: */
    Host first_host = Host.getDefinedHosts().get(0);
    HashMap <String, LunInfoFromHost> first_host_luns = hosts_with_work.get(first_host.getLabel());
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = Host.getDefinedHosts().get(i);
      HashMap <String, LunInfoFromHost> luns_on_host = hosts_with_work.get(host.getLabel());
      if (first_host_luns.size() != luns_on_host.size())
      {
        common.ptod("");
        common.ptod("ConcatMarkers: Unmatched lun count.");

        for (i = 0; i < Host.getDefinedHosts().size(); i++)
        {
          host = Host.getDefinedHosts().get(i);
          luns_on_host = hosts_with_work.get(host.getLabel());
          common.ptod("ConcatMarkers: host=%s has %d luns",
                      host.getLabel(), luns_on_host.size());
        }
        error("ConcatMarkers: Unmatched lun count.");
      }
    }
  }


  /**
   * Write concatenation markers at the start of the last 4k block in each lun.
   */
  public static void writeMarkers()
  {
    marker_tod = System.currentTimeMillis();

    HashMap <String, String> duplicates_map = new HashMap(8);

    /* Find out which Host is our localhost. */
    /* Concatenation MUST run a workload also on the master system: */
    local_host = null;
    for (Host host : Host.getDefinedHosts())
    {
      if (host.getSlaves().get(0).isLocalHost())
      {
        local_host = host;
        break;
      }
    }

    if (local_host == null)
      error("ConcatMarkers: Unable to find local host for writing markers");

    /* The parameter file MUST include a write workload.                        */
    /* We don't want to write markers if the user thinks he is doing only READS */
    for (SD_entry sd : Vdbmain.sd_list)
    {
      if (sd.concatenated_sd)
        continue;
      if (!sd.sd_is_referenced)
        continue;
      if (!sd.open_for_write)
        error("ConcatMarkers: Writing of markers for sd=%s can not be done for read-only workloads.",
              sd.sd_name);
    }



    /* Allocate data buffer: */
    long  buffer = Native.allocBuffer(4096);
    int[] array  = new int[ 4096 / 4 ];

    /* Go through all REAL SDs: */
    for (SD_entry sd : Vdbmain.sd_list)
    {
      if (sd.concatenated_sd)
        continue;
      if (!sd.sd_is_referenced)
        continue;

      String lun     = local_host.getLunNameForSd(sd);

      if (duplicates_map.put(lun, lun) != null)
        error("ConcatMarkers: host=%s,lun=%s defined more than once.", local_host.getLabel(),lun );

      /* Borrow some slave code to figure out what the size is: */
      long size = sd.end_lba;
      if (size == 0)
      {
        LunInfoFromHost linfo = new LunInfoFromHost();
        linfo.lun             = lun;
        if (lun.startsWith("/dev/") || lun.startsWith("\\\\.\\"))
          linfo.getRawInfo();
        else
          linfo.getFileInfo();
        size = linfo.lun_size;
        if (size == 0)
          error("ConcatMarkers: Unable to obtain lun size for '%s'", lun);
      }


      long   fhandle = Native.openFile(lun, 1);
      if (fhandle == -1)
        error("ConcatMarkers: Open for lun '%s' failed", lun);
      //
      //  long size = sd.end_lba;
      //  if (size == 0)
      //  {
      //    if (common.onLinux())
      //      size = Linux.getLinuxSize(lun);
      //    else
      //      size = Native.getSize(fhandle, lun);
      //  }

      /* Round size downward to 4k and subtract 4k */
      long lba = (size / 4096) * 4096 - 4096;

      /* Create marker: */
      array[0] = MARKER1;
      array[1] = MARKER2;
      array[2] = sd.relative_sd_num;
      array[3] = (int) (marker_tod >> 32);
      array[4] = (int) (marker_tod);

      /* Write the marker: */
      Native.arrayToBuffer(array, buffer);
      if (Native.writeFile(fhandle, lba, 4096, buffer) < 0)
        error("ConcatMarkers: Writing marker failed for lun '%s'", lun);
      Native.closeFile(fhandle);

      common.plog("ConcatMarkers: Writing SD Concatenation markers to lun=%s", lun);
    }

    Native.freeBuffer(4096, buffer);
  }


  /**
   * Read the marker for a specic lun and the info is returned to the master
   */
  public static void readMarker(LunInfoFromHost luninfo)
  {
    /* Allocate data buffer: */
    long  buffer = Native.allocBuffer(4096);
    int[] array  = new int[ 4096 / 4 ];

    long fhandle = Native.openFile(luninfo.lun, 0);
    if (fhandle == -1)
      error("ConcatMarkers: Open for lun '%s' failed", luninfo.lun);

    /* if user provided it, use it, regarless of real size: */
    long size = luninfo.end_lba;
    if (size == 0)
    {
      if (luninfo.lun.startsWith("/dev/") || luninfo.lun.startsWith("\\\\.\\"))
        luninfo.getRawInfo();
      else
        luninfo.getFileInfo();
      size = luninfo.lun_size;
      if (size == 0)
        error("ConcatMarkers: Unable to obtain lun size for '%s'", luninfo.lun);
    }

    /* Round size downward to 4k and subtract 4k */
    long lba = (size / 4096) * 4096 - 4096;

    /* Read marker: */
    if (Native.readFile(fhandle, lba, 4096, buffer) < 0)
    {
      common.ptod("ConcatMarkers: Reading marker for lun '%s' failed", luninfo.lun);
      Native.freeBuffer(4096, buffer);
      return;
    }

    /* Check contents: */
    Native.buffer_to_array(array, buffer, 4096);
    if (array[0] == MARKER1 && array[1] == MARKER2)
    {
      luninfo.marker_found  = true;
      luninfo.marker_sd_num = array[2];

      luninfo.marker_tod    = (long) array[3]  << 32;
      luninfo.marker_tod   |= (long) array[4] & 0x00000000ffffffffl;

      //common.ptod("luninfo.marker_tod %016x", luninfo.marker_tod);
      //common.ptod("marker0: %08x", array[0]);
      //common.ptod("marker1: %08x", array[1]);
      //common.ptod("marker2: %08x", array[2]);
      //common.ptod("marker3: %08x", array[3]);
      //common.ptod("marker4: %08x", array[4]);
    }
    else
    {
      common.ptod("ConcatMarkers: Invalid marker read for lun '%s'", luninfo.lun);
      common.ptod("marker0: %08x", array[0]);
      common.ptod("marker1: %08x", array[1]);
      common.ptod("marker2: %08x", array[2]);
      common.ptod("marker3: %08x", array[3]);
      common.ptod("marker4: %08x", array[4]);
      Native.freeBuffer(4096, buffer);
      return;
    }

    Native.closeFile(fhandle);
    Native.freeBuffer(4096, buffer);
  }


  public static void verifyMarkerResults(Vector <InfoFromHost> all_hosts)
  {
    /* Quick check: */
    int first_count = all_hosts.get(0).luns_on_host.size();
    for (int i = 0; i < all_hosts.size(); i++)
    {
      for (InfoFromHost luninfo : all_hosts)
      {
        if (luninfo.luns_on_host.size() != first_count)
          error("ConcatMarkers: mismatch in the amount of luns available on all hosts");
      }
    }

    /* Create a list with all luninfos: */
    ArrayList <LunInfoFromHost> all_luninfos = new ArrayList(16);
    for (InfoFromHost info : all_hosts)
    {
      for (LunInfoFromHost luninfo : info.luns_on_host)
        all_luninfos.add(luninfo);
    }

    /* Any info that does not have a proper marker: abort: */
    int errors = 0;
    for (LunInfoFromHost luninfo : all_luninfos)
    {
      if (!luninfo.marker_found)
      {
        common.ptod("ConcatMarkers: No valid SD Concatenation marker found for host=%s,lun=%s",
                    luninfo.host_name, luninfo.lun);
        errors++;
        continue;
      }

      if (luninfo.marker_tod != marker_tod)
      {
        common.ptod("ConcatMarkers: Timestamp mismatch found for host=%s,lun=%s",
                    luninfo.host_name, luninfo.lun);
        common.ptod("ConcatMarkers: Timestamp expected: " + new Date(marker_tod));
        common.ptod("ConcatMarkers: Timestamp found:    " + new Date(luninfo.marker_tod));
        errors++;
        continue;
      }
    }

    if (errors > 0)
      error("ConcatMarkers: SD Concatenation errors found");


    /* Starting with the local host (where the markers were written),  */
    /* compare all relative SD numbers to see if they are in sync:     */
    /* (I also loop through localhost again, but that's  obviously OK) */
    InfoFromHost local_info = null;
    for (InfoFromHost info : all_hosts)
    {
      if (info.host_label.equals(local_host.getLabel()))
        local_info = info;
    }


    /* Lists for corrections to be made: */
    ArrayList <String> rhost_list  = new ArrayList(4);
    ArrayList <String> sdname_list = new ArrayList(4);
    ArrayList <String> oldlun_list = new ArrayList(4);
    ArrayList <String> newlun_list = new ArrayList(4);

    /* Verification first, then corrections later: */
    for (InfoFromHost info : all_hosts)
    {
      Host remote_host = Host.findHost(info.host_label);
      for (LunInfoFromHost luninfo : info.luns_on_host)
      {
        String[] sd_names = remote_host.getSdNamesForLun(luninfo.lun);
        if (sd_names.length == 0)
        {
          common.ptod("ConcatMarkers: Expecting an SD name for host=%s,lun=%s",
                      luninfo.host_name, luninfo.lun);
          errors++;
          continue;
        }
        if (sd_names.length > 1)
        {
          common.ptod("ConcatMarkers: Expecting only ONE SD name for host=%s,lun=%s",
                      luninfo.host_name, luninfo.lun);
          errors++;
          continue;
        }

        String   sd_name = sd_names[0];
        SD_entry sd      = SD_entry.findSD(sd_name);
        if (sd == null)
        {
          common.ptod("ConcatMarkers: Can not find sd=%s", sd_name);
          errors++;
          continue;
        }

        /* if we have an SD# mismatch, save the changes needed: */
        if (luninfo.marker_sd_num != sd.relative_sd_num)
        {
          common.ptod("ConcatMarkers: Defined SD order mismatch between sd=%s,host=%s,lun=%s ",
                      sd.sd_name, local_host.getLabel(),
                      local_host.getLunNameForSd(sd));

          String filler_mask = "%" + (45 + sd.sd_name.length()) + "s";
          common.ptod(filler_mask + " sd=%s,host=%s,lun=%s ", "",
                      sd.sd_name,
                      info.host_label,
                      remote_host.getLunNameForSd(sd));

          /* Find the requested relative sd# on the remote host: */
          boolean found = false;
          for (LunInfoFromHost luninfo2 : info.luns_on_host)
          {
            if (luninfo2.marker_sd_num == sd.relative_sd_num)
            {
              if (found)
                common.failure("Duplicate 'replace SD order' found");
              found = true;
              rhost_list.add(remote_host.getLabel());
              sdname_list.add(sd_name);
              oldlun_list.add(luninfo.lun);
              newlun_list.add(luninfo2.lun);
            }
          }

        }

        //if (host != local_host)
        //  common.ptod("ConcatMarkers: Marker verification OK for host=%s,lun=%s",
        //              luninfo.host_name, luninfo.lun);
      }
    }

    if (errors > 0)
      error("ConcatMarkers: SD Concatenation errors found");

    if (rhost_list.size() == 0)
      return;

    common.ptod("");
    common.ptod("ConcatMarkers: SD/lun ordering before correction:");
    for (SD_entry sd : Vdbmain.sd_list)
    {
      int lines = 0;
      for (Host host : Host.getDefinedHosts())
      {
        if (!sd.sd_is_referenced || sd.concatenated_sd)
          continue;
        if (lines++ == 0)
          common.ptod("ConcatMarkers: sd=%s,host=%s,lun=%s,",
                      sd.sd_name, host.getLabel(), host.getLunNameForSd(sd));
        else
          common.ptod("ConcatMarkers: %" + (sd.sd_name.length() +4) + "shost=%s,lun=%s",
                      "", host.getLabel(), host.getLunNameForSd(sd));
      }
    }
    common.ptod("");


    /* Corrections if needed: */
    for (int i = 0; i < rhost_list.size(); i++)
    {
      Host   remote_host = Host.findHost(rhost_list.get(i));
      String sd_name     = sdname_list.get(i);
      String old_lun     = oldlun_list.get(i);
      String new_lun     = newlun_list.get(i);

      remote_host.replaceLunForSd(sd_name, new_lun);
      common.ptod("ConcatMarkers: sd=%s,host=%s replaced lun=%s with lun=%s ",
                  sd_name, remote_host.getLabel(),
                  old_lun, new_lun);
    }

    common.ptod("");
    common.ptod("ConcatMarkers: SD/lun ordering after correction:");
    for (SD_entry sd : Vdbmain.sd_list)
    {
      int lines = 0;
      for (Host host : Host.getDefinedHosts())
      {
        if (!sd.sd_is_referenced || sd.concatenated_sd)
          continue;
        if (lines++ == 0)
          common.ptod("ConcatMarkers: sd=%s,host=%s,lun=%s,",
                      sd.sd_name, host.getLabel(), host.getLunNameForSd(sd));
        else
          common.ptod("ConcatMarkers: %" + (sd.sd_name.length() +4) + "shost=%s,lun=%s",
                      "", host.getLabel(), host.getLunNameForSd(sd));
      }
    }
    common.ptod("");
  }


  private static void error(String format, Object ... args)
  {
    String txt = String.format(format, args);

    BoxPrint box = new BoxPrint();
    box.add("");
    box.add(txt);
    box.add("");
    box.add("SD concatenation writes a 'marker' to each lun so that we can be assured ");
    box.add("that the SD names and lun names identified across all hosts match.");
    box.add("");
    box.add("This will also CORRECT any discrepancies in possibly accidental");
    box.add("out-of-sequence sd/lun/host combinations defined in the parameter file.");
    box.add("");
    box.add("");
    box.print();

    common.failure(txt);
  }
}

