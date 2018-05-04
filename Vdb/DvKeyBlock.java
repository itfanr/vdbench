package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



/**
 * Post processing program for Data Validation errorlog.html file.
 *
 * BadKeyBlock: information about a Key Block that was found to contain one of
 * more bad sectors
 */
class DvKeyBlock implements Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  long     logical_lba;    /* file_start_lba + file_lba */
  long     file_start_lba;
  long     file_lba;
  String   lun;
  String   sd_wanted;
  int      sectors_reported;
  int      key_wanted;
  int      key_read;
  int      key_block_size;

  String first_dvpost_line_tod    = null;
  Date   first_sector_written_tod = null;
  String last_tod_valid           = null;
  String last_valid_rw            = null;
  Date   last_valid               = null;
  Date   block_first_seen         = null;
  Date   dvpost_date              = null;

  Sector[] sectors;   // Would like to change this to an ArrayList, but too much work!
  String   failed_operation;
  int      error_code = 0;
  int      different_words_in_block;
  ArrayList raw_input = new ArrayList(4096);

  static String sort_order = null;

  public DvKeyBlock()
  {
  }

  public int compareTo(Object obj)
  {
    DvKeyBlock bkb = (DvKeyBlock) obj;

    if (sort_order.equals("lba"))
      return(int) (logical_lba - bkb.logical_lba);

    /* For Lun, sort lun first, then lba: */
    if (sort_order.equals("lun"))
    {
      int diff = lun.compareTo(bkb.lun);
      if (diff != 0)
        return diff;
      return(int) (logical_lba - bkb.logical_lba);
    }
    else
      common.failure("Unknown sort flag");
    return 0;
  }

  public int countSingleBitWords()
  {
    int bitwords = 0;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].singlebit_words != 0)
        bitwords++;
    }
    return bitwords;
  }

  public String getBlockStatusShort()
  {
    String txt = "";
    if (getWrongKeys() != null)
      txt += "Key; ";
    if (getWrongLbas() != null)
      txt += "Lba; ";
    if (countSingleBitWords() > 0)
      txt += "Single bit; ";
    if (getWrongSdNames() != null)
      txt += "SD; ";
    if (getLFSRStatus())
      txt += "Bad lba data is bad; ";
    if (anyPartialSectors())
      txt += "Data, partial; ";
    else if (different_words_in_block > 0)
      txt += "Data; ";
    txt += getBadSectors() + " ";

    return txt;
  }

  public String getBlockStatus()
  {
    String txt = "";
    if (error_code == 0 && sectors_reported != (key_block_size / 512))
      txt += "- Not all sectors have been reported.\n";
    if (getWrongKeys() != null)
      txt += "- Invalid key(s) read.\n";
    if (getWrongLbas() != null)
      txt += "- Invalid lba read.\n";
    if (countSingleBitWords() > 0)
      txt += "- At least one single bit error.\n";
    if (getWrongSdNames() != null)
      txt += "- Invalid SD name read.\n";
    if (getLFSRStatus())
      txt += "- Data corruption even when using wrong lba or key.\n";
    if (anyPartialSectors())
      txt += "- At least one sector is partially correct.\n";
    else if (different_words_in_block > 0)
      txt += "- Data corruption.\n";
    txt += "- " + getBadSectors() + " ";

    return txt;
  }

  /**
   * Return the numbers of the sectors that are bad in ranges.
   */
  public String getBadSectors()
  {
    Vector numbers = new Vector(sectors.length);

    /* Put all bad sector numbers in a row: */
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null)
        numbers.add(new Integer(i));
    }

    // Note: this (incomplete) tells us that the last sector in a key block
    // has  not been found.
    // It does not tell us however that one of the EARLIER sectors has not been found.

    /* Identify whether we received all the sectors. Having an error_code */
    /* confirms that, but also having seen the last sector confirms that: */
    String txt = error_code != 0 ? "Bad sectors: " : "Bad sectors: (incomplete) ";
    if (sectors[ sectors.length - 1 ] != null)
      txt = "Bad sectors: ";

    txt += compressSectorNumbers(numbers, sectors.length);

    return txt;
  }


  /**
   * Return the Integers that are in the received Vector as a String,
   * reporting them for instance as 0-15,16,18-20 etc.
   */
  public static String compressSectorNumbers(Vector numbers, int max)
  {
    String txt = "";

    if (numbers.size() == 0)
      return "All " + max + " sectors good (based on lba and key that was read)";
    if (numbers.size() == max)
      return "All " + max + " sectors bad ";

    /* Convert Vector to array: */
    int[] array = new int[numbers.size()];
    for (int i = 0; i < numbers.size(); i++)
      array[i] = ((Integer) numbers.elementAt(i)).intValue();

    int first = array[0];
    int last  = array[0];
    for (int i = 1; i < array.length; i++)
    {
      //DVPost.print("loop: " + array[i] + " " + last);
      if (array[i] != (last + 1))
      {
        if (first == last)
          txt += first + ",";
        else
          txt += first + "-" + last + ",";
        first = array[i];
      }
      last = array[i];
    }

    if (first == last)
      txt += first;
    else
      txt += first + "-" + last;

    return txt + " (" + array.length + " of " + max + ")";
  }

  public String getWrongKeys()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].key_read != sectors[i].key_wanted)
        wrongs.put(new Integer(sectors[i].key_read), new Integer(sectors[i].key_read));
    }

    if (wrongs.size() == 0)
      return null;

    Integer[] ints = (Integer[]) wrongs.keySet().toArray(new Integer[0]);
    Arrays.sort(ints);
    String txt = "";
    for (int i = 0; i < ints.length; i++)
      txt += String.format("%02x ", ints[i].intValue() & 0xff);
    return txt;
  }

  /**
   * Return highest delta between key_wanted and key_read.
   * This helps identify those cases where we lose more than one generation of
   * data.
   *
   * This code right now does not handle the rollover from key 127 back to 1 !!
   */
  public int getHighestKeyDelta()
  {
    int high_delta = 0;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].key_read != sectors[i].key_wanted)
      {
        int delta_key = sectors[i].key_wanted - sectors[i].key_read;
        high_delta = Math.max(high_delta, Math.abs(delta_key));
      }
    }

    return high_delta;
  }



  public boolean anyPartialSectors()
  {
    boolean partial = false;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].different_words_in_sector != 120)
        partial = true;
    }

    return partial;
  }
  public String getTimestamps()
  {
    DateFormat df = new SimpleDateFormat( "(MM/dd/yy HH:mm:ss.SSS) " );
    HashMap times = new HashMap(8);
    for (Sector sector : sectors)
    {
      if (sector != null && sector.tod_in_sector != null)
        times.put(sector.tod_in_sector, sector.tod_in_sector);
    }

    if (times.size() == 0)
      return null;

    Date[] dates = (Date[]) times.keySet().toArray(new Date[0]);
    Arrays.sort(dates);
    String txt = "";
    for (int i = 0; i < dates.length; i++)
      txt += df.format(dates[i]);
    return txt;
  }

  public String getWrongLbas()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].lba_read != sectors[i].lba_wanted)
        wrongs.put(new Long(sectors[i].lba_read), new Long(sectors[i].lba_read));
    }

    if (wrongs.size() == 0)
      return null;

    Long[] longs = (Long[]) wrongs.keySet().toArray(new Long[0]);
    Arrays.sort(longs);
    String txt = "";
    for (int i = 0; i < longs.length; i++)
    {
      if (i > 2)
        return txt + "...";
      else
        txt += String.format("0x%x ", longs[i].longValue());
    }
    return txt;
  }


  public String getWrongSdNames()
  {
    HashMap wrongs = new HashMap(8);
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].sd_read != sectors[i].sd_wanted)
      {
        wrongs.put(sectors[i].sd_read, sectors[i].sd_read);
      }
    }

    if (wrongs.size() == 0)
      return null;

    String[] sds = (String[]) wrongs.keySet().toArray(new String[0]);
    Arrays.sort(sds);
    String txt = "";
    for (int i = 0; i < sds.length; i++)
      txt += sds[i] + " ";
    return txt;
  }

  public boolean getLFSRStatus()
  {
    boolean any_bad = false;
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null && sectors[i].lfsr_of_bad_lba_bad)
        any_bad = true;
    }

    return any_bad;
  }

  public void reReadSectors()
  {
    for (int i = 0; i < sectors.length; i++)
    {
      if (sectors[i] != null)
        sectors[i].reReadSector(this);
    }
  }

  public static String header()
  {
    //return String.format("\n" + DVPost.name_mask + " %6s %8s %12s", "Lun", "sd", "xfersize", "lba");


    return String.format(DVPost.name_mask + " %12s %12s %12s %7s %5s %9s ",
                         "lun/file",
                         "log_lba",
                         "start_lba",
                         "file_lba",
                         "sectors",
                         "(bad)",
                         "Key_e/r/d");
  }
  public String print()
  {
    int bad_sectors = 0;
    for (Sector sector : sectors)
    {
      if (sector != null)
        bad_sectors++;
    }

    String flag = (key_wanted - key_read > 1) ? "/" + (key_wanted - key_read) : "";

    String txt = String.format(DVPost.name_mask + " %12x %12x %12x %7d %5d %02x/%02x%-4s",
                               lun,
                               logical_lba,
                               file_start_lba,
                               logical_lba - file_start_lba,
                               sectors.length,
                               bad_sectors,
                               key_wanted, key_read, flag);

    return txt;
  }
}

