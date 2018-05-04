package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Fget;
import Utils.Getopt;



/**
 *
 **/
public class Sector
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  long   lba_wanted;
  long   lba_read;
  String sd_wanted;
  String sd_read;
  long   ts;
  int    key_wanted;
  int    key_read;
  int    checksum;
  int    different_words_in_sector;
  int    different_bits;
  int    singlebit_words;
  Date   tod_in_sector;   // timestamp found in block.
  int[]  expected = new int[128];
  int[]  was_read = new int[128];
  int[]  lfsr_sector;
  int[]  re_read;
  boolean  lfsr_of_bad_lba_bad = false;


  /**
   * Count how many words (32bits) are different in this sector.
   * We are only looking beyond the first 32 bytes.
   */
  public int countDifferences()
  {
    different_words_in_sector = 0;

    /* Look only beyond byte 32: */
    for (int i = 8; i < expected.length; i++)
    {
      int expd = expected[i];
      int read = was_read[i];

      /* Ignore timestamp: */
      if (i == 2 || i == 3)
        continue;

      /* Ignore checksum: */
      if (i == 4)
      {
        expd &= 0xff00ffff;
        read &= 0xff00ffff;
      }

      /* Ignore bad SD name: */
      if ((i == 5 || i == 6) && !sd_wanted.equals(sd_read))
        continue;
      if (expd != read)
      {
        different_words_in_sector++;

        /* Count miscomparing bits and how often we just have one single bit error: */
        int bits = countBits(expd, read);
        different_bits += bits;
        if (bits == 1)
        {
          singlebit_words++;
        }
      }
    }

    //if (different_words != 0)
    // DVPost.print("different_words: " + different_words + " bits: " + different_bits);
    //
    //common.ptod("different_words_in_sector: " + different_words_in_sector);
    return different_words_in_sector;
  }

  private int countBits(int word1, int word2)
  {
    int bits = 0;
    int w1 = word1;
    int w2 = word2;
    for (int i = 0; i < 32; i++)
    {
      if ((w1 & 1) != (w2 & 1))
        bits++;
      w1 >>>= 1;
      w2 >>>= 1;
    }

    //if (bits == 1)
    //  DVPost.print("countBits: %08x %08x %08x %d", lba_wanted, word1, word2, bits);

    return bits;
  }

  /**
   * Get the data pattern that goes with the BAD lba that we read.
   */
  public void getBadLbaData()
  {
    /* Allocate memory for what we want to do: */
    lfsr_sector = new int[512/4];
    //common.ptod("lfsr_sector: " + lfsr_sector);
    //common.ptod("lba_read:    " + lba_read);
    //common.ptod("key_read:    " + key_read);
    //common.ptod("sd_read:>>>%s<<<", sd_read);
    if (sd_read.length() == 0)
      common.failure("empty 'sd_read'");

    /* Create an LFSR array using this data, sd name must be 8 bytes: */
    Native.fillLfsrArray(lfsr_sector, lba_read, key_read, check8byteString(sd_read));

    /* fillLfsr fills bytes 0-511; we need bytes 32-511 placed at offset 32: */
    int[] p2 = new int[512/4];
    System.arraycopy(lfsr_sector, 0, p2, 32/4, 480/4);
    lfsr_sector = p2;

    /* Now compare that data so that we can report discrepancies: */
    for (int i = 8; i < 512/4; i++)
    {
      if (was_read[i] != lfsr_sector[i])
      {
        lfsr_of_bad_lba_bad = true;
      }
    }
    //common.ptod("lfsr_of_bad_lba_bad: " + lfsr_of_bad_lba_bad + " " + key_read);
  }


  /**
   * An attempt to immediately re-read the bad sector and validate it again.
   */
  public void reReadSector(DvKeyBlock bkb)
  {
    Vector lines = new Vector(64);

    long handle = Native.openFile(bkb.lun);
    if (handle < 0)
      common.failure("Can't open disk file");

    long data_buffer = Native.allocBuffer(512);
    int[] data_array = new int[512 / 4];

    common.ptod("lba_wanted: " + lba_wanted);
    common.ptod("bkb.file_start_lba: " + bkb.file_start_lba);
    long rc = Native.readFile(handle, lba_wanted - bkb.file_start_lba, 512, data_buffer);
    if (rc != 0)
      common.failure("Error reading block");
    Native.closeFile(handle);

    Native.buffer_to_array(data_array, data_buffer, 512);

    Native.freeBuffer(512, data_buffer);

    int[] return_array = new int[128];
    System.arraycopy(data_array, 0, return_array, (32/4), data_array.length);

    re_read = return_array;
  }


  /**
   * Check the content of the input String and return "garbage " if this
   * is not a valid String.
   * Note: String is first made 8 bytes long.
   */
  public static String check8byteString(String str)
  {
    String string = (str + "        ").substring(0,8);

    /* This top part generates a hex image of the input: */
    String txt = "";
    for (int i = 0; i < string.length(); i++)
      txt += String.format("%02x", (int) string.charAt(i));

    /* Check every single byte: */
    for (int i = 0; i < string.trim().length(); i++)
    {
      char onebyte = string.charAt(i);
      //common.ptod("charAt(i): %04x", (int) onebyte);
      if (!Character.isLetterOrDigit(onebyte))
      {
        //common.ptod("check8byteString: " + txt);
        return "garbage ";
      }
    }

    if (string.trim().length() == 0)
      return "garbageb ";

    String rc = (string + "        ").substring(0,8);
    if (string.trim().length() == 0)
      string = "blanks  ";
    //common.ptod("rc: >>>%s<<<", rc);
    return rc;
  }
}

