package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;
import Utils.Getopt;


/**
 * Print out journal records.
 * Main objective: debugging
 *
 * This may not be pretty, but it was never meant to be a generic tool.
 *
 * I also realize that maybe it would be useful to also scan the map portion and
 * report the current key.
 * TBD
 *
 * The journal currently does not have a unique/duplicate bit, nor does it have
 * room for a dedupset, so this will be really useless for Binia!
 */
public class PrintJournal
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";



  public static void main(String args[])
  {
    SimpleDateFormat df = new SimpleDateFormat( "MM/dd/yyyy HH:mm:ss.SSS zzz" );

    Getopt getopt = new Getopt(args, "l:d:", 1);
    if (!getopt.isOK())
    {
      common.ptod("Usage:");
      common.ptod("./vdbench printjournal xxx.jnl [-l nnn]");
      common.ptod("          xxx.jnl:      your .jnl file");
      common.ptod("          -l 4096       logical byte address");
      common.ptod("          -l 4k         k/m/g logical byte address");
      common.ptod("          -l 0x1000     Hex logical byte address");
      common.failure("Parameter scan failure");
    }

    boolean JOURNAL_ADD_TIMESTAMP = false;
    long    saved_journal_tod = 0;
    boolean found_jnl         = false;
    long    map_lba           = 0;
    long    tod_counts        = 0;
    long    find_lba          = -1;
    long    lbas_found        = 0;

    if (getopt.check('l'))
    {
      find_lba = common.parseSize(getopt.get_string());
    }

    if (getopt.get_positionals().size() == 0)
      common.failure("Input parameter required with .jnl file name");

    String fname = getopt.get_positional(0);
    long  buffer = Native.allocBuffer(512);
    int[] array  = new int[128];

    long    handle    = Native.openFile(fname, 0);
    long    jnl_size  = new File(args[0]).length();
    long    key_blksize  = -1;
    long    seek      = 0;
    int     key_block = 0;
    int     records   = 0;

    for (long i = 0; i < Long.MAX_VALUE ; i++)
    {
      if (seek >= jnl_size)
      {
        common.ptod("Trying to go beyond journal file size");
        break;
      }
      if (Native.readFile(handle, seek, 512, buffer) != 0)
      {
        common.ptod("read error");
        break;
      }
      Native.buffer_to_array(array, buffer, 512);


      /* This is the MAP: */
      if (array[0] == Jnl_entry.MAP_EYE_CATCHER)
      {
        /* Determine if the journal entries contain a timestamp: */
        if (array[7] == 1)
        {
          common.set_debug(common.JOURNAL_ADD_TIMESTAMP);
          JOURNAL_ADD_TIMESTAMP = true;
          Jnl_entry.overrideConstants();
        }


        /* The first map record, print info: */
        if (i == 0)
        {
          long tod = (long) array[4] << 32;
          tod += array[5];
          saved_journal_tod = array[5];
          //common.ptod("dump_journal_tod: %016x %s", tod, new Date(tod));
          //common.ptod("saved_journal_tod: %08x", saved_journal_tod);
        }

        /* Store the map's key blocksize: */
        if (key_blksize < 0)
          key_blksize = array[4];

        if (seek != 0)
        {
          for (int j = Jnl_entry.MAP_SKIP_HDR; j < 128; j++)
          {
            int key1 = array[ j ] >> 24 & 0xff;
            int key2 = array[ j ] >> 16 & 0xff;
            int key3 = array[ j ] >>  8 & 0xff;
            int key4 = array[ j ]       & 0xff;

            long lba1 = key_block++ * key_blksize;
            long lba2 = key_block++ * key_blksize;
            long lba3 = key_block++ * key_blksize;
            long lba4 = key_block++ * key_blksize;

            if (find_lba == -1)
            {
              common.ptod("Starting lba: 0x%012x key: 0x%02x", lba1, key1);
              common.ptod("Starting lba: 0x%012x key: 0x%02x", lba2, key2);
              common.ptod("Starting lba: 0x%012x key: 0x%02x", lba3, key3);
              common.ptod("Starting lba: 0x%012x key: 0x%02x", lba4, key4);
            }

            else
            {
              if (lba1 == find_lba)
              {
                lbas_found++;
                common.ptod("Starting lba: 0x%012x key: 0x%02x", lba1, key1);
              }
              else if (lba2 == find_lba)
              {
                lbas_found++;
                common.ptod("Starting lba: 0x%012x key: 0x%02x", lba2, key2);
              }
              else if (lba3 == find_lba)
              {
                lbas_found++;
                common.ptod("Starting lba: 0x%012x key: 0x%02x", lba3, key3);
              }
              else if (lba4 == find_lba)
              {
                lbas_found++;
                common.ptod("Starting lba: 0x%012x key: 0x%02x", lba4, key4);
              }
            }

            //common.ptod("lba1: %08x %02x", lba1, key1);
            //common.ptod("lba2: %08x %02x", lba2, key2);
            //common.ptod("lba3: %08x %02x", lba3, key3);
            //common.ptod("lba4: %08x %02x", lba4, key4);

          }
        }



        seek += 512;

        continue;
      }

      /* This is the journal portion: */
      if (array[0] == Jnl_entry.JNL_EYE_CATCHER && !found_jnl)
      {
        found_jnl = true;
        //common.ptod("Start of JNL records: %012x", seek);
      }



      /* Translate all journal entries in this record: */
      int entries = array[1];
      if (entries == 0)
        break;

      /* Scan through all entries inside of this 512 byte journal record: */
      for (int j = 0; j < entries; j++)
      {
        records++;
        int  key = array[ Jnl_entry.JNL_SKIP_HDR + (j * Jnl_entry.JNL_ENT_INTS) + 0 ] >>> 56;
        long lba = array[ Jnl_entry.JNL_SKIP_HDR + (j * Jnl_entry.JNL_ENT_INTS) + 1 ] * key_blksize;

        if (find_lba != -1 && lba != find_lba)
          continue;

        lbas_found++;
        if (JOURNAL_ADD_TIMESTAMP)
        {
          long tod = Jnl_entry.make64(array[ Jnl_entry.JNL_SKIP_HDR + (j * Jnl_entry.JNL_ENT_INTS) + 2 ],
                                      array[ Jnl_entry.JNL_SKIP_HDR + (j * Jnl_entry.JNL_ENT_INTS) + 3 ]);
          String date = df.format(new Date(tod));

          if (key != 0)
            common.ptod("Before:  lba: 0x%012x key: 0x%02x tod: %s", lba, key, date);
          else
            common.ptod("After:   lba: 0x%012x           tod: %s", lba, date);
        }
        else
        {
          if (key != 0)
            common.ptod("Before:  lba: 0x%012x key: 0x%02x  ", lba, key);
          else
            common.ptod("After:   lba: 0x%012x ", lba);
        }

      }

      if (entries != Jnl_entry.JNL_ENTRIES)
      {
        //if (records != 0)
        common.ptod("Found EOF after %6d journal records at seek: %08x ", records, seek);
        records = 0;
        break;
      }

      seek += 512;
    }

    Native.closeFile(handle);

    if (find_lba != -1 && lbas_found == 0)
    {
      common.ptod("Logical byte addres 0x%08x not found", find_lba);
    }
  }
}






