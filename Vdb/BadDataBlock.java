package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.Format;



/**
 * Describes one bad data block discovered by Data Validation.
 *
 * It points to BadKeyBlock which in turns points to BadSector.
 */
public class BadDataBlock
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


  private int    key_blksize;
  private int    data_blksize;
  private int    key_blocks;
  private int    key_blocks_added = 0;
  private int    sectors_added = 0;
  private long   file_lba;
  private String lun;

  private BadKeyBlock[] bad_keyblocks = null;

  /* A general lock around all corruption reporting.                      */
  /* It makes debugging much easier, and it also will allow the reporting */
  /* of ONE data block to complete before intermixing other blocks.       */
  public  static Object reporting_lock = new Object();

  /**
   * New bad data block: set things up.
   */
  public BadDataBlock(BadSector bads)
  {
    key_blksize   = bads.key_blksize;
    data_blksize  = bads.data_blksize;
    key_blocks    = data_blksize / key_blksize;
    bad_keyblocks = new BadKeyBlock[ key_blocks ];
    file_lba      = bads.file_lba;
    lun           = bads.lun;
  }

  /**
   * Add a new sector, including the one used during instantiation.
   */
  public void addSector(BadSector bads)
  {
    /* I try to be paranoid here: */
    if (bads.key_blksize != key_blksize)
      throw new RuntimeException("Unexpected key block size");

    if (bads.data_blksize != data_blksize)
      throw new RuntimeException("Unexpected data block size");


    /* Get or create the BadKeyBlock for this sector: */
    if (bad_keyblocks[ bads.keyblock_in_datablock ] == null)
    {
      BadKeyBlock bkb = new BadKeyBlock();
      bad_keyblocks[ bads.keyblock_in_datablock ] = bkb;
      bkb.owning_bdb = this;
      key_blocks_added++;
    }

    BadKeyBlock bkb = bad_keyblocks[ bads.keyblock_in_datablock ];
    bads.owning_bdb = this;
    bads.owning_bkb = bkb;

    sectors_added++;
    bkb.addSector(bads);
  }


  public ArrayList <BadKeyBlock> getBadKeyBlocks()
  {
    ArrayList <BadKeyBlock> list = new ArrayList(8);
    for (BadKeyBlock bkb : bad_keyblocks)
    {
      if (bkb != null)
        list.add(bkb);
    }
    return list;
  }



  /**
   * JNI has reported the 60003 data corruption error.
   *
   * Print all pending information about the key blocks and sectors that failed
   * in this data block.
   *
   * However, if this was a journal recovery read  of a pending write, try to
   * determine if this block just happens to be still valid.
   */
  public static boolean reportBadDataBlock(Object sd_or_fsd,
                                           long   file_lba,
                                           long   file_start_lba,
                                           long   buffer)
  {
    //common.where(16);

    try
    {
      synchronized (BadDataBlock.reporting_lock)
      {
        HashMap <Long, BadDataBlock> bad_data_map = null;
        SD_entry     sd  = null;
        ActiveFile   afe = null;
        if (sd_or_fsd instanceof SD_entry)
        {
          sd           = (SD_entry) sd_or_fsd;
          bad_data_map = sd.bad_data_map;
        }
        else
        {
          afe          = (ActiveFile) sd_or_fsd;
          bad_data_map = afe.getFileEntry().getAnchor().bad_data_map;
        }

        /* We must have at least seen one BadSector for this data block: */
        BadDataBlock bdb = bad_data_map.get(file_lba + file_start_lba);
        if (bdb == null)
          throw new Exception("Lba can not be found in bad data map: " + file_lba);

        /* Make sure we remove this bad data block, it is reported only once: */
        bad_data_map.remove(file_lba);


        /* During journal recovery we may get some 'pending write' blocks: */
        if (Validate.isJournalRecoveryActive())
        {
          BadKeyBlock first_bkb  = bdb.getBadKeyBlocks().get(0);
          BadSector   first_bads = first_bkb.getSectors().get(0);
          //common.ptod("first_bads.data_flag:  0x%04x", first_bads.data_flag);
          //common.ptod("first_bads.error_flag: 0x%04x", first_bads.error_flag);


          /* Check all the reported bad key blocks: */

          /* Interesting issue here: since all pending writes are read one key */
          /* block at the time there can never be more than one bad key block. */
          /* the 'for (BadKeyBlock bkb : bdb.bad_keyblocks)' loop therefore    */
          /* really does not need to be done.                                  */
          /* On top of that, the 'first_bkb.checkPendingKeyBlock()' call below */
          /* indeed is always the first and ONLY call.                         */

          /* This needs to be cleaned up, but I do not want to risk changing   */
          /* the stable code at this time, since the code does what it is      */
          /* supposed to do as of vdbench50405beta4.                           */
          /* Clean this up next go-around?                                     */

          boolean errors = false;
          if ((first_bads.data_flag & Validate.FLAG_PENDING_READ) != 0)
          {
            for (BadKeyBlock bkb : bdb.bad_keyblocks)
            {
              if (bkb != null)
              {
                if (!first_bkb.checkPendingKeyBlock(file_lba, file_start_lba, buffer))
                  errors = true;
              }
            }

            /* If the block was found to be OK, report and accept:         */
            /* Reporting is a problem here since I currently do not know   */
            /* what decisions were made above, and telling a block was bad */
            /* and then saying it is OK gets confusing.                    */
            /* Let's see if we can just NOT report here:                   */
            if (!errors)
            {
              //if (sd != null)
              //  plog("Data block for sd=%s,lun=%s; lba: 0x%08x xfersize=%d found to be valid",
              //        sd.sd_name, bdb.lun, file_lba, bdb.data_blksize);
              //else
              //  plog("Data block for fsd=%s,file=%s; file lba: 0x%08x xfersize=%d found to be valid",
              //        afe.getFileEntry().getAnchor().fsd_name_active,
              //        afe.getFileEntry().getFullName(),
              //        file_lba, bdb.data_blksize);
              ErrorLog.flush();
              return true;
            }
          }
        }



        printHeaders();

        plog("");
        plog("");
        plog("");

        if (sd != null)
          plog("Corrupted data block for sd=%s,lun=%s; lba: %,d (0x%08x) xfersize=%d",
                sd.sd_name, bdb.lun, file_lba, file_lba, bdb.data_blksize);
        else
          plog("Corrupted data block for fsd=%s,file=%s; file lba: 0x%08x xfersize=%d",
                afe.getFileEntry().getAnchor().fsd_name_active,
                afe.getFileEntry().getFullName(),
                file_lba, bdb.data_blksize);

        plog("");
        plog("Data block has %d key block(s) of %d bytes each.", bdb.key_blocks, bdb.key_blksize);
        if (bdb.key_blocks == bdb.key_blocks_added)
          plog("All key blocks are corrupted.");
        else
          plog("%d of %d key blocks are corrupted.", bdb.key_blocks_added, bdb.key_blocks);


        for (BadKeyBlock bkb : bdb.getBadKeyBlocks())
        {
          bkb.reportBadKeyBlock();
        }

        ErrorLog.flush();
        //BoxPrint.printOne("Debugging: early shutdown");
        //
        //common.sleep_some(250);
        //common.failure("Debugging: early shutdown");
      }
    }
    catch (Exception e)
    {
      common.ptod("Exception while reporting corrupted data.");
      common.ptod("Please report this problem");
      common.ptod(e);
      common.ptod("Vdbench will abort AFTER these %d pending messages", ErrorLog.size());
      ErrorLog.flush();
      common.failure(e);

    }

    return false;
  }

  private static boolean header_printed = false;
  public static void printHeaders()
  {
    if (header_printed)
      return;

    header_printed = true;

    /* This is here to get the DATE in the output: */
    plog("");
    plog("Time of first corruption: " + BadSector.dv_df.format(new Date()));
    plog("");
    plog("At least one Data Validation error has been detected.                               ");
    plog("");
    plog("Terminology:                                                                        ");
    plog("- Data block: a block of xfersize= bytes.                                           ");
    plog("- Key block:  the smallest xfersize specified by the user which is the unit of      ");
    plog("              data that Data Validation keeps track of.                             ");
    plog("- Sector:     512 bytes of disk storage, regardless of actual storage sector size.  ");
    plog("- Lba:        Logical Byte Address, not to be confused with Logical Block Address.  ");
    plog("");
    plog("");
    plog("The output starts with a summary of a data block, followed by a summary of each     ");
    plog("key block. If all sectors in a key block show a similar type of data corruption     ");
    plog("only the FIRST sector of the key block will be reported.                            ");
    plog("For all other cases, ALL sectors will be reported.                                  ");
    plog("");
    plog("Contents of the first 32 bytes of each sector:                                      ");
    plog("");

    if (!Dedup.isDedup())
    {
      plog("Byte 0x00 -  0x07: Byte offset of this disk block ");
      plog("Byte 0x08 -  0x0f: Timestamp: number of milliseconds since 1/1/1970 ");
      plog("Byte 0x10        : Data Validation key from 1 - 126 ");
      plog("Byte 0x11        : Checksum of timestamp ");
      plog("Byte 0x12 -  0x13: Reserved ");
      plog("Byte 0x14 -  0x1b: SD or FSD name in ASCII hexadecimal ");
      plog("Byte 0x1c -  0x1f: Process-id when written ");
      //plog("Byte 0x1c -  0x1f: Process-id when not using journaling or validate=continue        ");

      if (!Validate.isCompression())
        plog("Byte 0x20 - 0x1ff: 480 bytes of LFSR based data");
      else
        plog("Byte 0x20 - 0x1ff: 480 bytes of compression data pattern");
    }
    else
    {
      /* This header now must be generic since I can have a mix */
      /* For now, use across: */
      if (true) // Dedup.isDedupAcross())
      {
        plog("For unique blocks, dedupacross=yes:");
        plog("Byte 0x00 -  0x07: Byte offset of this disk block");
        plog("Byte 0x08 -  0x0f: Timestamp: number of milliseconds since 1/1/1970");
        plog("Byte 0x10        : Data Validation key from 1 - 126");
        plog("Byte 0x11        : Checksum of timestamp");
        plog("Byte 0x12 -  0x13: Reserved");
        plog("Byte 0x14 -  0x1b: SD or FSD name in ASCII hexadecimal");
        plog("Byte 0x1c -  0x1f: Process-id when written ");
        //plog("Byte 0x1c -  0x1f: Process-id when not using journaling or validate=continue ");
        plog("Byte 0x20 - 0x1ff: 480 bytes of compression data pattern");
        //print("(When the data compare of the 480 data bytes results in a mismatch ONLY because of a wrong ");
        //print(" Data Validation key and/or wrong lba the reporting of that data will be suppressed.)");
      }
      else
      {
        plog("For unique blocks, dedupacross=no:");
        plog("Byte 0x00 -  0x07: Byte offset of this disk block");
        plog("Byte 0x08 - 0x1ff: 504 bytes of compression data pattern");
        //print("(When the data compare of the 480 data bytes results in a mismatch ONLY because of a wrong ");
        //print(" Data Validation key and/or wrong lba the reporting of that data will be suppressed.)");
      }

      plog("");
      plog("For duplicate blocks:");
      plog("Byte 0x00 -  0x07: Vdbench calculated dedup set");
      plog("Byte 0x08 - 0x1ff: 504 bytes of compression data pattern");
    }

    plog("");
    plog("On the left:  the data that was expected ('.' marks unknown value).");
    plog("On the right: the data that was found.");
    plog("");

    ErrorLog.flush();
  }

  /**
   * All DV reporting first goes to a Vector so that we can send it all from the
   * slave to the master and errorlog.html in one big swoop.
   */
  public static void plog(String format, Object ... args)
  {
    ErrorLog.add(String.format(format, args));
  }
}

