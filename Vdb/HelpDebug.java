package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;



/**
 * This class helps with debugging corruptions.
 * The plan is to properly read and validate a block, but then modify the data
 * buffer, call the read and validate again, but bypass the read.
 *
 * This should be fun!
 */
public class HelpDebug
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


  private static boolean checked  = false;
  private static boolean anything = false;

  private static HashMap <String, HelpRequest> request_map = new HashMap(8);


  /**
   * Code to bypass writing an AFTER journal record and abort.
   */
  private static void parseParameters()
  {
    synchronized (request_map)
    {
      /* There can be an overlap between threads doing this parsing: */
      if (checked)
        return;

      String[] parms = MiscParms.getKeyParameters("HelpDebug");
      if (parms == null)
        return;

      anything = true;
      for (int i = 1; i < parms.length; i++)
      {
        String   parm  = parms[i].trim().toLowerCase();
        String[] split = parm.split("=");

        HelpRequest request = new HelpRequest(split[0]);
        request_map.put(request.option, request);

        if (split.length > 1)
          request.after_count = Long.parseLong(split[1]);
      }
      checked = true;
    }
  }

  public static boolean anything()
  {
    if (!checked) parseParameters();
    return anything;
  }


  public static boolean corruptBuffer(long read_buffer, long buffer_size)
  {
    /* Copy data buffer to java storage: */
    int[] int_buffer = new int[ (int) buffer_size/4 ];
    Native.buffer_to_array(int_buffer, read_buffer,  (int) buffer_size);


    /* Corrupt lba: */
    //int_buffer[000] = 0x07070707; int_buffer[128] = 0x07070707;

    /* Corrupt data: */
    //int_buffer[   8 ] = 0x07070707; int_buffer[ 136 ] = 0x07070707;

    /* Corrupt compressions: */
    //key_map.getCompressions()[0] = 0x07070707;

    /* Corrupt compressions: */
    //  int_buffer[ 5 ] = 0x07070707;

    /* Corrupt timestamp: */
    //  int_buffer[ 2 ] = 0x07070707; int_buffer[ 3 ] = 0x07070707;

    /* Corrupt mbz: */
    //  int_buffer[ 7 ] = 0x07070707;

    /* Corrupt key, assuming it is not 77 already: */
    int_buffer[4] = (int_buffer[4] & 0x00ffffff) | 0x77000000;


    // Could I force a 'write pending' here for journal recovery?
    // Not in THIS method, but in a different one that is called before/after
    // a write operation, possibly even writing HALF the block!

    // possibly: write a block, but don't write post-record
    //           write only half of the block, but don't write post-record
    // etc

    /* Copy the changed data back to the buffer: */
    Native.arrayToBuffer(int_buffer, read_buffer);

    return true;
  }


  /**
   * Abort after 'nn' calls.
   */
  public static void abortAfterCount(String which)
  {
    if (!checked)  parseParameters();
    if (!anything) return;

    HelpRequest request = request_map.get(which.toLowerCase());
    if (request == null)
      return;

    if (++request.calls == request.after_count)
    {
      BoxPrint.printOne("HelpDebug '%s' abort issued after %,d calls to abortAfterCount",
                        which, request.after_count);
      common.sleep_some(100);
      common.failure("HelpDebug '%s' abort issued after %,d calls to abortAfterCount",
                     which, request.after_count);
    }
  }


  /**
   * Count calls to this code and return true if we reach the requested count
   */
  public static boolean doAfterCount(String which)
  {
    if (!checked)  parseParameters();
    if (!anything) return false;

    HelpRequest badr = request_map.get(which.toLowerCase());
    if (badr == null)
      return false;

    if (badr.after_count == 0)
      common.failure("HelpDebug specification error, need a count, as in %s=1000", which);

    /* Synchronized to make sure the count runs up nicely: */
    synchronized (request_map)
    {
      //common.ptod("badr.calls: " + badr.calls);
      //common.ptod("badr.after_count: " + badr.after_count);
      if (++badr.calls == badr.after_count)
      {
        ErrorLog.add("");
        ErrorLog.add("HelpDebug.doAfterCount triggered for '%s=%,d'", which, badr.after_count);
        ErrorLog.add("");
        ErrorLog.flush();
        return true;
      }
      else
        return false;
    }
  }


  /**
   * Return 'true' if the requested 'after_count' lba is found.
   * Note that this check is a RANGE check using xfersize, so it is not for one
   * specific lba.
   */
  public static boolean doForLba(String which, long lba, long xfersize)
  {
    if (!checked)  parseParameters();
    if (!anything) return false;

    HelpRequest badr = request_map.get(which.toLowerCase());
    if (badr == null)
      return false;

    if (badr.after_count == 0)
      common.failure("HelpDebug specification error, need a count, as in %s=1000", which);

    /* Properly handle the fact that the lba can be inside of a larger block: */
    long end_lba = badr.after_count + xfersize;
    //common.ptod("lba:     %,12d", lba);
    //common.ptod("end_lba: %,12d", end_lba);
    if (lba >= badr.after_count && lba < end_lba)
      return true;
    else
      return false;
  }


  public static boolean hasRequest(String which)
  {
    if (!checked) parseParameters();
    if (!anything) return false;

    return request_map.get(which.toLowerCase()) != null;
  }



  public static void forceSdCorruptions(Cmd_entry cmd,
                                        int       data_flag,
                                        long      read_buffer,
                                        long      buffer_size,
                                        KeyMap    key_map)
  {
    if (!corruptBuffer(read_buffer, buffer_size))
      return;

    BoxPrint.printOne("forceSdCorruptions");

    /* Call read again, but now bypassing read (negative handle): */
    long rc = Native.multiKeyReadAndValidateBlock(cmd.sd_ptr.fhandle * -1,
                                                  data_flag | cmd.type_of_dv_read,
                                                  0,
                                                  cmd.cmd_lba,
                                                  (int) cmd.cmd_xfersize,
                                                  read_buffer,
                                                  key_map.getKeyCount(),
                                                  key_map.getKeys(),
                                                  key_map.getCompressions(),
                                                  key_map.getDedupsets(),
                                                  cmd.sd_ptr.sd_name8,
                                                  cmd.jni_index);

  }


  public static void forceFsdCorruptions(long   fhandle,
                                         int    data_flag,
                                         long   file_start_lba,
                                         long   next_lba,
                                         int    xfersize,
                                         long   read_buffer,
                                         int    buffer_size,
                                         KeyMap key_map,
                                         String fsd_name8)
  {
    if (!corruptBuffer(read_buffer, buffer_size))
      return;

    BoxPrint.printOne("forceFsdCorruptions");

    /* Call read again, but now bypassing read (negative handle): */
    long rc = Native.multiKeyReadAndValidateBlock(fhandle * -1,
                                                  data_flag,
                                                  file_start_lba,
                                                  next_lba,
                                                  xfersize,
                                                  read_buffer,
                                                  key_map.getKeyCount(),
                                                  key_map.getKeys(),
                                                  key_map.getCompressions(),
                                                  key_map.getDedupsets(),
                                                  fsd_name8,
                                                  -1);
  }

  /**
   * Corrupt a data block after 'n' writes.
   *
   * misc=(HelpDebug,corruptAfterWrite=1000)
   */
  public static void corruptBlock(long fhandle, int xfersize, long lba)
  {
    long  buffer = Native.allocBuffer(xfersize);
    int[] array  = new int[ xfersize / 4 ];
    long  rc     = Native.readFile(fhandle, lba, xfersize, buffer);
    if (rc != 0)
      common.failure("Read i/o error in HelpDebug");

    corruptBuffer(buffer, xfersize);

    rc = Native.writeFile(fhandle, lba, xfersize, buffer);
    if (rc != 0)
      common.failure("Write i/o error in HelpDebug");

    Native.freeBuffer(xfersize, buffer);
  }
}

class HelpRequest
{
  public String option;
  public long   calls;
  public long   after_count;

  public HelpRequest(String op)
  {
    option = op;
  }
}

/*

 How about creating some little script language to be read from a file that
 tells this code which corruption to create on which sector?
 And WHEN to abort, e.g,'continue until X errors?
 Maybe even a 'do this 'n' times'?
 Or even a 'only this lba, only this SD/FSD?
 sd=sd1
 lba=xxxxx
 corrupt=bad_data
 waitforXsecondsfirst
 multiple sectors
 maybe even triggered by reading the disk file over and over again?
 Locked by ONE io_task and static, read ever n seconds, (interval?)
 Getting too fancy!
 misc=(HelpDebug,.....)???



*/
