package Utils;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * This class handles binary data files.
 *
 * Create an array of longs that contains all the data
 * that we need. We can store any data values, Strings, char, short, int, byte.
 * Data is always stored on its boundaries, e.g. 4 bytes. It is therefore more
 * space efficient to store them in order of length, like first 8, then 4, then
 * 2, etc.
 *
 * The writer and the reader of the data must both use the same order of
 * fields and lengths used.
 *
 *
 * Each new record starts with:
 * - 2 bytes eye catcher
 * - 1 byte for record type
 * - 1 byte for record version
 * - 1 byte spare
 * - 3 byte array length
 */

/*
Since we depend on the order in which things are written we can possibly
create a bitmap of 'fields not 0' before we store the data.

First set a bit for every single field that needs to be stored, and
then do not store the zeros.
Then after read, read the bitmap and load only those fields that are needed.

This would save a lot of space for the interval data.

We could also add an other function to store only nonzero bytes!



Other possibilities:

If all the records are identical, why do we need an 8-byte header?
OK, some files have a mix of record types, like Collector data, but tnf.bin and
flatfile.bin have only one type, and all the records are identical in type and
length.
We can add a new flag at creation tim:
- if off, do it the old way.
- if on, then this header will be there only once.


If we start writing only the bytes that have content, then the need to store on
a data's boundary is gone, but still must be around for compatibility with the
original version. Since we need a flag anyway to identify compression yes/no,
this would be easy to identify.

Compression:

Until we do the write_record(), we have everything in 'longs' array.
At write_record() time we can investigate each single byte and create a bitmap
equal to the length of bytes of the 'longs' array.
Each bit then corresponds to a zero/nonzero byte.
Since, in the 'array length' we know the length of the array, we also know the
length of the bitmap, except when we suppress the header.

We only can compress the header because we know that every record is the same
type and length, so for compression, when the compressed length can be different,
we must find an other way.
Maybe don't allow header suppression with compression? But that goes against the
objective here: less space needed.

If we always store a three byte array length we also waste space. The three bytes
length allows huge records to be written, but is usually never needed.
So: one byte with the length. If bit0 of that length is on then the real length
needs more than one byte and is therefore automatically three bytes.
We then follow with the bitmap.

What is cheaper: a bit for each byte, or a half byte per element  ?
- A bit costs 1/8th of the total length.

new Deflater() Did not know about that. much easier, probably a little more
expensive at the compression side, but definitely worth it!!!
However, it will be on the same processor, and that might not be worth it.

Compression has been resolved with the new AsynchZip.

Just must switch between long and byte arrays.


*/

public class Bin
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private int        array_offset = 0;
  private int        bytes_left   = 8; /* Bytes left in last long */
  private long[]     longs        = new long[256];

  /* Record types must be unique. The maximum is 7 bits, or 128 values.   */
  /* Record types 32 and up are recommended for the put_array() methods   */
  /* to keep them separate from the 'real' records.                       */
  /* */
  public byte record_type;          /* type + version just read           */
  public byte record_version;

  public final static byte DATE_RECORD        = 1;
  public final static byte FIXED_RECORD       = 2;
  public final static byte KIO_RECORD         = 3;
  public final static byte TNF_RECORD         = 4;
  public final static byte spare1             = 5;
  public final static byte STF_RECORD         = 6;
  public final static byte CPU_RECORD         = 7;


  /* These are long[] or String[] arrays: */
  public final static byte STRING_ARRAY       = 31;
  public final static byte XFER_SIZES_RECORD  = 32;
  public final static byte XFER_COUNTS_RECORD = 33;
  public final static byte XFER_BYTES_RECORD  = 34;
  public final static byte SEQ_SIZES_RECORD   = 35;
  public final static byte SEQ_COUNTS_RECORD  = 36;
  public final static byte SEQ_STREAMS_RECORD = 37;
  public final static byte SEQ_BYTES_RECORD   = 38;

  /* These six field stay for a while after 3.01 for compatibility reasons. */
  /* Have been replaced with the next three record types: */
  public final static byte NFS2_FIELDS        = 39;
  public final static byte NFS2_RECORD        = 40;
  public final static byte NFS3_FIELDS        = 41;
  public final static byte NFS3_RECORD        = 42;
  public final static byte NFS4_FIELDS        = 43;
  public final static byte NFS4_RECORD        = 44;

  public final static byte NAMED_HEADER       = 45;
  public final static byte NAMED_FIELDS       = 46;
  public final static byte NAMED_LONGS        = 47;


  private Vector  vector_data       = null;
  private int     vector_index      = 0;
  private boolean vector_processing = false;

  private boolean opened_for_write;
  private String  fname             = "Bin Real time?";
  private boolean reached_eof = false;

  private long    file_size  = 0;
  private long    bytes_read = 0;

  private AsyncZip asynczip             = null;

  private WritableByteChannel write_channel = null;
  private ReadableByteChannel read_channel = null;

  private ByteBuffer bb = ByteBuffer.allocate(65536);
  private LongBuffer lb = bb.asLongBuffer();

  private int    how_many_longs = 0;    /* after read_record(): length of raw record */

  private boolean  report_async_results = false; /* Default used to be on */


  private static int   MAX_ARRAY_LENGTH = 0x7fffff; // 3 bytes worth of length */
  private static long  EYE_CATCHER  = 0xEEEE;


  private static Vector open_bin_files = new Vector(16, 0);



  public Bin(String dir, String fname)
  {
    this(new File(dir, fname).getAbsolutePath());
  }
  public Bin(String fname)
  {
    this.fname = fname;
    open_bin_files.addElement(this);

    /* Prevent loop or leaving Bin files open: */
    if (open_bin_files.size() > 15000)
      Vdb.common.failure("Too many (%d) Bin files currently open", open_bin_files.size());
  }



  public boolean isEOF()
  {
    return reached_eof;
  }

  /**
   * Open binary input file
   */
  public void input()
  {
    opened_for_write = false;
    try
    {
      /* If the file ends with '.gz', and we can't find it, remove '.gz': */
      if (fname.endsWith(".gz") && ! new File(fname).exists())
        fname = fname.substring(0, fname.indexOf(".gz"));

      /* If we have instead file ".jz1", use this: */
      if (new File(fname + ".jz1").exists())
        fname = fname + ".jz1";

      /* Piping from stdin: */
      if (fname.endsWith("-"))
      {
        read_channel = Channels.newChannel(System.in);
        bb.rewind();
        readBuffer();
      }

      /* Directly reading binary file: */
      else if (!fname.endsWith(".gz") && !fname.endsWith(".jz1"))
      {
        File file = new File(fname);
        read_channel = new RandomAccessFile(file, "r").getChannel();
        bb.rewind();
        readBuffer();
      }

      /* Asynchronous unzipping: */
      else
      {
        asynczip = new AsyncZip(fname, !opened_for_write);
        asynczip.setReport(report_async_results);
        asynczip.start();
        bb.rewind();
        readBuffer();
      }
    }
    catch (Exception e)
    {
      common.failure(e);
    }
  }



  /**
   * Open binary output file
   */
  public void output()
  {
    opened_for_write = true;
    try
    {
      /* Piping from stdin: */
      if (fname.endsWith("-"))
      {
        write_channel = Channels.newChannel(System.out);
      }

      /* Just directly writing of file: */
      else if (!fname.endsWith(".gz"))
      {
        write_channel = new FileOutputStream(fname).getChannel();
      }

      /* Use asynchronous zipping: */
      else
      {
        asynczip = new AsyncZip(fname, !opened_for_write);
        asynczip.setReport(report_async_results);
        asynczip.start();
        bb.rewind();
      }

      start_new_record();
    }
    catch (Exception e)
    {
      common.ptod("Exception for file: " + fname);
      common.failure(e);
    }

    Fput.chmod(fname);
  }

  public String getFileName()
  {
    return fname;
  }

  /**
   * Tell AsyncZip to not report.
   * It did happen however that AsyncZip was already finished after the
   * Bin.input() before we sent him this flag. Therefore the flag must
   * be set immediately before AsyncZip is started.
   *
   * We do allow setting AFTER the start though, but then we get time dependend.
   */
  public void setAsyncReport(boolean bool)
  {
    report_async_results = bool;
    if (asynczip != null)
      asynczip.setReport(report_async_results);
  }
  public double getCompression()
  {
    return asynczip.getCompression();
  }

  /**
   * See if a certain file exists.
   * If the file name ends with '.gz' then a file ending with '.jz1' is OK.
   */
  public static boolean exists(String fname)
  {
    /* This method is only for Bin files: */
    if (fname.indexOf(".bin") == -1)
      common.failure("Bin.exists() method is only allowed for '.bin' files: " + fname);

    /* If the complete file name is there, great: */
    if (new File(fname).exists())
      return true;

    /* If we have instead file ".gz", allow this: */
    if (new File(fname + ".gz").exists())
      return true;

    /* If the file name ends with '.gz', remove '.gz': */
    if (fname.endsWith(".gz"))
      fname = fname.substring(0, fname.indexOf(".gz"));
    else
      return false;

    /* If the file name without the .gz exists, fine too: */
    if (new File(fname).exists())
      return true;

    /* If we have instead file ".jz1", allow this: */
    if (new File(fname + ".jz1").exists())
      return true;

    return false;
  }


  /**
   * Calculate how far we are into the (input) file
   */
  public int howFar()
  {
    if (opened_for_write)
      return 0;

    if (file_size == 0)
      return 0;

    return(int) (bytes_read * 100 / file_size);
  }

  /**
   * Open fake binary input file. Data goes to/comes from Vector.
   */
  public void fake_input()
  {
    this.opened_for_write = false;
    vector_processing     = true;
    start_new_record();
  }
  public void fake_output()
  {
    this.opened_for_write = true;
    vector_processing     = true;
    start_new_record();
  }
  public boolean isFake()
  {
    return vector_processing;
  }



  /**
   * Fake i/o by using vectors instead.
   */
  public void set_vector_processing(Vector data)
  {
    vector_data = data;
    vector_index = 0;
  }
  public Vector getVector()
  {
    return common.assertit(vector_data);
  }


  /**
   * Initialize new record.
   * Each new record starts with:
   * - 2 bytes eye catcher
   * - 1 byte for record type
   * - 1 byte for record version
   * - 1 byte spare
   * - 3 byte array length
   */
  private void start_new_record()
  {
    array_offset = 0;
    bytes_left   = 0;
    longs[0]     = EYE_CATCHER << 48;

    /* Clear next available long: */
    longs[1]     = 0;
  }

  /**
   * Store record type and versio.
   */
  private void put_type(char type)
  {
    longs[0] |= ((long) type) << 32;
  }

  /**
   * Close output file
   */
  public void close()
  {
    /* Remove the file from the 'open' list: */
    if (!open_bin_files.remove(this))
      common.failure("Unable to remove Bin file: " + this);

    if (vector_processing)
      return;

    try
    {
      if (opened_for_write)
      {
        writeBuffer();
        if (asynczip != null)
        {
          asynczip.putAsyncBuffer(null);
          asynczip.join();
        }
        else
          write_channel.close();
      }

      else if (asynczip == null && read_channel != null)
        read_channel.close();

      if (asynczip != null)
        asynczip.interrupt();
      asynczip = null;

    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }

  public static void closeAll()
  {
    while(open_bin_files.size() > 0)
    {
      Bin bin = (Bin) open_bin_files.firstElement();
      bin.close();
      open_bin_files.remove(bin);
    }
  }


  /**
   * Make sure the data is stored on the right 2/4/8 byte boundary.
   */
  private void set_offset(int bytes)
  {

    if (bytes_left < bytes)
    {
      array_offset++;
      //common.ptod("array_offset: " + array_offset);
      bytes_left = 8;

      /* Reallocate the array if it is too small (only for writing): */
      if (array_offset >= longs.length)
      {
        long[] old = longs;
        longs = new long[old.length * 2];
        System.arraycopy(old, 0, longs, 0, old.length);
        common.ptod("extending Bin array from " + old.length + " to " + longs.length);
      }

      /* Clear next long (when writing): */
      if (opened_for_write)
        longs[ array_offset ] = 0;
    }

    else if (bytes == 4)
    {
      if ( (bytes_left & 3) != 0)
        bytes_left &= ~3;
    }

    else if (bytes == 2)
    {
      if ( (bytes_left & 1) != 0)
        bytes_left &= ~1;
    }

    else if (bytes == 1)
    {
    }
    else if (bytes != 8)
      common.failure("Unexpected byte count: " + bytes);

    //common.ptod("left: " + bytes_left + " bytes: " + bytes + " offset: " + array_offset);

    return;
  }

  public void put_long(long data)
  {
    set_offset(8);
    longs[array_offset] = data;
    bytes_left -= 8;
  }

  public void put_int(int data)
  {
    set_offset(4);

    int shift = (bytes_left - 4) * 8;
    longs[array_offset] |= ((long) data) << shift;
    bytes_left -= 4;
  }

  public void put_short(short data)
  {
    set_offset(2);
    int shift = (bytes_left - 2) * 8;
    longs[array_offset] |= ((long) data) << shift;
    bytes_left -= 2;
  }

  public void put_char(char data)
  {
    set_offset(2);
    int shift = (bytes_left - 2) * 8;
    longs[array_offset] |= ((long) data) << shift;
    bytes_left -= 2;
  }

  public void put_byte(byte data)
  {
    set_offset(1);
    int shift = (bytes_left - 1) * 8;
    longs[array_offset] |= ((long) data) << shift;
    bytes_left -= 1;
  }

  public void put_string(String data)
  {
    if (data == null)
    {
      put_short((short) 0);
      return;
    }

    if (data.length() > Short.MAX_VALUE)
      common.failure("Input data length larger than " + Short.MAX_VALUE + " bytes: " + data);

    put_short((short) data.length());
    for (int i = 0; i < data.length(); i++)
      put_char(data.charAt(i));
  }



  /**
   * Copy raw data for a record to an different Bin file.
   */
  public void copyTo(Bin output_bin)
  {
    //common.ptod("how_many_longs:          " + how_many_longs);
    //common.ptod("array_offset: " + array_offset);

    /* Reallocate the array if it is too small: */
    if (longs.length > output_bin.longs.length)
    {
      long[] old       = output_bin.longs;
      output_bin.longs = new long[longs.length + 256];
      common.ptod("extending Bin array from " + old.length + " to " + output_bin.longs.length);
    }



    /* Copy the raw data to the output file's 'long' array: */
    output_bin.start_new_record();


    //common.ptod("");
    //common.ptod("how_many_longs:          " + how_many_longs);
    //common.ptod("longs.length:            " + longs.length);
    //common.ptod("output_bin.longs.length: " + output_bin.longs.length);


    System.arraycopy(longs, 1, output_bin.longs, 1, longs.length - 1);

    /* Set the proper output length: */
    output_bin.array_offset = how_many_longs;
    output_bin.write_record(record_type, record_version);
  }

  public long get_long()
  {
    set_offset(8);
    long data  = longs[array_offset];
    bytes_left -= 8;
    return data;
  }

  public int get_int()
  {
    set_offset(4);
    int shift   = (bytes_left - 4) * 8;
    int data    = (int) (longs[array_offset] >>> shift);
    bytes_left -= 4;

    return data;
  }

  public short get_short()
  {
    set_offset(2);
    int  shift  = (bytes_left - 2) * 8;
    short data  = (short) (longs[array_offset] >>> shift);
    bytes_left -= 2;

    return data;
  }

  public char get_char()
  {
    set_offset(2);
    int  shift  = (bytes_left - 2) * 8;
    char data   = (char) (longs[array_offset] >>> shift);
    bytes_left -= 2;
    return data;
  }

  public byte get_byte()
  {
    set_offset(1);
    int  shift  = (bytes_left - 1) * 8;
    byte data   = (byte) (longs[array_offset] >>> shift);

    /*
    common.ptod("bytes_left:   " + bytes_left);
    common.ptod("shift:        " + shift);
    common.ptod("array_offset: " + array_offset);
    common.ptod("get_byte():   " + data);
    */
    bytes_left -= 1;

    return data;
  }

  public String get_string()
  {
    short len = get_short();

    if (len == 0)
      return null;
    //common.ptod("len: " + len);

    StringBuffer array = new StringBuffer(len);
    for (int i = 0; i < len; i++)
      array.append(get_char());

    return array.toString();
  }


  /**
   * Store array of longs into binary file
   */
  public void write_record(byte record_type, byte record_version)
  {
    /* Store type and version: */
    put_type((char) (record_type << 8 | record_version));

    /* Store length in longs after rounding to full 'long' size: */
    set_offset(8);
    long how_many = array_offset; // + 1;
    longs[0] |= how_many;

    if (how_many > MAX_ARRAY_LENGTH)
      common.failure("Binary array length too long: " + how_many);

    check_eyecatcher(); // just make sure!

    /* If we are faking i/o by using a vector store data: */
    if (vector_processing)
    {
      long[] fake = new long[ (int) how_many ];
      System.arraycopy(longs, 0, fake, 0, (int) how_many);
      //pbin();
      vector_data.addElement(fake);
      start_new_record();
      return;
    }


    try
    {
      /* if there is not enough room for this next array, flush: */
      if (lb.remaining() < how_many)
      {
        //common.ptod("lb.remaining(): " + lb + " " + lb.remaining() + " " + how_many);
        writeBuffer();
      }

      /* Add new data to buffer: */
      lb.put(longs, 0, (int) how_many);
    }
    catch (IOException e)
    {
      common.failure(e);
    }

    start_new_record();
  }


  /**
   * Tell the Byte Buffer where we are and then write
   */
  private void writeBuffer() throws IOException
  {
    bb.limit(lb.limit() * 8);
    bb.position(lb.position() * 8);
    bb.flip();

    if (asynczip == null)
    {
      if (write_channel != null)
        write_channel.write(bb);
    }

    else
    {
      try
      {
        asynczip.putAsyncBuffer(bb);
      }
      catch (InterruptedException e)
      {
        common.failure(e);
      }

      bb = ByteBuffer.allocate(65536);
      lb = bb.asLongBuffer();
    }

    lb.clear();
  }


  /**
   * Read more data in the buffer.
   */
  private boolean readBuffer()  throws IOException
  {
    int bytes = 0;

    /* Transfer LongBuffer's position to ByteBuffer: */
    bb.position(lb.position() * 8);

    /* If there are bytes at the end of the buffer, move them to front: */
    //common.ptod("pos: " + bb.position());
    if (bb.position() > 0)
      bb.compact();
    //common.ptod("pos: " + bb.position());

    /* Read until we have at least eight bytes in.                             */
    /* This is needed because there is no quarantee we get it all in one read: */
    do
    {
      /* Read from a regular file: */
      if (asynczip == null)
      {
        bytes = read_channel.read(bb);
        bb.flip();
      }

      /* Get from our zipper/unzipper: */
      else
      {
        try
        {
          bb = asynczip.getAsyncBuffer();
        }
        catch (InterruptedException e)
        {
          common.failure(e);
        }
        if (bb != null)
          bytes = bb.remaining();
        else
          bytes = -1;
      }

      /* Negative means EOF: */
      if (bytes < 0)
      {
        reached_eof = true;
        return false;
      }
    } while (bytes == 0);


    /* Create a new LongBuffer from the ByteBuffer we just received: */
    lb = bb.asLongBuffer();

    //common.ptod("read Buffer: " + bytes + " " + lb);

    return true;
  }



  /**
   * Flush the output stream.
   * It does not look as if flush() works.
   */
  public void flush()
  {
    try
    {
      if (opened_for_write)
        writeBuffer();
    }
    catch (IOException e)
    {
      common.failure(e);
    }
  }


  /**
   * Get array of longs from binary file
   */
  public boolean read_record()
  {
    long control_entry = 0;

    /* If we are faking i/o by using a Vector, pick up data: */
    if (vector_processing)
    {
      if ( vector_index >= vector_data.size())
      {
        reached_eof = true;
        return false;
      }
      long[] fake = (long[]) vector_data.elementAt(vector_index++);
      how_many_longs = fake.length;
      if (how_many_longs > longs.length)
        longs = new long[ how_many_longs + 512 ];
      System.arraycopy(fake, 0, longs, 0, how_many_longs);
      check_eyecatcher();
    }



    /* Use binary file: */
    else
    {
      try
      {
        //common.ptod("lb1: " + lb);
        //common.ptod("lb.remaining(): " + lb.remaining());
        /* Make sure we have at least one long: */
        if (lb.remaining() < 1)
        {
          if (!readBuffer())
            return false;
        }

        //common.ptod("lb2: " + lb);

        /* Pick up the first long. Gives us 3 length bytes: */
        longs[0] = control_entry = lb.get();
        //common.ptod("lb3: " + lb);
        bytes_read += 8;

        if (bytes_read >= 60000)
        {
          //common.ptod("lb: " + lines + " " + bytes_read + " " + lb);
        }
        how_many_longs = (int) control_entry & 0xffffff;

        check_eyecatcher();

        /* Reallocate the array if it is too small: */
        if (how_many_longs > longs.length)
        {
          long[] old = longs;
          longs      = new long[how_many_longs];
          longs[0]   = control_entry;
          common.ptod("extending Bin array from " + old.length + " to " + longs.length);
        }

        /* If we have enough data left in the buffer, just use them: */
        if (lb.remaining() >= how_many_longs - 1)
          lb.get(longs, 1, how_many_longs - 1);

        /* Otherwise pick the data up in pieces: */
        else
        {
          for (int i = 1; i < how_many_longs; i++)
          {
            if (lb.remaining() < 1)
            {
              if (!readBuffer())
                return false;
            }
            longs[i] = lb.get();
          }
        }

        bytes_read += (how_many_longs - 1) * 8;
      }

      /* This happens when a collector is still running and the file */
      /* has incomplete records, or just when the file is EOF. */
      catch (IOException e)
      {
        reached_eof = true;
        return false;
      }
    }


    /* Extract type and version: */
    record_type    = (byte) (longs[0] >>> 40);
    record_version = (byte) (longs[0] >>> 32);

    /* Point to the start of the just read record: */
    array_offset = 0;
    bytes_left   = 0;

    return true;
  }


  /**
   * Write a long array.
   * This avoids having to create different unique record types each time
   * we want to write something as simple as a bunch of longs.
   */
  public void put_array(long[] array, int record_type)
  {
    put_array(array, record_type, 0);
  }
  public void put_array(long[] array, int record_type, int version)
  {
    if (record_type > Byte.MAX_VALUE ||
        version     > Byte.MAX_VALUE)
      common.failure("Bin.put_array(): record type and version can be maximum " + Byte.MAX_VALUE);

    put_int(array.length);
    for (int i = 0; i < array.length; i++)
      put_long(array[i]);

    write_record((byte) record_type, (byte) version);
  }
  public long[] get_long_array()
  {
    int length = get_int();
    long[] array = new long[length];
    for (int i = 0; i < length; i++)
      array[i] = get_long();

    return array;
  }


  public void put_array(String just_one, int record_type)
  {
    String[] array = new String[] { just_one };
    put_array(array, record_type, 0);
  }
  public void put_array(String[] array, int record_type)
  {
    put_array(array, record_type, 0);
  }
  public void put_array(String[] array, int record_type, int version)
  {
    if (record_type > Byte.MAX_VALUE ||
        version     > Byte.MAX_VALUE)
      common.failure("Bin.put_array(): record type and version can be maximum " + Byte.MAX_VALUE);

    put_int(array.length);
    for (int i = 0; i < array.length; i++)
      put_string(array[i]);

    write_record((byte) record_type, (byte) version);
  }
  public String[] get_string_array()
  {
    int length = get_int();
    String[] array = new String[length];
    for (int i = 0; i < length; i++)
      array[i] = get_string();

    return array;
  }


  private void check_eyecatcher()
  {
    if (longs[0] >>> 48 != EYE_CATCHER)
      common.failure("Bin.read_record(): the first 16 bits of the " +
                     "record must match the eye catcher: " + fname + " " +
                     Format.f("(%04X) ", EYE_CATCHER) +
                     Format.X(longs[0]));
  }


  private void printBin()
  {
    for (int i = 0; i < 4; i++)
      System.out.print(Format.X(longs[i]));
    System.out.println("");

  }


  public static void save_this_main(String[] args) throws IOException
  {
    int COUNT = Integer.parseInt(args[0]);
    Vector data = new Vector(16, 0);

    //Bin bin = new Bin("test.bin", true);
    Bin bin = new Bin("test.bin");
    bin.set_vector_processing(data);

    /*
    for (int i = 0; i < 655363 ; i++)
    {
      bin.put( (byte) 1);
      bin.write_record();
    }
    bin.put( (byte)  1);
    bin.put( (short) 2);
    bin.put( (int)   4);
    bin.put( (long)  8);
    */


    bin.put_string( "1234567" );
    bin.put_string( "123456" );
    bin.put_string( "12345" );
    bin.put_string( "1234" );
    bin.put_string( "123" );
    bin.put_string( "12" );
    bin.put_string( "1" );
    for (int i = 0; i < COUNT; i++)
      bin.put_byte((byte) (i & 0x7f));
    //bin.pbin();
    bin.write_record((byte) 5, (byte) 5);

    bin.close();

    System.out.println("closed");

    //bin = new Bin("test.bin");
    bin = new Bin("test.bin");
    bin.set_vector_processing(data);
    /*
    common.ptod("byte  1: " + bin.get_byte());
    common.ptod("short 2: " + bin.get_short());
    common.ptod("int   4: " + bin.get_int());
    common.ptod("long  8: " + bin.get_long());
    for (int i = 0; i < 655363 ; i++)
    {
      bin.read_record();
      byte b = bin.get_byte();
      if (b != 1)
      {
        common.ptod("bad data: i=" + i + " b: " + b);
        common.exit(-99);
      }
    }
    */
    bin.read_record();
    //bin.pbin();
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    common.ptod("string: " + bin.get_string());
    for (int i = 0; i < COUNT; i++)
    {
      byte b = bin.get_byte();
      //common.ptod("byte: "   + b);
      if (b != (i & 0x7f))
        common.failure("bad contents, expected: " + i + " obtained: " + b);
    }

    bin.close();

  }



  /**
   * Delete a file that could be a compressed file.
   *
   * Delete the file name requested, and the file names that could have been created
   * by AsyncZip, e.g. file.bin, file.bin.gz and file.bin.jz1/2/3 etc.
   *
   */
  public static void delete(String fname)
  {
    File fptr = new File(fname);
    fname = fptr.getAbsolutePath();
    String parent = new File(fname).getParent();
    String child  = new File(fname).getName();

    /* If the file name ends with '.gz', remove it from file name: */
    if (child.endsWith(".gz"))
      child = child.substring(0, child.indexOf(".gz"));

    //common.ptod("parent: " + parent);
    //common.ptod("child: " + child);

    /* Now delete the file with and without the '.gz': */
    new File(parent, child + ".gz").delete();
    new File(parent, child).delete();

    /* Delete anything that includes '.jz': */
    String[] list = new File(parent).list();
    for (int i = 0; list != null && i < list.length; i++)
    {
      //common.ptod("list[i]: " + list[i]);
      if (list[i].startsWith(child + ".jz"))
      {
        if (!new File(parent, list[i]).delete())
          common.failure("Unable to delete file " + list[i]);
        //common.ptod("deleted: " + list[i]);
      }
    }
  }



  /**
   * Compress a binary file.
   * Skip when the file is empty.
   */
  public static void compressBinFile(String filename)
  {
    if (new File(filename).length() == 0)
      return;

    Bin infile = new Bin(filename);
    Bin otfile = new Bin(filename + ".gz");
    infile.input();
    otfile.output();

    while (infile.read_record())
    {
      infile.copyTo(otfile);
    }
    infile.close();
    otfile.close();

    new File(filename).delete();
    common.ptod("Compressed file " + filename);
  }


  public static void main(String[] args)
  {
    compressBinFile(args[0]);
  }
}




