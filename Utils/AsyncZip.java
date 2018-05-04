package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.nio.*;
import java.io.*;
import java.util.zip.*;



/**
 * Do asynchronous (un)zipping of an (in)output file.
 * At this time this is only planned for a Bin() file.
 * Probably works for any file.
 *
 *
 * I found th setLevel() override below on:
 * http://weblogs.java.net/blog/mister__m/archive/2003/12/achieving_bette.html
 *
 * This allowed me to set the compression level, therefore saving back the
 * extra 17% cpu time that I needed because having java do the zipping
 * is more expensive than doing gzip with default compression level.
 * Normally you can't override the default level 6.
 *
 * test results with a 128m raw file:
 * - gzip: 52 seconds cpu on a 900 mhz Sun-Fire-280R
 * - java: level 6 (default)): 62 seconds
 * -       level 5:            37
 * -       level 4:            28   This added 2.11% extra to compressed file size.
 * -       level 3:            27.
 *
 * Decided to go with level 4
 *
 */
public class AsyncZip extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  private String               fname      = null;
  private int                  file_number    = 0;
  private boolean              reading        = true;
  private BufferedInputStream  zip_in_stream  = null;
  private GZIPOutputStream     zip_out_stream = null;

  private int PIPE_LENGTH    = 8;
  private Semaphore    sema_get_slot = new Semaphore(0);
  private Semaphore    sema_put_slot = new Semaphore(PIPE_LENGTH);
  private ByteBuffer[] pipe          = new ByteBuffer[PIPE_LENGTH];
  private int          get_slot      = 0;
  private int          put_slot      = 0;

  private double total_bytes = 0;

  private static int DEFAULT_LEVEL = 4;

  private boolean  report_async_results = true;




  public AsyncZip(String fname_in, boolean read)
  {
    fname   = new File(fname_in).getAbsolutePath();
    reading = read;

    try
    {
      if (reading)
        createZipInStream();
      else
        createZipOutStream();
    }

    catch (Exception e)
    {
      common.ptod("Error while (un) compressing file: " + fname);
      Vdb.common.failure(e);
    }

    //this.start();
  }

  public void setReport(boolean bool)
  {
    report_async_results = bool;
  }


  /**
   * Create the next zip input stream. Because of the 2GB java zip limit
   * we will split a 2GB file into multiple pieces.
   */
  private boolean createZipInStream() throws Exception
  {
    /* If we come here the first time, just allocate the file: */
    if (file_number == 0)
    {
      BufferedInputStream bistream  = new BufferedInputStream(new FileInputStream(fname));
      DataInputStream     in_stream = new DataInputStream(bistream);
      zip_in_stream = new BufferedInputStream(new GZIPInputStream(in_stream));
      file_number++;

      return true;
    }

    /* If we come back here see if the next file exists: */
    file_number++;
    String name;
    if (fname.endsWith(".gz"))
      name = fname.substring(0, fname.lastIndexOf(".gz"));
    else
      name = fname.substring(0, fname.lastIndexOf(".jz"));

    /* If the file does not exist, then EOF: */
    if (!new File(name + ".jz" + file_number).exists())
      return false;

    /* The file exists, open it: */
    fname = name + ".jz" + file_number;

    BufferedInputStream bistream  = new BufferedInputStream(new FileInputStream(fname));
    DataInputStream     in_stream = new DataInputStream(bistream);
    zip_in_stream = new BufferedInputStream(new GZIPInputStream(in_stream));

    return true;
  }



  /**
   * Create the next zip output stream. Because of the 2GB java zip limit
   * we will split a 2GB file into multiple pieces.
   */
  private void createZipOutStream() throws Exception
  {
    /* If we come here for the first time, first delete all old '.jz' files: */
    /* (We don't want old longer files to stay around) */
    if (file_number == 0)
    {
      /* Delete all possible old files related to this file name: */
      Bin.delete(fname);

      file_number++;
      BufferedOutputStream bostream   = new BufferedOutputStream(new FileOutputStream(fname));
      DataOutputStream     out_stream = new DataOutputStream(bostream);
      zip_out_stream = new GZIPOutputStream(out_stream)
      {
        {
          def.setLevel(DEFAULT_LEVEL);
        }
      };

      return;
    }

    /* We are here again. Close output stream: */
    zip_out_stream.close();

    /* If this was the close for the first file, rename '.gz' to '.jz1': */
    if (file_number == 1)
    {
      String new_name = fname.substring(0, fname.indexOf(".gz")) + ".jz1";
      if (!new File(fname).renameTo(new File(new_name)))
        Vdb.common.failure("Unable to rename " + fname + " to " + new_name);
      common.ptod("");
      common.ptod("Compression code in Java only allows 2Gigabytes of uncompressed data. ");
      common.ptod("Since we have more data than that the file will be renamed from ");
      common.ptod("a file name ending with '.gz' to ending with '.jz1'");
      common.ptod("and new files will be created ending with '.jz2, .jz3, etc'");
      common.ptod("These file will then be all concatenated at read time.");
      common.ptod("");
    }

    /* Create new file name: */
    file_number++;
    fname = fname.substring(0, fname.lastIndexOf(".")) + ".jz" + file_number;
    BufferedOutputStream bostream   = new BufferedOutputStream(new FileOutputStream(fname));
    DataOutputStream     out_stream = new DataOutputStream(bostream);
    zip_out_stream = new GZIPOutputStream(out_stream)
    {
      {
        def.setLevel(DEFAULT_LEVEL);
      }
    };

    return;
  }



  public static void setLevel(int lvl)
  {
    DEFAULT_LEVEL = lvl;
  }


  /**
   * Send a ByteBuffer to be compressed.
   */
  public void putAsyncBuffer(ByteBuffer bb) throws InterruptedException
  {
    //common.ptod("putAsyncBuffer bb: " + bb);
    sema_put_slot.acquire();
    synchronized (this)
    {
      pipe[ put_slot++ % PIPE_LENGTH ] = bb;
      sema_get_slot.release();
    }
  }


  /**
   * Get a ByteBuffer of data which is now uncompressed.
   */
  public ByteBuffer getAsyncBuffer() throws InterruptedException
  {
    sema_get_slot.acquire();
    synchronized (this)
    {
      ByteBuffer bb = pipe[ get_slot++ % PIPE_LENGTH ];
      sema_put_slot.release();
      //common.ptod("getAsyncBuffer bb: " + bb);
      return bb;
    }

  }



  /**
   * Compress the data that we receive from getAsyncBuffer().
   *
   * Java zipping can not handle 2GB plus (uncompressed) files. We therefore
   * stop just before we hit 2GB, and then start creating file names ending
   * with .jz2, .jz3, etc. The original .gz is renamed to .jz1
   *
   */
  public void run()
  {
    long first_long;
    long GB2 = 1024l * 1024l * 1024l * 2;
    long MB2 = 1024l * 1024l * 2;

    try
    {
      if (!reading)
      {
        while (true)
        {
          ByteBuffer bb = getAsyncBuffer();
          if (bb == null)
            break;

          zip_out_stream.write(bb.array(), 0, bb.limit());
          total_bytes += bb.limit();

          /* Make sure you are LESS than 2 GB: */
          if (total_bytes > GB2 - MB2 )
          {
            zip_out_stream.close();
            reportCompression();
            createZipOutStream();
            total_bytes = 0;
          }
        }

        zip_out_stream.close();
        reportCompression();
      }

      else
      {
        while (true)
        {
          ByteBuffer bb = ByteBuffer.allocate(65536); // should be same as Bin size
          int bytes = zip_in_stream.read(bb.array(), 0, bb.capacity());
          if (bytes < 0)
          {
            zip_in_stream.close();
            reportUnCompression(total_bytes);
            if (! createZipInStream())
              break;
            total_bytes = 0;
            continue;
          }

          bb.position(bytes);
          total_bytes += bytes;
          bb.flip();
          putAsyncBuffer(bb);
        }
        putAsyncBuffer(null);

        //reportUnCompression(total_bytes);
      }
    }

    catch (InterruptedException e)
    {
    }

    catch (Exception e)
    {
      common.ptod("AsyncZip error; filename: " + fname);
      Vdb.common.failure(e);
    }

    catch (Throwable t)
    {
      Vdb.common.abnormal_term(t);
    }
  }


  public void close() throws Exception
  {
    if (!reading)
      zip_out_stream.close();
  }


  public double getCompression()
  {
    double filesize = new File(fname).length();
    return (filesize * 100 / total_bytes);
  }

  private void reportCompression()
  {
    if (report_async_results)
    {
      double filesize = new File(fname).length();
      printf pf = new printf("AsyncZip: Compression to file %s: in: %d; out: %d; compression: %.2f%%");
      pf.add(fname);
      pf.add(total_bytes);
      pf.add(filesize);
      pf.add((filesize * 100 / total_bytes));
      common.ptod(pf.print());
    }
  }

  private void reportUnCompression(double bytes)
  {
    if (report_async_results)
    {
      double filesize = new File(fname).length();
      printf pf = new printf("AsyncZip: Uncompress of file %s; in: %d; out: %d; compression: %.2f%%");
      pf.add(fname);
      pf.add(filesize);
      pf.add(bytes);
      pf.add((filesize * 100 / bytes));
      common.ptod(pf.print());
    }
  }


  public static void main(String[] args) throws Exception
  {
  }
}
