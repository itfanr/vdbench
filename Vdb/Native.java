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

import java.io.*;
import java.util.*;
import Utils.Format;

/**
 * All native functions.
 */
public class Native
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static long total_size = 0;
  private static long max_size = 0;

  static Object alloc_lock = new Object();

  private static boolean print_each_io = common.get_debug(common.PRINT_EACH_IO);

  static
  {
    common.get_shared_lib();
  }

  /**
   * Open a file name and return a file handle.
   */
  private static native long openfile(String filename, int open_flags, int write_flag);
  public  static        long openFile(String filename, int write_flag)
  {
    return openFile(filename, null, write_flag);
  }
  public  static        long openFile(String filename)
  {
    return openFile(filename, null, 0);
  }
  public  static        long openFile(String filename, OpenFlags open_flags, int write_flag)
  {
    int     flags         = 0;
    boolean sol_directio  = false;
    boolean sol_directiof = false;

    /* Prepare flags to be passed to open(): */
    if (open_flags != null)
    {
      flags         = open_flags.getOpenFlags();
      sol_directio  = open_flags.isOther(OpenFlags.SOL_DIRECTIO);
      sol_directiof = open_flags.isOther(OpenFlags.SOL_DIRECTIO_OFF);
    }

    if (print_each_io) common.ptod("Native.openFile start: " + flags + " " + write_flag + " " + filename);
    long handle = openfile(filename, flags, write_flag);
    if (print_each_io) common.ptod("Native.openFile end:   " + flags + " " + write_flag + " " + filename + " " + handle);

    /* Optionally call Solaris directio() function: */
    if (sol_directio)
    {
      long rc = Native.directio(handle, 1);
      if (rc != 0)
        common.failure("Failed call to directio: " + Errno.xlate_errno(rc));
    }

    /* If the previous run aborted, make sure we reset any stragglers: */
    if (sol_directiof)
    {
      long rc = Native.directio(handle, 0);
      if (rc != 0)
        common.failure("Failed call to directio: " + Errno.xlate_errno(rc));
    }

    return handle;
  }


  /**
   * Close a file
   **/
  private static native long closefile(long fhandle);
  public  static long closeFile(long fhandle)
  {
    return closeFile(fhandle, null);
  }
  public  static long closeFile(long fhandle, OpenFlags open_flags)
  {
    int     flags        = 0;
    boolean fsync        = false;
    boolean sol_directio = false;

    /* Prepare flags to be passed to open(): */
    if (open_flags != null)
    {
      flags        = open_flags.getOpenFlags();
      fsync        = open_flags.isOther(OpenFlags.FSYNC_ON_CLOSE);
      sol_directio = open_flags.isOther(OpenFlags.SOL_DIRECTIO);
    }

    /* Optionally call fsync before closing: */
    if (fsync)
    {
      long rc = Native.fsync(fhandle);
      if (rc != 0)
        common.failure("Native.closeFile(fhandle): fsync failed, rc= " + rc);
    }

    /* If directio was used, turn it off:                                       */
    /* (If run aborts, status will stay in in-memory Vnode. See 'directio_off') */
    if (sol_directio)
    {
      long rc = Native.directio(fhandle, 0);
      if (rc != 0)
        common.failure("Failed call to directio: " + Errno.xlate_errno(rc));
    }

    if (print_each_io) common.ptod("Native.closeFile start: " + fhandle);
    long rc = closefile(fhandle);
    if (print_each_io) common.ptod("Native.closeFile end:   " + fhandle + " " + rc);

    if (rc != 0)
      common.ptod("Native.closeFile(fhandle): close failed: %d", rc);

    return rc;
  }

  /** Unix world: fsync before close **/
  private static native long fsync(long fhandle);

  /** Unix world: directio before open (and close?)  */
  private static native long directio(long fhandle, long on_flag);

  /** Get the size of the file */
  private static native long getsize(long fhandle, String fname);
  public  static        long getSize(long fhandle, String fname)
  {
    if (print_each_io)
      common.ptod("Native.getSize start: " + fhandle + " " + fname);

    long rc = getsize(fhandle, fname);

    if (print_each_io)
      common.ptod("Native.getSize end:   " + fhandle + " " + fname + " " + rc);

    return rc;
  }

  /** Get a simple time of day in microseconds.
   * Simple means that it does not have to be a true time of day, it can be
   * any microsecond value that will allow accurate time value reporting.
   *
   * Tests 4/28/08: 2*2600 Athlon:
   * - using JNI, -  5,200,000 JNI calls using QueryPerformanceCounter()
   * -            - 14,000,000 using RDTSC
   *
   * Added this to my Athlon home system to avoid timer issues:
   * http://www.amd.com/us-en/Processors/TechnicalResources/0,,30_182_871_13118,00.html
   */
  public static native long get_simple_tod();  // usecs

  private static native void multi_io(long[] cmd_fast, int burst);
  public  static        void multiJniCalls(long[] cmd_fast, int burst)
  {
    if (print_each_io)
      common.ptod("Native.multi_io start: " + cmd_fast[6] + " " +
                  cmd_fast[0] + " " + cmd_fast[3] + " " + cmd_fast[4]);

    multi_io(cmd_fast, burst);

    if (print_each_io)
      common.ptod("Native.multi_io end:   " + cmd_fast[6] + " " +
                  cmd_fast[0] + " " + cmd_fast[3] + " " + cmd_fast[4]);
  }


  /** Start and complete the requested read */
  private static native long read(long fhandle, long seek, long length, long buffer);
  public  static        long readFile(long fhandle, long seek, long length, long buffer)
  {
    if (print_each_io)
      common.ptod("Native.readFile start: " + fhandle + " " + seek + " " + length);

    long rc = read(fhandle, seek, length, buffer);

    if (print_each_io)
      common.ptod("Native.readFile end:   " + fhandle + " " + seek + " " + length + " " + rc);

    return rc;
  }

  /**
   * Start and complete the requested write.
   *
   * Negative return indicates a failure.
   * Some day I want to change that so that I can return errors from JNI.
   *
   * However, IO_task.io_error_report() gets the correct errno already from JNI
   * directly, but only for multi_io and file system i/o, not for single
   * i/o.
   */

  private static native long write(long fhandle, long seek, long length, long buffer);
  public  static        long writeFile(long fhandle, long seek, long length, long buffer)
  {
    if (print_each_io)
      common.ptod("Native.writeFile start: " + fhandle + " " + seek + " " + length);

    long rc = write(fhandle, seek, length, buffer);

    if (print_each_io)
      common.ptod("Native.writeFile end:   " + fhandle + " " + seek + " " + length + " " + rc);

    return rc;
  }




  /** Store a Data pattern in C memory */
  static native void store_pattern(int[] array, int key);

  /** Translate an 'int' array to a native buffer */
  // we should have checks to make sure we are dealing with equal lengths!
  static native void array_to_buffer(int[] array, long buffer);

  /** Translate a native buffer to an 'int' array */
  static native void buffer_to_array(int[] array,
                                     long buffer,
                                     long bufsize);


  /** Allocate a native buffer */
  private static native long allocbuf(long bufsize);
  static long allocBuffer(long bufsize)
  {
    //common.ptod("allocbuffer: " + bufsize);
    //common.where(8);
    synchronized(alloc_lock)
    {
      total_size += bufsize;
      max_size = Math.max(total_size, max_size);

      long buffer =  Native.allocbuf(bufsize);
      if (buffer == 0)
      {
        printMemoryUsage();
        common.memory_usage();
        common.failure("allocbuffer() failed");
      }
      return buffer;
    }
  }


  /** Free a native buffer */
  private static native void freebuf(long bufsize, long buffer);
  static void freeBuffer(long bufsize, long buffer)
  {
    synchronized(alloc_lock)
    {
      total_size -= bufsize;
      Native.freebuf(bufsize, buffer);
    }
  }


  public static void printMemoryUsage()
  {
    common.plog("Maximum native memory allocation: " +
                Format.f("%12d", max_size) +
                "; Current allocation: " +
                Format.f("%12d", total_size));
    Native.max_size = 0;
  }

  static native long getKstatPointer(String instance);
  static native long getKstatData(Kstat_data kd, long pointer);
  static native long getCpuData(Kstat_cpu kd);

  private static native long openKstatGlobal();
  private static native long closeKstatGlobal();
  public static long ks_global = 0;
  public static void openKstat()
  {
    ks_global = openKstatGlobal();
  }
  public static void closeKstat()
  {
    if (ks_global == 0)
      common.failure("closeKstat(): null");
    closeKstatGlobal();
    ks_global = 0;
  }





  static native void setup_jni_context(int    xfersize,
                                       double seekpct,
                                       double readpct,
                                       double rhpct,
                                       double whpct,
                                       long   hseek_low,
                                       long   hseek_high,
                                       long   hseek_width,
                                       long   mseek_low,
                                       long   mseek_high,
                                       long   mseek_width,
                                       long   fhandle,
                                       int    threads_used,
                                       int    workload_no,
                                       String lun,
                                       String sd);


  static native void get_one_set_statistics(WG_stats stats,
                                            int       workload_no,
                                            boolean   workload_done);


  /**
   * Allocate shared memory.
   *
   * The boolean and the String are definitely obsolete, were used for vdblite.
   *
   * Memory used to count WG statistics, though there is no longer a need
   * for it to be --shared-- memory. As a matter of fact, we now just use maloc(),
   * so no longer are using shared memory.
   */
  static native void alloc_jni_shared_memory(boolean jni_in_control,
                                             String shared_lib);

  static native void free_jni_shared_memory();


  /**
   * Tape on windows requires explicit rewind and write tapemark.
   */
  static long windowsRewind(long handle, long wait)
  {
    //common.where(8);
    double start = System.currentTimeMillis();
    long   rc    = windows_rewind(handle, wait);
    double end   = System.currentTimeMillis();

    //common.ptod("Ending rewind: " + ((end - start / 1000.)));

    return rc;
  }

  static native String getWindowsErrorText(int msgno);

  static native long windows_rewind(long handle, long wait);
  static native long windows_tapemark(long handle, long count, long wait);

  /**
   * These functions are created for Data Validation for File System tests.
   * Normally it will be handled by multi_io, but for file system testing I
   * don't think it is needed to use multi_io.
   */
  private static native long readAndValidate(long   handle,
                                             long   logical_lba,
                                             long   lba,
                                             int    xfersize,
                                             long   buffer,
                                             int    key_count,
                                             int[]  keys,
                                             String name);
  public static long readAndValidateBlock(long   handle,
                                          long   logical_lba,
                                          long   lba,
                                          int    xfersize,
                                          long   buffer,
                                          int    key_count,
                                          int[]  keys,
                                          String name)
  {
    if (name.length() != 8)
      common.failure("fillAndWriteBlock(): 'name' must be 8 characters long: >>>" + name + "<<<");

    if (print_each_io) common.ptod("Native.readAndValidate start: %s %08x ", name, lba);
    long rc = readAndValidate(handle,logical_lba,lba,xfersize,buffer,key_count,keys,name);
    if (print_each_io) common.ptod("Native.readAndValidate end:   %s %08x %d", name, lba, rc);
    return rc;
  }

  private static native long fillAndWrite(long   handle,
                                          long   logical_lba,
                                          long   lba,
                                          int    xfersize,
                                          long   buffer,
                                          int    key_count,
                                          int[]  keys,
                                          String name);
  public static long fillAndWriteBlock(long   handle,
                                       long   logical_lba,
                                       long   lba,
                                       int    xfersize,
                                       long   buffer,
                                       int    key_count,
                                       int[]  keys,
                                       String name)
  {
    if (name.length() != 8)
      common.failure("fillAndWriteBlock(): 'name' must be 8 characters long: >>>" + name + "<<<");
    if (print_each_io) common.ptod("Native.fillAndWrite start: %s %08x", name, lba);
    long rc = fillAndWrite(handle, logical_lba,lba,xfersize,buffer,key_count,keys,name);
    if (print_each_io) common.ptod("Native.fillAndWrite end:   %s %08x %d", name, lba, rc);
    return rc;
  }

  static native void fillLFSR(int[] sector_array, long lba, int key, String name);

  static native int eraseFileSystemCache(long handle, long size);


  public static void main(String[] args)
  {
    long lba = 773586944;
    int  key = 13;
    String name = "12345678";

    int[] array = new int[512 / 4 ];
    fillLFSR(array, lba, key, name);

    for (int i = 0; i < array.length; i+=4)
    {
      String line = String.format("0x%03x: ", i*4);
      line += Format.f("%08x ", array[i]);
      line += Format.f("%08x ", array[i+1]);
      line += Format.f("%08x ", array[i+2]);
      line += Format.f("%08x ", array[i+3]);
      common.ptod("line: " + line);
    }
  }

}


