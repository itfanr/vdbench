package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static long total_size = 0;
  private static long max_size = 0;

  static Object alloc_lock = new Object();

  static
  {
    common.get_shared_lib();
  }


  /**
   * Solaris only: Native sleep.
   */
  public static native void nativeSleep(long wakeup);

  /**
   * Open a file name and return a file handle.
   */
  public  static native long openfile(String filename, int open_flags, int write_flag);
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

    if (write_flag != 0 && common.get_debug(common.NEVER_OPEN_FOR_WRITE))
      common.failure("'NEVER_OPEN_FOR_WRITE' is set. Trying to open " + filename);


    /* Prepare flags to be passed to open(): */
    if (open_flags != null)
    {
      flags = open_flags.getOpenFlags();

      /* directio only for files, not for raw volumes: */
      if (!filename.startsWith("/dev/"))
      {
        sol_directio  = open_flags.isOther(OpenFlags.SOL_DIRECTIO);
        sol_directiof = open_flags.isOther(OpenFlags.SOL_DIRECTIO_OFF);
      }
    }

    //common.ptod("Native.openFile start: %s %d write_flag: %d",    filename, flags, write_flag);
    long handle = openfile(filename, flags, write_flag);
    //common.ptod("Native.openFile end:   %s %d write_flag: %d %x", filename, flags, write_flag, handle);

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
    if (fhandle <= 0)
      common.failure("Bad file handle: %d", fhandle);
    return closeFile(fhandle, null);
  }
  public  static long closeFile(long fhandle, OpenFlags open_flags)
  {
    boolean fsync        = false;
    boolean sol_directio = false;

    /* Prepare flags to be passed to open(): */
    if (open_flags != null && !File_handles.getFileName(fhandle).startsWith("/dev/"))
    {
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

    long rc = closefile(fhandle);

    if (rc != 0)
      common.ptod("Native.closeFile(fhandle): close failed: %d", rc);

    return rc;
  }

  /** Unix world: fsync before close **/
  public static native long fsync(long fhandle);

  /** Unix world: directio before open (and close?)  */
  private static native long directio(long fhandle, long on_flag);

  /** Get the size of the file */
  private static native long getsize(long fhandle, String fname);
  public  static        long getSize(long fhandle, String fname)
  {
    long rc = getsize(fhandle, fname);

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


  /** Start and complete the requested read */
  private static native long read(     long fhandle, long seek, long length, long buffer, int wkl);
  public  static        long readFile( long fhandle, long seek, long length, long buffer)
  {
    if (fhandle <= 0)
      common.failure("Bad file handle: %d", fhandle);
    return readFile(fhandle, seek, length, buffer, -1);
  }

  private static long total_sleep = 0;
  private static long sleeps = 0;
  public  static        long readFile( long fhandle, long seek, long length, long buffer, int wkl)
  {
    if (fhandle <= 0)
      common.failure("Bad file handle: %d", fhandle);
    long rc = read(fhandle, seek, length, buffer, wkl);

    return rc;
  }

  /**
   * Start and complete the requested write.
   *
   * Negative return indicates a failure.
   * Some day I want to change that so that I can return errors from JNI.
   *
   * However, IO_task.io_error_report() gets the correct errno already from JNI
   * directly.
   */
  private static native long write(    long fhandle, long seek, long length, long buffer, int wkl);
  public  static        long writeFile(long fhandle, long seek, long length, long buffer)
  {
    if (fhandle <= 0)
      common.failure("Bad file handle: %d", fhandle);
    return writeFile(fhandle, seek, length, buffer, -1);
  }
  public  static        long writeFile(long fhandle, long seek, long length, long buffer, int wkl)
  {
    if (fhandle <= 0)
      common.failure("Bad file handle: %d", fhandle);
    long rc = write(fhandle, seek, length, buffer, wkl);

    return rc;
  }

  /**
  * Blocks written this way will have each 4k of the buffer overlaid with the
  * current TOD in microseconds, xor'ed with the lba.
  * This then prevents the data from accidentally (or purposely) benefitting from
  * possible undocumented Dedup.
  */
  private static native long noDedupWrite(   long fhandle, long seek, long length, long buffer, int jni_index);
  public  static        long noDedupAndWrite(long fhandle, long seek, long length, long buffer, int jni_index)
  {
    return noDedupWrite(fhandle, seek, length, buffer, jni_index);
  }


  /** Store a Data pattern in C memory  */
  static native void store_pattern(int[] array);

  /** Translate an 'int' array to a native buffer */
  private static native void array_to_buffer(int[] array, long buffer, int bytes);
  public  static        void arrayToBuffer(  int[] array, long buffer, int bytes)
  {
    array_to_buffer(array, buffer, bytes);
  }
  public static         void arrayToBuffer(  int[] array, long buffer)
  {
    array_to_buffer(array, buffer, array.length * 4);
  }

  /** Translate a native buffer to an 'int' array */
  static native void buffer_to_array(int[] array, long buffer, int bytes);

  /** Translate a native buffer to a 'long' array */
  static native void longBufferToArray(long[] array, long buffer, int bytes);
  static native void arrayToLongBuffer(long[] array, long buffer, int bytes);


  /** Allocate a native buffer */
  private static native long allocbuf(int bytes);
  static long allocBuffer(int bytes)
  {
    synchronized(alloc_lock)
    {

      long buffer =  Native.allocbuf(bytes);
      if (buffer == 0)
      {
        printMemoryUsage();
        common.memory_usage();
        common.failure("allocbuffer() failed. Asking for %d; already have %d allocated",
                       bytes, total_size);
      }

      total_size += bytes;
      max_size = Math.max(total_size, max_size);

      //common.ptod("allocBuffer: %,8d bytes from 0x%08x to 0x%08x",
      //            bytes, buffer, buffer+bytes-1);

      //common.ptod("allocBuffer: %,8d %,12d", bytes, total_size);
      //common.where(8);

      return buffer;
    }
  }


  /** Free a native buffer */
  private static native void freebuf(int bufsize, long buffer);
  static void freeBuffer(int bufsize, long buffer)
  {
    synchronized(alloc_lock)
    {
      total_size -= bufsize;
      Native.freebuf(bufsize, buffer);
    }
    //common.ptod("freeBuffer:  %,8d %,12d", bufsize, total_size);
    //common.where(8);
  }


  public static void printMemoryUsage()
  {
    common.plog("Maximum native memory allocation: %,12d; Current allocation: %,12d",
                max_size, total_size);
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





  static native void setup_jni_context(int    jni_index,
                                       String sd,
                                       long[] read_hist,
                                       long[] write_hist);


  static native String get_one_set_statistics(int    jni_index,
                                              long[] read_hist,
                                              long[] write_hist);


  /**
   * Allocate shared memory.
   */
  static native void alloc_jni_shared_memory(long pid);
  static        void allocSharedMemory()
  {
    /* Process id: (java 1.5+) */
    String pid = common.getProcessIdString();
    if (!common.isNumeric(pid))
      common.failure("Invalid process id: '%s'", pid);

    /* PID is only included when we know that everything will just stay within  */
    /* the same Vdbench execution, including validate=continue.                 */
    /* Maybe some day we'll have a list of 'old' pids included, unlikely though */
    if (Validate.isJournaling() || Validate.isContinueOldMap())
    {
      alloc_jni_shared_memory(common.getProcessId());
    }
    else
    {
      /* Temporarily removed because the journaling flag is not set yet */
      /* when this is called.                                           */
      /* Though I COULD store it in the DV header, but not CHECK it?    */
      /* THAT's what I decided!                                         */
      alloc_jni_shared_memory(common.getProcessId());
    }
  }




  public static native long getSolarisPids();

  static native String getWindowsErrorText(int msgno);

  private static native long multiKeyReadAndValidate(long   handle,
                                                     int    data_flag,
                                                     long   file_start_lba,
                                                     long   lba,
                                                     int    xfersize,
                                                     long   buffer,
                                                     int    key_count,
                                                     int[]  keys,
                                                     long[] compressions,
                                                     long[] dedup_sets,
                                                     String name,
                                                     int    wkl);
  public static long multiKeyReadAndValidateBlock(long   handle,
                                                  int    data_flag,
                                                  long   file_start_lba,
                                                  long   lba,
                                                  int    xfersize,
                                                  long   buffer,
                                                  int    key_count,
                                                  int[]  keys,
                                                  long[] compressions,
                                                  long[] dedup_sets,
                                                  String name,
                                                  int    wkl)
  {
    if (name.length() != 8)
      common.failure("Native.multiKeyReadAndValidate: 'name' must be 8 characters long: >>>" + name + "<<<");

    long rc = multiKeyReadAndValidate(handle, data_flag, file_start_lba, lba, xfersize, buffer, key_count,
                                      keys, compressions, dedup_sets, name, wkl);
    return rc;
  }

  private static native long multiKeyFillAndWrite(long   handle,
                                                  long   tod,
                                                  int    data_flag,
                                                  long   file_start_lba,
                                                  long   file_lba,
                                                  int    data_length,
                                                  long   pattern_lba,
                                                  int    pattern_length,
                                                  long   buffer,
                                                  int    key_count,
                                                  int[]  keys,
                                                  long[] compressions,
                                                  long[] dedup_sets,
                                                  String name,
                                                  int    wkl);
  public static long multiKeyFillAndWriteBlock(long   handle,
                                               long   tod,
                                               int    data_flag,
                                               long   file_start_lba,
                                               long   file_lba,
                                               int    data_length,
                                               long   pattern_lba,
                                               int    pattern_length,
                                               long   buffer,
                                               int    key_count,
                                               int[]  keys,
                                               long[] compressions,
                                               long[] dedup_sets,
                                               String name,
                                               int    wkl)

  {
    if (name.length() != 8)
      common.failure("multiKeyFillAndWriteBlock(): 'name' must be 8 characters long: >>>" + name + "<<<");
    long rc = multiKeyFillAndWrite(handle, tod, data_flag, file_start_lba,
                                   file_lba, data_length, pattern_lba,
                                   pattern_length, buffer, key_count,
                                   keys, compressions, dedup_sets, name, wkl);
    return rc;
  }

  static native void fillLfsrArray  (int[] sector_array, long lba, int key, String name);
  static native void fillLfsrBuffer (long  buffer,       int  xfersize, long lba, int key, String name);

  static native int eraseFileSystemCache(long handle, long size);


  static native String getErrorText(int errno);


  /**
   * Cause a chmod 777 fname
   */
  public static native long chmod(String fname);

  /**
   * Get the cpu tick count (only used for Linux at this time)
   */
  public static native long getTickCount();


  /**
   * Solaris only: ftruncate
   */
  public static native long truncateFile(long handle, long size);


  public static void main(String[] args)
  {
    long lba = 773586944;
    int  key = 13;
    String name = "12345678";

    int[] array = new int[512 / 4 ];
    fillLfsrArray(array, lba, key, name);

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


