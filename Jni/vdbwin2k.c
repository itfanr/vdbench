

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */


/*
 * Author: Henk Vandenbergh.
 */


#include "vdbjni.h"
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <windows.h>
#include <winioctl.h>


#define WINMSG(x)                                                \
{                                                                \
    printf("==========> %s GetLastError(): %d \n", x, GetLastError());           \
}


static char c[] =
"Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

static jlong hertz = 0;

#define GB ( (double) 1024 * 1024 * 1024)


extern void snap(JNIEnv *env, char *text, void* start, int length);

extern struct Shared_memory *shared_mem;


static jlong try_GetFileSize(jlong fhandle)
{
  LARGE_INTEGER size;

  //printf("Trying GetFileSize(): ");
  size.LowPart = GetFileSize((HANDLE) fhandle, &size.HighPart);

  if (size.LowPart == 0xFFFFFFFF && GetLastError() != NO_ERROR)
  {
    //printf("GetLastError(): %d\n", GetLastError());
    return 0;
  }

  //printf("size: %.9fGB \n", (double) size.QuadPart / GB);

  return(jlong) size.QuadPart;
}


static jlong try_GetDiskFreeSpaceEx(const char *fname)
{
  char work[1024];
  BOOL rc;
  ULARGE_INTEGER user;
  ULARGE_INTEGER l2;
  ULARGE_INTEGER l3;

  sprintf(work, "%s//", fname);
  //printf("Trying GetDiskFreeSpaceEx(): ");
  //strcpy(work, "//./k://");
  rc = GetDiskFreeSpaceEx(work, &user, &l2, &l3);
  if (!rc)
  {
    //printf("GetLastError(): %d\n", GetLastError());
    return 0;
  }

  //printf("size: %.9fGB \n", (double) (__int64) user.QuadPart / GB );

  return(jlong) user.QuadPart;
}


static jlong try_IOCTL_DISK_GET_PARTITION_INFO(jlong fhandle)
{
  PARTITION_INFORMATION pinfo;
  jlong bytes;
  jlong size;
  BOOL rc;

  //printf("Trying IOCTL_DISK_GET_DRIVE_GEOMETRY(): ");
  rc = DeviceIoControl((HANDLE) fhandle,
                       IOCTL_DISK_GET_PARTITION_INFO,
                       NULL,
                       0,
                       &pinfo,
                       (DWORD) sizeof(pinfo),
                       (LPDWORD) &bytes,

                       NULL);

  if (!rc)
  {
    //printf("try_IOCTL_DISK_GET_PARTITION_INFO(): %d\n", GetLastError());
    return 0;
  }

  size = pinfo.PartitionLength.QuadPart;

  //printf("try_IOCTL_DISK_GET_PARTITION_INFO: %.9fGB \n", (double) size / GB );

  return size;
}



static jlong try_IOCTL_DISK_GET_DRIVE_GEOMETRY(jlong fhandle)
{
  DISK_GEOMETRY geom;
  jlong bytes;
  jlong size;
  BOOL rc;

  //printf("Trying IOCTL_DISK_GET_DRIVE_GEOMETRY(): ");
  rc = DeviceIoControl((HANDLE) fhandle,
                       IOCTL_DISK_GET_DRIVE_GEOMETRY,
                       NULL,
                       0,
                       (LPVOID) &geom,
                       (DWORD) sizeof(geom),
                       (LPDWORD) &bytes,

                       NULL);

  if (!rc)
  {
    //printf("GetLastError(): %d\n", GetLastError());
    return 0;
  }

  size = geom.Cylinders.QuadPart * geom.TracksPerCylinder *
         geom.SectorsPerTrack    * geom.BytesPerSector;

  //printf("size: %.9fGB \n", (double) size / GB );

  return size;
}



extern jlong file_size(JNIEnv *env, jlong fhandle, const char* fname)
{
  jlong size, size1, size2, size3, size4;


  /* Try them all three, no matter what: */
  size1 = try_GetFileSize(fhandle);
  size2 = try_GetDiskFreeSpaceEx(fname);
  size3 = try_IOCTL_DISK_GET_PARTITION_INFO(fhandle);
  size4 = try_IOCTL_DISK_GET_DRIVE_GEOMETRY(fhandle);



  /* Return the first valid size: */
  if (size1) size = size1;
  //else if (size2) size = size2;    is free space, not lun space!!
  else if (size3) size = size3;
  else if (size4) size = size4;
  else            size = 0;

  //printf("Size1: %I64d\n", size1);
  //printf("Size2: %I64d\n", size2);
  //printf("size3: %I64d\n", size3);
  //printf("size4: %I64d\n", size4);
  //printf("Returning size to java: %.9fGB\n", size / GB);

  return size;
}


/**
 * Get message text for a windows getlastError() message
 */
char* getWindowsErrorText(int msg)
{
  LPVOID lpMsgBuf;
  FormatMessage(
               FORMAT_MESSAGE_ALLOCATE_BUFFER |
               FORMAT_MESSAGE_FROM_SYSTEM |
               FORMAT_MESSAGE_IGNORE_INSERTS,
               NULL,
               msg,
               0, // Default language
               (LPTSTR) &lpMsgBuf,
               0,
               NULL
               );

  return(char*) lpMsgBuf;
}

JNIEXPORT jstring JNICALL Java_Vdb_Native_getWindowsErrorText(JNIEnv *env,
                                                              jclass  this,
                                                              jint    msg)
{
  LPVOID lpMsgBuf = getWindowsErrorText(msg);

  return(*env)->NewStringUTF(env, lpMsgBuf);
}


extern jlong file_close(JNIEnv *env, jlong fhandle)
{
  jlong rc;
  rc = CloseHandle((HANDLE) fhandle);
  if (rc)
    return 0;
  else
    return(jlong) GetLastError();
}


extern jlong file_open(JNIEnv *env, const char *filename, int openflag, int write)
{
  HANDLE fhandle;
  DWORD  access_type;
  int WINDOWS_DIRECTIO = 1;
  DWORD OVERLAP = FILE_FLAG_OVERLAPPED;

  /* Set access type: */
  if (write)
    access_type = GENERIC_READ | GENERIC_WRITE;
  else
    access_type = GENERIC_READ;

  if (openflag != 0 && openflag !=  WINDOWS_DIRECTIO)
  {
    PTOD1("Invalid open parameter for Windows: 0x%08x", openflag);
    abort();
  }

  /* Open raw disk or a file system file with flush:              */
  /* (We want buffering on a file system, unless asked for flush) */
  //printf("\n\n\n\n\n Opening test version %s \n\n\n", filename);
  if (memcmp(filename, "\\\\", 2) == 0)
  {
    //PTOD1("Createfile physical: %s", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_EXISTING,
                         /*FILE_FLAG_WRITE_THROUGH  |  */ FILE_FLAG_NO_BUFFERING | OVERLAP,
                         NULL);
  }
  else if (openflag)
  {
    //PTOD1("Createfile flush: %s", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_ALWAYS ,
                         /*FILE_FLAG_WRITE_THROUGH  |  */ FILE_FLAG_NO_BUFFERING | OVERLAP,
                         NULL);
  }
  else
  {
    //PTOD1("Createfile filesystem: %s", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_ALWAYS ,
                         OVERLAP ,
                         NULL);
  }


  if (fhandle == (HANDLE)-1)
  {
    PTOD3("CreateFile failed: %s GetLastError: (%d) %s",
          filename, GetLastError(), getWindowsErrorText(GetLastError()));
    return -1;
  }

  return(jlong) fhandle;
}



extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int ret, last;
  DWORD bytes = 777;
  LARGE_INTEGER seeklong;
  HANDLE hEvent;
  OVERLAPPED ovl;

  //if (seek & 0x2)
  //  return 0;

  /* Store seek address in Overlap structure */
  seeklong.QuadPart = seek;
  memset(&ovl, sizeof(ovl), 0);
  ovl.Offset     = seeklong.LowPart;
  ovl.OffsetHigh = seeklong.HighPart;
  ovl.hEvent     = CreateEvent(NULL, TRUE, FALSE, NULL);
  if (!ovl.hEvent)
  {
    int last_error = GetLastError();
    printf("\nCreate event failed with error:%d", last_error);
    return last_error;
  }

  //PTOD2("read: %I64d %I64d", fhandle, seek);

  if (fhandle == 0)
    WINMSG("zero handle");
  if (buffer == 0)
    WINMSG("zero buffer");
  if (length == 0)
    WINMSG("zero length");


  /* Set fixed values at start and end of buffer: */
  //PTOD("debug: w1");
  prepare_read_buffer(env, buffer, length);
  //PTOD("debug: w2");

  /* Do the synchronous read: */
  ret  = ReadFile((HANDLE) fhandle, (LPVOID) buffer, (DWORD) length, &bytes, &ovl);
  last = GetLastError();
  //PTOD("debug: w3");
  //PTOD3("read1: %8I64d %12I64d %4d", fhandle, seek, last);

  /* Read will complete synchronous: */
  if (!ret)
  {
    if (last == ERROR_IO_PENDING)
    {
      ret  = GetOverlappedResult((HANDLE) fhandle, &ovl, &bytes, TRUE);
      last = GetLastError();
      //PTOD3("read2: %8I64d %12I64d %4d", fhandle, seek, last);
      if (!ret)
      {
        PTOD1("file_read error1: %d", last);
        PTOD1("handle: %p", fhandle);
        PTOD1("seek:   %p", seek);
        PTOD1("length: %p", length);
        PTOD1("buffer: %p", buffer);
        return last;
      }
    }
    else
    {
      PTOD1("file_read error2: %d", last);
      PTOD1("handle: %p", fhandle);
      PTOD1("seek:   %p", seek);
      PTOD1("length: %p", length);
      PTOD1("buffer: %p", buffer);
      return last;
    }
  }

  ResetEvent(ovl.hEvent);
  CloseHandle(ovl.hEvent);

  /* Double check byte count: */
  if (bytes != length)
  {
    PTOD2("Invalid byte count. Expecting %I64d, but transferred only %d bytes.", length, bytes);
    return 798;
  }

  /* Make sure read was REALLY OK: */
  return check_read_buffer(env, buffer, length);
}




extern jlong file_write(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int ret, last;
  DWORD bytes = 777;
  LARGE_INTEGER seeklong;
  OVERLAPPED ovl;

  //snap(env, "buf", (void*) buffer, 16);

  //if (seek & 0x2)
  //  return 0;

  /* Store seek address in Overlap structure */
  seeklong.QuadPart = seek;
  memset(&ovl, 0, sizeof(ovl));
  ovl.Offset     = seeklong.LowPart;
  ovl.OffsetHigh = seeklong.HighPart;
  ovl.hEvent     = CreateEvent(NULL, TRUE, FALSE, NULL);
  if (!ovl.hEvent)
  {
    int last_error = GetLastError();
    printf("\nCreate event failed with error:%d", last_error);
    return last_error;
  }

  if (seek < 0)
  {
    PTOD1("Negative lba: %p", seek);
    abort();
  }


  //PTOD1("handle: %p", fhandle);
  //PTOD1("seek:   %p", seek);
  //PTOD1("length: %p", length);
  //PTOD1("buffer: %p", buffer);
  //PTOD1("pbuff : %p", shared_mem->pattern);
  //PTOD1("plength : %d", shared_mem->pattern_length);
  //PTOD1("offset  : %d", buffer - (jlong) shared_mem->pattern);

  /* Do the synchronous write: */
  ret  = WriteFile((HANDLE) fhandle, (LPVOID) buffer, (DWORD) length, &bytes, &ovl);
  last = GetLastError();

  /* Write will complete synchronous: */
  if (!ret)
  {
    if (last == ERROR_IO_PENDING)
    {
      ret  = GetOverlappedResult((HANDLE) fhandle, &ovl, &bytes, TRUE);
      last = GetLastError();
      if (!ret)
      {
        PTOD1("file_write error1: %d", last);
        PTOD1("handle: %p", fhandle);
        PTOD1("seek:   %p", seek);
        PTOD1("length: %p", length);
        PTOD1("buffer: %p", buffer);
        return last;
      }
    }
    else
    {
      PTOD1("file_write error2: %d", last);
      PTOD1("handle: %p", fhandle);
      PTOD1("seek:   %p", seek);
      PTOD1("length: %p", length);
      PTOD1("buffer: %p", buffer);
      return last;
    }
  }

  ResetEvent(ovl.hEvent);
  CloseHandle(ovl.hEvent);

  /* Double check byte count: */
  if (bytes != length)
  {
    PTOD2("Invalid byte count. Expecting %I64d, but transferred only %d bytes.", length, bytes);
    return 798;
  }

  return 0;
}


extern jlong alloc_buffer(JNIEnv *env, int bufsize)
{
  LPVOID buffer;

  /* A very quick and dirty experiment to resolve i/o coalescing issues? */
  if (bufsize == -1)
  {
    return timeBeginPeriod(1);
  }

  buffer = VirtualAlloc(NULL, bufsize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
  if (buffer == NULL)
  {
    PTOD2("memory allocation failed: %d error: %d", bufsize, GetLastError());
    return 0;
  }

  return(jlong) buffer;
}


extern void free_buffer(int bufsize, jlong buffer)
{
  int ret;


  ret = VirtualFree((LPVOID) buffer, bufsize,  MEM_DECOMMIT);
  if (!ret)
  {
    printf("free_buffer() MEM_DECOMMIT: %d %08x \n", bufsize, (int) buffer);
    WINMSG("memory de-allocation failed\n");
  }

  ret = VirtualFree((LPVOID) buffer, 0,  MEM_RELEASE);
  if (!ret)
  {
    printf("free_buffer() MEM_RELEASE`: %d %08x \n", bufsize, (int) buffer);
    WINMSG("memory de-allocation failed\n");
  }

  return;
}




/* Simple time of day: does not have to be an accurate current tod, */
/* as long as it can be used for tod delta calculations.            */
// http://support.microsoft.com/kb/909944
extern jlong get_simple_tod(void)
{
  LARGE_INTEGER ctr;
  double dbl;

  if (hertz == 0)
  {
    BOOL          rc;
    LARGE_INTEGER hz;
    rc = QueryPerformanceFrequency(&hz);
    if (rc == 0)
    {
      printf("QueryPerformanceFrequency() failed\n");
      abort();
    }
    hertz = hz.QuadPart;
  }


#define slower    // See Native.java for perf numbers
#ifdef slower
  QueryPerformanceCounter(&ctr);
  dbl = (double) ctr.QuadPart * 1000000.0 / hertz;

  return(jlong) dbl;

  //ctr.QuadPart *= 1000000;
  //ctr.HighPart &= 0x7fffffff;
  //ctr.QuadPart /= hertz;

  //return ctr.QuadPart;

#else

  /* Get starting timestamp: */
  // does not work at this time ............................
  {
    __int64 ticks;
    __asm  rdtsc
    __asm  mov [DWORD PTR ticks], eax
    __asm  mov [DWORD PTR ticks+4], edx

    return ticks / hertz;
  }

#endif
}


/**
 * Experiment creating sparse files:
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_truncateFile(JNIEnv *env,
                                                     jclass  this,
                                                     jlong   handle,
                                                     jlong   filesize)
{
  LARGE_INTEGER large = { filesize };
  int rc = SetFilePointerEx((HANDLE) handle, large, NULL, FILE_BEGIN);
  if (rc == 0)
  {
    PTOD4("SetFilePointerEx failed: handle: %I64d GetLastError: (%d) %s (%d)",
          handle, GetLastError(), getWindowsErrorText(GetLastError()), rc);
    return -1;
  }

  rc = SetEndOfFile((HANDLE) handle);
  if (rc == 0)
  {
    PTOD4("SetEndOfFile failed: handle: %I64d GetLastError: (%d) %s (%d)",
          handle, GetLastError(), getWindowsErrorText(GetLastError()), rc);
    return -1;
  }


  return 0;
}

