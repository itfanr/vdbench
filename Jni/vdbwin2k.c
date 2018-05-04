

/*
 * Copyright (c) 2010 Sun Microsystems, Inc. All rights reserved.
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


#include <jni.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <windows.h>
#include <winioctl.h>
#include "vdbjni.h"


#define WINMSG(x)                                                \
{                                                                \
    printf("==========> %s GetLastError(): %d \n", x, GetLastError());           \
}


static jlong hertz = 0;

#define GB ( (double) 1024 * 1024 * 1024)


extern void snap(char *text, void* start, int length);



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

  return (char*) lpMsgBuf;
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

  /* Set access type: */
  if (write)
    access_type = GENERIC_READ | GENERIC_WRITE;
  else
    access_type = GENERIC_READ;

  if (openflag != 0 && openflag !=  WINDOWS_DIRECTIO)
  {
    PTODS("Invalid open parameter for Windows: 0x%08x", openflag);
    abort();
  }

  /* Open raw disk or a file system file with flush:              */
  /* (We want buffering on a file system, unless asked for flush) */
  //printf("\n\n\n\n\n Opening test version %s \n\n\n", filename);
  if (memcmp(filename, "\\\\", 2) == 0)
  {
    //printf("Createfile physical: %s\n", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_EXISTING,
                         /*FILE_FLAG_WRITE_THROUGH  |  */ FILE_FLAG_NO_BUFFERING,
                         NULL);
  }
  else if (openflag)
  {
    //printf("Createfile flush: %s\n", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_ALWAYS ,
                         /*FILE_FLAG_WRITE_THROUGH  |  */ FILE_FLAG_NO_BUFFERING,
                         NULL);
  }
  else
  {
    //printf("Createfile filesystem: %s\n", filename);
    fhandle = CreateFile(filename,
                         access_type,
                         FILE_SHARE_READ  | FILE_SHARE_WRITE ,
                         NULL,
                         OPEN_ALWAYS ,
                         0 ,
                         NULL);
  }


  if (fhandle == (HANDLE)-1)
  {
    sprintf(ptod_txt, "CreateFile failed: %s GetLastError: (%d) %s", filename,
            GetLastError(), getWindowsErrorText(GetLastError()));
    PTOD(ptod_txt);
    return -1;
  }

  return(jlong) fhandle;
}



extern jlong file_read(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  int ret;
  DWORD bytes;
  LARGE_INTEGER seeklong;
  OVERLAPPED ovl;

  /* Store seek address in Overlap structure */
  seeklong.QuadPart = seek;
  ovl.Offset     = seeklong.LowPart;
  ovl.OffsetHigh = seeklong.HighPart;
  ovl.hEvent     = NULL;

  //printf("read: %I64d \n", seek / 512);

  if (fhandle == 0)
    WINMSG("zero handle");
  if (buffer == 0)
    WINMSG("zero buffer");
  if (length == 0)
    WINMSG("zero length");

  /* Do the synchronous read: */
  ret = ReadFile((HANDLE) fhandle, (LPVOID) buffer, (DWORD) length, &bytes, &ovl);

  /* Read will complete synchronous: */
  if (!ret)
  {
    if (GetLastError() != ERROR_IO_PENDING)
    {
      int last_error = GetLastError();
      PTODS("file_read error: %d", last_error);
      PTODS("handle: %p", fhandle);
      PTODS("seek:   %p", seek);
      PTODS("length: %p", length);
      PTODS("buffer: %p", buffer);
      return last_error;
    }
  }

  return 0;
}




extern jlong file_write(JNIEnv *env, jlong fhandle, jlong seek, jlong length, jlong buffer)
{
  DWORD bytes;
  LARGE_INTEGER seeklong;
  OVERLAPPED ovl;

  seeklong.QuadPart = seek;

  /* Store seek address in Overlap structure */
  memset(&ovl, 0, sizeof(ovl));
  ovl.Offset     = seeklong.LowPart;
  ovl.OffsetHigh = seeklong.HighPart;
  ovl.hEvent     = NULL;

  //snap("seek",   &seek,    8);
  //snap("handle", &fhandle, 8);
  //snap("length", &length,  8);
  //snap("buffer", &buffer,   8);

  /* Do the synchronous write: */
  /* Write completes synchronous: */
  if (WriteFile((HANDLE) fhandle, (LPVOID) buffer, (DWORD) length, &bytes, &ovl) == 0)
  {
    if (GetLastError() != ERROR_IO_PENDING)
    {
      int last_error = GetLastError();
      PTODS("file_write error: %d", last_error);
      PTODS("handle: %p", fhandle);
      PTODS("seek:   %p", seek);
      PTODS("length: %p", length);
      PTODS("buffer: %p", buffer);
      return last_error;
    }
  }

  return 0;
}


extern jlong alloc_buffer(JNIEnv *env, int bufsize)
{
  LPVOID buffer;

  buffer = VirtualAlloc(NULL, bufsize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
  if (buffer == NULL)
  {
    char txt[256];
    sprintf(txt, "memory allocation failed: %d error: ", bufsize, GetLastError());
    PTOD(txt);
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


/**
 * Windows requires explicit handling of rewind and tapemark writes
 */
JNIEXPORT jlong JNICALL Java_Vdb_Native_windows_1rewind(JNIEnv *env,
                                                        jclass  this,
                                                        jlong   handle,
                                                        jlong   wait)
{
  DWORD rc;

  TAPE_GET_DRIVE_PARAMETERS DriveParms;
  TAPE_SET_MEDIA_PARAMETERS MediaParms;
  DWORD dwBufferSize;
  DWORD dwErrorCode;

  //snap ("wait", &wait, 8);

  if (wait)
    rc = SetTapePosition((HANDLE) handle, TAPE_REWIND, 0,0,0, FALSE);
  else
    rc = SetTapePosition((HANDLE) handle, TAPE_REWIND, 0,0,0, TRUE);


  //if (rc == ERROR_CRC)
  //{
  //  char txt[256];
  //  sprintf(txt, "SetTapePosition failed:  GetLastError() %d; error accepted", GetLastError());
  //  PTOD(txt);
  //  return 0;
  //}


  if (rc != NO_ERROR)
  {
    char txt[256];
    sprintf(txt, "SetTapePosition failed:  GetLastError() %d", GetLastError());
    PTOD(txt);
    return -1;
  }

  /*
  dwBufferSize = sizeof(DriveParms);

  dwErrorCode = GetTapeParameters( (HANDLE) handle,
                                   GET_TAPE_DRIVE_INFORMATION,
                                   &dwBufferSize,
                                   &DriveParms );
  if (dwErrorCode != NO_ERROR)
  {
    printf("error during gettape\n");
  }

  printf("1DriveParms.FeaturesLow %08x \n", DriveParms.FeaturesLow);
  printf("1DriveParms.MaximumBlockSize %08x \n", DriveParms.MaximumBlockSize);
  printf("1DriveParms.MinimumBlockSize %08x \n", DriveParms.MinimumBlockSize);
  printf("1DriveParms.DefaultBlockSize %08x \n", DriveParms.DefaultBlockSize);


  MediaParms.BlockSize = 0;

  dwErrorCode = SetTapeParameters( (HANDLE) handle,
                                   SET_TAPE_MEDIA_INFORMATION,
                                   &MediaParms );
  if (dwErrorCode != NO_ERROR)
  {
    printf("error during settape\n");
  }

  dwErrorCode = GetTapeParameters( (HANDLE) handle,
                                   GET_TAPE_DRIVE_INFORMATION,
                                   &dwBufferSize,
                                   &DriveParms );
  if (dwErrorCode != NO_ERROR)
  {
    printf("error during gettape\n");
  }

  printf("2DriveParms.FeaturesLow %08x \n", DriveParms.FeaturesLow);
  printf("2DriveParms.MaximumBlockSize %08x \n", DriveParms.MaximumBlockSize);
  printf("2DriveParms.MinimumBlockSize %08x \n", DriveParms.MinimumBlockSize);
  printf("1DriveParms.DefaultBlockSize %08x \n", DriveParms.DefaultBlockSize);
  */

  return 0;
}

JNIEXPORT jlong JNICALL Java_Vdb_Native_windows_1tapemark(JNIEnv *env,
                                                          jclass  this,
                                                          jlong   handle,
                                                          jlong   tcount,
                                                          jlong   wait)
{
  DWORD rc;

  if (wait)
    rc = WriteTapemark((HANDLE) handle, TAPE_FILEMARKS, (int) tcount, FALSE);
  else
    rc = WriteTapemark((HANDLE) handle, TAPE_FILEMARKS, (int) tcount, TRUE);
  /*
  if (rc == ERROR_CRC)
  {
    char txt[256];
    sprintf(txt, "WriteTapemark failed:  GetLastError() %d; error accepted", GetLastError());
    PTOD(txt);
    return 0;
  }
  */
  if (rc != NO_ERROR)
  {
    char txt[256];
    sprintf(txt, "WriteTapemark failed:  GetLastError() %d", GetLastError());
    PTOD(txt);
    return -1;
  }

  return 0;
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


