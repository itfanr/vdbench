package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;


/**
 * Translation table for Solaris, obtained from
 * SunOS unknown 5.10 Generic_142910-17 i86pc i386 i86pc
 * on 01/20/2015
  */
public class ErrnoSolaris
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static final String table[] = {

    "   1 EPERM              Not super-user                                ",
    "   2 ENOENT             No such file or directory. Seek error? File not open for output? 512 vs 4096 byte device? ",
    "   3 ESRCH              No such process                               ",
    "   4 EINTR              interrupted system call                       ",
    "   5 EIO                I/O error                                     ",
    "   6 ENXIO              No such device or address                     ",
    "   7 E2BIG              Arg list too long                             ",
    "   8 ENOEXEC            Exec format error                             ",
    "   9 EBADF              Bad file number                               ",
    "  10 ECHILD             No children                                   ",
    "  11 EAGAIN             Resource temporarily unavailable              ",
    "  12 ENOMEM             Not enough core                               ",
    "  13 EACCES             Permission denied                             ",
    "  14 EFAULT             Bad address                                   ",
    "  15 ENOTBLK            Block device required                         ",
    "  16 EBUSY              Mount device busy                             ",
    "  17 EEXIST             File exists                                   ",
    "  18 EXDEV              Cross-device link                             ",
    "  19 ENODEV             No such device                                ",
    "  20 ENOTDIR            Not a directory                               ",
    "  21 EISDIR             Is a directory                                ",
    "  22 EINVAL             Invalid argument                              ",
    "  23 ENFILE             File table overflow                           ",
    "  24 EMFILE             Too many open files                           ",
    "  25 ENOTTY             Inappropriate ioctl for device                ",
    "  26 ETXTBSY            Text file busy                                ",
    "  27 EFBIG              File too large                                ",
    "  28 ENOSPC             No space left on device                       ",
    "  29 ESPIPE             Illegal seek                                  ",
    "  30 EROFS              Read only file system                         ",
    "  31 EMLINK             Too many links                                ",
    "  32 EPIPE              Broken pipe                                   ",
    "  33 EDOM               Math arg out of domain of func                ",
    "  34 ERANGE             Math result not representable                 ",
    "  35 ENOMSG             No message of desired type                    ",
    "  36 EIDRM              Identifier removed                            ",
    "  37 ECHRNG             Channel number out of range                   ",
    "  38 EL2NSYNC           Level 2 not synchronized                      ",
    "  39 EL3HLT             Level 3 halted                                ",
    "  40 EL3RST             Level 3 reset                                 ",
    "  41 ELNRNG             Link number out of range                      ",
    "  42 EUNATCH            Protocol driver not attached                  ",
    "  43 ENOCSI             No CSI structure available                    ",
    "  44 EL2HLT             Level 2 halted                                ",
    "  45 EDEADLK            Deadlock condition.                           ",
    "  46 ENOLCK             No record locks available.                    ",
    "  47 ECANCELED          Operation canceled                            ",
    "  48 ENOTSUP            Operation not supported                       ",
    "  49 EDQUOT             Disc quota exceeded                           ",
    "  50 EBADE              invalid exchange                              ",
    "  51 EBADR              invalid request descriptor                    ",
    "  52 EXFULL             exchange full                                 ",
    "  53 ENOANO             no anode                                      ",
    "  54 EBADRQC            invalid request code                          ",
    "  55 EBADSLT            invalid slot                                  ",
    "  56 EDEADLOCK          file locking deadlock error                   ",
    "  57 EBFONT             bad font file fmt                             ",
    "  58 EOWNERDEAD         process died with the lock                    ",
    "  59 ENOTRECOVERABLE    lock is not recoverable                       ",
    "  60 ENOSTR             Device not a stream                           ",
    "  61 ENODATA            no data (for no delay io)                     ",
    "  62 ETIME              timer expired                                 ",
    "  63 ENOSR              out of streams resources                      ",
    "  64 ENONET             Machine is not on the network                 ",
    "  65 ENOPKG             Package not installed                         ",
    "  66 EREMOTE            The object is remote                          ",
    "  67 ENOLINK            the link has been severed                     ",
    "  68 EADV               advertise error                               ",
    "  69 ESRMNT             srmount error                                 ",
    "  70 ECOMM              Communication error on send                   ",
    "  71 EPROTO             Protocol error                                ",
    "  72 ELOCKUNMAPPED      locked lock was unmapped                      ",
    "  73 ENOTACTIVE         Facility is not active                        ",
    "  74 EMULTIHOP          multihop attempted                            ",
    "  77 EBADMSG            trying to read unreadable message             ",
    "  78 ENAMETOOLONG       path name is too long                         ",
    "  79 EOVERFLOW          value too large to be stored in data type     ",
    "  80 ENOTUNIQ           given log. name not unique                    ",
    "  81 EBADFD             f.d. invalid for this operation               ",
    "  82 EREMCHG            Remote address changed                        ",
    "  83 ELIBACC            Can't access a needed shared lib.             ",
    "  84 ELIBBAD            Accessing a corrupted shared lib.             ",
    "  85 ELIBSCN            .lib section in a.out corrupted.              ",
    "  86 ELIBMAX            Attempting to link in too many libs.          ",
    "  87 ELIBEXEC           Attempting to exec a shared library.          ",
    "  88 EILSEQ             Illegal byte sequence.                        ",
    "  89 ENOSYS             Unsupported file system operation             ",
    "  90 ELOOP              Symbolic link loop                            ",
    "  91 ERESTART           Restartable system call                       ",
    "  92 ESTRPIPE           if pipe/FIFO, don't sleep in stream head      ",
    "  93 ENOTEMPTY          directory not empty                           ",
    "  94 EUSERS             Too many users (for UFS)                      ",
    "  95 ENOTSOCK           Socket operation on non-socket                ",
    "  96 EDESTADDRREQ       Destination address required                  ",
    "  97 EMSGSIZE           Message too long                              ",
    "  98 EPROTOTYPE         Protocol wrong type for socket                ",
    "  99 ENOPROTOOPT        Protocol not available                        ",
    " 120 EPROTONOSUPPORT    Protocol not supported                        ",
    " 121 ESOCKTNOSUPPORT    Socket type not supported                     ",
    " 122 EOPNOTSUPP         Operation not supported on socket             ",
    " 123 EPFNOSUPPORT       Protocol family not supported                 ",
    " 124 EAFNOSUPPORT       Address family not supported by               ",
    " 125 EADDRINUSE         Address already in use                        ",
    " 126 EADDRNOTAVAIL      Can't assign requested address                ",
    " 127 ENETDOWN           Network is down                               ",
    " 128 ENETUNREACH        Network is unreachable                        ",
    " 129 ENETRESET          Network dropped connection because            ",
    " 130 ECONNABORTED       Software caused connection abort              ",
    " 131 ECONNRESET         Connection reset by peer                      ",
    " 132 ENOBUFS            No buffer space available                     ",
    " 133 EISCONN            Socket is already connected                   ",
    " 134 ENOTCONN           Socket is not connected                       ",
    " 143 ESHUTDOWN          Can't send after socket shutdown              ",
    " 144 ETOOMANYREFS       Too many references: can't splice             ",
    " 145 ETIMEDOUT          Connection timed out                          ",
    " 146 ECONNREFUSED       Connection refused                            ",
    " 147 EHOSTDOWN          Host is down                                  ",
    " 148 EHOSTUNREACH       No route to host                              ",
    " 149 EALREADY           operation already in progress                 ",
    " 150 EINPROGRESS        operation now in progress                     ",
    " 151 ESTALE             Stale NFS file handle                         "
  };


  /**
   * Translate errno to a text string.
   */
  static public String xlateErrno(long errno)
  {
    for (String line : table)
    {
      String[] split = line.trim().split(" +");
      if (Integer.parseInt(split[0]) == errno)
      {
        String tmp = String.format("%s: '%s'", split[1],
                                   line.substring(line.indexOf(split[1]) + split[1].length()).trim());
        return tmp;
      }
    }

    String txt = String.format("ErrnoSolaris: Unknown Solaris errno value number '%d'. "+
                               "Try to find errno=%d in 'errno.h'", errno, errno);
    return txt;
  }

  public static void main(String[] args)
  {
    int errno = Integer.parseInt(args[0]);
    String txt = xlateErrno(errno);
    common.ptod("txt: " + txt);
  }
}


