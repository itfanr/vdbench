#!/bin/ksh
#


# Pick up the proper parent directory from this current script:
dir=`dirname $0`
cd $dir
cd ..
LIB=`pwd`
echo LIB: $LIB

#
#  mount mf-ubrm-01:/usr/dist /usr/dist
#

# http://www.unix.com/programming/179119-stdio-h-not-found-solaris-11-a.html
# pkg install system/header
#

CC="cc"
CC="/usr/dist/share/sunstudio_sparc,v12.0/SUNWspro/bin/cc"
CC="/net/mf-ubrm-01/usr/dist/share/sunstudio_sparc,v12.0/SUNWspro/prod/bin/cc"
java="/net/sbm-240a.us.oracle.com/export/swat/swat_java/sparc/jdk1.6.0_27/include"


cd /tmp
rm *.o

echo Starting 32bit compiles


INCS="-m32 -fPIC -I$LIB/Jni -I$java/ -I$java/solaris "
$CC                        -c -g -xCC $INCS $LIB/Jni/solvtoc.c  -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdb_dv.c   -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdb.c      -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/snap.c     -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/chmod.c    -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/kstat.c    -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdbjni.c   -DSOLARIS -DKSTAT -g
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/kstatcpu.c -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdbsol.c   -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/nfs_kstat.c  -DSOLARIS


echo Starting 32bit link
$CC  -o  $LIB/solaris/sparc32.so -m32 -mt -G -ladm  -lkstat vdb.o solvtoc.o \
vdb_dv.o vdbjni.o vdbsol.o kstat.o kstatcpu.o nfs_kstat.o snap.o chmod.o -ldl -lrt

#chmod 777 $LIB/solaris/*




rm *.o

echo Starting 64bit compiles

INCS="-m64 -fPIC -I$LIB/Jni -I$java/ -I$java/solaris "
$CC                        -c -g -xCC $INCS $LIB/Jni/solvtoc.c  -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdb_dv.c   -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdb.c      -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/snap.c     -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/chmod.c    -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/kstat.c    -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdbjni.c   -DSOLARIS -DKSTAT
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/kstatcpu.c -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/vdbsol.c   -DSOLARIS
$CC -D_FILE_OFFSET_BITS=64 -c -g -xCC $INCS $LIB/Jni/nfs_kstat.c  -DSOLARIS


echo Starting 64bit link
$CC  -o  $LIB/solaris/sparc64.so -m64 -mt -G -ladm  -lkstat vdb.o solvtoc.o \
vdb_dv.o vdbjni.o vdbsol.o kstat.o kstatcpu.o nfs_kstat.o snap.o chmod.o -ldl -lrt

chmod 777 $LIB/solaris/sparc*


