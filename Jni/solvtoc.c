
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include "vdbjni.h"
#include <sys/types.h>
#include <sys/dklabel.h>
#include <sys/vtoc.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dlfcn.h>
#include <link.h>
#include <vdb_efi_partition.h>


static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";



extern struct Shared_memory *shared_mem;

/**
 * Determine size of the raw disk or of the file system file
 */
extern jlong get_vtoc(JNIEnv *env, jlong fhandle, const char* fname)
{
  int    rc;
  struct vtoc vtoc;
  jlong  filesize, size;
  struct stat64 xstat;
  int    slice, i;

  /* Get fstat for either minor name or filesize: */
  rc = fstat64(fhandle, &xstat);
  if ( rc < 0 )
  {
    PTOD2("get_vtoc(), fstat %s failed: %s\n", fname, strerror(errno));
    abort();
  }

  /* We don't do catalogs! */
  if ( S_ISDIR(xstat.st_mode) )
  {
    PTOD1("get_vtoc(): Requested file '%s' is a directory, not a file or a disk", fname);
    abort();
  }

  /* Set default filesize: */
  filesize = xstat.st_size;

  /* If we can get the vtoc then we know we have a raw disk: */
  slice = read_vtoc((int) fhandle, &vtoc);
  if (slice >= 0 && slice < 16)
  {
    /* Return partition's length: */
    return(jlong)  ((jlong) vtoc.v_sectorsz * (jlong) vtoc.v_part[slice].p_size);
  }

  /* If we can't get the vtoc, maybe we have efi? */
  else if ( errno == ENOTSUP)
  {
    struct dk_gpt *efi;
    void  *efi_handle;
    short (*my_efi_alloc_and_read)(int fd, struct dk_gpt **);
    void (*my_efi_free)(struct dk_gpt *);

    if ( (efi_handle = dlopen("libefi.so", RTLD_NOW)) == NULL )
      return filesize;

    my_efi_alloc_and_read = (short (*)(int, struct dk_gpt **))
                            dlsym(efi_handle, "efi_alloc_and_read");
    my_efi_free = (void (*)(struct dk_gpt *))dlsym(efi_handle, "efi_free");


    /* Read efi vtoc: */
    slice = my_efi_alloc_and_read((int) fhandle, &efi);
    if ( slice < 0 )
    {
      dlclose(efi_handle);
      return filesize;
    }

    /* Calculate file size: */
    filesize = efi->efi_parts[slice].p_size * efi->efi_lbasize;
    my_efi_free(efi);
    dlclose(efi_handle);
    PLOG("Returning EFI lun size");

    return filesize;
  }

  else
  {
    return filesize;
  }
}


