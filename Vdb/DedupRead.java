package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

import Utils.Getopt;


/**
 * This test program reads a deduped disk/file and reports the dedupratio
 * observed.
 */
public class DedupRead implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  /**
   * vdbench Vdb.Dedup dir1 dir2 file -u 128k -s nn
   *    * For now this only handles dedup=100 compression=100 sets=1
   */
  public static void main(String[] args)
  {
    common.ptod("Note that this program only works at this time for a single file/lun");

    String filename = args[0];
    int xfersize = Integer.parseInt(args[1]) * 1024;
    int[] key_count = new int[256];

    long handle = Native.openFile(filename);
    if (handle < 0)
      common.failure("Unable to open file: " + filename);

    long size   = Native.getSize(handle, filename);
    long blocks = size/xfersize;

    long buffer = Native.allocBuffer(xfersize);
    int[] array = new int[xfersize/4];

    HashMap counter_map = new HashMap(32768);

    for (long i = 0; i < blocks; i++)
    {
      long lba = i * xfersize;
      if (Native.readFile(handle, lba, xfersize, buffer) != 0)
        common.failure("Error reading %s", filename);

      /* I use only the first 8 bytes, so why read the whole block? */
      Native.buffer_to_array(array, buffer, 512);

      //common.ptod("array[0]: %3d %08x", (array[0] >>> 24), lba);
      key_count[array[0] >>> 24]++;

      long key = array[1];
      key &= 0x00000000ffffffffl;
      key |= ((long) array[0]) << 32;

      Long counter = (Long) counter_map.get(new Long(key));
      if (counter == null)
        counter = new Long(0);
      counter = new Long(counter.longValue() + 1);
      counter_map.put(key, counter);

      //common.ptod("array: %08x %08x %016x", array[0], array[1], key );
    }

    Long[] keys = (Long[]) counter_map.keySet().toArray(new Long[0]);
    Arrays.sort(keys);
    for (int i = 0; i < keys.length; i++)
    {
      Long counter = (Long) counter_map.get(new Long(keys[i]));
      //common.ptod("keys: %016x %4d", keys[i].longValue(), counter.longValue());
    }

    common.ptod("blocks: %d keys: %d ratio: %.2f:1", blocks, keys.length, ((double) blocks / keys.length));

    for (int i = 0; i < key_count.length; i++)
    {
      if (key_count[i] != 0)
        common.ptod("key_count: %3d %6d", i, key_count[i]);
    }

    common.ptod("If there is any key=2 here it means that after format the lun " +
                "or file has been written to again");
  }
}






