package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.*;
import Utils.Bin;


/**
 * Binary file containing 'flatfile'
 */
public class Flat_record
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  public long start;      /* relative start time for request   Nanoseconds */
  public long resp;       /* Response time from trace, or zero */
  public long device;     /* Device number                     */
  public long lba;        /* Logical byte address              */
  public int  xfersize;   /* Transfer size                     */
  public int  pid;        /* Process id from trace, or zero    */
  public byte flag;       /* 0: write, 1: read                 */

  final static byte record_type    = 5;
  final static byte record_version = 0;


  /**
   * Convert this data to the binary data format.
   */
  public void export(Bin bin)
  {
    bin.put_long(start);
    bin.put_long(resp);
    bin.put_long(device);
    bin.put_long(lba);
    bin.put_int(xfersize);
    bin.put_int(pid);
    bin.put_byte(flag);

    bin.write_record(record_type, record_version);
  }


  /**
   * Convert exported data back to java.
   */
  public void emport(Bin bin)
  {
    start    = bin.get_long();
    resp     = bin.get_long();
    device   = bin.get_long();
    lba      = bin.get_long();
    xfersize = bin.get_int();
    pid      = bin.get_int();
    flag     = bin.get_byte();
  }

  public String toString()
  {
    return String.format("start: %16.6f; device: %8d; lba: %12d; xfer: %6d; flag: %d",
                         (start / 1000.), device, lba, xfersize, flag);
  }
}


