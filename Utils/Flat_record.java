package Utils;

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
import Utils.Bin;


/**
 * Binary file containing 'flatfile'
 */
public class Flat_record
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public long start;      /* relative start time for request   */
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
}


