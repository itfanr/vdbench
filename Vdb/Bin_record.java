package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.Date;
import java.io.Serializable;
import Utils.Bin;

/**
 * Binary record for Swat
 */
abstract class Bin_record implements Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private byte record_type    = -1;
  private byte record_version = -1;

  //private static Instances counters = new Instances("Bin_record", 0);

  abstract public void export(Bin bin);
  abstract public void emport(Bin bin);

  public Bin_record(byte type, byte version)
  {
    record_type    = type;
    record_version = version;
    //counters.add(this);
  }

  public byte getType()
  {
    return record_type;
  }

  public byte getversion()
  {
    return record_version;
  }
}
