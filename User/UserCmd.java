package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.Serializable;

import Utils.Format;

import Vdb.Cmd_entry;

/**
 * This class represents a Cmd_entry instance for the user API
 */
public class UserCmd
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private Cmd_entry original_cmd;
  private long      lba;
  private long      xfersize;
  private boolean   read_flag;

  public UserCmd(Cmd_entry cmd)
  {
    original_cmd = cmd;
    lba          = cmd.cmd_lba;
    xfersize     = cmd.cmd_xfersize;
    read_flag    = cmd.cmd_read_flag;
  }

  public void updateCommand()
  {
    original_cmd.cmd_lba       = lba;
    original_cmd.cmd_xfersize  = xfersize;
    original_cmd.cmd_read_flag = read_flag;
  }

  public void setLba(long l)
  {
    lba = l;
  }
  public long getLba()
  {
    return lba;
  }

  public void setXfersize(long x)
  {
    xfersize = x;
  }
  public long getXfersize()
  {
    return xfersize;
  }

  public void setReadFlag(boolean bool)
  {
    read_flag = bool;
  }
  public boolean isRead()
  {
    return read_flag;
  }
}


