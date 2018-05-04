package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.Serializable;

import Vdb.SlaveJvm;
import Vdb.common;

public class UserData implements Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String slave_label;
  private String class_name;
  private String method_name;
  private String sd_name;

  private double rate;
  private double rdpct;
  private double resp;

  public UserData(String name)
  {
    sd_name     = name;
    slave_label = SlaveJvm.getSlaveLabel();
  }

  public void setClassName(String c)
  {
    class_name = c;
  }
  public void setMethodName(String m)
  {
    method_name = m;
  }
  public String GetSlaveLabel()
  {
    return slave_label;
  }
  public String getmethodName()
  {
    return method_name;
  }
  public String GetSdName()
  {
    return sd_name;
  }

  public void setRate(double d)
  {
    rate = d;
  }
  public void setRdPct(double d)
  {
    rdpct = d;
  }
  public void setResp(double d)
  {
    resp = d;
  }
  public double getRate()
  {
    return rate;
  }

  public boolean equals(Object obj)
  {
    UserData ud = (UserData) obj;
    return
    slave_label.equals(ud.slave_label) &&
    class_name.equals(ud.class_name) &&
    method_name.equals(ud.method_name) &&
    sd_name.equals(ud.sd_name);
  }
}


