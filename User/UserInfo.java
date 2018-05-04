package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.HashMap;

import Vdb.common;

/**
 * This class contains information per WD/user class combination..
 */
public class UserInfo
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String    wd_name;
  private String    cname;
  private String[]  parms;
  private UserClass instance;

  private static HashMap <String, UserClass> class_map = new HashMap(16);

  public UserInfo(String wd_name, String cname, String[] parms)
  {
    this.wd_name = wd_name;
    this.cname   = cname;
    this.parms   = parms;
  }

  public String getWdname()
  {
    return wd_name;
  }
  public String getClassName()
  {
    return cname;
  }
  public String[] getParms()
  {
    return parms;
  }
  public void setInstance(UserClass obj)
  {
    instance = obj;
  }
  public UserClass getInstance()
  {
    return instance;
  }
}
