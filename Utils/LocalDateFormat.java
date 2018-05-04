package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.text.*;
import java.util.*;
import java.io.Serializable;

/**
 * This class handles the creation and formatting of Date() instance
 * that need to be printed with a value ignoring timestamps.
 */
public class LocalDateFormat
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private SimpleDateFormat df = null;

  public LocalDateFormat(String fmt)
  {
    df = new SimpleDateFormat(fmt);
    //df.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  public String format(Date dt)
  {
    return df.format(dt);
  }
}

