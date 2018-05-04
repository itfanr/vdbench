package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.text.*;
import java.util.*;

/**
 * Create a DateFormat that is always set to GMT.
 *
 * This helps for Swat data whose timestamps are always read by the reporter
 * as being GMT and are then always printed as being GMT.
 * This eliminates a lot of headaches around timezone problems.
 */
public class GmtFormat extends SimpleDateFormat
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private static long   dflt_timezone_offset = new Date().getTimezoneOffset() * 60 * 1000;

  private static String dflt = "EEE MMM dd HH:mm:ss yyyy";

  public GmtFormat()
  {
    super(dflt);
    this.setTimeZone(TimeZone.getTimeZone("GMT"));
  }
  public GmtFormat(String str)
  {
    super(str);
    this.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  public static long getOffset()
  {
    return dflt_timezone_offset;
  }


  public static Date getGmtDate()
  {
    Date dt = new Date();
    dt      = new Date(dt.getTime() - dflt_timezone_offset);
    return dt;
  }
  public static Date getGmtDate(long tm)
  {
    Date dt = new Date(tm);
    dt      = new Date(dt.getTime() - dflt_timezone_offset);
    return dt;
  }

  public static String print()
  {
    return new GmtFormat().format(getGmtDate());
  }
  public static String print(long tm)
  {
    return new GmtFormat().format(getGmtDate(tm));
  }
}

