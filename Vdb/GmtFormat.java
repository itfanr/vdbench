package Vdb;

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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


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

