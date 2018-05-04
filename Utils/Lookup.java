package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.*;
import java.io.*;


/**
 * Allow for fast symbol lookup.
 *
 * Using an int to serve as a '#define' is handy, except for when you need to
 * link that int to something else. Using this class we now instead of using
 * that int for a reference, we use a Lookup instance which then can be
 * quickly translated to an int and to whatever else we need to link to, in
 * this case, a relative index into the data that we received from Kstat.
 */
public class Lookup  implements Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String       field        = null;
  private String       title        = null;
  private int          lookup_index = 0;
  private LookupAnchor anchor       = null;
  private long         scale        = 1;
  private int          order        = 999;


  /**
   * Create a Lookup instance using the complete field for report title.
   *
   * When a title is suffixed with '\\nn' 'nn' is used as a sorting order
   * when asking the anchor for a sorted title list.
   */
  public Lookup(LookupAnchor anchor, String field)
  {
    this(anchor, field, field, 1);
  }

  /**
   * Create a Lookup instance using a separate title.
   */
  public Lookup(LookupAnchor anchor, String title, String field)
  {
    this(anchor, title, field, 1);
  }

  public Lookup(LookupAnchor anchor, String title, long scale)
  {
    this(anchor, title, title, scale);
  }

  public Lookup(LookupAnchor anchor, String title, String field, long scale)
  {
    this.anchor       = anchor;
    this.field        = field.trim();
    this.title        = title.trim();
    this.scale        = scale;
    this.lookup_index = anchor.storeLookupEntry(this);

    if (this.title.indexOf(" ") != -1)
      common.failure("Lookup title may not contain embedded blanks: " + this.title);

    /* When the title has a '\' use it as a sorting order for the Chooser: */
    if (title.indexOf("\\") != -1)
    {
      StringTokenizer st = new StringTokenizer(title, " \\");
      if (st.countTokens() != 2)
      {
        common.ptod("Invalid token while creating Lookup: " + title);
        common.where(8);
      }

      else
      {
        this.title = st.nextToken();
        order = Integer.parseInt(st.nextToken());
      }
    }
  }


  public void setIndex(int idx)
  {
    lookup_index = idx;
  }
  public int getIndex()
  {
    return lookup_index;
  }
  public String getFieldName()
  {
    return field;
  }
  public String getTitle()
  {
    return title;
  }
  public void setScale(long sc)
  {
    scale = sc;
  }
  public int getOrder()
  {
    return order;
  }
  public double getScale()
  {
    if (anchor.useDoubles())
      return 1000. * scale;
    return scale;
  }

}




