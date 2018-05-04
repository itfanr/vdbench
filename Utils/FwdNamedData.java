package Utils;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import Utils.Lookup;
import Utils.LookupAnchor;
import Utils.NamedData;

/**
 * Swat Named Data for file system workloads.
 */
public class FwdNamedData extends NamedData
{
  private final static String c =
  "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private static LookupAnchor anchor = new LookupAnchor("Utils.FwdNamedData", "Vdbench", "Vdbench");

  private static boolean first = true;

  public FwdNamedData()
  {
    super(anchor);
    if (first)
    {
      first = false;
      anchor.setDoubles();
    }

  }
}


































