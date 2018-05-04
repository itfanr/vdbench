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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;

import Utils.Bin;
import Utils.Fget;
import Utils.Lookup;
import Utils.LookupAnchor;
import Utils.NamedData;

/**
 * Read data picked up from Analytics.
 *
 * Read all files, and return records from each file in timestamp order for it
 * to be written out to a Bin file.
 */
public class FwdNamedData extends NamedData
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


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


































