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


/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

/**
 * All native functions.
 */
public class NamedKstat
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  /**
   * Get a kstat_ctl_t pointer to Kstat
   */
  public static native long kstat_open();


  /**
   * Close kstat kstat_ctl_t pointer
   */
  public static native long kstat_close(long kstat_ctl_t);

  /**
   * Using named Kstat data, return a String with
   * label number label number .....
   *
   * Can return with String: "JNI failure: ...."
   */
  public static native String kstat_lookup_stuff(long   kstat_ctl_t,
                                                 String module,
                                                 String name);



  public static void main(String[] args)
  {
    /*
    Swt.common.load_shared_library();

    long kstat_ctl_t = kstat_open();

    String data = kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v3");

    common.ptod("data: " + data);

    StringTokenizer st = new StringTokenizer(data);
    while (st.hasMoreTokens())
    {
      common.ptod(st.nextToken());
    }

    kstat_close(kstat_ctl_t);
    */
  }
}
