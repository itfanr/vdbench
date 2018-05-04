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


/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;



/**
 * Class used to store references to file handles.
 *
 * It was shown that windows allows only one concurrent i/o to a file handle,
 * this compared to Solaris where you can do as many as you want.
 * We get around this problem by having a separate handle for each active thread.
 *
 * For completeness, we store all file handles, windows and others.
 */
class File_handles
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String   label   = null;   /* Label only used if there's no SD */
  private SD_entry sd      = null;
  private long     fhandle = 0;

  private static HashMap handle_map = new HashMap(256);




  public static synchronized void addHandle(long fhandle, SD_entry sd)
  {
    Long handle = new Long(fhandle);
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + sd.sd_name);

    handle_map.put(handle, sd);
  }

  public static synchronized void addHandle(long fhandle, ActiveFile afe)
  {
    Long handle = new Long(fhandle);
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + afe.getFileEntry().getName());

    handle_map.put(handle, afe);
    //common.ptod("handle_map: " + handle_map.size());
  }


  public static synchronized void addHandle(long fhandle, String label)
  {
    Long handle = new Long(fhandle);
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + label);

    handle_map.put(handle, label);
  }

  public static synchronized void remove(long fhandle)
  {
    Long handle = new Long(fhandle);
    if (handle_map.remove(handle) == null)
      common.failure("File_handles.remove(): unkown handle: " + fhandle);
  }



  public static Object findHandle(long fhandle)
  {
    Long handle = new Long(fhandle);
    Object obj  = handle_map.get(handle);
    if (obj == null)
      common.failure("File_handles.findHandle(): unkown handle: " + fhandle);

    return obj;
  }
}
