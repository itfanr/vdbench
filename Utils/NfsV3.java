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
 * NFS V3 Kstat statistics.
 *
 * These field names below only need to be specified if we want to use
 * any direct Lookup. In other words, see these as #defines
 */
public class NfsV3 extends NamedData implements java.io.Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private static LookupAnchor anchor = new LookupAnchor("Utils.NfsV3", "NfsV3", "nfsstat");
  public static Lookup GETATTR     = new Lookup(anchor, "getattr  ");
  public static Lookup SETATTR     = new Lookup(anchor, "setattr  ");
  public static Lookup LOOKUP      = new Lookup(anchor, "lookup   ");
  public static Lookup ACCESS      = new Lookup(anchor, "access   ");
  public static Lookup READ        = new Lookup(anchor, "read     ");
  public static Lookup WRITE       = new Lookup(anchor, "write    ");
  public static Lookup COMMIT      = new Lookup(anchor, "commit   ");
  public static Lookup CREATE      = new Lookup(anchor, "create   ");
  public static Lookup MKDIR       = new Lookup(anchor, "mkdir    ");
  public static Lookup SYMLINK     = new Lookup(anchor, "symlink  ");
  public static Lookup MKNOD       = new Lookup(anchor, "mknod    ");
  public static Lookup REMOVE      = new Lookup(anchor, "remove   ");
  public static Lookup RMDIR       = new Lookup(anchor, "rmdir    ");
  public static Lookup RENAME      = new Lookup(anchor, "rename   ");
  public static Lookup LINK        = new Lookup(anchor, "link     ");
  public static Lookup READDIR     = new Lookup(anchor, "readdir  ");
  public static Lookup READDIRPLUS = new Lookup(anchor, "readdir+ ",  "readdirplus");
  public static Lookup FSSTAT      = new Lookup(anchor, "fsstat   ");
  public static Lookup FSINFO      = new Lookup(anchor, "fsinfo   ");
  public static Lookup PATHCONF    = new Lookup(anchor, "pathconf ");
  public static Lookup READLINK    = new Lookup(anchor, "readlink ");


  public NfsV3()
  {
    super(anchor);
  }
}



