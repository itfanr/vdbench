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

/**
 * This class determines whether a tool has an expiration date or not.
 * If this class does NOT exist, then the expiration date stored in Support.tmp0
 * is honored. If this class DOES exist it contains the name of the person who
 * downloaded the tool and this name will be displayed.
 *
 * This also of course means that anyone who creates this class can change his
 * install to a non-expiring install.
 * However, I can not be a policeman for the whole world.
 *
 *
 *
 * ps: this class is removed during the creation of the distribution files and is
 *     only re-inserted by our own apache download server.
 *
 */
public class User
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static String tmp  = "Henk Vandenbergh; Personal development library";

  public static String getName()
  {
    return tmp;
  }
}


