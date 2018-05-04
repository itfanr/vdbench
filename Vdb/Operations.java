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

/**
 * This class handles proper naming of requested operations.
 */
class Operations
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static String[] operations =
  {
    "read",
    "write",
    "mkdir",
    "rmdir",
    "copy",
    "move",
    "create",
    "delete",
    "getattr",
    "setattr",
    "open",
    "close"

    // we should add a 'backward sequential' file selection?


  };

  public static final int READ    = getOperationIdentifier("Read");
  public static final int WRITE   = getOperationIdentifier("Write");
  public static final int MKDIR   = getOperationIdentifier("Mkdir");
  public static final int RMDIR   = getOperationIdentifier("Rmdir");
  public static final int COPY    = getOperationIdentifier("Copy");
  public static final int MOVE    = getOperationIdentifier("Move");
  public static final int CREATE  = getOperationIdentifier("Create");
  public static final int DELETE  = getOperationIdentifier("Delete");
  public static final int GETATTR = getOperationIdentifier("Getattr");
  public static final int SETATTR = getOperationIdentifier("Setattr");
  public static final int OPEN    = getOperationIdentifier("Open");
  public static final int CLOSE   = getOperationIdentifier("Close");


  public static int getOperationIdentifier(String operation)
  {
    for (int i = 0; i < operations.length; i++)
    {
      if (operations[i].equalsIgnoreCase(operation))
        return i;
    }

    //common.ptod("getOperationIdentifier(): unknown operation: " + operation);
    return -1;
  }


  public static String getOperationText(int op)
  {
    if (op == -1)
      return "n/a";
    return operations[op];
  }

  public static int getOperationCount()
  {
    return operations.length;
  }
}
