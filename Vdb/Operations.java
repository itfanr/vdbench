package Vdb;
import java.util.HashMap;
import java.util.Vector;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

/**
 * This class handles proper naming of requested operations.
 */
class Operations
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

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
    "access",
    "open",
    "close"

    // we should add a 'backward sequential' file selection?
  };

  private static HashMap <String, Integer> operations_map = new HashMap(32);

  private static boolean[] operations_used = new boolean[64];

  public static final int READ    = addOperation("Read");
  public static final int WRITE   = addOperation("Write");
  public static final int MKDIR   = addOperation("Mkdir");
  public static final int RMDIR   = addOperation("Rmdir");
  public static final int COPY    = addOperation("Copy");
  public static final int MOVE    = addOperation("Move");
  public static final int CREATE  = addOperation("Create");
  public static final int DELETE  = addOperation("Delete");
  public static final int GETATTR = addOperation("Getattr");
  public static final int SETATTR = addOperation("Setattr");
  public static final int ACCESS  = addOperation("Access");
  public static final int OPEN    = addOperation("Open");
  public static final int CLOSE   = addOperation("Close");


  private static int addOperation(String operation)
  {
    int opnumber = operations_map.size();
    operation    = operation.toLowerCase();
    operations_map.put(operation, opnumber);
    return opnumber;
  }

  /**
   * Translate operation String to an integer.
   * Since this call is made very early on, also keep track of which
   * operations are really used.
   */
  public static int getOperationIdentifier(String operation)
  {
    operation = operation.toLowerCase();

    Integer opnumber = operations_map.get(operation);
    if (opnumber == null)
      return -1;
    else
    {
      operations_used [ opnumber ] = true;
      return opnumber;
    }
  }


  public static String getOperationText(int op)
  {
    if (op == -1)
      return "n/a";
    return operations[op];
  }

  /**
   * This method is meant for some infrequently used operations to allow them
   * ONLY to be included in the output when used.
   * BTW: flatfile is left alone.
   */
  public static boolean isOperationUsed(int op)
  {
    return operations_used [ op ];
  }

  public static int getOperationCount()
  {
    return operations.length;
  }

  public static boolean keepControlFile(Vector <FwgEntry> fwgs_for_slave)
  {
    for (FwgEntry fwg : fwgs_for_slave)
    {
      if (fwg.getOperation() == Operations.CREATE) return false;
      if (fwg.getOperation() == Operations.MKDIR) return false;
      if (fwg.getOperation() == Operations.RMDIR) return false;
      if (fwg.getOperation() == Operations.DELETE) return false;
    }
    return true;
  }
}
