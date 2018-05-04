package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.File;
import java.util.*;



/**
 * Class used to store references to file handles.
 */
class File_handles
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String   label   = null;   /* Label only used if there's no SD */
  private SD_entry sd      = null;

  private static HashMap <Long, Object> handle_map = new HashMap(256);




  public static synchronized void addHandle(long handle, SD_entry sd)
  {
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + sd.sd_name);

    if (handle_map.put(handle, sd) != null)
      common.failure("Duplicate file handle %d for sd=%s", handle, sd.sd_name);
  }

  public static synchronized void addHandle(long handle, ActiveFile afe)
  {
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + afe.getFileEntry().getFullName());

    if (handle_map.put(handle, afe) != null)
      common.failure("Duplicate file handle %d for file=%s", handle, afe.getFileEntry().getFullName());
  }


  public static synchronized void addHandle(long handle, String label)
  {
    if (handle_map.get(handle) != null)
      common.failure("File_handles.addHandle(): duplicate handle: " + label);

    if (handle_map.put(handle, label) != null)
      common.failure("Duplicate file handle %d for label=%s", handle, label);
  }

  public static synchronized void remove(long handle)
  {
    if (handle_map.remove(handle) == null)
      common.failure("File_handles.remove(): unknown handle: " + handle);
  }

  public static Object findHandle(long handle)
  {
    Object obj  = handle_map.get(handle);
    if (obj == null)
      common.failure("File_handles.findHandle(): unknown handle: " + handle);

    return obj;
  }


  /**
   * Return the file name for this handle.
   * If we don't have this handle, that's fine, return a fake name.
   */
  public static String getFileName(long handle)
  {
    Object obj  = handle_map.get(handle);
    if (obj == null)
      return "File_handles.getFileName(handle=" + handle + ")";

    if (obj instanceof SD_entry)
    {
      SD_entry sd = (SD_entry) obj;
      return sd.lun;
    }
    if (obj instanceof ActiveFile)
    {
      ActiveFile afe = (ActiveFile) obj;
      return afe.getFileEntry().getFullName();
    }
    if (obj instanceof String)
      return (String) obj;

    //common.failure("getFileName() invalid request: " + obj);

    return "File_handles.getFileName(" + handle + ")";
  }

  public static String getLastName(long handle)
  {
    String fname = getFileName(handle);
    return new File(fname).getName();
  }
}
