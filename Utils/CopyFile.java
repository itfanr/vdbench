package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Copy a binary file using Simple Buffered functions.
 */
public class CopyFile
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  public static boolean copy(String infile, String otfile)
  {
    try
    {
      FileInputStream input   = new FileInputStream(infile);
      FileOutputStream output = new FileOutputStream(otfile);
      byte[] buffer           = new byte[32768];
      int bytes               = 0;
      while ((bytes = input.read(buffer)) != -1)
        output.write(buffer, 0, bytes);

      input.close();
      output.close();
      return true;
    }
    catch (Exception e)
    {
      common.ptod(e);
      return false;
    }
  }

  public static String copyToTemp(String infile)
  {
    try
    {
      FileInputStream  input  = new FileInputStream(infile);
      String           otfile = Fput.createTempFileName(".temp");
      FileOutputStream output = new FileOutputStream(otfile);
      byte[] buffer           = new byte[32768];
      int bytes               = 0;
      while ((bytes = input.read(buffer)) != -1)
        output.write(buffer, 0, bytes);

      input.close();
      output.close();
      return otfile;
    }
    catch (Exception e)
    {
      common.ptod(e);
      return null;
    }
  }

  public static void main(String[] args)
  {
    String rc = copyToTemp(args[0]);
    common.ptod("rc: " + rc);

  }
}
