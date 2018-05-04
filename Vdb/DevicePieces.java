package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.*;

/**
 * Split a device name into the 4 components of a device name, with all the
 * validity checking.
 *
 * Hopefully some day I will go around the code and change everything
 * to start using this.
 */
public class DevicePieces
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  boolean valid = false;
  String  dev;

  String  controller_name;
  String  target_name;
  String  disk_name;
  String  slice_name;
  int     disk;
  int     slice = -1;
  String  error_txt;




  public DevicePieces(String d)
  {
    try
    {
      dev = d.toLowerCase();

      /* Start with removing /dev/dsk/ and /dv/rdsk/: */
      if (dev.startsWith("/dev/dsk/"))
        dev = dev.substring(9);
      else if (dev.startsWith("/dev/rdsk/"))
        dev = dev.substring(10);

      /* Begin with the easy part: */
      error_txt = "Not starting with 'c'";
      if (!dev.startsWith("c"))
        return;

      error_txt = "t or d missing or in the wrong place";

      /* Can this be without a target, cxdxsx? */
      if (dev.indexOf("t") == -1 && dev.indexOf("d") != -1)
      {
        controller_name = dev.substring(0, dev.indexOf("d"));
      }

      /* Must have 't' and 'd': */
      else if (dev.indexOf("t") == -1 ||
               dev.indexOf("d") == -1 ||
               dev.indexOf("t") > dev.lastIndexOf("d"))
        return;

      /* Pick up normal controller and target: */
      else
      {
        controller_name = dev.substring(0, dev.indexOf("t"));
        target_name     = dev.substring(dev.indexOf("t"), dev.lastIndexOf("d"));
      }

      // c2tATASEAGATEST37500NSSUN750G0814A5CWW05QD5CWW0d0
      // c2tatasteczeusiopsts0s000093c9d0 error: Exception: String index out of range: -9    c2 tatasteczeusiopsts0s000093c9 null null

      /* Now get the disk and slice name: */
      int offset_s = dev.lastIndexOf("s");
      int offset_d = dev.lastIndexOf("d");
      int offset_p = dev.lastIndexOf("p");

      /* If there is an 's' BEFORE the 'd' then we have an 's' in the target */
      /*  name, e.g. c2tatasteczeusiopsts0s000093a6d0                        */
      // this whole thing is a mess, since at one point I decided to no longer
      // look for kstat with parititions. But it works.

      /* The offset to 'p' or 's' must be beyond the 'd': */
      if ((offset_s != -1 && offset_s > offset_d) ||
          (offset_p != -1 && offset_p > offset_d))
      {
        if (dev.lastIndexOf("s") != -1)
        {
          disk_name  = dev.substring(dev.lastIndexOf("d"), dev.lastIndexOf("s"));
          slice_name = dev.substring(dev.lastIndexOf("s"));
        }
        else
        {
          disk_name  = dev.substring(dev.lastIndexOf("d"), dev.lastIndexOf("p"));
          slice_name = dev.substring(dev.lastIndexOf("p"));
        }
      }

      else
      {
        /* We have no slice name, so force it to be 's2': */
        disk_name = dev.substring(dev.lastIndexOf("d"));
        slice_name = "s2";
      }

      error_txt = "bad controller number";
      if (!isDec(controller_name.substring(1)))
        return;

      error_txt = "bad target number";
      if (target_name != null && !isHex(target_name.substring(1)))
        return;

      error_txt = "bad disk number";
      if (!isDec(disk_name.substring(1)))
        return;

      error_txt = "bad slice number";
      if ((slice = translateSliceToNumber(slice_name)) == -1)
        return;

      error_txt = null;
      valid = true;
    }

    catch (Exception e)
    {
      error_txt = "Exception: " + e.getMessage();
      common.ptod("this: " + this);
      common.ptod(e);
    }

  }


  /**
   * Translate 'cxtxdxsx' or 'cxtxdxpx' to a relative partition number.
   */
  private static int translateSliceToNumber(String name)
  {
    /* Check for 's' or 'p': */
    char identifier = name.charAt(name.length() - 2);
    if (identifier != 's' && identifier != 'p')
      return -1;

    /* First start with 's': */
    if (identifier == 's')
    {
      if (name.indexOf("s") != name.length() - 2)
        return -1;

      char slice = name.charAt(name.length() - 1);
      if (slice >= '0' && slice <= '9')
        return slice - 48;
      if (slice >= 'a' && slice <= 'f')
        return slice - 97 + 10;
    }

    /* Now do with 'p': */
    if (identifier == 'p')
    {
      if (name.indexOf("p") != name.length() - 2)
        return -1;

      char slice = name.charAt(name.length() - 1);
      if (slice >= 'g' && slice <= 'z')
        return slice - 97 + 10;
    }


    return -1;
  }

  /**
   * Return the full device name --without-- the slice/partition number
   */
  public String getNameNoSlice()
  {
    String dev = "/dev/rdsk/" + controller_name;
    if (target_name != null)
      dev += target_name;
    dev += disk_name;

    //common.ptod("getNameNoSlice: " + dev);
    return dev;
  }



  /**
   * Return true if the input argument character is
   * a digit, or A-F.
   */
  public static final boolean isHexStringChar(char c)
  {
    return(Character.isDigit(c) || (("0123456789abcdefABCDEF".indexOf(c)) >= 0));
  }

  public static final boolean isDecStringChar(char c)
  {
    return(Character.isDigit(c) || (("0123456789".indexOf(c)) >= 0));
  }

  /**
   * Return true if the argument string seems to be a
   * Hex data string, like "a0 13 2f ".  Whitespace is
   * ignored.
   */
  public static final boolean isHex(String sampleData)
  {
    if (sampleData.length() == 0)
      return false;
    for (int i = 0; i < sampleData.length(); i++)
    {
      if (!isHexStringChar(sampleData.charAt(i)))
        return false;
    }
    return true;
  }

  public static final boolean isDec(String sampleData)
  {
    if (sampleData.length() == 0)
      return false;
    for (int i = 0; i < sampleData.length(); i++)
    {
      if (!isDecStringChar(sampleData.charAt(i))) return false;
    }
    return true;
  }

  public String toString()
  {
    return("DevicePieces(): " + dev + " " +
           ((error_txt == null) ? "" : "error: " + error_txt + "    ") +
           controller_name + " " +
           target_name + " " +
           disk_name + " " +
           slice_name);
  }


  public static void main(String[] args)
  {
    String dev = args[0];
    DevicePieces dp = new DevicePieces(dev);
    common.ptod("dp: " + dp);
  }

}





