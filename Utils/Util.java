package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.reflect.*;

/**
 * This is a little 'trick' class.
 * There are some self-standing utilities in my tools that can be executed
 * from the standard classpath, but I am too lazy to figure out which
 * that classpath really is.
 * Since swat, swat.bat, vdbench and vdbench.bat contain the complete path
 * I used to have clones to call the utilities, but I kept forgetting to
 * update those clones, and also came to the conclusion that some times
 * I would like to use these utilities in the field.
 * I therefore decided to make these utilities accessible via the standard scripts.
 *
 * When the first argument (args[0]) contains a "." and is found as an
 * existing main class, it will be called from here and the utility executed.
 */
public class Util
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  public static boolean checkUtil(String[] args)
  {
    Class wanted_class = null;

    /* If the first parameter does not contain a ".", return: */
    if (args.length == 0)
      return false;

    String parm = args[0];
    if (parm.indexOf(".") == -1)
      return false;

    /* Two pieces only, package.class: */
    String[] split = args[0].split("\\.");
    if (split.length != 2)
      return false;

    /* Make up for a frequent typo: */
    if (split[0].equals("vdb"))
      split[0] = "Vdb";

    /* See if it is a valid class: */
    String classname = split[0] + "." + split[1];

    try
    {
      wanted_class = Class.forName(classname);
    }
    catch (ClassNotFoundException e)
    {
      return false;
    }

    /* Strip first two arguments: */
    String[] new_args = new String[args.length-1];
    for (int i = 1; i < args.length; i++)
      new_args[i-1] = args[i]; //.toLowerCase();
                               //
    /* Figure out which main to get: */
    Method[] methods = wanted_class.getMethods();

    for (int i = 0; i < methods.length; i++)
    {
      if (methods[i].getName().equals("main"))
      {
        try
        {
          methods[i].invoke(null, new Object[] { new_args});
          return true;
        }
        catch (InvocationTargetException e)
        {
          common.failure(e);
        }
        catch (IllegalAccessException e)
        {
          common.failure(e);
        }
      }
    }

    return false;
  }
}


