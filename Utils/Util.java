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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



  public static boolean checkUtil(String[] args)
  {
    Class wanted_class = null;

    /* If the first parameter does not contain a ".", return: */
    if (args.length == 0)
      return false;

    String parm = args[0];
    if (parm.indexOf(".") == -1)
      return false;

    /* See if it is a valid class: */
    try
    {
      wanted_class = Class.forName(args[0]);
    }
    catch (ClassNotFoundException e)
    {
      return false;
    }

    /* Strip first two arguments: */
    String[] new_args = new String[args.length-1];
    for (int i = 1; i < args.length; i++)
      new_args[i-1] = args[i]; //.toLowerCase();

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


  public static String test()
  {
    return "user name from Utils.Util.test()";
  }

}


