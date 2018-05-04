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
import Utils.Fget;

public class ClassPath
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static String classpath_directory_name = load_classpath_dir();

  /**
   * Obtain classpath.
   * If there is a concatenation then we always use only the first one.
   * If this is a jar file, then we use the parent of this jar.
   */
  public static String load_classpath_dir()
  {
    String classpath = System.getProperty("java.class.path");

    /* Remove separator: */
    String tmp = (common.onWindows()) ? ";" : ":";
    if (classpath.indexOf(tmp) != -1)
      classpath = classpath.substring(0, classpath.indexOf(tmp) );

    try
    {
      classpath_directory_name = new File(classpath).getCanonicalPath();
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    /** If this is a jar file, then we use the parent of this jar.: */
    if (classpath_directory_name.endsWith(".jar"))
      classpath_directory_name = new File(classpath_directory_name).getParent();

    if (!classpath_directory_name.endsWith(File.separator))
      classpath_directory_name += File.separator;

    /* Because of the build change for 4.02, remove 'classes': */
    if (classpath_directory_name.indexOf(File.separator + "classes" + File.separator) != -1)
    {
      String tmp1 = classpath_directory_name;
      tmp1 = tmp1.substring(0, tmp1.indexOf(File.separator + "classes" + File.separator));
      classpath_directory_name = tmp1 + File.separator;
    }

    //common.ptod("classpath_directory_name: " + classpath_directory_name);

    return classpath_directory_name;
  }

  public static String classPath()
  {
    return classpath_directory_name;
  }
  public static String classPath(String file)
  {
    return classpath_directory_name + file;
  }


  public static String wholePath()
  {
    return System.getProperty("java.class.path");
  }

  public static void reportVdbenchScript()
  {
    if (common.onWindows())
    {
      String fname = classPath("vdbench.bat");
      if (!new File(fname).exists())
      {
        common.plog("Unable to find file " + fname);
        return;
      }

      common.plog("");
      common.plog("reportVdbenchScript() contents of " + fname + ": ");
      Fget fg = new Fget(fname);
      String line = null;
      while ((line = fg.get()) != null)
      {
        line = line.trim();
        if (line.length() > 0 && !line.startsWith("rem"))
          common.plog(line);
      }
      fg.close();
    }

    else
    {
      String fname = classPath("vdbench");
      if (!new File(fname).exists())
      {
        common.plog("Unable to find file " + fname);
        return;
      }

      common.plog("");
      common.plog("reportVdbenchScript() contents of " + fname + ": ");
      Fget fg = new Fget(fname);
      String line = null;
      while ((line = fg.get()) != null)
      {
        line = line.trim();
        if (line.length() > 0 && !line.startsWith("#"))
          common.plog(line);
      }
      fg.close();
    }
  }
}
