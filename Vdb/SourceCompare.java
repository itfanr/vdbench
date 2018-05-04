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

import java.util.*;
import java.io.*;
import Utils.*;




/**
 * Code used to compare source files.
 * This is used to facilitate not bringing unchanged source files back from
 * my home windows system to the network files.
 *
 * Equal files are deleted from the output directory, mismatched or new
 * files are copied to the output directory.
 */
class SourceCompare
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  static String source_dir;
  static String last_dir;

  static Vector jar_names  = new Vector(64, 0);

  static String sep = File.separator;

  static boolean debug = false;

  /**
   * We depend on the current directory being args[0]\args[1]
   */
  public static void main(String[] args)
  {
    source_dir = "c:\\" + args[0] + sep + args[1];
    last_dir   = "c:\\last_load\\" + args[0] + sep + args[1];

    //common.ptod("source_dir: " + source_dir);
    //common.ptod("last_dir: " + last_dir);


    String[] files = new File(source_dir).list();


    for (int i = 0; i < files.length; i++)
    {
      String file = files[i];
      if (!file.endsWith(".java"))
        continue;

      //if (!file.startsWith("Source"))
      //  continue;

      /* If the file matches, continue; */
      if (!compare(file ))
        continue;

      /* If there is a mismatch add file name to list of files to add to jar: */
      jar_names.addElement(file);
      //common.ptod("added to jar: " + file);
    }

    if (jar_names.size() == 0)
    {
      common.ptod("Nothing to put in jar");
      System.exit(0);
    }

    /* Generate a 'jar' command: */
    /* This command depends on current directoty being c:\xxxx\src */
    String cmd = "jar -Mcvf c:\\store.jar ";
    for (int i = 0; i < jar_names.size(); i++)
      cmd += " " + (String) jar_names.elementAt(i);


    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(cmd);

    //common.ptod("cmd: " + ocmd.getCmd());
    if (!debug)
      ocmd.execute();

    String[] stderr = ocmd.getStderr();

    common.ptod("");
    String[] stdout = ocmd.getStdout();
    for (int i = 0; i < stdout.length; i++)
      common.ptod("jar stdout: " + stdout[i]);
    common.ptod("");

    ocmd = new OS_cmd();
    ocmd.addText("cp c:\\store.jar h:\\" + args[0] + sep + args[1]);
    //common.ptod("cmd: " + ocmd.getCmd());
    if (!debug)
      ocmd.execute();

    stderr = ocmd.getStderr();
    for (int i = 0; i < stderr.length; i++)
      common.ptod("cp stderr: " + stderr[i]);

    stdout = ocmd.getStdout();
    for (int i = 0; i < stdout.length; i++)
      common.ptod("cp stdout: " + stdout[i]);

    ocmd = new OS_cmd();
    ocmd.addText("rsh sbm-240a.central.sun.com -l hv104788 /home/hv104788/tools/store " +
                 args[0] + " " + args[1]);
    //common.ptod("cmd: " + ocmd.getCmd());
    if (!debug)
      ocmd.execute();

    stderr = ocmd.getStderr();
    //for (int i = 0; i < stderr.length; i++)
    // common.ptod("stderr: " + stderr[i]);

    common.ptod("");
    stdout = ocmd.getStdout();
    for (int i = 0; i < stdout.length; i++)
      common.ptod("rsh stdout: " + stdout[i]);
    common.ptod("");

  }


  private static boolean compare(String file)
  {
    if (!new File(last_dir, file).exists())
      return true;

    Fget left  = new Fget(source_dir, file);
    Fget right = new Fget(last_dir, file);

    boolean mismatch = false;
    while (!mismatch)
    {
      String leftl = left.get();
      String rightl = right.get();
      //common.ptod("leftl:  " + leftl);
      //common.ptod("rightl: " + rightl);

      if (leftl == null && rightl == null)
      {
        break;
      }

      if (leftl == null || rightl == null )
      {
        mismatch = true;
        break;
      }

      if (leftl.compareTo(rightl) != 0)
      {
        mismatch = true;
        break;
      }

    }

    left.close();
    right.close();

    //common.ptod("mismatch for " + file + " " + mismatch);

    return mismatch;

  }
}
