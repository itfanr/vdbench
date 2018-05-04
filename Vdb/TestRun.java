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

import java.io.*;
import java.util.*;
import Utils.*;

/**
 * Class to help with starting test runs
 */
public class TestRun
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  static String parmfile;
  static Fput   status_file;
  static Fput   log_file;
  static boolean any_errors = false;


  /**
   * Current directory MUST be the directory with the parameter files in it!
   *
   * ../vdbench Vdb.TestRun -v ../ -e5 -o vdbench500 sst_onepga
   *
   * 'parmfile' is a either a parmfile or a file containing a list of parmfile
   * names. That list then must start with '*list".
   */
  public static void main(String[] args)
  {
    ClassPath.load_classpath_dir();
    String   target_vdbench = ClassPath.classPath();
    String   output_dir  = "output";
    String[] positionals = null;
    Vector   parmfiles = new Vector (8, 0);

    if (args.length == 0)
    {
      common.ptod("Usage:");
      common.ptod("TestRun -v target_vdbench -o output_directory parmfile|parmlist ....  ");
      common.failure("No parameters specified");
    }

    Getopt g = new Getopt(args, "e:i:v:o:", 99);
    if (!g.isOK())
      common.failure("Parameter scan error");

    g.print("TestRun");

    if (g.check('v'))
      target_vdbench = g.get_string();

    if (g.check('o'))
      output_dir = g.get_string();

    positionals = (String[]) g.get_positionals().toArray(new String[0]);
    if (positionals.length == 0)
      common.failure("No parameter files specified");


    /* Find out if the positional parameters are parmfiles or lists: */
    for (int i = 0; i < positionals.length; i++)
    {
      String[] lines = Fget.readFileToArray(positionals[i]);
      String line = lines[0];

      /* If this is just a parmfile, add it to list: */
      if (!line.startsWith("*list"))
      {
        parmfiles.add(positionals[i]);
        continue;
      }

      /* it is a list, just copy the file names: */
      for (int j= 0; j < lines.length; j++)
      {
        line = lines[j];
        line = line.trim();
        if (line.length() == 0 || line.startsWith("*"))
          continue;
        parmfiles.add(line);
      }
    }



    /* Make sure all the parmfiles exist: */
    for (int i = 0; i < parmfiles.size(); i++)
    {
      String file = (String) parmfiles.elementAt(i);
      common.ptod("Parameter files: " + file);
      if (!Fget.file_exists(file))
        common.failure("Parameter file does not exist: " + new File(file).getAbsolutePath());
    }


    /* status.txt shows return codes for each test: */
    status_file = new Fput(output_dir, "status.txt");

    /* logfile.txt shows the running stdout/stderr of all runs: */
    log_file    = new Fput(output_dir, "logfile.txt");

    /* Exceute all tests: */
    for (int i = 0; i < parmfiles.size(); i++)
    {
      parmfile = (String) parmfiles.elementAt(i);
      OS_cmd ocmd = new OS_cmd();

      ocmd.addQuot(target_vdbench + File.separator + "vdbench");
      ocmd.addText("-f " + parmfile);
      ocmd.addText("-o " + output_dir + "/" + parmfile);

      if (g.check('e'))
        ocmd.addText("-e " + g.get_string());
      if (g.check('i'))
        ocmd.addText("-i " + g.get_string());

      ocmd.setOutputMethod(new CommandOutput()
                           {
                             public boolean newLine(String line, String type, boolean more)
                             {
                               System.out.println(parmfile + " " + type + " " + line);
                               log_file.println(parmfile + " " + type + " " + line);
                               return true;
                             }
                           });

      boolean rc = ocmd.execute();
      if (!rc)
        any_errors = true;

      /* Write out status and flush files: */
      System.out.println(parmfile + " return code: " + rc);
      log_file.println(parmfile + " return code: " + rc);
      status_file.println(common.tod() + " " + parmfile + " return code: " + rc);

      for (int j = 0; j < 5; j++) System.out.println("");
      for (int j = 0; j < 5; j++) log_file.println("");

      status_file.flush();
      log_file.flush();
    }

    if (any_errors)
    {
      System.out.println("At least one failed testrun. ");
      status_file.println("At least one failed testrun. ");
    }
    else
    {
      System.out.println("All tests were successfull.");
      status_file.println("All tests were successfull.");
    }

    status_file.close();
    log_file.close();
  }
}


