package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.File;
import java.util.*;

import Utils.*;


/**
* Helper class for debugging of Dtrace scripts
*
* ./vdbench Vdb.Dtrace script.d $parm-1 .... $parm-n -xxxx -yyyy
*
* Add '-n' to NOT run the script, but instead print it.
*
* The program reads the script.
* If it sees any '/* xxxx' at the start of a line, it will replace the commented
* line(s) with '/* xxxx * / ' and also removes the trailing '* /'
* it also removes lines with //, or any data on a line beyond //
*
*
*/
public class Dtrace
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  static Vector script_parms = new Vector(16, 0);
  static Vector debug_parms  = new Vector(16, 0);
  static Vector new_script   = new Vector(256, 0);
  static boolean run_script = true;

  public static void main(String[] args) throws Exception
  {
    String script = args[0];
    int linecount = 0;

    for (int i = 1; i < args.length; i++)
    {
      if (args[i].startsWith("-n"))
        run_script = false;
      else if (args[i].startsWith("-"))
        debug_parms.add(args[i].substring(1));
      else
        script_parms.add(args[i]);
    }


    Fget fg = new Fget(script);
    String line = null;
    String change = null;

    /* Read the script: */
    while ((line = fg.get()) != null)
    {
      String original_line = line;
      linecount++;

      /* Remove a line starting with //: */
      if (line.trim().startsWith("//"))
      {
        new_script.add("/* '//' line removed */");
        continue;
      }

      /* Remove everything beyond '//': */
      if (line.indexOf("//") != -1)
      {
        new_script.add(line.substring(0, line.indexOf("//")) + " /* removed // */");
        continue;
      }

      /* If we are in the middle of a 'change' remove terminating comment '* /' */
      if (change != null)
      {
        if (line.indexOf("*/") != -1)
        {
          new_script.add(line.substring(0, line.indexOf("*/")));
          change = null;
        }
        else
          new_script.add(line);
        continue;
      }

      /* If the line does NOT start with a '/*', just copy it: */
      if (!line.trim().startsWith("/*"))
      {
        new_script.add(line);
        continue;
      }

      /* We have a line starting with a '/*'. Comment out the label */
      if ((change = mustChange(line)) != null)
      {
        /* Remove the first '/*': */
        line = common.replace_string(line, "/*", "");
        //common.ptod("line1: " + line);

        /* Comment out the label: */
        line = common.replace_string(line, change, "/* " + change + " */");
        //common.ptod("line2: " + line);

        if (original_line.trim().endsWith("*/"))
        {
          line = line.substring(0, line.lastIndexOf("*/"));
          new_script.add(line);
          change = null;
        }
        else
          new_script.add(line);
        continue;
      }

      /* Comment out just the debug label: */
      //String[] split = line.trim().split(" +");
      //line = common.replace_string(line, split[0], "/*" + split[0] + "*/");
      new_script.add(line);
    }

    if (linecount != new_script.size())
      common.failure("Unmatched linecount: " + linecount + "/" + new_script.size());

    /* Copy the new script to a temp file: */
    Fput fp = new Fput("/tmp/dscript.d");
    for (int i = 0; i < new_script.size(); i++)
      fp.println((String) new_script.elementAt(i));
    fp.close();

    /* Create a command line with the Dscript parameters: */
    String cmd = "/tmp/dscript.d ";
    for (int i = 0; i < script_parms.size(); i++)
      cmd += (String) script_parms.elementAt(i) + " ";

    /* Now execute the command:
    if (run_script)
    {
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText(cmd);
      System.out.println();
      ocmd.setOutputMethod(new CommandOutput()
                           {
                             public boolean newLine(String line, String type, boolean more)
                             {
                               if (type.equals("stderr"))
                                 System.out.println("stderr: " + line);
                               else
                                 System.out.println(line);
                               return true;
                             }
                           });

      ocmd.execute(false);
    }

    else
    {
      for (int i = 0; i < new_script.size(); i++)
        System.out.println(String.format("%3d %s", (i+1), new_script.elementAt(i)));
    } */
  }

  private static String mustChange(String line)
  {
    String[] split = line.trim().split(" +");
    if (split.length < 2)
      return null;
    //common.ptod("split0: " + split[0]);
    //common.ptod("split1: " + split[1]);

    for (int i = 0; i < debug_parms.size(); i++)
    {
      String deb = (String) debug_parms.elementAt(i);
      //common.ptod("deb: " + deb);
      if (deb.equals(split[1]) || ("/*" + deb).equals(split[0]))
        return deb;
    }
    return null;
  }

}
