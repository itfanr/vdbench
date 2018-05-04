package VdbGui;

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

/**
 * <p>Title: TextFilter.java</p>
 * <p>Description: This class creates a text filter to be used with
 * JFileChooser to allow the user to view only those files in a given
 * directory which end with the suffix "txt".</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import javax.swing.filechooser.FileFilter;
import java.io.File;

public class TextFilter extends FileFilter
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // Mandatory implementation of function to determine which files meet
  // the filter criterion.  In this case, directories and files with suffix
  // "txt" are accepted.
  public boolean accept(File f)
  {
    // Is f a directory?
    boolean accept = f.isDirectory();

    // If f is not a directory...
    if(!accept)
    {
      //...get its suffix.
      String suffix = getSuffix(f);

      // If a suffix exists, see if it's equal to "txt", the desired suffix.
      if(suffix != null)
      {
        accept = suffix.equals("txt");
      }
    }
    return accept;
  }

  // Mandatory method which provides a descriptive string for the filter
  // which appears in the filter selection window of the file chooser
  // which uses this filter.
  public String getDescription()
  {
    return "Text Files(*.txt)";
  }

  // Determines the suffix of a file object.
  private String getSuffix(File f)
  {
    String s = f.getPath();
    String suffix = null;

    // Get everything after the last ".".
    int i = s.lastIndexOf('.');

    if(i > 0 && i < s.length() - 1)
    {
      suffix = s.substring(i + 1).toLowerCase();
    }
    return suffix;
  }
}
