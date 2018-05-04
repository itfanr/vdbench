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
 * <p>Title: OperatingSystemIdentifier.java</p>
 * <p>Description: This class allows the user to discern whether the application
 * is running under windows or solaris.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import java.util.*;
import javax.swing.*;

public class OperatingSystemIdentifier
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public final static int WINDOWS = 0;
  public final static int SOLARIS = 1;
  public final static int MAC     = 2;
  public final static int LINUX   = 3;
  public final static int AIX     = 4;
  public final static int HP      = 5;
  public final static int UNKNOWN = 999;

  /**
   * Determines whether the operating system is windows or solaris.
   * If it is neither of these, a value corresponding to "unknown" is returned.
   * @return one of the above integers indicating the operating system type.
   */
  public static int determineOperatingSystem()
  {
    int osIdentifier = UNKNOWN;

    try
    {
      // Parse the operating system name string to get the first word.
      StringTokenizer st = new StringTokenizer(System.getProperty("os.name"));
      String osName = st.nextToken().trim().toLowerCase();

      // Set the identifier depending on the first word.
      if(osName.equalsIgnoreCase("windows"))
        osIdentifier = WINDOWS;

      else if(osName.equalsIgnoreCase("solaris") ||
              osName.equalsIgnoreCase("sunos"))
        osIdentifier = SOLARIS;

      else if(osName.startsWith("mac"))
        osIdentifier = MAC;

      else if(osName.startsWith("linux"))
        osIdentifier = LINUX;

      else if(osName.startsWith("aix"))
        osIdentifier = AIX;

      else if(osName.startsWith("hp"))
        osIdentifier = HP;

      else
      {
        osIdentifier = UNKNOWN;

        // If the operating system is unknown, tell the user.
        JOptionPane.showMessageDialog(null, "Running unsupported operating system.  This application will not function.", "Error", JOptionPane.WARNING_MESSAGE);
      }
    }
    catch(SecurityException se)
    {
      se.printStackTrace();
    }
    return osIdentifier;
  }
}
