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

import java.io.Serializable;

/**
 * This class stores 'format=' information for a Run Definiton (RD), or for
 * a set of 'forxxx=' Run Definitions
 */
public class FormatFlags implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  public boolean format_requested       = false;
  public boolean format_restart         = false;
  public boolean format_clean           = false;
  public boolean format_only_requested  = false;
  public boolean format_dirs_requested  = false;
  public boolean format_once_requested  = false;

  /* To handle 'format=once' for any group of forxxx= RDs */
  public boolean one_format_done        = false;



  public void parseParameters(String[] parms)
  {
    if (common.get_debug(common.RESTART_FILLING))
      format_restart = true;

    /* If ANY parameter is 'n' or 'no', finish here.                         */
    /* This allows you to just add 'no' to any sequence of format parameters */
    /* to suppress the formatting:                                           */
    for (int i = 0; i < parms.length; i++)
    {
      if ("no".startsWith(parms[i]))
      {
        /* Debugging overrides: */
        if (Vdbmain.force_format_no)
          format_requested = false;
        if (Vdbmain.force_format_yes)
          format_requested = true;
        if (Vdbmain.force_format_only)
        {
          format_requested      = true;
          format_only_requested = true;
        }

        return;
      }
    }

    /* This is a format. Determine the type: */
    format_requested = true;
    if (Validate.isJournalRecovery())
      common.failure("'format=yes' and Journal recovery are mutually exclusive");
    format_requested = true;

    for (int i = 0; i < parms.length; i++)
    {
      if ("directories".startsWith(parms[i]))
        format_dirs_requested = true;

      else if ("restart".equals(parms[i]))
        format_restart = true;

      else if ("only".equals(parms[i]))
        format_only_requested = true;

      else if ("once".equals(parms[i]))
        format_once_requested = true;

      else if ("clean".equals(parms[i]))
        format_only_requested = format_clean = true;

      else if (!"yes".startsWith(parms[i]))
        common.failure("Invalid contents of 'format=' parameter: " + parms[i]);
    }

    /* Debugging overrides: */
    if (Vdbmain.force_format_no)
      format_requested = false;
    if (Vdbmain.force_format_yes)
      format_requested = true;
    if (Vdbmain.force_format_only)
    {
      format_requested      = true;
      format_only_requested = true;
    }

    //common.ptod("FormatFlags: " + this);
  }

  public String toString()
  {
    String txt = "\n";

    txt += "format_requested:      " + format_requested      + "\n";
    txt += "format_restart:        " + format_restart        + "\n";
    txt += "format_only_requested: " + format_only_requested + "\n";
    txt += "format_dirs_requested: " + format_dirs_requested + "\n";
    txt += "format_clean:          " + format_clean          + "\n";
    txt += "format_once_requested: " + format_once_requested + "\n";
    return txt;
  }
}
