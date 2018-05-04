package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.Serializable;

/**
 * This class stores 'format=' information for a Run Definiton (RD), or for
 * a set of 'forxxx=' Run Definitions
 */
public class FormatFlags implements Serializable, Cloneable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  public boolean format_requested       = false;
  public boolean format_restart         = false;
  public boolean format_clean           = false;
  public boolean format_only_requested  = false;
  public boolean format_dirs_requested  = false;
  public boolean format_once_requested  = false;
  public boolean format_limited         = false;
  public boolean format_complete        = false;

  /* To handle 'format=once' for any group of forxxx= RDs */
  public boolean one_format_done        = false;


  public Object clone()
  {
    try
    {
      return super.clone();
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public void parseParameters(String[] parms)
  {
    if (common.get_debug(common.RESTART_FILLING))
      format_restart = true;

    /* See if 'complete' is specified anywhere: */
    for (String parm : parms)
    {
      if ("complete".equals(parm))
        format_complete = true;
    }

    /* If ANY parameter is 'n' or 'no', finish here.                         */
    /* This allows you to just add 'no' to any sequence of format parameters */
    /* to suppress the formatting. However, command line can override again. */
    for (int i = 0; i < parms.length; i++)
    {
      if ("no".startsWith(parms[i]))
      {
        format_requested = false;

        /* Debugging overrides: */
        if (Vdbmain.force_format_no)   format_requested = false;
        if (Vdbmain.force_format_yes)  format_requested = true;
        if (Vdbmain.force_format_only)
        {
          format_requested      = true;
          format_only_requested = true;
        }

        if (format_requested && format_complete)
          common.failure("'format=yes' and 'format=complete' are mutually exclusive.");

        return;
      }
    }


    for (int i = 0; i < parms.length; i++)
    {
      if ("directories".startsWith(parms[i]))
        format_dirs_requested = format_requested =true;

      else if ("restart".equals(parms[i]))
        format_restart = format_requested = true;

      else if ("only".equals(parms[i]))
        format_only_requested = format_requested = true;

      else if ("once".equals(parms[i]))
        format_once_requested = format_requested = true;

      else if ("clean".equals(parms[i]))
        format_only_requested = format_clean = format_requested = true;

      else if ("limited".equals(parms[i]))
        format_limited = format_requested = true;

      else if ("complete".equals(parms[i]))
        format_complete = format_requested = true;

      else if ("yes".startsWith(parms[i]))
        format_requested = true;

      else
        common.failure("Invalid contents of 'format=' parameter: " + parms[i]);
    }

    /* Debugging overrides: */
    if (Vdbmain.force_format_no)   format_requested = false;
    if (Vdbmain.force_format_yes)  format_requested = true;
    if (Vdbmain.force_format_only)
    {
      format_requested      = true;
      format_only_requested = true;
    }

    if (format_requested && format_complete)
      common.failure("'format=yes' and 'format=complete' are mutually exclusive.");

    if (format_limited && !format_only_requested)
      common.failure("The use of 'format=limited' requires the use of 'format=(only,limited)'");

    if (format_requested && Validate.isJournalRecovery())
      common.failure("'format=yes' and Journal recovery are mutually exclusive");
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
