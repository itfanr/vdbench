package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import Utils.*;

/**
 * This class handles code around an idea that I had to assist users with
 * complex multi host runs.
 *
 * Some times it is necessary to repeat SDs, FSD, WDs or FWDs over and over
 * again for each separate host. That's a waste of time. The computer can do
 * that.
 * After initially writing the GenParms() program as an external attempt to
 * do this I decided to move the logic of that code to THIS class.
 *
 * Every occurrence of '$host' or '#host' in a name or a lun will cause that
 * parameter to be automatically repeated once for each parameter for each host,
 * replacing $host with the host name and #host with the relative host number.
 * '#hosts' is replaced by the --number-- of hosts.
 *
 * ????? Still true?
 * This has been resolved in getParameterLinex().
 * There is a limit though at this time: the whole sd/fsd/wd/fwd has to be on
 * one single line of input in the parameter file, since at this time it is not
 * worth the effort to handle mulit-line input.
 * (repeating one line 'n' times is easier than repeating 'n' lines 'n' times.)
 *
 */
public class InsertHosts
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private static Vector newlines;


  private static String insertHostName(String line, String hostname, int host_count, int index)
  {
    String out = line;

    /* Also allow '!' to be used instead of '$' to get around $ being recognized in scripts: */
    out = common.replace(out, "$host",       hostname);
    out = common.replace(out, "!host",       hostname);
    out = common.replace(out, "#host",  "" + index);

    if (!out.equals(line))
      return out;

    return null;
  }

  public static boolean anyChangesMade(String[] original, String[] current)
  {
    for (int i = 0; i < original.length && original[i] != null; i++)
    {
      if (!original[i].equals(current[i]))
        return true;
    }
    return false;
  }

  /**
   * Replace (possible) occurrences of #hosts:
   */
  public static String[] replaceNumberOfHosts(String[] lines)
  {
    String[] out = new String[lines.length];
    Vector hosts = Host.getDefinedHosts();

    for (int i = 0; i < lines.length && lines[i] != null; i++)
      out[i] = common.replace(lines[i], "#hosts", "" + hosts.size());

    return out;
  }

  public static String[] repeatParameters(String[] lines)
  {
    int index;

    /* The new array first starts off as a Vector: */
    newlines = new Vector(lines.length * 2, 0);
    Vector hosts = Host.getDefinedHosts();


    /* Scan and copy until we find an SD: */
    for (index = 0;  index < lines.length; index++)
    {
      String line = lines[index];

      /* A null line indicates 'last line' (the array is over allocated): */
      if (line == null)
        break;
      //common.ptod("linexx: " + line);

      /* Copy until we find the start of a parameter: */
      if (!isStart(line) || line.startsWith("rd="))
      {
        addLine(line);
        continue;
      }

      /* We have a parameter, gather every line belonging to this: */
      Vector <String> parm_lines = getParameterLines(lines, index);
      index += parm_lines.size() - 1;

      /* If no changes needed, just add the lines: */
      if (!anyChangesNeeded(lines))
      {
        for (int i = 0; i < parm_lines.size(); i++)
          addLine(parm_lines.elementAt(i));
        continue;
      }

      /* Now repeat these lines once for each host: */
      loop:
      for (int h = 0; h < hosts.size(); h++)
      {
        Host host = (Host) hosts.elementAt(h);

        /* obsolete? 'filerserver' is NOT repeated: */
        if (host.getLabel().equals("fileserver"))
          continue ;

        for (int i = 0; i < parm_lines.size(); i++)
        {
          line = insertHostName((String) parm_lines.elementAt(i), host.getLabel(), hosts.size(), h);
          //common.ptod("parm_lines.elementAt(i): " + parm_lines.elementAt(i));
          //common.ptod("line:                    " + line);

          /* If not changed, add the original: */
          if (line == null)
          {
            addLine((String) parm_lines.elementAt(i));
            break loop;
          }
          else
            addLine(line);
        }
      }
    }

    /* Now replace the original array coming from Vdb_scan: */
    String[] array = (String[]) newlines.toArray(new String[0]);

    return array;
  }

  private static void addLine(String txt)
  {
    //if (txt.contains("wd=read"))
    //{
    //  common.ptod("txt: " + txt);
    //  common.where(8);
    //}
    newlines.add(txt);
  }

  private static boolean anyChangesNeeded(String[] lines)
  {
    for (int i = 0; i < lines.length && lines[i] != null; i++)
    {
      if (insertHostName((String) lines[i], "dummy", 999, 9999) != null)
        return true;
    }
    return false;
  }


  private static boolean isStart(String line)
  {
    return(line.startsWith("sd"  ) ||
           line.startsWith("fsd" ) ||
           line.startsWith("wd"  ) ||
           line.startsWith("fwd" ) ||
           line.startsWith("rd"  ) );
  }

  private static Vector getParameterLines(String[] lines, int index)
  {
    /* Begin with adding the current line: */
    Vector parm_lines = new Vector(16, 0);
    parm_lines.add(lines[index]);

    /* Now add the rest: */
    for (index++;index < lines.length; index++)
    {
      String line = lines[index];
      if (line == null || isStart(line))
        break;
      parm_lines.add(line);
    }

    //for (int i = 0; i < parm_lines.size(); i++)
    //  common.ptod("returning parm_lines: " + i + " " + parm_lines.elementAt(i));

    return parm_lines;
  }


  /**
   * Parameter substitution allows parameter file overrides through the command
   * line, e.g. size=2g, where $size in the parameter file is then replaced with
   * '2g'.
   */
  private static HashMap <String, String> substitute_map   = new HashMap(16);
  private static HashMap <String, Long>   substitute_count = new HashMap(16);
  public static void addSubstitute(String parm)
  {
    if (parm.indexOf("=") == -1)
      common.failure("Invalid parameter substitution request: " + parm);

    String key   = parm.substring(0, parm.indexOf("="));
    String value = parm.substring(parm.indexOf("=") + 1);

    substitute_map.put(key, value);
    substitute_count.put(key, 0L);
  }


  public static String substituteLine(String input)
  {
    String line = input;

    for (String key : substitute_map.keySet())
    {
      String value = substitute_map.get(key);

      boolean found = false;
      if (line.contains("$" + key))
      {
        found = true;
        line = common.replace(line, "$" + key, value);
      }
      if (line.contains("!" + key))
      {
        found = true;
        line = common.replace(line, "!" + key, value);
      }


      /* Use this only for unique keywords, so not for 'wd=' */
      /* This was a failed experiment, causing problems when people had */
      /* both lun= and $lun in the input. Oh well... */
      if (false)
      {
        /* Look for ANY 'key=' value in the line: */
        boolean debug = false;
        if (line.contains(key + "="))
        {
          if (debug) common.ptod("key: " + key);
          if (debug) common.ptod("line: " + line);
          found = true;

          /* Get the beginning of the old line: */
          int index = line.indexOf(key);
          if (debug) common.ptod("index: " + index);
          String begin = line.substring(0, index + key.length() + 1);
          if (debug) common.ptod("begin: " + begin);

          /* Now get the ending of the old line: */
          String ending = line.substring(begin.length());
          if (debug) common.ptod("ending1: " + ending);

          /* Is there is a comma, end it there, otherwise, that's the end of the line: */
          if (ending.contains(","))
            ending = ending.substring(ending.indexOf(","));
          else
            ending = "";
          if (debug) common.ptod("ending2: " + ending);

          /* Contstruct the new line with begin + value + end: */
          line = begin + value + ending;
          if (debug) common.ptod("line: " + line);
        }
      }


      if (found)
      {
        long old = substitute_count.get(key);
        substitute_count.put(key, ++old);
      }
    }

    /* Bypass since we did not do checking in the past: */
    if (!common.get_debug(common.NO_MISSING_SUB_CHECK))
    {
      /* We are here too early for 'Multi-host parameter replication', */
      /* so we first need to make sure we filter out those:            */
      if (!line.contains("$host") && !line.contains("!host"))
      {
        if (line.contains("=$") || line.contains("=!"))
          common.failure("Unknown variable substitution request in parameter file: " + line);
      }
    }

    return line;
  }

  /**
   * Look through all non-comment lines of the parameter file, looking for any
   * $xxx or !xxx substitutions that were not resolved.
   * (Comment lines already have been removed before this).
   */
  public static void lookForMissingSubstitutes(String[] lines)
  {
    /* Bypass since the could did not do checking in the past: */
    if (common.get_debug(common.NO_MISSING_SUB_CHECK))
      return;

    int errors = 0;
    for (String key : substitute_count.keySet())
    {
      Long count = substitute_count.get(key);
      if (count == 0)
      {
        common.ptod("Unused parameter substitution: %s=%s", key, substitute_map.get(key));
        errors++;
      }
    }

    if (errors > 0)
      common.failure("Unused parameter substitution.");


    // for (int i = 0; i < lines.length && lines[i] != null; i++)
    // {
    //   /* Scan the line only until the first blank. */
    //   /* (If user has a blank embedded file name we'll have a problem) */
    //   String line = lines[i].trim() + " ";
    //   line = line.substring(0, line.indexOf(" "));
    //   String[] split = line.split(" +");
    //
    //   /* Scan through all words, picking up all $xxx or !xxx that are */
    //   /* NOT embedded in quotes: */
    //   boolean pending_quote = false;
    //   for (int j = 0; j < split.length; j++)
    //   {
    //     String word = split[j];
    //     if (!pending_quote && word.indexOf("\"") != -1)
    //     {
    //       pending_quote = true;
    //       continue;
    //     }
    //     if (pending_quote && word.indexOf("\"") != -1)
    //     {
    //       pending_quote = false;
    //       continue;
    //     }
    //
    //     /* If we're now inside of quotes, don't look for substitutions: */
    //     if (pending_quote)
    //       continue ;
    //
    //     /* No $ or !, skip: */
    //     if (word.indexOf("$") == -1 && word.indexOf("!") == -1)
    //       continue;
    //
    //     String which = (word.indexOf("$") != -1) ? "$" : "!";
    //
    //     String tmp = word.substring(word.indexOf(which));
    //     common.ptod("");
    //     common.ptod("Parameter file: " + lines[i]);
    //     common.ptod("Missing substitution: " + tmp);
    //     common.failure("Missing substituion parameter.");
    //   }
    // }
  }
}

