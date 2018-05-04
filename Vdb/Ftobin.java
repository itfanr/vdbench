package Vdb;

/*
 *
 * Copyright (c) 2000-2008 Sun Microsystems, Inc. All Rights Reserved.
 *
 */

import java.util.*;
import Utils.Bin;
import Utils.Flat_record;
import Utils.Getopt;
import Utils.Fget;


/**
 * Convert TNF ascii record to binary
 */
public class Ftobin
{
  private final static String c = "Copyright (c) 2000-2008 Sun Microsystems, Inc. " +
                                  "All Rights Reserved.";


  private static int exceptions = 0;

  private static String  input_file  = null;
  private static String  output_file = null;
  private static int     linecount = Integer.MAX_VALUE;

  /* No need for a new Flat_record each time. One is enough. */
  private static Flat_record frec = new Flat_record();


  /**
   * File conversions.
   */
  public static void main(String[] args)
  {
    /* Read execution parameters, look for output directory first: */
    Getopt g = new Getopt(args, ":l:d: ", 2);
    scan_args(g);

    flat_to_bin();
    Ctrl_c.removeShutdownHook();
  }



  /**
   * Read and interpret al parameters.
   */
  public static void scan_args(Getopt g)
  {

    if (!g.isOK())
      common.failure("parameter scan error");

    if (g.check('l'))
      linecount = (int) g.get_long();

    g.print("Ftobin");

    Vector pos = g.get_positionals();
    if (pos != null)
    {
      if (pos.size() > 0)
        input_file = (String) pos.elementAt(0);
      if (pos.size () > 1)
        output_file = (String) pos.elementAt(1);
      if (pos.size () > 2)
        usage();
    }

    /* Double check some things: */
    if (input_file == null || output_file == null)
    {
      System.err.println("File name(s) missing");
      usage();
      common.exit(99);
    }
  }


  /** Now can we survive without a man file?
   */
  private static void usage()
  {
    System.err.println("Usage: ");
    System.err.println("./vdbench Vdb.Ftobin input_file output_file ");
    System.err.println("input_file: file name or '-' for stdin");
    System.err.println("output_file: file name or '-' for stdout");
    common.exit(99);
  }




  /**
   * Convert tnfdump/tnfedump ascii file to Swat binary
   */
  private static void flat_to_bin()
  {
    String line;
    Fget fg = new Fget(input_file);
    int lines = 0;


    /* 'null' is used for debugging and skips the output: */
    Bin bin = new Bin(output_file);
    if (!output_file.equals("null"))
      bin.output();
    else
      return;

    /* read all lines: */
    while ((line = fg.get()) != null)
    {
      line = line.trim();
      if (line.length() == 0)
        continue;
      if (line.startsWith("*"))
        continue;

      if (++lines > linecount)
        break;


/*
*   start    resp     device          lba    size r/w     pid
    0.000  70.634       8272  70734643200    8192  r     5306
*/
      try
      {
        StringTokenizer st = new StringTokenizer(line);

        /* Fixed portion: */
        frec.start  = (long) (Double.parseDouble(st.nextToken()) * 1000l);
        frec.resp   = (long) (Double.parseDouble(st.nextToken()) * 1000l);
        frec.device = Long.parseLong(st.nextToken());
        frec.lba    = Long.parseLong(st.nextToken());
        frec.xfersize = Integer.parseInt(st.nextToken());
        if (st.nextToken().toLowerCase().startsWith("r"))
          frec.flag = 1;
        else
          frec.flag = 0;
        frec.pid      = Integer.parseInt(st.nextToken());

        frec.export(bin);
      }
      catch (Exception e)
      {
        System.err.println("Exception while parsing input file " + input_file);
        System.err.println("Line#: " + lines + " Contents: " + line);
        common.failure(e);
      }

    }

    fg.close();
    bin.close();
  }
}

