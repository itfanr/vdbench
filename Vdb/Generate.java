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
import Utils.Fget;


/**
 * This class contains code and information related to each device number
 * that must be replayed.
 */
public class Generate
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private GenerateBack callback;
  private Vector data_for_this_sd;

  private double inter_arrival_time;
  private long   start_time;
  private long   lun_size;
  private long   lba = 0;
  private long   next_seq = 0;
  private Random seek_randomizer;
  private String filename = null;

  private static int     max_xfersize    = 0;
  private static boolean adjust_xfersize = false;
  private static boolean repeat          = false;
  private static int     distribution = 0;


  public Generate(GenerateBack call,
                  String[]     generate_parms,
                  String       sd_name)
  {
    callback = call;
    filename = generate_parms[0];

    /* Read all the data right away. In this case it won't be done */
    /* when all threads are synchronized and we can't waste any time */
    data_for_this_sd = readFile(sd_name, filename);

    /* Some values must be set before we begin: */
    calculateFirstStartTime();
    lun_size = callback.getLunSize();
    seek_randomizer = new Random(Native.get_simple_tod()); // set seed

    /* This must be set BEFORE the i/o tasks are started: */
    SlaveWorker.work.maximum_xfersize = max_xfersize;
  }


  /**
  * Generate i/o requests.
  *
  * This runs under its own thread (WG_task) and all it needs to do is
  * send i/o requests to the callback class (generateBack()).
  */
  public void generate()
  {
    int current_second = 0;

    /* The 'repeat' parameter keeps looping through the file: */
    do /* while (repeat) */
    {
      /* Use each line one at the time: */
      for (int i = 0; i < data_for_this_sd.size(); i++)
      {
        if (callback.isWorkloadDone())
          return;

        GenLine gl = (GenLine) data_for_this_sd.elementAt(i);

        /* Calculate new io inter arrival time for this new interval: */
        inter_arrival_time = (double)  (1000000 / gl.iops);

        for (int j = 0; j < gl.duration * gl.iops; j++)
        {
          if (callback.isWorkloadDone())
            break;

          /* Calculate start time for the next i/o: */
          calculateNextStartTime();

          boolean read = gl.readorWrite();
          int xfersize = gl.getXfersize(read);
          calculateLba(xfersize, gl.randomOrSequential());

          /* Send the i/o to be started at the requested time: */
          callback.scheduleRequest((long) start_time, lba, xfersize, read);
        }
      }
    } while (repeat);
  }

  /**
   * Validate the contents of the input file as much as possible.
   * We don't want to abort after 10 hours because of a silly typo!
   *
   * Method is synchronized because it is run by each thread, but there is
   * no need for more than one thread to complain about the same error.
   */
  public synchronized static Vector readFile(String sd_name, String filename)
  {
    Vector  gen_data = new Vector(64);
    Fget    fg       = new Fget(filename);
    GenLine gl       = null;
    String  line     = null;
    int     lines    = 0;

    while ((line = fg.get()) != null)
    {
      //common.ptod("line: " + line);
      lines++;

      line = line.trim();
      if (line.length() == 0)
        continue;

      if (line.startsWith("/")   ||
          line.startsWith("#")   ||
          line.startsWith("*")   )
        continue;

      if (line.startsWith("adjustxfersize"))
        adjust_xfersize = true;

      else if (line.startsWith("repeat"))
        repeat = true;

      else if (line.startsWith("distribution="))
      {
        if (line.startsWith("distribution=e"))
          distribution = 0;
        else if (line.startsWith("distribution=u"))
          distribution = 1;
        else if (line.startsWith("distribution=d"))
          distribution = 2;
        else
          common.failure("Unknown parameter value: " + line);
      }

      else
      {
        if ((gl = parseLine(line)) == null)
          common.failure("Error parsing file '" + filename + "' in line " + lines);

        /* Only save the stuff for our own SD: */
        if (!gl.sdname.equals(sd_name))
          continue;

        gen_data.add(gl);
      }
    }

    if (gen_data.size() == 0)
      common.failure("Error parsing file '" + filename +
                     ": no data found for sd=" + sd_name);

    return gen_data;

  }


  /**
   * Parse a parameter line. Result: a GenLine() instance.
   */
  private static GenLine parseLine(String line)
  {
    GenLine gl = new GenLine();
    String[] split = line.trim().split(" +");
    if (split.length != 7)
      return(GenLine) error("Expecting only 7 columns of data: " + line);

    gl.sdname = split[GenLine.SDNAME];

    try
    {
      gl.duration = Integer.parseInt(split[GenLine.DURATION]);
      gl.iops     = Integer.parseInt(split[GenLine.IOPS]);
      gl.read_pct = Double.parseDouble(split[GenLine.READ_PCT]);
      gl.seek_pct = Double.parseDouble(split[GenLine.SEEK_PCT]);

      gl.read_xfer = new double[1];
      if (!split[GenLine.READ_XFER].startsWith("("))
      {
        gl.read_xfer[0] = Double.parseDouble(split[GenLine.READ_XFER]);
        setMaxXfersize(gl.read_xfer[0]);
      }
      else
        gl.read_xfer = parseXfersizeMatrix(split[GenLine.READ_XFER]);


      gl.write_xfer = new double[1];
      if (!split[GenLine.WRITE_XFER].startsWith("("))
      {
        gl.write_xfer[0] = Double.parseDouble(split[GenLine.WRITE_XFER]);
        setMaxXfersize(gl.write_xfer[0]);
      }
      else
        gl.write_xfer = parseXfersizeMatrix(split[GenLine.WRITE_XFER]);


      if (gl.read_xfer == null || gl.write_xfer == null)
        return null;

      if (gl.read_xfer[0] %512 != 0)
        return(GenLine) error("Xfersize value must be multiple of 512: " + line);

      if (gl.write_xfer[0] %512 != 0)
        return(GenLine) error("Xfersize value must be multiple of 512: " + line);

      /* Adjust xfersizes if needed: */
      if (adjust_xfersize)
      {
        gl.read_xfer  = adjustXfersize(gl.read_xfer);
        gl.write_xfer = adjustXfersize(gl.write_xfer);
      }
    }

    catch (Exception e)
    {
      return(GenLine) error("Exception parsing: " + e.getLocalizedMessage() +
                            ": " + line);
    }

    return gl;
  }

  private static Object error(String txt)
  {
    common.ptod(txt);
    return null;
  }

  private static synchronized void setMaxXfersize(double xfer)
  {
    max_xfersize = (int) Math.max(max_xfersize, xfer);
  }


  private static double[] parseXfersizeMatrix(String split)
  {
    StringTokenizer st = new StringTokenizer(split, "(,)");
    if (st.countTokens() %2 != 0)
      return(double[]) error("Xfersize values must be in pairs: " + split);

    double[] dist = new double[st.countTokens()];

    int tokens = st.countTokens();
    for (int i = 0; i < tokens; i++)
    {
      try
      {
        dist[i] = Double.parseDouble(st.nextToken());
      }

      catch (Exception e)
      {
        return(double[]) error("Exception parsing xfersizes: " + e.getLocalizedMessage() +
                               ": " + split);
      }
    }

    double total = 0;
    for (int j = 0; j < dist.length; j += 2)
      total += dist[j+1];

    if (total != 100)
      return(double[]) error("Xfersize distribution must add up to 100%: " + split);

    for (int j = 0; j < dist.length; j += 2)
    {
      if (dist[j+0] %512 != 0)
        return(double[]) error("Xfersize value must be multiple of 512: " + split);

      setMaxXfersize(dist[j+0]);
    }

    return dist;
  }




  /**
  * When xfersize adjustment is requested, any single xfersize that is not
  * a power of two will be adjusted to a decent mix of the powers of two
  * straddling the requested xfersize.
  *
  * e.g. 5120 bytes will be 75% * 4096 and 25% * 8192
  *
  * This code was written to help people with putting together workloads
  * using average observed xfersizes. Since 'normal' operation suggests
  * that most blocks are some power of two this seemed to be the best
  * solution.
  * If thtsi is not 'normal', then the user must specify the xfersize
  * distributuion list himself, though where he could get the values would
  * be a serious question (Swat?).
  */
  private static double[] adjustXfersize(double[] xfers)
  {
    if (xfers.length != 1)
      return xfers;

    /* There can only be ONE bit set here: */
    double xfersize = (int) xfers[0];
    int xfer = (int) xfersize;
    int bits = 0;
    int last = 0;
    for (int i = 0; i < 32; i++)
    {
      if ((xfer & 1) == 1)
        bits++;
      xfer = xfer >> 1;

      /* Keep the last left-most bit. That is then the lower of ther pairs of */
      /* xfersizes we use: */
      last = i;
      if (xfer == 0)
        break;
    }

    /* Only one bit? OK: */
    if (bits == 1)
      return xfers;

    double low  = 1 << last;
    double high = 1 << last + 1;
    //common.ptod("xfersize: " + xfersize);
    //common.ptod("low:  " + low);
    //common.ptod("high: " + high);

    /* Calculate the percentage of the block that must be larger than the      */
    /* requested size. This then results in the percentage that must be lower. */
    /* All with as result that we have two xfersizes that averaged are         */
    /* reasonably close to the ultimate xfersize requested.                    */
    int pct_high = (int) ((xfersize / low - 1.) * 100);
    int pct_low  = 100 - pct_high;
    //common.ptod("pct_high: " + pct_high);
    //common.ptod("pct_low: " + pct_low);
    //common.ptod("xx: " + ((pct_low * low + pct_high * high) / 100) + " " + xfersize);

    double[] new_xfers = new double[] { low, pct_low, high, pct_high};

    common.ptod("adjustXfersize for " + (int) xfersize + ": (" +
                (int) new_xfers[0] + "," + (int) new_xfers[1] + "," +
                (int) new_xfers[2] + "," + (int) new_xfers[3] + ") " +
                ((pct_low * low + pct_high * high) / 100));

    setMaxXfersize(new_xfers[2]);

    return new_xfers;
  }


  /**
   * The first start time must be properly set.
   * If we don't set it then there is a chance that all devices start
   * their first i/o at the same time.
   */
  private void calculateFirstStartTime()
  {
    if (distribution == 0)
      start_time = (long) ownmath.exponential(inter_arrival_time);

    else if (distribution == 1)
      start_time = (long) ownmath.uniform(0, inter_arrival_time * 2);

    else
      start_time = (long) ownmath.uniform(0, inter_arrival_time * 2);
  }

  /**
   * Calculate the next arrival time for the next i/o.
   *
   * Unless 'distribution=d' is used, there is a chance that some ios from
   * one interval/duration will run either in the previous interval or in
   * the next interval.
   */
  private void calculateNextStartTime()
  {
    if (distribution == 0)
    {
      double delta = ownmath.exponential(inter_arrival_time);
      if (delta > 180 * 1000000)
        delta = 180 * 1000000;
      start_time += delta;
    }

    else if (distribution == 1)
    {
      double delta = ownmath.uniform(0, inter_arrival_time * 2);
      start_time += delta;
      if (delta > 1000000)
        delta = 1000000;
    }

    else
      start_time += inter_arrival_time;
  }

  public void calculateLba(int xfersize, boolean random)
  {
    /* Never use lba0: */
    do
    {
      if (random)
      {
        /* All i/o on block bounderies: */
        long blocks = lun_size / xfersize;
        double rand = seek_randomizer.nextDouble();
        blocks      = (long) (rand * blocks);
        lba         = blocks * xfersize;
      }

      else
      {
        if (next_seq + xfersize > lun_size)
          next_seq = xfersize;
        lba = next_seq;
      }

      /* Preserve in case we use sequential: */
      next_seq = lba + xfersize;

    } while (lba == 0);

  }

  public static void main(String[] args)
  {
    double[] xfers = new double[1];
    xfers[0] = Integer.parseInt(args[0]);
    adjustXfersize(xfers);
  }

}

class GenLine
{
  String   sdname;
  int      duration;
  double   iops;
  double   read_pct;
  double[] read_xfer;
  double[] write_xfer;
  double   seek_pct;

  public static int SDNAME     = 0;
  public static int DURATION   = 1;
  public static int IOPS       = 2;
  public static int READ_PCT   = 3;
  public static int READ_XFER  = 4;
  public static int WRITE_XFER = 5;
  public static int SEEK_PCT   = 6;


  /**
   * Calculate transfer size: fixed, or using distribution list.
   *
   * (Maybe write some code to prevent the need to scan through the
   * distribution list and instead just pick up something like xfer[pct]?)
   */
  public int getXfersize(boolean read)
  {
    double[] xfers = (read) ? read_xfer : write_xfer;

    /* For fixed xfersize don't bother with randomizer: */
    if (xfers.length == 1)
      return(int) xfers[0];

    int pct    = (int) (ownmath.zero_to_one() * 100);
    int cumpct = 0;
    int i;

    for (i = 0; i < xfers.length; i+=2)
    {
      cumpct += xfers[i+1];
      if (pct < cumpct)
        break;
    }
    int size = (int)  xfers[i];

    return size;
  }


  /**
   * Determine whether an i/o should be read or write.
   */
  public boolean readorWrite()
  {
    if (read_pct == 100)
      return true;
    else if (read_pct == 0)
      return false;

    if (ownmath.zero_to_one() * 100 < read_pct)
      return true;
    else
      return false;
  }


  /**
   * Determine whether an i/o should be random or sequential
   */
  public boolean randomOrSequential()
  {
    /* Calculate what we should do: sequential or random seek: */
    /* Negative value means terminate after EOF */
    if (seek_pct <= 0)
      return false;
    else if (seek_pct == 100)
      return true;

    if (ownmath.zero_to_one() * 100 < seek_pct)
      return true;
    else
      return false;
  }

}

/*
adjustxfersize   means to do the 4k - 8k trick.

*sd duration iops read% rdxfer            wrxfer seekpct
sd1 30       100  20    4096              8192   0
sd1 30       120  20    5120              8192   0
sd2 30       90   10    (4096,50,8192,50) 8192   0
*/
