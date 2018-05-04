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

import java.util.Vector;
import java.io.Serializable;
import Utils.Format;

/**
 * This class provides assistance with execution of requested 'for' loops.
 */
public class For_loop implements Serializable, Cloneable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String label;                /* 'forxfersize' etc.                          */
  private double value[];              /* Array of values for each loop               */

  private double forxfersize;          /* After for_get(), values for each run        */
  private double forseekpct;
  private double forhitarea;
  private double forthreads;
  private double forrdpct;
  private double forrhpct;
  private double forwhpct;
  private double foriorate;
  private double forcomp;
  private double fordepth;
  private double forwidth;
  private double forfiles;
  private double foroperation;
  private double forsizes;
  private double fortotal;
  private double forwss;

  private String display_text;  /* Valid when this has been translated into a do_list */

  private static Vector override_display;

  public  static double NOVALUE = Double.MAX_VALUE;

  private static For_loop last = new For_loop(); /* last value of each type in for_get() */


  For_loop()
  {
    //common.ptod("For_loop1 seqno: " + seqno);
    init();
  }


  /**
   * Add a 'for loop' request to the request list.
   * <pre>
   * 'for' requests will be honored in the order in which they are defined
   * in the Run Definition. Entries are stored in the request list
   * on a FIFO basis.
   */
  For_loop(String intype, double[] list, Vector for_list)
  {

    //common.ptod("For_loop2 seqno: " + seqno);
    init();
    label = intype;

    /* Allocate new list: */
    value = new double[list.length];

    if (list.length == 0)
      common.failure("For_loop(): '" + intype + "=' " + "the list of NUMERIC values is empty. ");

    /* If this type already exists, remove the old one: */
    for (int i = 0; i < for_list.size(); i++)
    {
      For_loop fl = (For_loop) for_list.elementAt(i);
      if (fl.label.equals(intype))
        for_list.removeElementAt(i);
    }


    /* Copy list of values: */
    for (int j = 0; j < list.length; j++)
      value[j] = list[j];

    for_list.add(this);

    if (false)
    {
      common.ptod("for_loop() after adding: " + intype + " " + for_list.size());
      for (int i = 0; i < for_list.size(); i++)
      {
        For_loop fl = (For_loop) for_list.elementAt(i);
        common.ptod("entry:        " + i );
        common.ptod("type:         " + fl.label );
        /*
        common.ptod("forxfersize:  " + fl.forxfersize  );
        common.ptod("forseekpct:   " + fl.forseekpct   );
        common.ptod("forhitarea:   " + fl.forhitarea   );
        common.ptod("forthreads:   " + fl.forthreads   );
        common.ptod("forrdpct:     " + fl.forrdpct     );
        common.ptod("forrhpct:     " + fl.forrhpct     );
        common.ptod("forwhpct:     " + fl.forwhpct     );
        common.ptod("foriorate:    " + fl.foriorate    );
        common.ptod("forcomp:      " + fl.forcomp      );
        common.ptod("fordepth:     " + fl.fordepth     );
        common.ptod("forwidth:     " + fl.forwidth     );
        common.ptod("forfiles:     " + fl.forfiles     );
        common.ptod("foroperation: " + fl.foroperation );
        common.ptod("forsizes:     " + fl.forsizes     );
        common.ptod("fortotal:     " + fl.fortotal     );
        common.ptod("forwss:       " + fl.forwss       );
        */
        for (int j = 0; j < value.length; j++)
          common.ptod(Format.f("value:        %d", fl.value[j])  );
      }
    }
  }

  public double getThreads()
  {
    return forthreads;
  }



  private void init()
  {
    forxfersize  = NOVALUE;
    forseekpct   = NOVALUE;
    forhitarea   = NOVALUE;
    forthreads   = NOVALUE;
    forrdpct     = NOVALUE;
    forrhpct     = NOVALUE;
    forwhpct     = NOVALUE;
    foriorate    = NOVALUE;
    forcomp      = NOVALUE;
    fordepth     = NOVALUE;
    forwidth     = NOVALUE;
    forfiles     = NOVALUE;
    foroperation = NOVALUE;
    forsizes     = NOVALUE;
    fortotal     = NOVALUE;
    forwss       = NOVALUE;
  }


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

  public String getText()
  {
    if (display_text == null)
      return "For loops: None";
    return "For loops: " + display_text;
  }

  /**
    * The 'for' request list is translated into a 'todo' list.
    *
    * If the request list is empty, at least ONE entry is created into the list
    * with all the 'for' values set to zero. This means in later code that
    * there are NO overrides of earlier defined values.
    *
    * This method is called recursively because that allows us to execute the
    * requested 'for' loop in the same order as they have been entered in the
    * input parameters.
    */
  public static void for_get(int start, RD_entry rd, Vector do_list)
  {
    For_loop fp = null;
    For_loop nfp = null;
    int j = 0;

    /* If we are just starting, clear do_list: */
    if (start == 0)
    {
      last.init();
      do_list.removeAllElements();
      override_display = new Vector(8, 0);
    }


    /* Return an empty list if no forxxx parameters were used: */
    if (start == 0 && rd.for_list.size() == 0)
    {
      fp = new For_loop();
      do_list.addElement(fp);
      return;
    }


    for (int i = start; i < rd.for_list.size(); i++)
    {
      fp = (For_loop) rd.for_list.elementAt(i);

      for (j = 0; j < fp.value.length; j++)
      {
        //common.ptod("fp.label:>>" + fp.label + "<<");
        /**/
        if (fp.label.equals("forxfersize"))  last.forxfersize  = fp.value[j];
        else if (fp.label.equals("forseekpct"))   last.forseekpct   = fp.value[j];
        else if (fp.label.equals("forhitarea"))   last.forhitarea   = fp.value[j];
        else if (fp.label.equals("forthreads"))   last.forthreads   = fp.value[j];
        else if (fp.label.equals("forrdpct"))     last.forrdpct     = fp.value[j];
        else if (fp.label.equals("forrhpct"))     last.forrhpct     = fp.value[j];
        else if (fp.label.equals("forwhpct"))     last.forwhpct     = fp.value[j];
        else if (fp.label.equals("foriorate"))    last.foriorate    = fp.value[j];
        else if (fp.label.equals("forcomp"))      last.forcomp      = fp.value[j];
        else if (fp.label.equals("fordepth"))     last.fordepth     = fp.value[j];
        else if (fp.label.equals("forwidth"))     last.forwidth     = fp.value[j];
        else if (fp.label.equals("forfiles"))     last.forfiles     = fp.value[j];
        else if (fp.label.equals("foroperation")) last.foroperation = fp.value[j];
        else if (fp.label.equals("forsizes"))     last.forsizes     = fp.value[j];
        else if (fp.label.equals("fortotalsize")) last.fortotal     = fp.value[j];
        else if (fp.label.equals("forwss"))       last.forwss       = fp.value[j];

        else
          continue;

        /* Maintain a displayable and ordered list for the overrides: */
        fp.updateDisplayList(rd, fp.value[j]);

        /* We picked up one forxxx. Recursive call to go look for an other one: */
        fp.for_get(i+1, rd, do_list);
      }

      if (j == fp.value.length)
      {
        return;
      }
    }

    /* After the very last forxx, add the 'last' entry containing all the */
    /* proper numbers to the 'do_list': */
    nfp = (For_loop) last.clone();
    do_list.add(nfp);

    /* Store the properly formatted For_loop text, but without 'for': */
    nfp.display_text = "";
    for (int i = 0; i < override_display.size(); i++)
    {
      String display = (String) override_display.elementAt(i);
      display = display.substring(3);
      nfp.display_text += display + " ";
    }
    nfp.display_text = nfp.display_text.trim();

    return;
  }


  /**
   * To display the correct forxxx overrides in the proper order we maintain this
   * Vector here.
   */
  private void updateDisplayList(RD_entry rd, double value)
  {
    String value_text;
    double MB = 1024 * 1024;
    double GB = 1024 * 1024 * 1024;

    if (label.equals("foroperation"))
      value_text = label + "=" + Operations.getOperationText((int) value);

    else
    {
      if (value < 0)
        value_text = label + "=" + (int) (value * -1) + "%";

      else if (label.equals("foriorate"))
        value_text = label + "=" + (int) value;

      else
        value_text = label + "=" + FileAnchor.whatSize(value);
    }

    /* If the current type is in this Vector, replace it. */
    boolean replaced = false;
    for (int i = 0; i < override_display.size(); i++)
    {
      String display = (String) override_display.elementAt(i);
      if (display.startsWith(label))
      {
        override_display.set(i, value_text);
        replaced = true;
        break;
      }
    }

    /* If it is not there, add it at the end: */
    if (!replaced)
      override_display.add(value_text);


    for (int i = 999990; i < override_display.size(); i++)
    {
      String display = (String) override_display.elementAt(i);
      common.ptod("display: " + i + " " + display);
    }
  }


  /**
   * Add data points for a performance curve to the 'todo' list.
   * <pre>
   * The i/o rate of a 'maximum' run is taken and used to generate data points
   * for a curve, using either the pre-defined percentage table, or a
   * manually entered percentage table.
   *
   * I/O rates above one thousand are rounded upward to 100; below 100 they will
   * be rounded upward to 10. No i/o rates below 10 are done.
   *
   * We basically create a For_loop("foriorate") list containing the curve points.
   * This list however is not connected to the real For_loop list of the RD.
   *
   */
  public static void for_add_curve(Vector   curve_point_list,
                                   For_loop fp_in,
                                   double   max_curve_rate,
                                   double[] curve_points)
  {
    For_loop fp;
    int newrate;
    int i, j;
    double rtable[] = new double[] {10, 50, 70, 80, 90, 100};

    /* Override curve point default: */
    if (curve_points != null)
      rtable = curve_points;

    /* This is too embarassing! */
    if (max_curve_rate < 1)
      common.failure("for_add_curve(): iorate < 1 " + max_curve_rate +
                     "; was your run too short to establish proper iorate?");

    /* Calculate new iorate for each curve point.     */
    for (i = j = 0; i < rtable.length; i++)
    {
      newrate = (int) max_curve_rate * (int) rtable[i] / 100;

      if (newrate > 1000)
        newrate = ((newrate + 99) / 100) * 100;    // round up to 100
      else
        newrate = ((newrate +  9) /  10) *  10;    // round up to 10
      if (newrate < 10)
        break;

      fp = (For_loop) fp_in.clone();
      fp.label        = "foriorate";
      fp.foriorate   = (int) rtable[i] * -1;
      curve_point_list.insertElementAt(fp, j++ );
    }
  }



  /**
   * A 'forxxx' RD parameter may override certain values.
   */
  public static void forLoopOverrideFwd(RD_entry rd)
  {
    For_loop fp = rd.current_override;

    /* Go through all Fwg entries: */
    for (int i = 0; i < rd.fwgs_for_rd.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) rd.fwgs_for_rd.elementAt(i);
      if (fp.fordepth   != NOVALUE) fwg.depth       = (int)  fp.fordepth;
      if (fp.forwidth   != NOVALUE) fwg.width       = (int)  fp.forwidth;
      if (fp.forfiles   != NOVALUE) fwg.files       = (int)  fp.forfiles;
      if (fp.fortotal   != NOVALUE) fwg.total_size  = (long) fp.fortotal;
      if (fp.forwss     != NOVALUE) fwg.working_set = (long) fp.forwss;
      if (fp.forrdpct   != NOVALUE) fwg.readpct     =        fp.forrdpct;

      if (fp.foriorate  != NOVALUE) rd.fwd_rate     = (int)  fp.foriorate;

      /* Threads were already picked up at createFwgListForOneRd(): */
      //if (fp.forthreads != -1) fwg.threads     = (int)  fp.forthreads;

      if (!rd.rd_name.startsWith(RD_entry.FORMAT_RUN))
      {
        if (fp.foroperation  != NOVALUE) fwg.setOperation((int) fp.foroperation);
      }

      if (fp.forxfersize != NOVALUE)
      {
        fwg.xfersizes    = new double[1];
        fwg.xfersizes[0] = fp.forxfersize;
      }
      if (fp.forsizes != NOVALUE)
      {
        fwg.filesizes    = new double[1];
        fwg.filesizes[0] = fp.forsizes;
      }

      /* Data Validation requires that xfersizes are multiples of each other. */
      if (!rd.rd_name.equals(Jnl_entry.RECOVERY_RUN_NAME))
      {
        fwg.anchor.trackXfersizes(fwg.xfersizes);
        fwg.anchor.trackFileSizes(fwg.filesizes);
      }

      /* fortotal= allows for a percentage: */
      if (fwg.total_size < 0)
      {
        FsdEntry fsd = (FsdEntry) FsdEntry.findFsd(fwg.fsd_name);
        if (fsd.total_size == Long.MAX_VALUE)
          common.failure("Specifying 'totalsize=" + (int) (fwg.total_size * -1) +
                         "% without a totalsize= value specified for the FSD");
        fwg.total_size = (long) ((double) fsd.total_size * (fwg.total_size * -1) / 100.);
      }

      /* If the fortotal= equals zero, pick up the value from the FSD: */
      if (fwg.total_size == 0)
      {
        FsdEntry fsd = (FsdEntry) FsdEntry.findFsd(fwg.fsd_name);
        fwg.total_size = fsd.total_size;
      }
    }
  }



  /**
   * Transfer overrides from rd=forxxx parameter to the WG_Entry instance
   */
  public static void useForOverrides(WG_entry wg, RD_entry rd, For_loop fp, SD_entry sd, boolean estimate)
  {
    if (fp.forxfersize != NOVALUE)
    {
      wg.xfersize = (int) fp.forxfersize;
      if ( wg.xfersize % 512 != 0)
        common.failure("data transfer size not multiple of 512 bytes: " + wg.xfersize);
    }
    if (fp.forrdpct != NOVALUE)
      wg.readpct = fp.forrdpct;

    if (fp.foriorate != NOVALUE)
      rd.iorate_req = fp.foriorate;

    if (fp.forrhpct != NOVALUE)
      wg.rhpct = fp.forrhpct;

    if (fp.forwhpct != NOVALUE)
      wg.whpct = fp.forwhpct;

    if (fp.forseekpct != NOVALUE)
      wg.seekpct = fp.forseekpct;

    if (fp.forcomp != NOVALUE)
      rd.compression_rate_to_use = fp.forcomp;
    else
      rd.compression_rate_to_use = DV_map.compression_rate;


    /* Set hitarea if we ask for either read or write hits: */
    wg.hitarea_used = 0;
    if ( ((int) (wg.rhpct + wg.whpct) != 0))
      wg.hitarea_used = sd.hitarea;

    if (fp.forhitarea != NOVALUE)
      wg.hitarea_used = (long) fp.forhitarea;

    //common.ptod("xxxxxxxxxxxxxxforhitarea: " + hitarea_used + " " + this);

    if (fp.forthreads != NOVALUE)
      fp.forthreads = (int) fp.forthreads;

    if (fp.forthreads == 0)
    {
      fp.forthreads = 1;
      common.ptod("Setting some threadcounts to a minimum of 1 ");
    }

    /* Is the SD ever written to? */
    if (wg.readpct != 100)
      sd.open_for_write = true;
  }





  /**
   * If the parameter specified is one of the 'forxxx' parameters, add
   * this to the RD_entry for_list.
   * Starting 5.02 we also honor the regular 'xxx' parameter.
   */
  public static boolean checkForLoop(RD_entry rd, Vdb_scan prm)
  {
    if /**/  ("forxfersize".startsWith(prm.keyword))
      new For_loop("forxfersize", prm.numerics, rd.for_list);

    else if ("forthreads".startsWith(prm.keyword))
      new For_loop("forthreads", prm.numerics, rd.for_list);

    else if ("forrdpct".startsWith(prm.keyword))
      new For_loop("forrdpct", prm.numerics, rd.for_list);

    else if ("forrhpct".startsWith(prm.keyword))
      new For_loop("forrhpct", prm.numerics, rd.for_list);

    else if ("forwhpct".startsWith(prm.keyword))
      new For_loop("forwhpct", prm.numerics, rd.for_list);

    else if ("forseekpct".startsWith(prm.keyword))
      new For_loop("forseekpct", prm.numerics, rd.for_list);

    else if ("forhitarea".startsWith(prm.keyword))
      new For_loop("forhitarea", prm.numerics, rd.for_list);

    else if ("forcompression".startsWith(prm.keyword))
      new For_loop("forcomp", prm.numerics, rd.for_list);

    else if ("fordepth".startsWith(prm.keyword))
      new For_loop("fordepth", prm.numerics, rd.for_list);

    else if ("forwidth".startsWith(prm.keyword))
      new For_loop("forwidth", prm.numerics, rd.for_list);

    else if ("forfiles".startsWith(prm.keyword))
      new For_loop("forfiles", prm.numerics, rd.for_list);

    else if ("forsizes".startsWith(prm.keyword))
      new For_loop("forsizes", prm.numerics, rd.for_list);

    else if ("fortotalsize".startsWith(prm.keyword))
      new For_loop("fortotalsize", prm.numerics, rd.for_list);

    else if ("forwss".startsWith(prm.keyword) ||
             "forworkingsetsize".startsWith(prm.keyword))
      new For_loop("forwss", prm.numerics, rd.for_list);

    else if ("foroperations".startsWith(prm.keyword))
      rd.storeForOperations(prm);




    else if ("xfersize".startsWith(prm.keyword))
      new For_loop("forxfersize", prm.numerics, rd.for_list);

    else if ("threads".startsWith(prm.keyword))
      new For_loop("forthreads", prm.numerics, rd.for_list);

    else if ("rdpct".startsWith(prm.keyword))
      new For_loop("forrdpct", prm.numerics, rd.for_list);

    else if ("rhpct".startsWith(prm.keyword))
      new For_loop("forrhpct", prm.numerics, rd.for_list);

    else if ("whpct".startsWith(prm.keyword))
      new For_loop("forwhpct", prm.numerics, rd.for_list);

    else if ("seekpct".startsWith(prm.keyword))
      new For_loop("forseekpct", prm.numerics, rd.for_list);

    else if ("hitarea".startsWith(prm.keyword))
      new For_loop("forhitarea", prm.numerics, rd.for_list);

    else if ("compression".startsWith(prm.keyword))
      new For_loop("forcomp", prm.numerics, rd.for_list);

    else if ("depth".startsWith(prm.keyword))
      new For_loop("fordepth", prm.numerics, rd.for_list);

    else if ("width".startsWith(prm.keyword))
      new For_loop("forwidth", prm.numerics, rd.for_list);

    else if ("files".startsWith(prm.keyword))
      new For_loop("forfiles", prm.numerics, rd.for_list);

    else if ("sizes".startsWith(prm.keyword))
      new For_loop("forsizes", prm.numerics, rd.for_list);

    else if ("totalsize".startsWith(prm.keyword))
      new For_loop("fortotalsize", prm.numerics, rd.for_list);

    else if ("wss".startsWith(prm.keyword) ||
             "workingsetsize".startsWith(prm.keyword))
      new For_loop("forwss", prm.numerics, rd.for_list);

    else
      return false;

    return true;

  }
}

