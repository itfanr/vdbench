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

import Utils.Format;

/**
 * Enable reporting of data with one or two column headers and one or two
 * data columns.
 * This facilitates pre-defining all headers and data with a shared editimg
 * mask used for the Format.f() method.
 */
public class FwdPrint
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  String hdr1    = null;
  String hdr2    = null;
  String fmt2    = null;
  int    width2  = 0;

  String hdr2a   = null;
  String hdr2b   = null;
  String fmt2a   = null;
  String fmt2b   = null;
  int    width2a = 0;
  int    width2b = 0;


  /**
   * Two column headers and one data column.
   */
  public FwdPrint(String h1, String h2, String f2)
  {
    hdr1   = h1;
    hdr2   = h2;
    fmt2   = f2;
    width2 = calcWidth(f2);
    if (width2 < hdr1.length())
    {
      common.ptod("FwdPrint() hdr1: '" + hdr1 + "'");
      common.ptod("FwdPrint() hdr2: '" + hdr2 + "'");
      common.ptod("FwdPrint() fmt2: '" + fmt2 + "'");
      common.ptod("hdr1 length may not be longer than fmt2");
      common.failure("formatting error");
    }
  }


  /**
   * One wide column header, followed by two narrow column headers, with two
   * data columns.
   */
  public FwdPrint(String h1, String h2a, String f2a, String h2b, String f2b)
  {
    hdr1    = h1;
    hdr2a   = h2a;
    fmt2a   = f2a;
    width2a = calcWidth(f2a);
    if (width2a < h2a.length())
    {
      common.ptod("header1: " + hdr1);
      common.failure("FwdPrint(): header2 length (" + h2a +
                     ") larger than column width (" + f2a + ")");
    }

    hdr2b   = h2b;
    fmt2b   = f2b;
    width2b = calcWidth(f2b);
    if (width2b < h2b.length())
    {
      common.ptod("header1: " + hdr1);
      common.failure("FwdPrint(): header2 length (" + h2b +
                     ") larger than column width (" + f2b + ")");
    }
  }


  /**
   * Return all values for the first header.
   * If the column width is larger than the length of the header center the
   * headers around "..." values.
   */
  public String getHeader1()
  {

    if (hdr2 != null)
    {
      /* Single column: */
      int extra = width2 - hdr1.length();
      String dots1 = "..................".substring(0, extra / 2);
      String dots2 = "..................".substring(0, extra - dots1.length());
      String tmp = dots1 + hdr1 + dots2 ;

      //common.plog("getHeader1a:==>" + tmp + "<===");
      return tmp;
    }

    else
    {
      /* Multi column: */
      int extra = width2a + width2b - hdr1.length() + 1;
      String dots1 = "..................".substring(0, extra / 2);
      String dots2 = "..................".substring(0, extra - dots1.length());
      String tmp = dots1 + hdr1 + dots2;

      //common.plog("getHeader1b:==>" + tmp + "<===");
      return tmp;
    }
  }


  /**
   * Get all vaues for the second header.
   */
  public String getHeader2()
  {
    if (hdr2 != null)
    {
      String txt = Format.f("%" + width2 + "s", hdr2);
      return txt;
    }

    else
    {
      String txt = Format.f("%" + width2a + "s", hdr2a) +
                   Format.f(" %" + width2b + "s", hdr2b);
      return txt;
    }
  }


  /**
   * Return properly edited String for a single double value
   */
  protected String getData(double data)
  {
    return Format.f("%" + fmt2 + "F", data);
  }


  /**
   * return properly edited String for two double values.
   */
  protected String getData(double data1, double data2)
  {
    String txt = Format.f("%" + fmt2a + "F", data1) +
                 Format.f(" %" + fmt2b + "F", data2);
    return txt;
  }

  /**
   * Return properly edited String for a single String value
   */
  protected String getData(String data)
  {
    return Format.f("%" + width2 + "s", data);
  }


  /**
   * Calculate the width of the format mask.
   * Mask must be in form n.n; n.0 is OK.
   * Maximum width n.99
   */
  protected static int calcWidth(String fmt)
  {
    int left  = Integer.parseInt(fmt.substring(0, fmt.indexOf(".")));

    return left;
  }


  public static void main(String[] args)
  {
    FwdPrint fp = null;

    fp = new FwdPrint("hdr1", "hdr2", "12.1");
    common.ptod("one: " + fp.getHeader1());
    common.ptod("one: " + fp.getHeader2());
    common.ptod("one: " + fp.getData(5.5));

    common.ptod("");

    fp = new FwdPrint("hdr1", "7.1", "7.1", "7.1", "7.1");
    common.ptod("two: " + fp.getHeader1());
    common.ptod("two: " + fp.getHeader2());
    common.ptod("two: " + fp.getData(555555.55555, 666666.66666));


    FwdPrint acp  = new FwdPrint("Operations",  "7.1", "7.1");
    FwdPrint xcp  = new FwdPrint("Operations",  "5.1", "5.1",  "5.1",  "5.1");
    common.ptod(acp.getHeader1() + " " + xcp.getHeader1());
    common.ptod(acp.getHeader2() + " " + xcp.getHeader2());
    common.ptod(acp.getData(55555) + " " + xcp.getData(55555, 66666));
  }
}
