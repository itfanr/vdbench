package Utils;

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
import java.util.StringTokenizer;



public class printf
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private Vector masks;
  public  String text;
  private int    index = 0;

  private static String double_mask = "$^&";

  /**
   * Create new instance.
   * Split the input string into separate '%' prefixed masks.
   *
   * Do not handle masks with "%%" yet!
   *
   */
  public printf(String mask)
  {
    /* If we use double '%%' we must preserve those: */
    mask = common.replace(mask, "%%", double_mask);
    StringTokenizer st = new StringTokenizer(mask, "%");
    if (st.countTokens() == 0)
      common.failure("printf of '" + mask + "' failed; bad mask");

    masks = new Vector(st.countTokens());
    text  = "";
    while (st.hasMoreTokens())
    {
      String t1 = st.nextToken();
      if (masks.size() > 0 || mask.startsWith("%"))
        t1 = "%" + t1;

      /* Get back the possibly preserved '%%': */
      if (t1.indexOf(double_mask) != -1)
        t1 = common.replace(t1, double_mask, "%%");

      masks.addElement(t1);
      //common.ptod(t1);
    }
  }


  public void add(int num)
  {
    String mask = find_mask();
    text += Format.f(mask, num);
  }


  public void add(long num)
  {
    String mask = find_mask();
    text += Format.f(mask, num);
  }


  public void add(double dbl)
  {
    String mask = find_mask();
    text += Format.f(mask, dbl);
  }


  public void add(String txt)
  {
    String mask = find_mask();
    text += Format.f(mask, txt);
  }


  public void add(boolean bool)
  {
    String mask = find_mask();
    text += Format.f(mask, "" + bool);
  }


  /**
   * Generate a title string as wide as the length of the mask.
   */
  public void leftTitle(String title)
  {
    String mask = find_mask();

    /* Create any string that will have the proper length: */
    String tmp = Format.f(mask, 0);

    /* Now generate the title: */
    text += Format.f("%-" + tmp.length() + "s", title);
  }
  public void rightTitle(String title)
  {
    String mask = find_mask();

    /* Count the number of blanks that 'mask' has on the end: */
    int chars =0;
    while (mask.endsWith(" "))
    {
      mask = mask.substring(0, mask.length() - 1);
      chars++;
    }

    /* Create any string that will have the proper length: */
    String tmp = Format.f(mask.trim(), 0);

    /* Now generate the title, add the extra blanks back: */
    text += Format.f("%" + tmp.length() + "s", title) +
      Format.f("%" + chars + "s", " ");

  }


  private String find_mask()
  {
    String mask = null;
    for (; index < masks.size(); index++)
    {
      mask = (String) masks.elementAt(index);
      if (mask.indexOf("%") != -1)
      {
        index++;
        break;
      }
      text += mask;
    }
    if (mask == null)
      common.failure("printf find_mask(): not enough masks");
    return mask;
  }

  public String print()
  {
    String ret = text;
    index = 0;
    text = "";
    return ret;
  }


  public static void main2(String args[])
  {


    printf pf = new printf("   %7.2f   %7.2f      %7.2f");
    pf.leftTitle("a234");
    pf.leftTitle("b23");
    pf.leftTitle("c2");
    common.ptod(pf.print());

    pf.add(1234.5);
    pf.add(1234.6);
    pf.add(1234.7);
    common.ptod(pf.print());

    //pf = new printf("%7.2f %7.2f %7.2f");
    pf.rightTitle("a234");
    pf.rightTitle("b23");
    pf.rightTitle("c2");
    common.ptod(pf.print());

    pf.add(1234.5);
    pf.add(1234.6);
    pf.add(1234.7);
    common.ptod(pf.print());
  }
}

